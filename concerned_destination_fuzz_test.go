package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func FuzzConcernedSCONAdmission(f *testing.F) {
	for _, apcs := range [][]byte{
		{0, 0, 0, 0}, {0, 0xff, 0xff, 0xff}, {8, 0x12, 0x34, 0},
		{0, 0x12, 0x34, 0x56, 0, 0x23, 0x45, 0x67},
	} {
		for _, cd := range []bool{false, true} {
			f.Add(apcs, uint32(0), cd, uint8(3))
		}
	}
	f.Fuzz(func(t *testing.T, words []byte, cd uint32, cdSet bool, level uint8) {
		// APC cardinality is bounded; malformed TLV lengths belong to codec fuzzing.
		words = words[:min(len(words), 64)&^3]
		if len(words) == 0 {
			words = []byte{0, 0, 0, 0}
		}
		encoded := make([]uint32, len(words)/4)
		destinations := make([]PointCodeRange, len(encoded))
		for i := range encoded {
			encoded[i] = binary.BigEndian.Uint32(words[4*i:])
			destinations[i] = PointCodeRange{PointCode: encoded[i] & 0xffffff, Mask: uint8(encoded[i] >> 24)}
		}
		cd &= 0xffffff
		level %= 4
		var concerned *params.Param
		if cdSet {
			concerned = params.NewConcernedDestination(cd)
		}
		msg := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(encoded...), concerned, params.NewCongestionIndications(level), nil)
		raw, err := msg.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		single := len(encoded) == 1 && words[0] == 0
		sender, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		sender.noteRoutingContextsAcked(params.NewRoutingContext(1))
		n, err := sender.WriteSignal(msg)
		if cdSet && !single {
			if n != 0 || !errors.Is(err, ErrInvalidParameterValue) || len(*sent) != 0 {
				t.Fatalf("ambiguous CD sent n=%d err=%v", n, err)
			}
		} else if err != nil || len(*sent) != 1 {
			t.Fatalf("valid send err=%v sent=%d", err, len(*sent))
		}
		receiver, replies := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
		receiver.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
		if err := firstErr(receiver); err != nil || len(*replies) != 0 {
			t.Fatalf("well-formed SCON refused: err=%v replies=%v", err, typeNames(*replies))
		}
		report := nextSSNMReport(t, receiver)
		if !reflect.DeepEqual(report.Destinations, destinations) || report.CongestionLevel != level || !report.CongestionLevelSet {
			t.Fatalf("lost congestion: %+v", report)
		}
		if report.ConcernedDestinationSet != (cdSet && single) {
			t.Fatalf("wrong trigger eligibility: %+v", report)
		}
		if cdSet && single && report.ConcernedDestination != cd {
			t.Fatalf("changed trigger: %+v", report)
		}
		if cdSet && !single {
			if report.UnmappedConcernedDestination == nil || *report.UnmappedConcernedDestination != cd {
				t.Fatalf("lost diagnostic: %+v", report)
			}
		} else if report.UnmappedConcernedDestination != nil {
			t.Fatalf("spurious diagnostic: %+v", report)
		}

	})
}
