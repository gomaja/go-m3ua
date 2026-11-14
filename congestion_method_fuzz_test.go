package m3ua

import (
	"context"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func FuzzCongestionProfileAdmission(f *testing.F) {
	for _, method := range []uint8{1, 2, 3} {
		for _, mp := range []uint8{0, 1, 3, 255} {
			f.Add(method, uint8(2), mp, uint8(3), true)
		}
	}
	f.Fuzz(func(t *testing.T, method, max, mp, level uint8, levelSet bool) {
		method = 1 + method%3
		max = 1 + max%3
		level %= 4
		profileMax := max
		if method == 1 {
			profileMax = 0
		}
		c, capture := newDataWriteAssociation(t, 1)
		c.signalWriter = nil
		c.endpoint = methodEndpointForTest(t, RoleASP, method, profileMax)
		if !c.endpoint.trackAssociation(c) {
			t.Fatal("track")
		}
		observeSSNM(t, c)
		payload := testProtocolData([]byte("synthetic"))
		payload.MessagePriority = mp
		frame := c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: payload})
		message, err := messages.Parse(frame)
		if err != nil {
			t.Fatal(err)
		}
		n, err := c.WriteSignal(message)
		allowed := method == 2 && mp <= max || method != 2 && mp == 0
		if allowed {
			if err != nil || capture.submissions() != 1 {
				t.Fatalf("valid DATA n=%d err=%v", n, err)
			}
		} else if n != 0 || !errors.Is(err, ErrInvalidParameterValue) || capture.submissions() != 0 {
			t.Fatalf("invalid DATA n=%d err=%v", n, err)
		}
		c.dispatchRaw(context.Background(), inbound{data: frame, stream: 1, ppid: M3UAPPID})
		select {
		case delivered := <-c.dataChan:
			if delivered.ProtocolData.MessagePriority != mp {
				t.Fatal("MP changed")
			}
		default:
			t.Fatal("profile-mismatched DATA dropped")
		}
		var ci *params.Param
		if levelSet {
			ci = params.NewCongestionIndications(level)
		}
		scon := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, ci, nil)
		raw, err := scon.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		c.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
		report := nextSSNMReport(t, c)
		if report.CongestionLevelSet != levelSet || levelSet && report.CongestionLevel != level {
			t.Fatal("received level changed")
		}
		mismatch := levelSet && (method == 1 || level > max)
		if (len(report.ProfileMismatches) > 0) != mismatch {
			t.Fatalf("wrong mismatch: %+v", report)
		}
	})
}
