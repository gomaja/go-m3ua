package m3ua

import (
	"errors"
	"slices"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSCONProfileRefusalDoesNotAbandonRestartBurst(t *testing.T) {
	for _, international := range []bool{false, true} {
		t.Run(map[bool]string{false: "national", true: "international"}[international], func(t *testing.T) {
			e, c, capture, _, _ := multiAssociationDialedSGPFixture(t)
			profile := CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 1}
			if international {
				profile = CongestionProfile{Method: CongestionInternational}
			}
			c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: profile}}
			c.notificationQueue = make(chan mandatoryControl, 8)
			var staged []stagedDestination
			for _, row := range []struct {
				pc           uint32
				availability DestinationAvailability
				level        uint8
			}{{100, DestinationAvailable, 2}, {101, DestinationRestricted, 1}, {102, DestinationUnavailable, 0}} {
				dimensions := destinationAvailabilityDimension
				if row.level > 0 {
					dimensions |= destinationCongestionDimension
				}
				staged = append(staged, stagedDestination{rangeValue: destinationRange{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true, PointCode: row.pc, State: DestinationNetworkState{Availability: row.availability, Congestion: CongestionState{Congested: row.level > 0, Level: row.level, LevelSet: row.level > 0}}}, dimensions: dimensions})
			}
			failure := publishDestinationRanges(e.as, staged, false, true)
			if failure == nil || len(failure.Failed) != 1 || failure.Failed[0].Association != c.ID() || !errors.Is(failure.Failed[0].Cause, ErrInvalidParameterValue) {
				t.Fatalf("per-target burst refusal=%+v", failure)
			}
			replies := ssnmMessages(capture.snapshot())
			want := []uint8{messages.MsgTypeDestinationAvailable, messages.MsgTypeSignallingCongestion, messages.MsgTypeDestinationRestricted, messages.MsgTypeDestinationUnavailable}
			if international {
				want = []uint8{messages.MsgTypeDestinationAvailable, messages.MsgTypeDestinationRestricted, messages.MsgTypeDestinationUnavailable}
			}
			if len(replies) != len(want) {
				t.Fatalf("refused SCON abandoned burst: %d replies, want %d", len(replies), len(want))
			}
			for i, message := range replies {
				if message.MessageType() != want[i] {
					t.Fatalf("reply[%d]=%T", i, message)
				}
			}
			if err := c.writeMandatoryControls([]messages.M3UA{messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)}, false, true); err != nil {
				t.Fatalf("burst refusal stopped queue: %v", err)
			}
		})
	}
}

func TestDAUDProfileRefusalPreservesAvailabilityAndOtherReplies(t *testing.T) {
	e, c, capture, _, _ := multiAssociationDialedSGPFixture(t)
	c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 1}}}
	scope := testWireScope(7, true, 1)
	for _, row := range []struct {
		pc    uint32
		state DestinationAvailability
	}{{100, DestinationAvailable}, {101, DestinationRestricted}, {102, DestinationUnavailable}, {103, DestinationAvailable}} {
		if err := reportAvailability(e, scope, row.pc, 0, row.state); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range []struct {
		pc    uint32
		level uint8
	}{{100, 2}, {101, 2}, {103, 1}} {
		err := e.SignallingCongestion(SignallingCongestionRequest{Scope: scope, Destinations: []PointCodeRange{{PointCode: row.pc}}, CongestionLevel: row.level, CongestionLevelSet: true})
		if row.level == 2 {
			if !errors.Is(err, ErrInvalidParameterValue) {
				t.Fatalf("publication refusal=%v", err)
			}
		} else if err != nil {
			t.Fatal(err)
		}
	}
	capture.reset()
	management := c.ManagementIndications()
	audit := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100, 101, 102, 103, 104), nil)
	for attempt := 0; attempt < 2; attempt++ {
		capture.reset()
		if err := c.handleDestinationStateAudit(audit); err != nil {
			t.Fatalf("audit stopped at local refusal: %v", err)
		}
		replies := ssnmMessages(capture.snapshot())
		if len(replies) != 4 {
			t.Fatalf("audit replies=%d, want SCON, DAVA, DRST, DUNA: %+v", len(replies), replies)
		}
		want := []uint8{messages.MsgTypeSignallingCongestion, messages.MsgTypeDestinationAvailable, messages.MsgTypeDestinationRestricted, messages.MsgTypeDestinationUnavailable}
		for i, message := range replies {
			if message.MessageType() != want[i] {
				t.Fatalf("reply[%d]=%T", i, message)
			}
		}
		wantPCs := [][]uint32{{103}, {100, 103}, {101}, {102, 104}}
		for i, reply := range replies {
			apc := affectedPointCodeParamOf(reply)
			if apc == nil || !slices.Equal(apc.AffectedPointCodes(), wantPCs[i]) {
				t.Fatalf("reply[%d] APC=%v want %v", i, apc, wantPCs[i])
			}
			na := ssnmNetworkAppearance(reply)
			rc, _ := ssnmRoutingContext(reply)
			if na == nil || na.NetworkAppearance() != 7 || rc == nil || !slices.Equal(rc.RoutingContexts(), []uint32{1}) {
				t.Fatalf("reply[%d] scope=%v/%v", i, na, rc)
			}
		}
		if s := replies[0].(*messages.SignallingCongestion); s.CongestionIndications.CongestionLevel() != 1 || len(s.AffectedPointCode.AffectedPointCodes()) != 1 || s.AffectedPointCode.AffectedPointCodes()[0] != 103 {
			t.Fatalf("permitted congestion reply=%+v", s)
		}
		for _, message := range capture.snapshot() {
			if _, ok := message.(*messages.Error); ok {
				t.Fatal("local profile refusal blamed peer")
			}
		}
		select {
		case indication := <-management:
			if indication.Kind != ManagementError || !errors.Is(indication.Cause, ErrInvalidParameterValue) {
				t.Fatalf("local refusal indication=%+v", indication)
			}
			if indication.NetworkAppearance != 7 || !indication.NetworkAppearanceSet || !slices.Equal(indication.RoutingContexts, []uint32{1}) || len(indication.AffectedDestinations) != 5 {
				t.Fatalf("partial audit scope=%+v", indication)
			}
			for i, destination := range indication.AffectedDestinations {
				if destination.PointCode != uint32(100+i) || destination.Mask != 0 || destination.NetworkAppearance != 7 || !destination.NetworkAppearanceSet || destination.RoutingContext != 1 || !destination.RoutingContextSet {
					t.Fatalf("partial audit destination=%+v", destination)
				}
			}
		default:
			t.Fatal("local audit omission was not reported to management")
		}
		select {
		case extra := <-management:
			t.Fatalf("audit flooded management: %+v", extra)
		default:
		}
		if c.State() != StateASPActive {
			t.Fatal("audit omission closed healthy association")
		}
	}
}

func TestSCONProfileRefusalDoesNotHideLaterTransportFailure(t *testing.T) {
	_, c, _, _, _ := multiAssociationDialedSGPFixture(t)
	c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 1}}}
	transportFailure := errors.New("control transport failed")
	attempts := 0
	c.signalWriter = func(message messages.M3UA) (int, error) { attempts++; return 0, transportFailure }
	err := c.writeMandatoryControls([]messages.M3UA{
		messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil),
		messages.NewDestinationAvailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil),
		messages.NewDestinationRestricted(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(101), nil),
	}, true, true)
	var refusal *controlAdmissionError
	if !errors.Is(err, transportFailure) || errors.As(err, &refusal) || attempts != 1 {
		t.Fatalf("fatal result=%v attempts=%d localRefusal=%v", err, attempts, refusal)
	}
}
