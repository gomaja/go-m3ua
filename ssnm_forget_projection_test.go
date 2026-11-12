package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestIPSPForgetDestinationsRefreshesMappedCongestion(t *testing.T) {
	exerciseIPSPDestinationForget(t, false, 0)
}

func TestIPSPPauseDestinationsRefreshesMappedCongestion(t *testing.T) {
	exerciseIPSPDestinationForget(t, true, 0)
}

func TestIPSPDestinationForgetPreservesTransportCongestion(t *testing.T) {
	for _, pause := range []bool{false, true} {
		for _, level := range []uint8{1, 3} {
			t.Run(fmt.Sprintf("pause=%v/transport=%d", pause, level), func(t *testing.T) { exerciseIPSPDestinationForget(t, pause, level) })
		}
	}
}

func exerciseIPSPDestinationForget(t *testing.T, pause bool, transport uint8) {
	t.Helper()
	e, a := newMappedCongestionAssociation(t)
	if transport > 0 {
		setAssociationTransport(t, e, a, transport)
	}
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(9), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	requireMappedCongestionEvent(t, e, max(uint8(2), transport))
	before := a.TransportCongestion()
	if pause {
		a.pauseDestinations()
	} else if released := a.ForgetDestinations(); released != 1 {
		t.Errorf("forget released %d records, want one", released)
	}
	if len(a.destinations.state) != 0 {
		t.Error("received destination records survived forget")
	}
	if after := a.TransportCongestion(); after != before {
		t.Errorf("forget changed independent transport observation: %+v", after)
	}
	want := MTPDestinationStatus{Destination: MTPDestination{MTPRoute: "mapped", PointCode: 100}, Availability: DestinationAvailable, Congested: transport > 0, CongestionLevel: transport, CongestionLevelSet: transport > 0, TransportCongestionLevel: transport}
	if got, ok := e.MTPDestinationStatus(want.Destination); !ok || got != want {
		t.Errorf("single mapped status stale after forget: %+v, want %+v", got, want)
	}
	all := e.MTPDestinationStatuses()
	if len(all) != 1 || all[0] != want {
		t.Errorf("enumerated mapped status stale after forget: %+v, want %+v", all, want)
	}
	expected := 0
	if transport < 2 {
		expected = 1
	}
	seen := 0
	for {
		select {
		case indication := <-e.MTPIndications():
			if indication == nil {
				t.Fatal("MTP stream closed during forget")
			}
			if indication.Kind == MTPStatusIndication {
				seen++
				if indication.ResyncRequired || indication.Destination != want {
					t.Errorf("forget published incorrect MTP-STATUS: %+v", indication)
				}
			}
		default:
			if seen != expected {
				t.Errorf("forget MTP-STATUS count=%d, want %d", seen, expected)
			}
			return
		}
	}
}
