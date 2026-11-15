package m3ua

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func newOfferedStatusFixture(t *testing.T, method CongestionMethod, mode RouteSelectionMode, queues ...int) (*Endpoint, map[string]*Association) {
	t.Helper()
	config := oneSignallingGatewayTwoSGPConfig(mode)
	if len(queues) > 0 {
		config.MTPIndicationQueueSize = queues[0]
	}
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: method}})
	var level *params.Param
	if method != CongestionInternational {
		level = params.NewCongestionIndications(2)
	}
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, level)
	}
	drainMTPIndications(e.MTPIndications())
	return e, peers
}

func offeredStatusPositions(t *testing.T, e *Endpoint, count int) []int {
	t.Helper()
	var positions []int
	for offer := 1; offer <= count; offer++ {
		// Vary UserData length across the octet alternative; only offers matter.
		payload := make([]byte, 1+(offer%3)*500)
		expected, known := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
		if !known {
			t.Fatal("effective destination unknown")
		}
		_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 0, payload)})
		if err != nil {
			t.Fatalf("offer %d: %v", offer, err)
		}
		for _, indication := range takeOfferedIndications(e.MTPIndications()) {
			if indication.ResyncRequired || indication.Kind != MTPStatusIndication || indication.Destination.Destination != (MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}) || !indication.Destination.Congested || indication.Cause != MTPStatusSignallingNetworkCongested || indication.Reason != MTPStatusOfferedTraffic {
				t.Fatalf("offer %d: %+v", offer, indication)
			}
			if indication.Destination.CongestionLevelSet && indication.Destination.CongestionLevel != 2 {
				t.Fatalf("wrong national level: %+v", indication)
			}
			if indication.Destination != expected {
				t.Fatalf("offer %d: status=%+v want current aggregate=%+v", offer, indication.Destination, expected)
			}
			positions = append(positions, offer)
		}
	}
	return positions
}

func TestInternationalCongestionIndicatesInitialAndEveryEightOffers(t *testing.T) {
	e, _ := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionLoadshare)
	if got := offeredStatusPositions(t, e, 33); !reflect.DeepEqual(got, []int{1, 9, 17, 25, 33}) {
		t.Fatalf("status offers = %v, want [1 9 17 25 33]", got)
	}
}
func TestNationalWithoutPriorityIncludesLevelInRepeatedStatus(t *testing.T) {
	e, _ := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	if got := offeredStatusPositions(t, e, 17); !reflect.DeepEqual(got, []int{1, 9, 17}) {
		t.Fatalf("status offers = %v, want [1 9 17]", got)
	}
}
func TestBroadcastCopiesCountAsOneOfferedMessage(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionInternational, RouteSelectionBroadcast)
	calls := 0
	for _, a := range peers {
		a.dataWriter = func(data []byte, _ *sctp.SndInfo) (int, error) { calls++; return len(data), nil }
	}
	if got := offeredStatusPositions(t, e, 17); !reflect.DeepEqual(got, []int{1, 9, 17}) {
		t.Fatalf("status offers = %v, want [1 9 17]", got)
	}
	if calls != 34 {
		t.Fatalf("broadcast writes=%d, want 34", calls)
	}
}

func takeOfferedIndications(stream <-chan *MTPIndication) []*MTPIndication {
	var result []*MTPIndication
	for {
		select {
		case indication, ok := <-stream:
			if !ok {
				return result
			}
			result = append(result, indication)
		default:
			return result
		}
	}
}
