package m3ua

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestCachedDATARefusalStillCountsEachRoutedOfferOnce(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionInternational, CongestionNationalWithoutPriorities} {
		for _, mode := range []RouteSelectionMode{RouteSelectionLoadshare, RouteSelectionBroadcast} {
			t.Run(fmt.Sprintf("%d/%d", method, mode), func(t *testing.T) {
				e, peers := newOfferedStatusFixture(t, method, mode)
				writes := 0
				for _, a := range peers {
					a.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
					now := admissionFrozenClock(a)
					admissionCachedSample(a, now, 90, 100)
					a.dataWriter = func(raw []byte, _ *sctp.SndInfo) (int, error) {
						writes++
						return len(raw), nil
					}
				}
				positions := map[uint32][]int{}
				for offer := 1; offer <= 17; offer++ {
					for _, pc := range []uint32{0x123456, 0x123457} {
						result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pc, 0, []byte("refused-offer"))})
						var refused *DataWriteError
						if !errors.Is(err, ErrDataCongestionRefused) || !errors.As(err, &refused) || refused.Outcome != DataNotSent {
							t.Fatal("cached routed refusal lost its known-not-sent result", err)
						}
						wantTargets := 1
						if mode == RouteSelectionBroadcast {
							wantTargets = 2
						}
						var failure *MTPTransferError
						if !errors.As(err, &failure) || len(failure.Failures) != wantTargets || len(result.SuccessfulPaths) != 0 {
							t.Fatal("refusal changed the frozen target obligations", result, err)
						}
						for _, indication := range takeOfferedIndications(e.MTPIndications()) {
							if indication.Reason != MTPStatusOfferedTraffic || indication.Destination.Destination.PointCode != pc || indication.ResyncRequired {
								t.Fatal("refusal produced an unrelated status", indication)
							}
							positions[pc] = append(positions[pc], offer)
						}
					}
				}
				for _, pc := range []uint32{0x123456, 0x123457} {
					if !reflect.DeepEqual(positions[pc], []int{1, 9, 17}) {
						t.Error("refused routed offers did not preserve independent one-per-request cadence", pc, positions[pc])
					}
				}
				if writes != 0 {
					t.Error("refused routed DATA reached transport", writes)
				}
			})
		}
	}
}

func TestLocalPublicationDoesNotResetUnchangedRoutedCongestion(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	e.congestionAutomationOnce.Do(func() {})
	now := time.Unix(100, 0)
	e.ssnm.now = func() time.Time { return now }
	a := peers["sg-a/sgp-a1"]
	key := *staticASKey(7, 1)
	request := localRequest(a, key, "service", 3)
	request.Destination = 0x123456
	var positions []int
	for offer := 1; offer <= 17; offer++ {
		switch offer {
		case 3:
			if err := e.ReportLocalCongestion(request); err != nil {
				t.Fatal(err)
			}
			if len(e.ssnm.localCongestionSnapshot(key)) != 1 {
				t.Fatal("local contribution was not inspectable")
			}
		case 5:
			e.runCongestionAutomation(now)
		case 7:
			if err := e.WithdrawLocalCongestion(a.ID(), a.Epoch(), key, request.Destination, request.Source); err != nil {
				t.Fatal(err)
			}
		case 10:
			now = now.Add(time.Second)
			e.runCongestionAutomation(now)
		}
		status, known := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: request.Destination})
		if !known || !status.Congested || !status.CongestionLevelSet || status.CongestionLevel != 2 {
			t.Fatal("local SPMC evidence changed outbound received congestion", status)
		}
		if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(request.Destination, 0, nil)}); err != nil {
			t.Fatal(err)
		}
		for _, indication := range takeOfferedIndications(e.MTPIndications()) {
			if indication.Reason != MTPStatusOfferedTraffic {
				t.Fatal("local-only work changed routed status", indication)
			}
			positions = append(positions, offer)
		}
	}
	if !reflect.DeepEqual(positions, []int{1, 9, 17}) {
		t.Error("local-only contributions or publication reset an unchanged interval", positions)
	}
	if e.CongestionPublicationCounters().Submitted != 2 || len(e.ssnm.localCongestionSnapshot(key)) != 0 {
		t.Fatal("fixture did not submit local onset/clear independently of received congestion", e.CongestionPublicationCounters())
	}
}
