package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestPendingReporterWithdrawalRefreshesSharedMTPStatus(t *testing.T) {
	for _, survivingCongestion := range []bool{false, true} {
		name := "last-congestion-source"
		if survivingCongestion {
			name = "independent-congestion-survives"
		}
		t.Run(name, func(t *testing.T) {
			config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
			e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			// Keep the fixture deterministic: no automation worker or live SCTP socket.
			e.ssnm.validity = time.Hour
			active := attachASPRouteAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
			capture := &mtpTransferCapture{}
			active.dataWriter = capture.write
			pending := attachActivatingSSNMAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 1)
			applyASPDAVA(t, active, 7, 1, 0x120000, 16)
			if survivingCongestion {
				applyASPSCON(t, active, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
			}
			applyASPSCON(t, pending, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
			destination := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}
			before, known := e.MTPDestinationStatus(destination)
			if !known || !before.Congested || before.Availability != DestinationAvailable {
				t.Fatalf("pending reporter control: status=%+v known=%v", before, known)
			}
			drainMTPIndications(e.MTPIndications())
			requireOfferedStatus(t, offerStatusAt(t, e, destination.PointCode), true)

			// RFC 4666 §4.5.1 permits reports before ASPAC Ack; abandoning that
			// admission removes the reporter without changing its traffic capability.
			if !pending.stopTAck(requestAspActive) {
				t.Fatal("missing pending activation")
			}
			pending.notifyASPRouteStateChanged()
			partition := ssnmPartitionKnowledge(t, e.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core"))
			if len(partition.Bindings) != 1 || partition.Bindings[0].Association != active.ID() {
				t.Fatalf("withdrawal retained the pending binding: %+v", partition.Bindings)
			}
			for _, destination := range partition.Destinations {
				for _, source := range destination.CongestionSources {
					if source.Association == pending.ID() {
						t.Fatal("withdrawal retained the pending reporter's congestion")
					}
				}
			}
			after, known := e.MTPDestinationStatus(destination)
			if !known || after.Availability != DestinationAvailable || after.Congested != survivingCongestion || after.ReceivedCongestionUnknown {
				t.Errorf("pending withdrawal left stale MTP status: %+v known=%v, want available congestion=%v", after, known, survivingCongestion)
			}
			drainMTPIndications(e.MTPIndications())
			if !survivingCongestion {
				// The remaining active sibling may carry traffic, but retired
				// source knowledge must not drive another congestion reminder.
				for range 8 {
					requireOfferedStatus(t, offerStatusAt(t, e, destination.PointCode), false)
				}
			}

			if survivingCongestion {
				// Retiring a positive source conservatively restarts even
				// when another contribution keeps the aggregate congested.
				requireOfferedStatus(t, offerStatusAt(t, e, destination.PointCode), true)
			}
			// After consuming that retirement mark, an equal fresh renewal
			// must retain the interval. A newly congested destination starts
			// its interval on the first offer.
			applyASPSCON(t, active, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
			drainMTPIndications(e.MTPIndications())
			requireOfferedStatus(t, offerStatusAt(t, e, destination.PointCode), !survivingCongestion)
			if survivingCongestion {
				for offer := 3; offer <= 9; offer++ {
					requireOfferedStatus(t, offerStatusAt(t, e, destination.PointCode), offer == 9)
				}
			}
		})
	}
}
