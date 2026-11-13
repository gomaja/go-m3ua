package m3ua

import (
	"testing"
	"time"
)

func transportBacklogASPConfig() *ASPConfig {
	cfg := validASPConfig()
	useSignallingGateways(cfg, "sg-a")
	cfg.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey = staticASKey(0, 1)
	cfg.Routing.MTPRoutes[0].DestinationPointCode = 0x222222
	cfg.Routing.MTPRoutes[0].Mask = 0
	// This stalled raw SCTP peer acknowledges activation but sends no SSNM.
	// Admit the unknown destination exactly as MTPTransfer would require.
	cfg.Routing.AllowUnknownDestinations = true
	return cfg
}

func TestTransportCongestionStalledPeerOnsetAndIdleAbatement(t *testing.T) {
	cfg := transportBacklogASPConfig()
	a, drain := stalledSCTPSenderWithEndpoint(t, EndpointConfig{Role: RoleASP, ASP: cfg})
	e := a.endpoint
	d := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x222222}
	for deadline := time.Now().Add(5 * time.Second); ; {
		state := a.TransportCongestion()
		status, ok := e.MTPDestinationStatus(d)
		if state.Known && state.Level == 3 && state.SampleValid && state.Charged > 0 && state.Limit > 0 && ok && status.Congested && status.CongestionLevel == 3 {
			t.Logf("onset: %+v destination=%+v", state, status)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no transport onset: %+v destination=%+v", state, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	drain()
	for deadline := time.Now().Add(8 * time.Second); ; {
		state := a.TransportCongestion()
		status, ok := e.MTPDestinationStatus(d)
		if state.Known && state.Level == 0 && state.SampleValid && state.Charged == 0 && ok && !status.Congested {
			t.Logf("abatement: %+v destination=%+v", state, status)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no idle abatement: %+v destination=%+v", state, status)
		}
		time.Sleep(20 * time.Millisecond)
	}
	onset, clear := false, false
	for len(e.MTPIndications()) > 0 {
		ind := <-e.MTPIndications()
		if ind.Kind == MTPStatusIndication {
			if ind.Destination.Congested {
				onset = true
			} else if onset && !ind.Destination.TransportCongestionUnknown {
				clear = true
			}
		}
	}
	if !onset || !clear {
		t.Fatalf("MTP-STATUS onset=%v abatement=%v", onset, clear)
	}
}
