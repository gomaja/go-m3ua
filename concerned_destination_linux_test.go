package m3ua

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestConcernedSCONValidTriggerOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3521)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	observeSSNM(t, sgp)
	request := SignallingCongestionRequest{Scope: testWireScope(0, true, 1), Destinations: []PointCodeRange{{PointCode: 0x111100}}, ConcernedDestination: 0x111101, ConcernedDestinationSet: true, CongestionLevel: 2, CongestionLevelSet: true}
	if err := asp.SignallingCongestion(request); err != nil {
		t.Fatal(err)
	}
	report := nextSSNMReport(t, sgp)
	if !report.ConcernedDestinationSet || report.ConcernedDestination != request.ConcernedDestination || !reflect.DeepEqual(report.Destinations, request.Destinations) || report.Scope.NetworkAppearance != 0 || !report.Scope.NetworkAppearanceSet || !reflect.DeepEqual(report.Scope.RoutingContexts, []uint32{1}) {
		t.Fatalf("valid trigger=%+v", report)
	}
}

func TestConcernedSCONSendRefusalOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3522)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	request := SignallingCongestionRequest{Scope: testWireScope(0, true, 1), Destinations: []PointCodeRange{{PointCode: 0x222201}, {PointCode: 0x222202}}, ConcernedDestination: 0x222200, ConcernedDestinationSet: true}
	if err := asp.SignallingCongestion(request); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("typed ambiguous send=%v", err)
	}
	request.Destinations = []PointCodeRange{{PointCode: 0x333300, Mask: 8}}
	request.ConcernedDestination = 0x333301
	if n, err := asp.WriteSignal(concernedSCONMessage(t, request, true)); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("raw ambiguous send n=%d err=%v", n, err)
	}
	// This valid report is an ordered fence and a positive capture control.
	observeSSNM(t, sgp)
	if err := asp.SignallingCongestion(SignallingCongestionRequest{Scope: request.Scope, Destinations: []PointCodeRange{{PointCode: 0x333399}}, ConcernedDestination: 0x333398, ConcernedDestinationSet: true}); err != nil {
		t.Fatal(err)
	}
	report := nextSSNMReport(t, sgp)
	if !reflect.DeepEqual(report.Destinations, []PointCodeRange{{PointCode: 0x333399}}) {
		t.Fatalf("refused send reached the peer before the fence: %+v", report)
	}
}

func TestConcernedSCONUnmappedTriggerOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3523)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	observeSSNM(t, sgp)
	peerManagement := asp.ManagementIndications()
	sgp.ManagementIndications()
	for _, destinations := range [][]PointCodeRange{{{PointCode: 0x444401}, {PointCode: 0x444402}}, {{PointCode: 0x555500, Mask: 8}}} {
		request := SignallingCongestionRequest{Scope: testWireScope(0, true, 1), Destinations: destinations, ConcernedDestination: 0x444400, ConcernedDestinationSet: true, CongestionLevel: 3, CongestionLevelSet: true}
		raw, err := concernedSCONMessage(t, request, true).MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		// Model an unrestricted peer: bypass the M3UA sender's admission only.
		info := *asp.sctpInfo
		if _, err := asp.sctpConn.SendMsg(raw, sctp.SendOptions{Info: &info}); err != nil {
			t.Fatal(err)
		}
		report := nextSSNMReport(t, sgp)
		if !report.PeerReported || report.CongestionLevel != 3 || !report.CongestionLevelSet || !reflect.DeepEqual(report.Destinations, destinations) {
			t.Fatalf("ambiguous report lost congestion: %+v", report)
		}
		if report.ConcernedDestinationSet || report.ConcernedDestination != 0 {
			t.Errorf("ambiguous report exposes a TFC trigger: %+v", report)
		}
	}
	// Congestion reports precede this DATA on the same outbound call sequence;
	// receiving it proves that accepting the mismatch did not withdraw traffic.
	if _, err := asp.WriteData(DataRequest{AS: ASKey{NetworkAppearance: 0, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}, ProtocolData: *transferProtocolData(0x444401, 1, []byte("synthetic-congestion-check"))}); err != nil {
		t.Fatal(err)
	}
	if _, err := sgp.ReadData(ctx); err != nil {
		t.Fatal(err)
	}
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case indication := <-peerManagement:
			if indication != nil && indication.Kind == ManagementError {
				t.Fatalf("accepted SCON produced peer Error: %+v", indication)
			}
		case <-timer.C:
			return
		}
	}
}
