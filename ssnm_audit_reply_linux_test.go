package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDAUDRangeExceptionsOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3399)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	observeSSNM(t, asp)
	seedDestinationRangeInScope(sgp.destinations, 0, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	seedDestinationRangeInScope(sgp.destinations, 0, 1, 0x123456, 0, availabilityState(DestinationUnavailable))
	if _, err := asp.WriteSignal(messages.NewDestinationStateAudit(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	covered := 0
	for covered < 256 {
		report := nextSSNMReport(t, asp)
		for _, destination := range report.Destinations {
			covered += 1 << effectiveDestinationMask(destination.Mask)
			if report.Kind == SSNMDestinationAvailableReport && destinationRangeCovers(destinationRange{PointCode: destination.PointCode, Mask: destination.Mask}, 0x123456, 0) {
				t.Fatal("DAVA covers known unavailable destination")
			}
		}
	}
	for pc := uint32(0x123400); pc < 0x123500; pc++ {
		want := DestinationAvailable
		if pc == 0x123456 {
			want = DestinationUnavailable
		}
		state, known := asp.destinations.lookupRange(destinationScopeKey(0, 1), pc, 0)
		if !known || state.Availability != want {
			t.Fatalf("%#x state=%+v known=%v want=%v", pc, state, known, want)
		}
	}
}

func TestDAUDBudgetRefusalOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	asp, sgp, err := setupConn(t, ctx, 3401)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close(); _ = sgp.Close() }()
	// Repeated forgetting can leave restart history beyond the ordinary store
	// budget. Exercise that public sequence against a one-record store.
	sgp.destinations.setRecordLimit(1)
	observeSSNM(t, asp)
	for i := uint32(0); i < 256; i++ {
		sgp.ForgetDestinations()
		if _, err := sgp.endpoint.BeginMTP3Restart(ownerDestination(0, 1, i<<16, 0)); err != nil {
			t.Fatal(err)
		}
		if report := nextSSNMReport(t, asp); report.Kind != SSNMDestinationUnavailableReport {
			t.Fatalf("restart report=%+v", report)
		}
	}
	sgp.ForgetDestinations()
	if err := reportAvailability(sgp.endpoint, testWireScope(0, true, 1), 0, 24, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	if report := nextSSNMReport(t, asp); report.Kind != SSNMDestinationAvailableReport {
		t.Fatalf("availability report=%+v", report)
	}
	before, knownBefore := asp.destinations.lookupRange(destinationScopeKey(0, 1), 0, 0)
	indications := asp.ManagementIndications()
	if _, err := asp.WriteSignal(messages.NewDestinationStateAudit(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(24, 0), nil)); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case indication := <-indications:
			if indication.Kind != ManagementError || indication.ErrorCode != params.ErrDestinationStatusUnknown {
				continue
			}
			if !indication.NetworkAppearanceSet || indication.NetworkAppearance != 0 || len(indication.RoutingContexts) != 1 || indication.RoutingContexts[0] != 1 || len(indication.AffectedDestinations) != 1 {
				t.Fatalf("refusal scope=%+v", indication)
			}
			if asp.State() != StateASPActive || !asp.routingContextAcked(1) {
				t.Fatal("refusal changed ASP authorization")
			}
			after, knownAfter := asp.destinations.lookupRange(destinationScopeKey(0, 1), 0, 0)
			if after != before || knownAfter != knownBefore {
				t.Fatal("ERR changed destination state")
			}
			assertNoSSNMReport(t, asp)
			return
		case <-ctx.Done():
			t.Fatal("missing scoped audit refusal")
		}
	}
}
