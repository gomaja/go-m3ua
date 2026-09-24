package m3ua

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

var ssnmTestSubscriptions sync.Map

func TestApplySSNMGuardsEventOnlyReports(testContext *testing.T) {
	for _, report := range []SSNMReport{
		{Kind: SSNMDestinationUserPartUnavailableReport, UserCauseSet: true},
		{Kind: SSNMSignallingCongestionReport, PeerReported: true, CongestionLevelSet: true},
	} {
		testContext.Run(report.Kind.String(), func(testContext *testing.T) {
			association, _ := newSSNMTestConn(testContext, StateASPActive, RoleASP)
			cached := DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}
			seedDestinationNetworkState(association, 0x1234, cached)
			before := association.endpoint.SSNMKnowledge()
			if err := association.applySSNM(report, nil, nil, apc(0x1234), availabilityState(DestinationAvailable), destinationAvailabilityDimension, nil); err != nil {
				testContext.Fatal(err)
			}
			received := nextSSNMReport(testContext, association)
			if received.Kind != report.Kind || received.PeerReported != report.PeerReported {
				testContext.Fatalf("event-only report = %+v", received)
			}
			after := association.endpoint.SSNMKnowledge()
			if !reflect.DeepEqual(before.Partitions, after.Partitions) || retainedDestinationState(association, 0x1234) != cached {
				testContext.Fatal("event-only apply rewrote retained destination dimensions")
			}
		})
	}
}

func TestEventOnlySSNMDoesNotRewriteEitherDimension(testContext *testing.T) {
	testContext.Run("DUPU", func(testContext *testing.T) {
		association, _ := newSSNMTestConn(testContext, StateASPActive, RoleASP)
		if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil)); err != nil {
			testContext.Fatal(err)
		}
		_ = nextSSNMReport(testContext, association)
		if err := association.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, apc(0x1234), nil, params.NewCongestionIndications(2), nil)); err != nil {
			testContext.Fatal(err)
		}
		_ = nextSSNMReport(testContext, association)
		before := association.endpoint.SSNMKnowledge()
		cached := retainedDestinationState(association, 0x1234)
		cause := params.NewUserCause(params.SCCP, params.Inaccessible)
		if err := association.handleDestinationUserPartUnavailable(messages.NewDestinationUserPartUnavailable(nil, nil, apc(0x1234), cause, nil)); err != nil {
			testContext.Fatal(err)
		}
		report := nextSSNMReport(testContext, association)
		if report.Kind != SSNMDestinationUserPartUnavailableReport || !report.UserCauseSet || report.UserCause != cause.UserCause() {
			testContext.Fatalf("DUPU report = %+v", report)
		}
		after, err := observedSSNMSubscription(testContext, association).Resync()
		if err != nil {
			testContext.Fatal(err)
		}
		if !reflect.DeepEqual(before.Partitions, after.Partitions) || retainedDestinationState(association, 0x1234) != cached {
			testContext.Fatal("DUPU rewrote retained destination dimensions")
		}
	})
	testContext.Run("peer-SCON", func(testContext *testing.T) {
		association, _ := newSSNMTestConn(testContext, StateASPActive, RoleSGP)
		cached := DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 3, LevelSet: true}}
		seedDestinationNetworkState(association, 0x1234, cached)
		before := association.endpoint.SSNMKnowledge()
		if err := association.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, apc(0x1234), params.NewConcernedDestination(0x5678), params.NewCongestionIndications(0), nil)); err != nil {
			testContext.Fatal(err)
		}
		report := nextSSNMReport(testContext, association)
		if report.Kind != SSNMSignallingCongestionReport || !report.PeerReported || !report.CongestionLevelSet || report.CongestionLevel != 0 || !report.ConcernedDestinationSet || report.ConcernedDestination != 0x5678 {
			testContext.Fatalf("peer SCON = %+v", report)
		}
		after, err := observedSSNMSubscription(testContext, association).Resync()
		if err != nil {
			testContext.Fatal(err)
		}
		if !reflect.DeepEqual(before.Partitions, after.Partitions) || retainedDestinationState(association, 0x1234) != cached {
			testContext.Fatal("peer SCON rewrote authoritative destination dimensions")
		}
	})
}

func TestPauseDestinationsPreservesCongestionOrdering(testContext *testing.T) {
	store := newDestinations()
	store.setRanges([]destinationRange{{PointCode: 0x1200, Mask: 8, State: availabilityState(DestinationAvailable)}})
	for _, update := range []destinationRange{
		{PointCode: 0x1200, Mask: 8, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 3, LevelSet: true}}},
		{PointCode: 0x1230, Mask: 4, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 1, LevelSet: true}}},
	} {
		if err := store.setCongestionRangesWithinBudget([]destinationRange{update}); err != nil {
			testContext.Fatal(err)
		}
	}
	for iteration := 0; iteration < 8; iteration++ {
		store.pause()
		state, known := store.lookupRange(destinationKey{}, 0x1234, 0)
		if !known || state.Availability != DestinationUnavailable || state.Congestion.Level != 1 {
			testContext.Fatalf("paused state = %+v, known=%v", state, known)
		}
	}
}

func reportedSSNMAvailability(testContext testing.TB, report SSNMReport) DestinationAvailability {
	testContext.Helper()
	switch report.Kind {
	case SSNMDestinationUnavailableReport:
		return DestinationUnavailable
	case SSNMDestinationRestrictedReport:
		return DestinationRestricted
	case SSNMDestinationAvailableReport:
		return DestinationAvailable
	default:
		testContext.Fatalf("expected availability report, received %s", report.Kind)
		return DestinationUnavailable
	}
}

func reportedSSNMCongestion(testContext testing.TB, report SSNMReport) CongestionState {
	testContext.Helper()
	if report.Kind != SSNMSignallingCongestionReport {
		testContext.Fatalf("expected SCON, received %s", report.Kind)
	}
	return CongestionState{Congested: !report.CongestionLevelSet || report.CongestionLevel != 0, Level: report.CongestionLevel, LevelSet: report.CongestionLevelSet}
}

func observeSSNM(testContext *testing.T, association *Association) *SSNMSubscription {
	testContext.Helper()
	if existing, ok := ssnmTestSubscriptions.Load(association); ok {
		return existing.(*SSNMSubscription)
	}
	endpoint := association.endpoint
	if endpoint == nil {
		var err error
		endpoint, err = NewEndpoint(EndpointConfig{Role: association.role})
		if err != nil {
			testContext.Fatal(err)
		}
		testContext.Cleanup(func() { _ = endpoint.Close() })
		if !endpoint.trackAssociation(association) {
			testContext.Fatal("attach observed association")
		}
	}
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	ssnmTestSubscriptions.Store(association, subscription)
	testContext.Cleanup(func() { ssnmTestSubscriptions.Delete(association); _ = subscription.Close() })
	return subscription
}

func observedSSNMSubscription(testContext *testing.T, association *Association) *SSNMSubscription {
	testContext.Helper()
	value, present := ssnmTestSubscriptions.Load(association)
	if !present {
		testContext.Fatal("subscribe before producing the SSNM report")
	}
	return value.(*SSNMSubscription)
}

func newObservedSSNMConn(testContext *testing.T, state State, role Role, contexts ...uint32) (*Association, *[]messages.M3UA) {
	testContext.Helper()
	association, sent := newTestConnWithContexts(testContext, state, role, contexts...)
	observeSSNM(testContext, association)
	return association, sent
}

func receiveSSNMReport(testContext *testing.T, ctx context.Context, association *Association) SSNMReport {
	testContext.Helper()
	subscription := observedSSNMSubscription(testContext, association)
	for {
		event, err := subscription.Next(ctx)
		if err != nil {
			testContext.Fatalf("SSNM report: %v", err)
		}
		if event.Kind == SSNMReportEvent && event.ReportSet {
			if event.Report.Association != association.ID() {
				continue
			}
			return event.Report
		}
		if event.Kind != SSNMBindingAdmittedEvent && event.Kind != SSNMBindingActivatedEvent {
			testContext.Fatalf("unexpected SSNM event: %+v", event)
		}
	}
}

func nextSSNMReport(testContext *testing.T, association *Association) SSNMReport {
	testContext.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return receiveSSNMReport(testContext, ctx, association)
}

func assertNoSSNMReport(testContext *testing.T, association *Association) {
	testContext.Helper()
	subscription := observedSSNMSubscription(testContext, association)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		event, err := subscription.Next(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			testContext.Fatal(err)
		}
		if event.Kind != SSNMBindingAdmittedEvent && event.Kind != SSNMBindingActivatedEvent {
			testContext.Fatalf("unexpected SSNM publication: %+v", event)
		}
	}
}
