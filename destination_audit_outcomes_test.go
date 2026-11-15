package m3ua

import (
	"reflect"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func outcomeProbeSubscription() (*ssnmState, *SSNMSubscription) {
	state := &ssnmState{}
	sub := newSSNMSubscription(state, DefaultSSNMSubscriptionQueueSize, DefaultSSNMSubscriptionQueueBytes)
	state.subscribers = map[*SSNMSubscription]struct{}{sub: {}}
	return state, sub
}
func outcomeProbeEvents(sub *SSNMSubscription) ([]SSNMEvent, int, bool) {
	sub.mu.Lock()
	defer sub.mu.Unlock()
	events := make([]SSNMEvent, len(sub.queue))
	for i := range sub.queue {
		events[i] = sub.queue[i].event.clone()
	}
	return events, sub.queuedBytes, sub.continuityLost
}

func TestAuditQueueFullDiagnosticsCountMaximumReportRefusals(t *testing.T) {
	r := newAuditTestRig(t)
	_, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	points := make([]uint32, 1024)
	for i := range points {
		points[i] = 100 + uint32(i)
	}
	if err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(points...), nil)); err != nil {
		t.Fatal(err)
	}
	events, _, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("one maximum report overflowed the default subscription")
	}
	count, diagnostics := uint64(0), 0
	for _, event := range events {
		if event.Kind != SSNMAuditOutcomeEvent {
			continue
		}
		diagnostics++
		if event.Reason != "queue-full" || event.ReportSet || len(event.Report.Destinations) != 0 {
			t.Errorf("unexpected report diagnostic: %+v", event)
		}
		count += event.AuditCount
	}
	if diagnostics != 1 || count != 768 {
		t.Fatalf("queue-full diagnostics=%d refused-job transitions=%d, want 1 and 768", diagnostics, count)
	}
	if len(r.e.ssnm.audits) != 256 {
		t.Fatalf("pending jobs=%d, want 256", len(r.e.ssnm.audits))
	}
}

func TestAuditOutcomeBatchBoundsEveryReasonAndCountsTransitions(t *testing.T) {
	state, sub := outcomeProbeSubscription()
	scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: make([]uint32, 64)}
	for i := range scope.RoutingContexts {
		scope.RoutingContexts[i] = uint32(i)
	}
	var batch auditOutcomeBatch
	// Reverse insertion proves publication order does not depend on the pass's
	// map iteration order. Count transitions rather than submitted wire messages.
	reasons := []string{"queue-full", "rate-deferred", "reporter-retired", "send-deferred", "contended", "send-failed", "submitted"}
	for i := len(reasons) - 1; i >= 0; i-- {
		for n := 0; n < 1000; n++ {
			job := &destinationAuditJob{key: destinationAuditKey{association: 9, destination: ssnmDestinationKey{pointCode: uint32(n)}}, scope: scope, info: strings.Repeat("private-info", 300), partitions: []SSNMPartition{canonicalSSNMPartition("private-gateway", "private-server")}}
			batch.add(job, reasons[i])
		}
	}
	batch.publishLocked(state)
	events, bytes, lost := outcomeProbeEvents(sub)
	if lost || len(events) != 7 || bytes > 7*928 {
		t.Fatalf("outcome bounds: lost=%v slots=%d bytes=%d", lost, len(events), bytes)
	}
	for i, event := range events {
		if event.Kind != SSNMAuditOutcomeEvent || event.Reason != reasons[i] || event.AuditCount != 1000 || event.ReportSet || event.AuditScopeOmitted || event.Report.Association != 9 || !reflect.DeepEqual(event.Report.Scope, scope) {
			t.Errorf("reason %s summary: %+v", reasons[i], event)
		}
		if len(event.Report.Destinations) != 0 || event.Partition != (SSNMPartition{}) || event.Report.Partition != (SSNMPartition{}) {
			t.Errorf("diagnostic retained destination or configured identity: %+v", event)
		}
		accounted, fits := ssnmEventAccountedBytes(event, 928)
		if !fits || accounted > 784 {
			t.Errorf("fixed-reason event accounting=%d fits=%v", accounted, fits)
		}
		if i > 0 && event.Revision <= events[i-1].Revision {
			t.Error("diagnostics did not preserve revision order")
		}
	}
	batch.publishLocked(state)
	again, _, _ := outcomeProbeEvents(sub)
	if len(again) != 7 {
		t.Error("empty reused batch replayed diagnostics")
	}
}

func TestAuditOutcomeBatchOmitsHeterogeneousOrOversizedScope(t *testing.T) {
	base := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}
	for _, kind := range []string{"network", "network-presence", "context", "context-presence", "oversized", "association"} {
		t.Run(kind, func(t *testing.T) {
			state, sub := outcomeProbeSubscription()
			first, second := base.clone(), base.clone()
			association := AssociationID(9)
			switch kind {
			case "network":
				second.NetworkAppearance++
			case "network-presence":
				second.NetworkAppearanceSet = false
			case "context":
				second.RoutingContexts[0]++
			case "context-presence":
				second.RoutingContextSet = false
			case "oversized":
				first.RoutingContexts = make([]uint32, 65)
				second = first
			case "association":
				association++
			}
			var batch auditOutcomeBatch
			batch.add(&destinationAuditJob{key: destinationAuditKey{association: 9}, scope: first}, "submitted")
			batch.add(&destinationAuditJob{key: destinationAuditKey{association: association}, scope: second}, "submitted")
			batch.publishLocked(state)
			events, _, lost := outcomeProbeEvents(sub)
			if lost || len(events) != 1 {
				t.Fatalf("summary lost=%v events=%d", lost, len(events))
			}
			event := events[0]
			if event.AuditCount != 2 {
				t.Errorf("transition count=%d", event.AuditCount)
			}
			if kind == "association" {
				if event.Report.Association != 0 || event.AuditScopeOmitted || !reflect.DeepEqual(event.Report.Scope, base) {
					t.Errorf("mixed association summary: %+v", event)
				}
			} else if !event.AuditScopeOmitted || !reflect.DeepEqual(event.Report.Scope, WireScope{}) || event.Report.Association != 9 {
				t.Errorf("scope not explicitly omitted: %+v", event)
			}
		})
	}
}

func TestAuditOutcomePublishedScopeOwnsItsSourceStorage(t *testing.T) {
	state, sub := outcomeProbeSubscription()
	scope := WireScope{RoutingContextSet: true, RoutingContexts: []uint32{7}}
	job := &destinationAuditJob{key: destinationAuditKey{association: 9}, scope: scope}
	var batch auditOutcomeBatch
	batch.add(job, "submitted")
	batch.publishLocked(state)
	// Scheduled scopes are immutable while accumulating. Once published, even
	// later source reuse must not mutate the subscriber's owned queued value.
	scope.RoutingContexts[0] = 999
	events, _, _ := outcomeProbeEvents(sub)
	if len(events) != 1 || events[0].Report.Scope.RoutingContexts[0] != 7 {
		t.Fatalf("published scope aliases source storage: %+v", events)
	}
	events[0].Report.Scope.RoutingContexts[0] = 888
	again, _, _ := outcomeProbeEvents(sub)
	if again[0].Report.Scope.RoutingContexts[0] != 7 {
		t.Fatal("subscriber-owned copy aliases queued scope")
	}
}
