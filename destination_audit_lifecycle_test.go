package m3ua

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func newScopedAutomationRig(t *testing.T) *auditTestRig {
	t.Helper()
	r := &auditTestRig{e: newSSNMStateEndpoint(t, nil, nil), now: time.Now()}
	configureAutomationForTest(r.e, &r.now)
	r.a, _ = newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	setInventoryNetworkAppearance(&r.a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	r.a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	if !r.e.trackAssociation(r.a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = r.a.Close() })
	r.a.signalWriter = func(m messages.M3UA) (int, error) { r.sent = append(r.sent, m); return 0, nil }
	return r
}

func TestScopedInvalidationRetiresOnlyRemovedAuditOrigins(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, r.a, 7, 2, congestionLevel(2), 200)
	if len(r.e.ssnm.audits) != 2 {
		t.Fatal("setup missing audits")
	}
	err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(300, 301), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if len(r.e.ssnm.audits) != 1 {
		t.Fatalf("scoped invalidation retained removed audit origin: %d", len(r.e.ssnm.audits))
	}
	for _, job := range r.e.ssnm.audits {
		if job.key.destination.pointCode != 200 {
			t.Fatal("unrelated audit lost")
		}
	}
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("only surviving audit must submit", len(r.sent))
	}
	d := r.sent[0].(*messages.DestinationStateAudit)
	if got := d.AffectedPointCode.AffectedPointCodes(); len(got) != 1 || got[0] != 200 {
		t.Fatal("invalidated audit submitted", got)
	}
}

func TestScopedInvalidationRetiresOnlyMatchingValidityHistory(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, r.a, 7, 2, congestionLevel(3), 100)
	partition := r.a.ssnmPartitionFor(*staticASKey(7, 1))
	before, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(300, 301), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	one := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 1))
	two := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 2))
	if one.congested || one.receivedUnknown || !two.congested || two.congestionLevel != 3 {
		t.Fatal("scoped invalidation kept contradicted history or lost survivor", one, two)
	}
	after := r.e.SSNMKnowledge()
	replica := before.Partitions[0]
	for revision := before.Revision; revision < after.Revision; {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		event, nextErr := sub.Next(ctx)
		cancel()
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		revision = event.Revision
		if event.Kind == SSNMDestinationsInvalidatedEvent {
			if !event.ApplyInvalidation(&replica) {
				t.Fatal("invalidation epoch mismatch")
			}
		}
	}
	if len(replica.Destinations) != len(after.Partitions[0].Destinations) {
		t.Fatal("source invalidation replay changed destination count")
	}
	if len(replica.Destinations[0].CongestionSources) != 1 || replica.Destinations[0].CongestionSources[0].AS.RoutingContext != 2 {
		t.Fatal("source invalidation did not replay exact survivor")
	}
}

func TestSeparateScopedClearsRetireGroupedAudits(t *testing.T) {
	for _, congestion := range []bool{true, false} {
		name := "availability"
		if congestion {
			name = "congestion"
		}
		t.Run(name, func(t *testing.T) {
			r := newScopedAutomationRig(t)
			if congestion {
				if err := r.a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil)); err != nil {
					t.Fatal(err)
				}
			}
			clear := func(rc uint32) {
				if congestion {
					sendSCON(t, r.a, 7, rc, congestionLevel(0), 100)
				} else {
					sendDAVA(t, r.a, 7, rc, 100)
				}
			}
			clear(1)
			r.advance(30 * time.Second)
			if len(r.sent) != 1 {
				t.Fatal("first scoped clear stopped another AS audit")
			}
			clear(2)
			r.advance(30 * time.Second)
			if len(r.e.ssnm.audits) != 0 || len(r.sent) != 1 {
				t.Fatal("separate clears left grouped audit live", len(r.e.ssnm.audits), len(r.sent))
			}
			audit := r.sent[0].(*messages.DestinationStateAudit)
			if got := audit.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 2 {
				t.Fatal("audit retained cleared AS in outgoing scope", got)
			}
		})
	}
}

func TestEndpointCloseJoinsCongestionAutomation(t *testing.T) {
	r := newScopedAutomationRig(t)
	e := r.e
	var audits atomic.Int32
	r.a.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.DestinationStateAudit); ok {
			audits.Add(1)
		}
		return 0, nil
	}
	sendDUNA(t, r.a, 7, 1, 100)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	e.ssnm.validity = time.Millisecond
	var entry sync.Once
	e.ssnm.now = func() time.Time { entry.Do(func() { close(entered) }); <-release; return time.Now().Add(time.Hour) }
	e.startCongestionAutomation()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	closed := make(chan struct{})
	go func() { _ = e.Close(); close(closed) }()
	select {
	case <-e.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel worker")
	}
	early := false
	select {
	case <-closed:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	unblock()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close failed to join released worker")
	}
	if early {
		t.Fatal("Endpoint.Close returned while automation worker remained in flight")
	}
	select {
	case <-e.congestionAutomationDone:
	default:
		t.Fatal("Close returned before worker exit")
	}
	revision := e.ssnm.revision
	e.runCongestionAutomation(time.Now().Add(time.Hour))
	if audits.Load() != 0 {
		t.Fatal("automation submitted an audit during or after Close")
	}
	if len(e.ssnm.audits) != 0 {
		t.Fatal("Close retained audit jobs")
	}
	if e.ssnm.revision != revision {
		t.Fatal("automation published after Close")
	}
}

func TestAuditDiagnosticsFitDefaultQueueForMaximumReport(t *testing.T) {
	r := newScopedAutomationRig(t)
	_, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	pcs := make([]uint32, 1024)
	for i := range pcs {
		pcs[i] = uint32(100 + i)
	}
	if err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(pcs...), nil)); err != nil {
		t.Fatal(err)
	}
	r.advance(30 * time.Second)
	sub.mu.Lock()
	lost, n, bytes := sub.continuityLost, len(sub.queue), sub.queuedBytes
	diagnosticCount, diagnosticBytes := 0, 0
	for _, queued := range sub.queue {
		if queued.event.Kind == SSNMAuditOutcomeEvent {
			diagnosticCount++
			diagnosticBytes += queued.bytes
		}
	}
	sub.mu.Unlock()
	// The received report now includes charged availability history. Bound
	// diagnostics separately from that report's independently accounted payload.
	if lost || n > 8 || bytes > DefaultSSNMSubscriptionQueueBytes || diagnosticCount > 7 || diagnosticBytes > 7*928 {
		t.Fatalf("diagnostics overflowed drained default queue: lost=%v events=%d bytes=%d", lost, n, bytes)
	}
}

func TestOptionalAuditLivenessDocumentsRKMCompletionJoin(t *testing.T) {
	source, err := os.ReadFile("scon_submission.go")
	if err != nil {
		t.Fatal(err)
	}
	design, err := os.ReadFile("docs/design/endpoint-management-and-ssnm.md")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(source), "RKM correlation") || !strings.Contains(string(design), "RKM correlation") || !strings.Contains(string(design), "cancelled optional") {
		t.Fatal("optional audit liveness omits the RKM completion join")
	}
}

func TestScopedInvalidationCancelsAvailabilityOriginsAfterOtherASUpdate(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	if err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil)); err != nil {
		t.Fatal(err)
	}
	sendDAVA(t, r.a, 7, 1, 100)
	sendDUNA(t, r.a, 7, 1, 101)
	r.a.scheduleDestinationAudit(SSNMReport{Kind: SSNMDestinationStateAuditReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 2), Destinations: []PointCodeRange{{PointCode: 102}}}, true)
	err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(2), params.NewAffectedPointCode(200, 201), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if len(r.e.ssnm.audits) != 2 {
		t.Fatal("invalidation retained contradicted availability origin", len(r.e.ssnm.audits))
	}
	for key, job := range r.e.ssnm.audits {
		if key.destination.pointCode == 100 || key.destination.pointCode == 102 && !job.isolation {
			t.Fatal("invalidation changed unrelated or isolation origin")
		}
	}
}
