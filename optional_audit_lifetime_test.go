package m3ua

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestScopedAuditWithdrawalCannotCrossAliasReactivation(t *testing.T) {
	r := newScopedAutomationRig(t)
	if r.a.ssnmPartitionFor(*staticASKey(7, 1)) != r.a.ssnmPartitionFor(*staticASKey(7, 2)) {
		t.Fatal("fixture does not share a partition between its two AS scopes")
	}
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 101)
	sendSCON(t, r.a, 7, 2, congestionLevel(2), 100)
	var withdrawn *destinationAuditJob
	for _, job := range r.e.ssnm.audits {
		if job.key.destination.pointCode == 100 {
			withdrawn = job
		}
	}
	if withdrawn == nil || len(r.e.ssnm.audits) != 2 {
		t.Fatal("fixture did not retain both scoped audits")
	}

	// Delay post-mark reconciliation at its existing T(ack) snapshot, while
	// allowing the production override and positive-Ack ledger marks to commit.
	r.a.tack.mu.Lock()
	var release sync.Once
	unlock := func() { release.Do(r.a.tack.mu.Unlock) }
	defer unlock()
	overridden := make(chan struct{})
	go func() { r.a.noteRoutingContextsOverridden([]uint32{2}); close(overridden) }()
	if !waitFor(func() bool { return r.a.routingContextOverridden(2) }, time.Second) {
		unlock()
		<-overridden
		t.Fatal("override mark did not commit before reconciliation")
	}
	reactivated := make(chan struct{})
	go func() { r.a.noteRoutingContextsAcked(params.NewRoutingContext(2)); close(reactivated) }()
	if !waitFor(func() bool { return !r.a.routingContextOverridden(2) }, time.Second) {
		unlock()
		<-overridden
		<-reactivated
		t.Fatal("reactivation mark did not commit before reconciliation")
	}
	unlock()
	for _, done := range []<-chan struct{}{overridden, reactivated} {
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("scope reconciliation did not finish after T(ack) release")
		}
	}
	if withdrawn.live.Load() || len(r.e.ssnm.audits) != 1 {
		t.Error("withdrawn audit origin survived inactive/reactivation through a shared partition")
	}
	r.advance(30 * time.Second)
	r.advance(time.Second)
	audits := 0
	for _, message := range r.sent {
		if audit, ok := message.(*messages.DestinationStateAudit); ok {
			audits++
			points := audit.AffectedPointCode.AffectedPointCodes()
			if len(points) != 1 || points[0] != 101 {
				t.Errorf("old withdrawn audit was submitted after reactivation: %v", points)
			}
		}
	}
	if audits != 1 {
		t.Errorf("audit submissions=%d, want only the untouched AS scope", audits)
	}
}
