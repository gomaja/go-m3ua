package m3ua

import (
	"context"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

type acceptedOptionalAuditSender struct {
	accepted chan struct{}
	release  chan struct{}
}

func (s *acceptedOptionalAuditSender) SendMsg([]byte, sctp.SendOptions) (int, error) {
	panic("contextless send")
}
func (s *acceptedOptionalAuditSender) SendMsgContext(_ context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	if !opts.NoWait {
		panic("blocking audit")
	}
	close(s.accepted)
	<-s.release
	return len(frame), nil
}
func TestRestartWaitsOnlyForAlreadyAcceptedOptionalAttempt(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	oldEpoch := r.a.Epoch()
	sender := &acceptedOptionalAuditSender{accepted: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(sender.release) }) }
	defer unblock()
	r.a.signalWriter = nil
	r.a.controlTransport = sender
	sent := make(chan struct{})
	go func() { _, _ = r.a.tryOptionalSSNM(r.job()); close(sent) }()
	select {
	case <-sender.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("transport did not accept current attempt")
	}
	reset := make(chan struct{})
	go func() { r.a.resetTransportCongestionEpoch(); close(reset) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.a.sconSubmissions.mu.Lock()
		changing := r.a.sconSubmissions.optionalChanging != nil
		r.a.sconSubmissions.mu.Unlock()
		if changing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("restart did not close optional admission")
		}
		runtime.Gosched()
	}
	if r.a.Epoch() != oldEpoch {
		t.Fatal("epoch committed before accepted attempt settled")
	}
	probe := make(chan struct{})
	go func() {
		r.a.transportMu.Lock()
		defer r.a.transportMu.Unlock()
		close(probe)
	}()
	select {
	case <-probe:
	case <-time.After(5 * time.Second):
		t.Fatal("restart held transport sampler lock while joining optional completion")
	}
	admission := r.a.captureReceivedSSNMAdmission(testWireScope(7, true, 1))
	if admission.valid {
		t.Fatal("receive admitted knowledge during epoch change")
	}
	unblock()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("accepted optional attempt did not finish")
	}
	select {
	case <-reset:
	case <-time.After(5 * time.Second):
		t.Fatal("restart failed after known acceptance settled")
	}
	if r.a.Epoch() != oldEpoch+1 {
		t.Fatal("restart did not commit exactly one epoch")
	}
}
func TestRestartDoesNotJoinDataOrOrdinarySCON(t *testing.T) {
	r := newAuditTestRig(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	submission := &sconSubmission{ctx: ctx, cancel: cancel, done: make(chan struct{})}
	r.a.sconSubmissions.mu.Lock()
	if r.a.sconSubmissions.active == nil {
		r.a.sconSubmissions.active = make(map[*sconSubmission]struct{})
	}
	r.a.sconSubmissions.active[submission] = struct{}{}
	r.a.sconSubmissions.mu.Unlock()
	defer r.a.finishSCONSubmission(submission)
	r.a.sendMu.Lock()
	defer r.a.sendMu.Unlock()
	done := make(chan struct{})
	go func() { r.a.resetTransportCongestionEpoch(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("restart joined an application writer or ordinary SCON")
	}
	if ctx.Err() != nil {
		t.Fatal("optional epoch fence cancelled ordinary SCON")
	}
}
func TestForgetRejectsCapturedReportBeforeLegacyCommit(t *testing.T) {
	r := newAuditTestRig(t)
	reached := false
	r.e.aspRoutes.applyResolved = func() { reached = true; r.a.ForgetDestinations() }
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.e.aspRoutes.applyResolved = nil
	if !reached {
		t.Fatal("receive did not reach scope resolution seam")
	}
	if len(r.a.destinations.state) != 0 || r.e.ssnm.records != 0 || len(r.e.ssnm.audits) != 0 {
		t.Fatal("pre-forget report refilled legacy or canonical knowledge")
	}
	sendSCON(t, r.a, 7, 1, congestionLevel(3), 100)
	if r.e.ssnm.records != 2 || len(r.e.ssnm.audits) != 1 {
		t.Fatal("forget rejected a genuinely new report")
	}
	assertSSNMScopeAccounting(t, r.e)
}
func TestForgetSettlesOptionalAttemptBeforeReclaim(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sender := &epochBoundAuditSender{a: r.a, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	unblock := func() { release.Do(func() { close(sender.release) }) }
	defer unblock()
	r.a.signalWriter = nil
	r.a.controlTransport = sender
	sent := make(chan struct{})
	go func() { _, _ = r.a.tryOptionalSSNM(r.job()); close(sent) }()
	select {
	case <-sender.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("audit did not reach transport")
	}
	oldEpoch := r.a.Epoch()
	forgotten := make(chan int, 1)
	go func() { forgotten <- r.a.ForgetDestinations() }()
	select {
	case count := <-forgotten:
		if count != 1 {
			t.Fatal("forget changed legacy row count", count)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forget did not cancel and settle optional attempt")
	}
	unblock()
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("forgotten optional attempt did not complete")
	}
	if sender.accepted.Load() != 0 {
		t.Fatal("old audit accepted after forget returned")
	}
	if r.a.Epoch() != oldEpoch || r.e.ssnm.records != 0 || len(r.e.ssnm.audits) != 0 {
		t.Fatal("forget changed transport epoch or retained source work")
	}
}
func TestForgetPreservesCanonicalSiblingAndReplaysRemoval(t *testing.T) {
	r := newAuditTestRig(t)
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, b, 7, 2, congestionLevel(3), 100)
	before, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	oldEpoch := r.a.Epoch()
	if count := r.a.ForgetDestinations(); count != 1 {
		t.Fatal("legacy release count changed", count)
	}
	after := r.e.SSNMKnowledge()
	replica := before.Partitions[0]
	events, _, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("forget overflowed subscription")
	}
	for _, event := range events {
		if event.Kind == SSNMDestinationsInvalidatedEvent {
			event.ApplyInvalidation(&replica)
		}
	}
	if !reflect.DeepEqual(replica, after.Partitions[0]) {
		t.Fatal("forget source removal did not replay")
	}
	value := after.Partitions[0].Destinations[0]
	if len(value.CongestionSources) != 1 || value.CongestionSources[0].Association != b.ID() || !value.Congestion.Congested || value.Congestion.Association != b.ID() {
		t.Fatal("forget removed sibling's congestion", value)
	}
	if len(r.e.ssnm.audits) != 1 {
		t.Fatal("forget removed sibling's periodic work")
	}
	if r.a.Epoch() != oldEpoch {
		t.Fatal("local reclaim changed transport epoch")
	}
	assertSSNMScopeAccounting(t, r.e)
}
func TestForgetPreservesConfiguredIsolation(t *testing.T) {
	r := newAuditTestRig(t)
	r.a.scheduleDestinationAudit(SSNMReport{Kind: SSNMDestinationStateAuditReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}, true)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.a.ForgetDestinations()
	if len(r.e.ssnm.audits) != 1 || !r.job().isolation || r.job().congestion || r.job().availability {
		t.Fatal("forget did not separate configured repair from evidence-derived audit")
	}
}

func TestConcurrentLifetimeChangesShareAdmissionWithoutJoiningCleanup(t *testing.T) {
	r := newAuditTestRig(t)
	release := r.a.beginOptionalSSNMChange(true)
	defer release()
	done := make(chan struct{})
	go func() { r.a.resetTransportCongestionEpoch(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("restart joined another lifetime owner's cleanup instead of only optional completion")
	}
	if r.a.captureReceivedSSNMAdmission(testWireScope(7, true, 1)).valid {
		t.Fatal("concurrent lifetime release reopened another owner's admission gate")
	}
}

func TestConcurrentForgetRetainsConfiguredActivationAudit(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.OnActivation = []DestinationStateAuditRequest{{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}}
	// Pause one owner between its canonical reclaim and legacy cleanup. The
	// second public forget must not consume activation while admission is closed.
	release := r.a.beginOptionalSSNMChange(true)
	r.a.forgetReceivedDestinations()
	r.a.ForgetDestinations()
	release()
	r.a.reconcileDestinationAudits()
	if len(r.e.ssnm.audits) != 1 || !r.job().isolation {
		t.Fatal("concurrent forget consumed configured activation without scheduling its audit")
	}
	r.advance(time.Second)
	if len(r.sent) != 1 {
		t.Fatal("configured activation audit did not run after concurrent forget")
	}
	assertSSNMScopeAccounting(t, r.e)
}
