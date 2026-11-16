package m3ua

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

func publicationRig(t *testing.T, limit int) *auditTestRig {
	t.Helper()
	r := newAuditTestRig(t)
	if err := configurePublicationForTest(r.e, &CongestionPublicationConfig{MaxPending: limit, MaxPendingPerAssociation: limit}); err != nil {
		t.Fatal(err)
	}
	return r
}

func queuePublication(r *auditTestRig, dpc, opc uint32, state CongestionState, revision uint64) {
	scope := testWireScope(7, true, 1)
	r.e.enqueueCongestionPublication(r.a, scope, r.a.queueCongestionBinding(r.a.configuredLocalASKeysForStatus()[0], scope), dpc, opc, true, state, revision, false)
}

func nationalPublication(level uint8) CongestionState {
	return CongestionState{Congested: level != 0, LevelSet: true, Level: level}
}

func TestSaturatedOptionalSCONRetriesWithoutBlockingMandatoryControl(t *testing.T) {
	r := publicationRig(t, 4)
	r.a.signalWriter = nil
	w := &optionalAuditSender{err: syscall.EAGAIN}
	r.a.controlTransport = w
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	if w.attempts != 1 || !w.opts.NoWait {
		t.Fatalf("optional attempt=%d NoWait=%v", w.attempts, w.opts.NoWait)
	}
	for range 20 {
		r.e.runCongestionAutomation(r.now)
	}
	if w.attempts != 1 {
		t.Fatal("retry hot loop")
	}
	if p := r.e.CongestionPublications(); len(p) != 1 || !p[0].Pending || p[0].LastError == "" {
		t.Fatalf("failed report lost: %+v", p)
	}
	w.err = nil
	r.advance(time.Second)
	if w.attempts != 2 || r.e.CongestionPublications()[0].Pending {
		t.Fatal("saturated report was not retried")
	}
}

func TestEndedUnsentOnsetIsNotPublishedAfterDrain(t *testing.T) {
	r := publicationRig(t, 2)
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	queuePublication(r, 100, 10, nationalPublication(0), 2)
	r.advance(time.Second)
	if len(r.sent) != 0 || len(r.e.CongestionPublications()) != 0 {
		t.Fatal("ended unsent onset retained or submitted")
	}
}

func TestNationalClearRemainsPendingUntilSubmissionOrSupersession(t *testing.T) {
	r := publicationRig(t, 2)
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	r.a.signalWriter = func(messages.M3UA) (int, error) { return 0, syscall.EAGAIN }
	queuePublication(r, 100, 10, nationalPublication(0), 2)
	r.advance(time.Second)
	p := r.e.CongestionPublications()
	if len(p) != 1 || !p[0].Pending || p[0].SubmittedRevision != 1 || p[0].DesiredRevision != 2 {
		t.Fatalf("clear obligation lost: %+v", p)
	}
	queuePublication(r, 100, 10, nationalPublication(2), 3)
	r.a.signalWriter = func(m messages.M3UA) (int, error) { r.sent = append(r.sent, m); return 0, nil }
	r.advance(time.Second)
	if len(r.sent) != 2 || r.sent[1].(*messages.SignallingCongestion).CongestionIndications.CongestionLevel() != 2 {
		t.Fatal("superseded clear overtook current onset")
	}
}

func TestOptionalReportCannotWaitBehindTrafficBarrier(t *testing.T) {
	for _, lock := range []string{"traffic", "writer", "tracker", "state"} {
		t.Run(lock, func(t *testing.T) {
			r := publicationRig(t, 2)
			queuePublication(r, 100, 10, nationalPublication(3), 1)
			var unlock func()
			switch lock {
			case "traffic":
				r.a.aspTransferMu.Lock()
				unlock = r.a.aspTransferMu.Unlock
			case "writer":
				r.a.sendMu.Lock()
				unlock = r.a.sendMu.Unlock
			case "tracker":
				r.a.sconSubmissions.mu.Lock()
				unlock = r.a.sconSubmissions.mu.Unlock
			case "state":
				r.a.muState.Lock()
				unlock = r.a.muState.Unlock
			}
			done := make(chan struct{})
			go func() { r.e.runCongestionAutomation(r.now); close(done) }()
			select {
			case <-done:
				unlock()
			case <-time.After(time.Second):
				unlock()
				<-done
				t.Fatal("optional work waited on " + lock)
			}
			if len(r.sent) != 0 {
				t.Fatal("contended optional work submitted")
			}
			r.advance(time.Second)
			if len(r.sent) != 1 {
				t.Fatal("contended work not retried")
			}
		})
	}
}

func TestSubmittedNationalOnsetsReserveTheirClearCapacity(t *testing.T) {
	r := publicationRig(t, 1)
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	queuePublication(r, 101, 11, nationalPublication(3), 2)
	if r.e.CongestionPublicationCounters().Refused != 1 {
		t.Fatal("no visible full reservation refusal")
	}
	queuePublication(r, 100, 10, nationalPublication(0), 3)
	r.advance(time.Second)
	if len(r.sent) != 2 || r.sent[1].(*messages.SignallingCongestion).CongestionIndications.CongestionLevel() != 0 {
		t.Fatal("reserved clear could not submit")
	}
	queuePublication(r, 101, 11, nationalPublication(3), 4)
	r.advance(time.Second)
	if len(r.sent) != 3 {
		t.Fatal("submitted clear did not release reservation")
	}
}

func TestOptionalPublicationBoundsExposeRefusal(t *testing.T) {
	r := publicationRig(t, 2)
	for range 100 {
		queuePublication(r, 100, 10, nationalPublication(3), 1)
	}
	queuePublication(r, 100, 11, nationalPublication(3), 2)
	queuePublication(r, 100, 12, nationalPublication(3), 3)
	if len(r.a.mgmtChan) > 3 {
		t.Fatal("coalesced publication flooded the management queue")
	}
	p := r.e.CongestionPublications()
	if len(p) != 2 || r.e.CongestionPublicationCounters().Refused != 1 || r.e.CongestionPublicationCounters().Coalesced != 99 {
		t.Fatalf("unbounded or merged CD origins: %+v %+v", p, r.e.CongestionPublicationCounters())
	}
	r.e.runCongestionAutomation(r.now)
	if len(r.sent) != 1 {
		t.Fatalf("optional submissions=%d, want one per association/pass; states=%+v counters=%+v", len(r.sent), r.e.CongestionPublications(), r.e.CongestionPublicationCounters())
	}
}

func TestMandatoryPendingControlDefersOptionalPublication(t *testing.T) {
	r := publicationRig(t, 2)
	r.a.notificationQueue = make(chan mandatoryControl, 1)
	r.a.notificationQueue <- mandatoryControl{}
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	if len(r.sent) != 0 {
		t.Fatal("optional SCON overtook pending mandatory control")
	}
	<-r.a.notificationQueue
	r.advance(time.Second)
	if len(r.sent) != 1 {
		t.Fatal("mandatory deferral discarded current report")
	}
}

func TestAuditCannotEvictNationalClearReservation(t *testing.T) {
	r := publicationRig(t, 1)
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	sendDUNA(t, r.a, 7, 1, 101)
	r.advance(30 * time.Second)
	if len(r.sent) != 1 || r.job() == nil {
		t.Fatal("audit evicted national onset reservation")
	}
	queuePublication(r, 100, 10, nationalPublication(0), 2)
	r.advance(time.Second)
	r.advance(time.Second)
	if len(r.sent) != 3 {
		t.Fatalf("clear or deferred audit lost: %d reports", len(r.sent))
	}
	if _, ok := r.sent[1].(*messages.SignallingCongestion); !ok {
		t.Fatal("audit overtook reserved national clear")
	}
	if _, ok := r.sent[2].(*messages.DestinationStateAudit); !ok {
		t.Fatal("waiting audit did not resume")
	}
}

type publicationLockProbe struct {
	check    func()
	attempts int
}

func (w *publicationLockProbe) SendMsg(_ []byte, _ sctp.SendOptions) (int, error) {
	w.attempts++
	w.check()
	return 0, nil
}
func (w *publicationLockProbe) SendMsgContext(_ context.Context, b []byte, o sctp.SendOptions) (int, error) {
	return w.SendMsg(b, o)
}

func TestOptionalTransportNeverHoldsBookkeepingLocks(t *testing.T) {
	r := publicationRig(t, 2)
	r.a.signalWriter = nil
	w := &publicationLockProbe{}
	w.check = func() {
		if !r.a.sconSubmissions.mu.TryLock() {
			t.Error("optional transport holds submission tracker")
		} else {
			r.a.sconSubmissions.mu.Unlock()
		}
		if !r.a.muState.TryLock() {
			t.Error("optional transport holds association state")
		} else {
			r.a.muState.Unlock()
		}
		if !r.a.muDataOverflow.TryLock() {
			t.Error("optional transport holds queue tracker")
		} else {
			r.a.muDataOverflow.Unlock()
		}
		if !r.e.ssnm.mu.TryLock() {
			t.Error("optional transport holds SSNM store")
		} else {
			r.e.ssnm.mu.Unlock()
		}
	}
	r.a.controlTransport = w
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	if w.attempts != 1 {
		t.Fatal("no optional transport attempt exercised")
	}
}

func TestMandatoryTimeoutStillClosesPermanentlyStalledAssociation(t *testing.T) {
	transport := newFullSendBuffer()
	c := newBackpressureTestConn(t, transport, 50*time.Millisecond)
	result := make(chan error, 1)
	go func() { _, err := c.writeControl(heartbeatAckForTest()); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrControlWriteTimeout) {
			t.Fatalf("stalled control returned %v", err)
		}
	case <-time.After(time.Second):
		_ = c.Close()
		if err := <-result; err == nil {
			t.Error("closing the stalled transport returned no error")
		}
		t.Fatal("mandatory control timeout did not stop stalled transport")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("control timeout did not close association")
	}
}

func configurePublicationForTest(e *Endpoint, config *CongestionPublicationConfig) error {
	c, err := resolveCongestionPublication(config)
	if err != nil {
		return err
	}
	e.ssnm.mu.Lock()
	defer e.ssnm.mu.Unlock()
	e.ssnm.publicationConfig = c
	if e.ssnm.publications == nil {
		e.ssnm.publications = make(map[congestionPublicationKey]*congestionPublication)
	}
	return nil
}

func TestPublicationConfigurationDefaultsAndOwnedLimits(t *testing.T) {
	defaults, err := resolveCongestionPublication(nil)
	if err != nil || defaults != (CongestionPublicationConfig{MaxPending: 64, MaxPendingPerAssociation: 32, MaxBytes: 64 << 10, RetryInterval: 100 * time.Millisecond, MinReportInterval: time.Second}) {
		t.Fatalf("publication defaults=%+v, %v", defaults, err)
	}
	for _, config := range []CongestionPublicationConfig{{MaxPending: -1}, {MaxPendingPerAssociation: -1}, {MaxBytes: -1}, {RetryInterval: -1}, {MinReportInterval: -1}, {MaxPending: 1, MaxPendingPerAssociation: 2}} {
		if _, err := resolveCongestionPublication(&config); !errors.Is(err, ErrInvalidRoleConfiguration) {
			t.Fatalf("invalid publication configuration accepted: %+v, %v", config, err)
		}
	}
	config := &CongestionPublicationConfig{MaxPending: 4, MaxPendingPerAssociation: 2, MaxBytes: 4096}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Publication: config}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := e.Close(); err != nil {
			t.Error(err)
		}
	}()
	config.MaxPending = 99
	if e.ssnm.publicationConfig.MaxPending != 4 {
		t.Fatal("publication scheduler borrowed caller configuration")
	}
}

func TestPublicationByteAndAssociationBoundsPreserveClear(t *testing.T) {
	for _, byBytes := range []bool{false, true} {
		name := "association"
		if byBytes {
			name = "bytes"
		}
		t.Run(name, func(t *testing.T) {
			r := publicationRig(t, 4)
			config := CongestionPublicationConfig{MaxPending: 4, MaxPendingPerAssociation: 1}
			charge := publicationCharge(testWireScope(7, true, 1), 1)
			if byBytes {
				config.MaxPendingPerAssociation = 4
				config.MaxBytes = charge
			}
			if err := configurePublicationForTest(r.e, &config); err != nil {
				t.Fatal(err)
			}
			queuePublication(r, 100, 10, nationalPublication(3), 1)
			queuePublication(r, 101, 11, nationalPublication(3), 2)
			if len(r.e.CongestionPublications()) != 1 || r.e.CongestionPublicationCounters().Refused != 1 {
				t.Fatal("publication bound admitted a second onset")
			}
			r.e.runCongestionAutomation(r.now)
			queuePublication(r, 100, 10, nationalPublication(0), 3)
			r.advance(time.Second)
			if len(r.sent) != 2 || r.sent[1].(*messages.SignallingCongestion).CongestionIndications.CongestionLevel() != 0 {
				t.Fatal("capacity bound refused a reserved clear")
			}
			r.e.ssnm.mu.Lock()
			remaining := r.e.ssnm.publicationBytes
			r.e.ssnm.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("submitted clear retained %d accounted bytes", remaining)
			}
			queuePublication(r, 101, 11, nationalPublication(3), 4)
			r.advance(time.Second)
			if len(r.sent) != 3 {
				t.Fatal("clear did not return publication capacity")
			}
		})
	}
}
