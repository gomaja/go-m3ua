package m3ua

import (
	"context"
	"errors"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type auditTestRig struct {
	e    *Endpoint
	a    *Association
	now  time.Time
	sent []messages.M3UA
}

func newAuditTestRig(t *testing.T) *auditTestRig {
	t.Helper()
	r := &auditTestRig{e: newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil), now: time.Now()}
	configureAutomationForTest(r.e, &r.now)
	r.a = attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	r.a.signalWriter = func(m messages.M3UA) (int, error) { r.sent = append(r.sent, m); return 0, nil }
	return r
}
func (r *auditTestRig) advance(d time.Duration) {
	r.now = r.now.Add(d)
	r.e.runCongestionAutomation(r.now)
}
func (r *auditTestRig) status() aspSelectionStatus {
	return r.e.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), 100)
}
func (r *auditTestRig) job() *destinationAuditJob {
	for _, job := range r.e.ssnm.audits {
		return job
	}
	return nil
}
func TestCongestionAutomationConfigDefaultsAndErrors(t *testing.T) {
	after, cfg, err := resolveCongestionAutomation(RoleASP, &CongestionConfig{})
	if err != nil || after != 0 || cfg != nil {
		t.Fatal(after, cfg, err)
	}
	input := &CongestionConfig{Validity: &CongestionValidityConfig{}, Auditing: &DestinationAuditConfig{}}
	after, cfg, err = resolveCongestionAutomation(RoleASP, input)
	if err != nil || after != 30*time.Second || cfg.Period != 30*time.Second || cfg.MaxPending != 256 || cfg.MaxMessagesPerSecond != 10 {
		t.Fatal(after, cfg, err)
	}
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		if _, _, err = resolveCongestionAutomation(role, input); err == nil {
			t.Fatalf("role %v accepted audits", role)
		}
	}
	for _, negative := range []*CongestionConfig{{Validity: &CongestionValidityConfig{After: -1}}, {Auditing: &DestinationAuditConfig{Period: -1}}, {Auditing: &DestinationAuditConfig{MaxPending: -1}}, {Auditing: &DestinationAuditConfig{MaxMessagesPerSecond: -1}}} {
		if _, _, err = resolveCongestionAutomation(RoleASP, negative); err == nil {
			t.Fatal("negative configuration accepted")
		}
	}
	request := DestinationStateAuditRequest{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}
	input.Auditing.OnActivation = []DestinationStateAuditRequest{request}
	_, cfg, err = resolveCongestionAutomation(RoleASP, input)
	if err != nil {
		t.Fatal(err)
	}
	input.Auditing.OnActivation[0].Scope.RoutingContexts[0] = 99
	if cfg.OnActivation[0].Scope.RoutingContexts[0] != 1 {
		t.Fatal("config not owned")
	}
}
func TestSCONZeroDoesNotScheduleCongestionAudit(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(0), 100)
	if r.job() != nil {
		t.Fatal("SCON0 scheduled congestion audit")
	}
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendDUNA(t, r.a, 7, 1, 100)
	old := r.job().due
	r.now = r.now.Add(time.Second)
	sendSCON(t, r.a, 7, 1, congestionLevel(0), 100)
	if job := r.job(); job == nil || job.congestion || !job.availability || !job.due.Equal(old) {
		t.Fatalf("SCON0 cancelled/reset availability origin: %+v", job)
	}
	r.advance(29 * time.Second)
	if len(r.sent) != 1 {
		t.Fatalf("availability audit missing: %v", r.sent)
	}
	record := ssnmDestination(t, ssnmPartitionKnowledge(t, r.e.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")), 100, 0).Congestion
	if !record.ExpiresAt.IsZero() || record.Freshness != CongestionFresh {
		t.Fatal("clear retained validity timer")
	}
}
func TestDAVAUpdatesAuditDeadlineWithoutClearingSCON(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.now = r.now.Add(10 * time.Second)
	sendDAVA(t, r.a, 7, 1, 100)
	if !r.status().congested || !r.job().due.Equal(r.now.Add(30*time.Second)) {
		t.Fatal("DAVA cleared congestion or failed to reset due")
	}
	r.advance(20 * time.Second)
	if len(r.sent) != 0 || !r.status().receivedUnknown {
		t.Fatal("DAVA renewed validity or kept old audit deadline")
	}
	r.advance(10 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("reset audit never sent")
	}
}
func TestLateExpiryCannotRemoveFreshSCON(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.now = r.now.Add(20 * time.Second)
	sendSCON(t, r.a, 7, 1, congestionLevel(3), 100)
	r.advance(10 * time.Second)
	if s := r.status(); !s.congested || s.congestionLevel != 3 || s.receivedUnknown {
		t.Fatalf("old expiry removed fresh report: %+v", s)
	}
	r.advance(20 * time.Second)
	if !r.status().receivedUnknown || r.status().congested {
		t.Fatal("renewed deadline did not expire")
	}
}
func TestExpiredNarrowStatementShadowsOlderBroadCongestion(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(3), uint32(24)<<24)
	r.now = r.now.Add(time.Second)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.advance(30 * time.Second)
	s := r.status()
	if s.congested || !s.receivedUnknown {
		t.Fatalf("expiry revived older broad congestion: %+v", s)
	}
	// Same state apart from freshness: a fresh narrow clear also shadows it.
	sendSCON(t, r.a, 7, 1, congestionLevel(0), 100)
	if s = r.status(); s.congested || s.receivedUnknown {
		t.Fatalf("new clear failed to shadow expired history: %+v", s)
	}
}
func TestAnotherFreshSourceSurvivesCongestionExpiry(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	r.now = r.now.Add(20 * time.Second)
	sendSCON(t, b, 7, 2, congestionLevel(3), 100)
	r.advance(10 * time.Second)
	if s := r.status(); !s.congested || s.congestionLevel != 3 || !s.receivedUnknown {
		t.Fatalf("fresh source lost with expired sibling: %+v", s)
	}
	if methodPermits(CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}, 3, r.status()) {
		t.Fatal("unknown priority candidate admitted")
	}
	if !methodPermits(CongestionProfile{Method: CongestionInternational}, 0, r.status()) {
		t.Fatal("international unknown rejected as priority gate")
	}
}
func TestWithdrawalCancelsAuditWithoutReactivation(t *testing.T) {
	for _, reason := range []string{"withdrawal", "restart", "close"} {
		t.Run(reason, func(t *testing.T) {
			r := newAuditTestRig(t)
			sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
			_ = attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
			job := r.job()
			switch reason {
			case "withdrawal":
				r.a.noteRoutingContextsOverridden([]uint32{1})
			case "restart":
				r.a.resetTransportCongestionEpoch()
				r.a.syncSSNMBindings()
			case "close":
				if err := r.a.Close(); err != nil {
					t.Fatal(err)
				}
			}
			r.advance(time.Minute)
			if len(r.sent) != 0 || r.job() != nil || job.live.Load() {
				t.Fatalf("retired reporter produced traffic/retained job: %+v", r.sent)
			}
			if submitted, _ := r.a.tryOptionalSSNM(job); submitted {
				t.Fatal("retired snapshot submitted")
			}
		})
	}
}
func TestActivationAuditRunsAfterStateCommit(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.OnActivation = []DestinationStateAuditRequest{{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}}
	r.a.setState(StateASPInactive)
	r.a.reconcileDestinationAudits()
	r.advance(time.Second)
	if len(r.sent) != 0 || r.job() != nil {
		t.Fatal("audit before activation")
	}
	r.a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	r.a.setState(StateASPActive)
	r.e.runCongestionAutomation(r.now)
	if len(r.sent) != 1 || r.a.State() != StateASPActive {
		t.Fatal("no committed-activation audit")
	}
	sendDAVA(t, r.a, 7, 1, 100)
	r.advance(time.Minute)
	if len(r.sent) != 1 {
		t.Fatal("DAVA did not end isolation job")
	}
}
func TestMaskedAuditsRemainBounded(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.MaxPending = 1
	sendSCON(t, r.a, 7, 1, congestionLevel(2), uint32(24)<<24)
	if len(r.e.ssnm.audits) != 1 || r.job().key.destination.mask != 24 {
		t.Fatal("masked range expanded")
	}
	job := r.job()
	bytes := r.e.ssnm.bytes
	for range 20 {
		sendSCON(t, r.a, 7, 1, congestionLevel(2), uint32(24)<<24)
	}
	if len(r.e.ssnm.audits) != 1 || r.e.ssnm.bytes != bytes || job.live.Load() {
		t.Fatal("refresh not bounded in place")
	}
	sendDUNA(t, r.a, 7, 1, 101)
	if len(r.e.ssnm.audits) != 1 {
		t.Fatal("MaxPending exceeded")
	}
	r.e.ssnm.auditing.MaxPending = 2
	r.e.ssnm.limits.MaxBytes = r.e.ssnm.bytes
	r.a.scheduleDestinationAudit(SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 102}}}, false)
	if len(r.e.ssnm.audits) != 1 || r.e.ssnm.bytes > r.e.ssnm.limits.MaxBytes {
		t.Fatal("audit escaped byte budget")
	}
}
func TestCongestionAuditsRespectMethodAndDisabledPolicies(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionInternational, CongestionNationalWithoutPriorities} {
		t.Run(methodName(method), func(t *testing.T) {
			r := newAuditTestRig(t)
			r.e.congestion.defaultProfile = CongestionProfile{Method: method}
			sendSCON(t, r.a, 7, 1, nil, 100)
			if r.job() != nil {
				t.Fatal("numeric-congestion audit outside priority method")
			}
			sendDRST(t, r.a, 7, 1, 100)
			r.advance(30 * time.Second)
			if len(r.sent) != 1 {
				t.Fatal("availability-origin audit missing")
			}
		})
	}
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	sendSCON(t, a, 7, 1, congestionLevel(2), 100)
	e.runCongestionAutomation(time.Now().Add(time.Hour))
	if len(e.ssnm.audits) != 0 || !e.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), 100).congested {
		t.Fatal("nil policy activated automation")
	}
}
func methodName(m CongestionMethod) string {
	if m == CongestionInternational {
		return "international"
	}
	return "national-without-priorities"
}

func TestPeerValidityDoesNotStartReverseDAUD(t *testing.T) {
	s, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	now := time.Now()
	s.now = func() time.Time { return now }
	s.validity = time.Second
	peerRetentionApply(t, s, report, 1, bindings)
	before := s.snapshot().PeerCongestion[0]
	now = now.Add(time.Second)
	s.expireCongestion(now)
	after := s.snapshot().PeerCongestion[0]
	if after.Freshness != CongestionExpired || !after.Congestion.Congested || after.Congestion.Level != before.Congestion.Level || after.ExpiresAt.IsZero() || after.Revision != before.Revision || len(s.audits) != 0 {
		t.Fatalf("peer expiry fabricated clear/reverse audit: %+v", after)
	}
}

type optionalAuditSender struct {
	attempts int
	err      error
	opts     sctp.SendOptions
}

func (w *optionalAuditSender) SendMsg(_ []byte, o sctp.SendOptions) (int, error) {
	w.attempts++
	w.opts = o
	return 0, w.err
}
func (w *optionalAuditSender) SendMsgContext(_ context.Context, b []byte, o sctp.SendOptions) (int, error) {
	return w.SendMsg(b, o)
}

func TestOptionalAuditDefersContendedAndEAGAINWork(t *testing.T) {
	for _, lock := range []string{"traffic", "writer", "state", "binding", "dynamic", "active"} {
		t.Run(lock, func(t *testing.T) {
			r := newAuditTestRig(t)
			sendDUNA(t, r.a, 7, 1, 100)
			var unlock func()
			switch lock {
			case "traffic":
				r.a.aspTransferMu.Lock()
				unlock = r.a.aspTransferMu.Unlock
			case "writer":
				r.a.sendMu.Lock()
				unlock = r.a.sendMu.Unlock
			case "state":
				r.a.muState.Lock()
				unlock = r.a.muState.Unlock
			case "binding":
				r.a.sconSubmissions.mu.Lock()
				unlock = r.a.sconSubmissions.mu.Unlock
			case "dynamic":
				r.a.muDynamicASKeys.Lock()
				unlock = r.a.muDynamicASKeys.Unlock
			case "active":
				r.a.muAckedRCs.Lock()
				unlock = r.a.muAckedRCs.Unlock
			}
			done := make(chan struct{})
			go func() { r.advance(30 * time.Second); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				unlock()
				t.Fatal("optional audit waited on " + lock)
			}
			unlock()
			if len(r.sent) != 0 || r.job() == nil {
				t.Fatal("contended audit lost/submitted")
			}
			r.e.runCongestionAutomation(r.now)
			if len(r.sent) != 1 {
				t.Fatal("deferred job never retried")
			}
		})
	}
	r := newAuditTestRig(t)
	r.a.signalWriter = nil
	sender := &optionalAuditSender{err: syscall.EAGAIN}
	r.a.controlTransport = sender
	sendDUNA(t, r.a, 7, 1, 100)
	r.advance(30 * time.Second)
	if sender.attempts != 1 || !sender.opts.NoWait || r.job() == nil || r.a.State() != StateASPActive {
		t.Fatal("EAGAIN retried, lost work, changed state or used blocking send")
	}
	sender.err = nil
	r.advance(time.Second)
	if sender.attempts != 2 || !r.job().due.After(r.now) {
		t.Fatal("EAGAIN job never recovered")
	}
}
func TestOptionalAuditRejectsReplacedBindingAndStaleRevision(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	old := r.job()
	sendDUNA(t, r.a, 7, 1, 100)
	if submitted, _ := r.a.tryOptionalSSNM(old); submitted {
		t.Fatal("stale report revision submitted")
	}
	job := r.job()
	r.a.muDynamicASKeys.Lock()
	r.a.dynamicPeerASKeyVersions = map[uint32]uint64{1: 99}
	r.a.muDynamicASKeys.Unlock()
	if submitted, err := r.a.tryOptionalSSNM(job); submitted || !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatal("replaced registration submitted", submitted, err)
	}
	if len(r.sent) != 0 {
		t.Fatal("retired binding wrote DAUD")
	}
}
func TestAuditRateAndSendFailureAreLocalOutcomes(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.MaxMessagesPerSecond = 1
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	b.signalWriter = func(m messages.M3UA) (int, error) { r.sent = append(r.sent, m); return 0, nil }
	sendDUNA(t, r.a, 7, 1, 100)
	sendDUNA(t, b, 7, 2, 101)
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("rate budget not enforced")
	}
	r.e.runCongestionAutomation(r.now.Add(time.Second - time.Nanosecond))
	if len(r.sent) != 1 {
		t.Fatal("rate boundary admitted early")
	}
	r.advance(time.Second)
	if len(r.sent) != 2 {
		t.Fatal("rate deferred job lost")
	}
	r.a.signalWriter = func(messages.M3UA) (int, error) { return 0, syscall.EPIPE }
	r.advance(30 * time.Second)
	if r.a.State() != StateASPActive || r.a.terminating.Load() {
		t.Fatal("optional send failure changed ASP state")
	}
	found := false
	for _, job := range r.e.ssnm.audits {
		found = found || job.outcome == "send-failed"
	}
	if !found {
		t.Fatal("failure invisible")
	}
}
func TestValidityExpiryDeltaOwnsHistoricalSources(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	replica, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	r.advance(30 * time.Second)
	event := peerRetentionNext(t, sub)
	if event.Kind != SSNMCongestionExpiredEvent || event.ReportSet || len(event.Expired) != 1 || len(event.Updated) != 0 || event.PeerCongestion != nil || !event.ApplyExpiry(&replica) {
		t.Fatalf("expiry is not a compact versioned unknown delta: %+v", event)
	}
	snapshot := r.e.SSNMKnowledge()
	if !reflect.DeepEqual(replica.Partitions, snapshot.Partitions) {
		t.Fatal("expiry identities do not replay retained historical sources")
	}
	event.Expired[0].AS.RoutingContext = 999
	if snapshot.Partitions[0].Destinations[0].CongestionSources[0].AS.RoutingContext != 1 {
		t.Fatal("expiry identity aliases retained source history")
	}
}

func TestCongestionExpiryKeepsTransportContribution(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(3), 100)
	r.advance(30 * time.Second)
	setAssociationTransport(t, r.e, r.a, 2)
	status := methodTransportStatus(r.a, r.status())
	if !status.congested || status.congestionLevel != 2 || !status.receivedUnknown {
		t.Fatalf("expiry cleared independent transport input: %+v", status)
	}
}

func TestIPSPValidityProjectionCommitsBeforeIndication(t *testing.T) {
	key := routingContextASKey(1)
	destination := MTPDestination{MTPRoute: "peer", PointCode: 100}
	e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleIPSP, IPSPDestinations: []IPSPDestinationConfig{{Destination: destination, AS: key}}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	now := time.Now()
	e.ssnm.validity = time.Second
	e.ssnm.now = func() time.Time { return now }
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	receive := func(level uint8) {
		t.Helper()
		if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(level), nil)); err != nil {
			t.Fatal(err)
		}
	}
	receive(2)
	if s, _ := e.MTPDestinationStatus(destination); !s.Congested || s.ReceivedCongestionUnknown {
		t.Fatalf("onset projection lagged: %+v", s)
	}
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	if s, _ := e.MTPDestinationStatus(destination); s.Congested || !s.ReceivedCongestionUnknown {
		t.Fatalf("expiry projection lagged: %+v", s)
	}
	receive(3)
	if s, _ := e.MTPDestinationStatus(destination); !s.Congested || s.CongestionLevel != 3 || s.ReceivedCongestionUnknown {
		t.Fatalf("renewal projection lagged: %+v", s)
	}
}

func TestAuditRateCountsSubmissionDuringReportRefresh(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.MaxMessagesPerSecond = 1
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	sendDUNA(t, r.a, 7, 1, 100)
	sendDUNA(t, b, 7, 2, 101)
	count := 0
	r.a.signalWriter = func(messages.M3UA) (int, error) {
		count++
		if count == 1 {
			r.a.scheduleDestinationAudit(SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}, false)
		}
		return 0, nil
	}
	b.signalWriter = func(messages.M3UA) (int, error) { count++; return 0, nil }
	r.advance(30 * time.Second)
	if count != 1 || !r.e.ssnm.auditNext.After(r.now) {
		t.Fatal("concurrent report refresh erased an accepted rate slot", count)
	}
}

func TestASPExpiryIsVisibleWithoutAvailabilityAdmission(t *testing.T) {
	e := newSSNMStateEndpoint(t, validASPConfig(), nil)
	now := time.Now()
	configureAutomationForTest(e, &now)
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	sendSCON(t, a, 7, 1, congestionLevel(2), 0x123456)
	now = now.Add(30 * time.Second)
	e.runCongestionAutomation(now)
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}
	s, ok := e.MTPDestinationStatus(destination)
	if !ok || s.Congested || !s.ReceivedCongestionUnknown {
		t.Fatalf("expiry hidden by missing availability: %+v %v", s, ok)
	}
}

func TestOptionalAuditRejectsNonASPRoles(t *testing.T) {
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		r := newAuditTestRig(t)
		sendDUNA(t, r.a, 7, 1, 100)
		job := r.job()
		r.a.role = role
		submitted, _ := r.a.tryOptionalSSNM(job)
		r.a.role = RoleASP
		if submitted || len(r.sent) != 0 {
			t.Fatalf("role %v originated DAUD", role)
		}
	}
}

func TestCoveringClearCancelsSupersededCongestionAudit(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, r.a, 7, 1, congestionLevel(0), uint32(24)<<24)
	r.advance(30 * time.Second)
	if len(r.sent) != 0 || len(r.e.ssnm.audits) != 0 {
		t.Fatal("covering clear retained congestion-origin audit")
	}
}

func TestAdditionalActivationDoesNotRetriggerEarlierIsolationAudit(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Now()
	configureAutomationForTest(e, &now)
	e.ssnm.auditing.OnActivation = []DestinationStateAuditRequest{
		{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}},
		{Scope: testWireScope(7, true, 3), Destinations: []PointCodeRange{{PointCode: 101}}},
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 3)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	peer := SGPIdentity{"sg-a", "sgp-a1"}
	a.cfg.PeerSGP = &peer
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	count := 0
	a.signalWriter = func(messages.M3UA) (int, error) { count++; return 0, nil }
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	e.runCongestionAutomation(now)
	if count != 1 {
		t.Fatal("initial activation", count)
	}
	sendDAVA(t, a, 7, 1, 100)
	now = now.Add(time.Second)
	a.noteRoutingContextsAcked(params.NewRoutingContext(3))
	e.runCongestionAutomation(now)
	if count != 2 || len(e.ssnm.audits) != 1 {
		t.Fatalf("unrelated activation restarted prior scope: %d %+v", count, e.ssnm.audits)
	}
	for key := range e.ssnm.audits {
		if key.destination.pointCode != 101 {
			t.Fatal("wrong AS restarted")
		}
	}
}

func TestCongestionAutomationConstructorRejectsInvalidPolicies(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		cfg := EndpointConfig{Role: role, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Validity: &CongestionValidityConfig{After: -1}}}
		if e, err := NewEndpoint(cfg); err == nil {
			_ = e.Close()
			t.Fatalf("role %v accepted negative validity", role)
		}
		if role != RoleASP {
			cfg.Congestion.Validity = nil
			cfg.Congestion.Auditing = &DestinationAuditConfig{}
			if e, err := NewEndpoint(cfg); err == nil {
				_ = e.Close()
				t.Fatalf("role %v accepted audits", role)
			}
		}
	}
}

func TestDueAuditsCoalesceOnlyWithinOriginalScope(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100, 101)
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatalf("same-scope due work was not coalesced: %d", len(r.sent))
	}
	audit := r.sent[0].(*messages.DestinationStateAudit)
	if len(audit.AffectedPointCode.AffectedPointCodes()) != 2 {
		t.Fatal("coalesced audit lost a range")
	}
	for _, job := range r.e.ssnm.audits {
		if !job.due.After(r.now) {
			t.Fatal("coalesced job not refreshed")
		}
	}
}

func TestValiditySourceHistoryRefusesWholeOverBudgetReport(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxRecords: 3, MaxRecordsPerPeer: 3, MaxRecordsPerPartition: 3, MaxBytes: 4096})
	now := time.Now()
	configureAutomationForTest(e, &now)
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100, 101), nil, params.NewCongestionIndications(2), nil))
	if !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("expected whole source-history refusal, got %v", err)
	}
	snapshot := e.SSNMKnowledge()
	if snapshot.ReportsRefused != 1 || len(snapshot.Partitions[0].Destinations) != 0 || e.ssnm.records != 0 || len(e.ssnm.audits) != 0 {
		t.Fatalf("over-budget report partially retained: %+v", snapshot)
	}
}

func TestOptionalAuditCannotSendBeforeCommittedActivation(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	job := r.job()
	r.a.muState.Lock()
	r.a.state = StateASPInactive
	r.a.muState.Unlock()
	submitted, _ := r.a.tryOptionalSSNM(job)
	r.a.muState.Lock()
	r.a.state = StateASPActive
	r.a.muState.Unlock()
	if submitted || len(r.sent) != 0 {
		t.Fatal("audit submitted before committed activation")
	}
	if submitted, err := r.a.tryOptionalSSNM(job); !submitted || err != nil {
		t.Fatal("committed activation did not admit identical job", submitted, err)
	}
}

func TestCapturedExpiryVersionCannotExpireRenewedReport(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	r.now = r.now.Add(30 * time.Second)
	due := r.e.ssnm.congestionExpiries(r.now)
	if len(due) != 1 {
		t.Fatal("missing expiry candidate")
	}
	sendSCON(t, r.a, 7, 1, congestionLevel(3), 100)
	if r.e.ssnm.expireCongestionStatement(due[0], r.now.Add(time.Hour)) {
		t.Fatal("old timer expired newer source version")
	}
	if s := r.status(); !s.congested || s.congestionLevel != 3 || s.receivedUnknown {
		t.Fatalf("fresh source changed: %+v", s)
	}
}

func TestBlockedReporterCannotStarveHealthyAudit(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.MaxMessagesPerSecond = 1
	r.a.signalWriter = nil
	r.a.controlTransport = &optionalAuditSender{err: syscall.EAGAIN}
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	count := 0
	b.signalWriter = func(messages.M3UA) (int, error) { count++; return 0, nil }
	sendDUNA(t, r.a, 7, 1, 100)
	sendDUNA(t, b, 7, 2, 101)
	r.advance(30 * time.Second)
	r.advance(time.Second)
	if count != 1 {
		t.Fatal("blocked reporter starved healthy audit")
	}
}

func TestCoveringDAVAEndsAvailabilityAuditButKeepsCongestion(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	sendDAVA(t, r.a, 7, 1, uint32(24)<<24)
	if len(r.e.ssnm.audits) != 0 {
		t.Fatal("covering DAVA retained availability audit")
	}
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendDUNA(t, r.a, 7, 1, 100)
	r.now = r.now.Add(time.Second)
	sendDAVA(t, r.a, 7, 1, uint32(24)<<24)
	if j := r.job(); j == nil || j.availability || !j.congestion || !j.due.Equal(r.now.Add(30*time.Second)) {
		t.Fatal("DAVA removed independent congestion or did not reset due")
	}
}

func TestAuditBatchRejectsLaterStaleBinding(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100, 101)
	var jobs []*destinationAuditJob
	for _, job := range r.e.ssnm.audits {
		jobs = append(jobs, job)
	}
	jobs[1].bindings = append([]queueCongestionBinding(nil), jobs[1].bindings...)
	jobs[1].bindings[0].version++
	if submitted, err := r.a.tryOptionalSSNMBatch(jobs); submitted || !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatal("later stale binding escaped batch fence", submitted, err)
	}
	if len(r.sent) != 0 {
		t.Fatal("stale destination submitted")
	}
}

func TestAuditRateStartsAtActualSubmission(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.auditing.MaxMessagesPerSecond = 1
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	count := 0
	r.a.signalWriter = func(messages.M3UA) (int, error) { count++; r.now = r.now.Add(5 * time.Second); return 0, nil }
	b.signalWriter = func(messages.M3UA) (int, error) { count++; return 0, nil }
	sendDUNA(t, r.a, 7, 1, 100)
	sendDUNA(t, b, 7, 2, 101)
	r.advance(30 * time.Second)
	r.e.runCongestionAutomation(r.now)
	if count != 1 {
		t.Fatal("stale pass timestamp permitted an audit burst", count)
	}
	r.advance(time.Second)
	if count != 2 {
		t.Fatal("actual submission rate slot never released", count)
	}
}

func TestAuditRateRoundsUpFractionalIntervals(t *testing.T) {
	if interval := auditRateInterval(3); interval != 333333334*time.Nanosecond {
		t.Fatalf("fractional rate interval rounded down: %v", interval)
	}
	if interval := auditRateInterval(int(^uint(0) >> 1)); interval <= 0 {
		t.Fatal("high rate produced non-positive interval")
	}
}

func TestMaskedAuditDoesNotAllocatePerPointCode(t *testing.T) {
	r := newAuditTestRig(t)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), uint32(1)<<24|100)
	if len(r.e.ssnm.audits) != 1 || r.job().key.destination.mask != 1 {
		t.Fatal("one masked range allocated per-point-code audit work")
	}
}

func TestEarlySSNMAuditSurvivesCommittedActivation(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Now()
	configureAutomationForTest(e, &now)
	a := attachActivatingSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	count := 0
	a.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.DestinationStateAudit); ok {
			count++
		}
		return 0, nil
	}
	sendSCON(t, a, 7, 1, congestionLevel(2), 100)
	now = now.Add(30 * time.Second)
	e.runCongestionAutomation(now)
	if count != 0 || a.State() != StateASPInactive {
		t.Fatal("early report authorized audit/activation")
	}
	if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	e.runCongestionAutomation(now)
	if count != 1 || a.State() != StateASPActive {
		t.Fatal("activation bookkeeping lost retained source audit", count, a.State())
	}
}

func TestAcceptedDuplicateRoutingContextsProduceOneScopedAudit(t *testing.T) {
	r := newAuditTestRig(t)
	if err := r.a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatalf("duplicate-RC input not admitted: %v", err)
	}
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("accepted Routing Context set was never audited", len(r.sent))
	}
	audit := r.sent[0].(*messages.DestinationStateAudit)
	if got := audit.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("automatic DAUD did not normalize logical scope: %v", got)
	}
	record := ssnmDestination(t, ssnmPartitionKnowledge(t, r.e.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")), 100, 0).Congestion
	if len(record.Scope.RoutingContexts) != 2 {
		t.Fatal("audit normalization rewrote received provenance")
	}
}
