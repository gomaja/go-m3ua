package m3ua

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOptionalPublicationCannotWaitForASRegistry(t *testing.T) {
	c, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	e := c.endpoint
	c.as = e.as
	if c.as == nil {
		t.Fatal("single-exchange IPSP has no AS registry")
	}
	key := c.configuredLocalASKeysForStatus()[0]
	server := c.as.get(key)
	c.noteRoutingContextsActive([]uint32{key.RoutingContext})
	server.setASPState(c, StateASPActive, time.Hour)
	var writes atomic.Int64
	c.signalWriter = func(message messages.M3UA) (int, error) {
		writes.Add(1)
		return message.MarshalLen(), nil
	}
	scope := localCongestionScope(key)
	e.enqueueCongestionPublication(c, scope, c.queueCongestionBinding(key, scope), 2, 0, false, nationalPublication(3), 1, false)
	if len(e.CongestionPublications()) != 1 {
		t.Fatal("fixture did not reserve its onset before registry contention")
	}
	pass := func() *optionalPublicationPass {
		return &optionalPublicationPass{used: make(map[AssociationID]bool), remaining: 64, bytes: 64 << 10}
	}
	now := time.Now()
	c.as.mu.Lock()
	var once sync.Once
	unlock := func() { once.Do(c.as.mu.Unlock) }
	defer unlock()
	done := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		e.runLocalCongestionPublications(now, pass())
		close(done)
	}()
	<-started
	blocked := false
	select {
	case <-done:
	case <-time.After(time.Second):
		blocked = true
	}
	unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("optional pass did not finish after releasing registry")
	}
	if blocked {
		t.Fatal("optional publication pass waited for AS registry mutex")
	}
	if writes.Load() != 0 {
		t.Fatal("optional publication submitted while registry was unavailable")
	}
	e.runLocalCongestionPublications(now, pass())
	if writes.Load() != 1 {
		t.Fatal("registry contention discarded the current pending onset")
	}
}

func TestPermanentOptionalSCONFailureRetiresScope(t *testing.T) {
	r := publicationRig(t, 2)
	r.a.signalWriter = nil
	_ = r.a.ManagementIndications()
	transport := &optionalAuditSender{err: ErrAssociationClosed}
	r.a.controlTransport = transport
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	if transport.attempts != 1 || !transport.opts.NoWait {
		t.Fatal("permanent-failure fixture did not reach one NoWait attempt")
	}
	if len(r.e.CongestionPublications()) != 0 {
		t.Error("permanent NoWait failure retained local publication reservation")
	}
	r.advance(time.Second)
	if transport.attempts != 1 {
		t.Errorf("permanent local publication failure retried: attempts=%d", transport.attempts)
	}
	if r.e.CongestionPublicationCounters().Submitted != 0 {
		t.Fatal("permanent local publication failure reported successful submission")
	}
	requirePermanentPublicationObservation(t, r.a, CongestionPublicationLocalSCON)
}

func TestPermanentOptionalDAUDFailureRetiresScope(t *testing.T) {
	r := publicationRig(t, 2)
	r.a.signalWriter = nil
	_ = r.a.ManagementIndications()
	transport := &optionalAuditSender{err: ErrAssociationClosed}
	r.a.controlTransport = transport
	_, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	sendDUNA(t, r.a, 7, 1, 100)
	r.advance(30 * time.Second)
	if transport.attempts != 1 || !transport.opts.NoWait {
		t.Fatal("permanent-audit fixture did not reach one NoWait attempt")
	}
	if r.job() != nil {
		t.Error("permanent NoWait failure retained current destination audit")
	}
	r.advance(time.Second)
	if transport.attempts != 1 {
		t.Errorf("permanent destination audit failure retried: attempts=%d", transport.attempts)
	}
	if r.e.CongestionPublicationCounters().Submitted != 0 {
		t.Error("failed destination audit counted as successful submission")
	}
	requirePermanentPublicationObservation(t, r.a, CongestionPublicationDAUD)
	events, _, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("bounded permanent failure outcomes lost subscription continuity")
	}
	for _, event := range events {
		if event.ReportSet && event.Report.Source == SSNMLocalReport && event.Report.Kind == SSNMDestinationStateAuditReport {
			t.Fatal("permanent NoWait failure manufactured a successful local DAUD report")
		}
	}
}

func requirePermanentPublicationObservation(t *testing.T, c *Association, kind CongestionPublicationKind) {
	t.Helper()
	failures := 0
	indications := c.ManagementIndications()
drain:
	for {
		select {
		case ind, ok := <-indications:
			if !ok {
				break drain
			}
			state := ind.CongestionPublication
			if state == nil || state.Kind != kind || !errors.Is(ind.Cause, ErrAssociationClosed) {
				continue
			}
			failures++
			if state.Pending || state.LastError == "" {
				t.Errorf("retired permanent failure reported pending or omitted failure: %+v", state)
			}
		default:
			break drain
		}
	}
	if failures != 1 {
		t.Errorf("permanent failure management observations=%d, want one", failures)
	}
}

func TestOptionalAuditSurvivesSecondaryASActivation(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.a.cfg.TAck = time.Hour
	if err := r.a.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	if r.a.State() != StateASPActive || !r.a.routingContextAcked(1) || r.a.routingContextAcked(2) {
		t.Fatal("fixture did not retain first AS while withdrawing second AS")
	}
	if err := r.a.ActivateRoutingContexts(2); err != nil {
		t.Fatal(err)
	}
	sendSCON(t, r.a, 7, 2, congestionLevel(2), 100)
	if len(r.e.ssnm.audits) != 1 {
		t.Fatal("early second-AS report did not retain one audit")
	}
	audits := func() int {
		count := 0
		for _, message := range r.sent {
			if _, ok := message.(*messages.DestinationStateAudit); ok {
				count++
			}
		}
		return count
	}
	r.advance(30 * time.Second)
	if audits() != 0 || r.a.routingContextAcked(2) {
		t.Fatal("pending second-AS activation authorized an audit")
	}
	if len(r.e.ssnm.audits) != 1 {
		t.Error("pending second-AS activation lost its retained audit")
	}
	if err := r.a.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	r.e.runCongestionAutomation(r.now)
	if audits() != 1 {
		t.Fatalf("committed second-AS activation did not resume retained audit: sent=%d", audits())
	}
}
