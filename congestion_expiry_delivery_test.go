package m3ua

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

type epochBoundAuditSender struct {
	a             *Association
	entered       chan struct{}
	release       chan struct{}
	accepted      atomic.Int32
	acceptedEpoch atomic.Uint64
}

func (s *epochBoundAuditSender) SendMsg([]byte, sctp.SendOptions) (int, error) {
	panic("contextless optional send")
}
func (s *epochBoundAuditSender) SendMsgContext(ctx context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	if !opts.NoWait {
		panic("blocking optional send")
	}
	if _, err := messages.ParseDestinationStateAudit(frame); err != nil {
		return 0, err
	}
	close(s.entered)
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-s.release:
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.acceptedEpoch.Store(s.a.Epoch())
	s.accepted.Add(1)
	return len(frame), nil
}
func TestRestartSettlesOptionalAuditBeforeEpochCommit(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	job := r.job()
	sender := &epochBoundAuditSender{a: r.a, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(sender.release) }) })
	r.a.signalWriter = nil
	r.a.controlTransport = sender
	sent := make(chan struct{})
	go func() { _, _ = r.a.tryOptionalSSNM(job); close(sent) }()
	select {
	case <-sender.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("audit did not enter NoWait attempt")
	}
	reset := make(chan struct{})
	go func() { r.a.resetTransportCongestionEpoch(); close(reset) }()
	select {
	case <-reset:
	case <-time.After(5 * time.Second):
		release.Do(func() { close(sender.release) })
		t.Fatal("restart failed to cancel and settle the NoWait attempt")
	}
	release.Do(func() { close(sender.release) })
	select {
	case <-sent:
	case <-time.After(5 * time.Second):
		t.Fatal("optional completion remained blocked")
	}
	if sender.accepted.Load() != 0 && sender.acceptedEpoch.Load() != job.key.epoch {
		t.Fatal("old optional DAUD accepted after epoch change committed", sender.acceptedEpoch.Load(), job.key.epoch)
	}
	if job.live.Load() || len(r.e.ssnm.audits) != 0 {
		t.Fatal("restart retained old audit")
	}
}

func TestForgetRetiresValidityKnowledgeAndAudits(t *testing.T) {
	t.Run("mapped IPSP", func(t *testing.T) {
		e, a := newMappedCongestionAssociation(t)
		now := time.Now()
		e.ssnm.now = func() time.Time { return now }
		e.ssnm.validity = time.Second
		sendSCON(t, a, 10, 9, congestionLevel(2), 100)
		destination := MTPDestination{MTPRoute: "mapped", PointCode: 100}
		if before, _ := e.MTPDestinationStatus(destination); !before.Congested {
			t.Fatal("setup source missing")
		}
		transport := a.TransportCongestion()
		a.ForgetDestinations()
		if status, _ := e.MTPDestinationStatus(destination); status.Congested || status.ReceivedCongestionUnknown {
			t.Fatal("forget left validity-backed mapped congestion effective", status)
		}
		if a.TransportCongestion() != transport {
			t.Fatal("forget changed transport detector")
		}
		if e.ssnm.records != 0 {
			t.Fatal("forget retained canonical source records/charges", e.ssnm.records)
		}
		now = now.Add(time.Hour)
		e.runCongestionAutomation(now)
		if status, _ := e.MTPDestinationStatus(destination); status.Congested || status.ReceivedCongestionUnknown {
			t.Fatal("forgotten source returned at expiry", status)
		}
	})
	t.Run("ASP audit origins", func(t *testing.T) {
		r := newAuditTestRig(t)
		sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
		sendDUNA(t, r.a, 7, 1, 100)
		job := r.job()
		r.a.ForgetDestinations()
		if len(r.e.ssnm.audits) != 0 || job.live.Load() {
			t.Fatal("forget retained audit origins")
		}
		r.advance(time.Minute)
		if len(r.sent) != 0 {
			t.Fatal("forgotten knowledge continued periodic audit")
		}
	})
}
func TestMaximumReportExpiryFitsEmptyDefaultSubscription(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Now()
	e.ssnm.now = func() time.Time { return now }
	e.ssnm.validity = time.Second
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	points := make([]uint32, 1024)
	for i := range points {
		points[i] = uint32(100 + i)
	}
	sendSCON(t, a, 7, 1, congestionLevel(2), points...)
	before, sub, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	events, bytes, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("one legal report expiry overflowed empty default subscription", len(events), bytes)
	}
	if len(events) > 64 || bytes > DefaultSSNMSubscriptionQueueBytes {
		t.Fatal("expiry pass exceeded compact default bounds", len(events), bytes)
	}
	after := e.SSNMKnowledge()
	if reflect.DeepEqual(before.Partitions, after.Partitions) {
		t.Fatal("expiry did not change retained freshness")
	}
	expired := 0
	for _, p := range after.Partitions {
		for _, d := range p.Destinations {
			for _, source := range d.CongestionSources {
				if source.Freshness == CongestionExpired {
					expired++
				}
			}
		}
	}
	if expired != 1024 {
		t.Fatal("expiry lost statements", expired)
	}
}
