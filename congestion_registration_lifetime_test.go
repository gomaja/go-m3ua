package m3ua

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func newDynamicValidityAssociation(t *testing.T, auditing bool) (*Endpoint, *Association, ASKey, *time.Time) {
	t.Helper()
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	if auditing {
		e.ssnm.auditing = &DestinationAuditConfig{Period: time.Minute, MaxPending: 16, MaxMessagesPerSecond: 10}
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 8)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	key := *staticASKey(7, 7)
	a.addDynamicASKey(key, testRoutingKey(7, 100, params.ServiceIndSCCP), false)
	a.noteRoutingContextsActive([]uint32{7, 8})
	a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	return e, a, key, &now
}

func TestDynamicDeregistrationRetiresValidityHistoryBeforeRoutingContextReuse(t *testing.T) {
	for _, expired := range []bool{false, true} {
		name := "fresh"
		if expired {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			e, a, key, now := newDynamicValidityAssociation(t, false)
			sendSCON(t, a, 7, 7, congestionLevel(2), 100)
			sendSCON(t, a, 7, 8, congestionLevel(1), 100)
			payload := &params.ProtocolDataPayload{DestinationPointCode: 100, MessagePriority: 0}
			if err := a.validateMethodData(key, payload); err == nil {
				t.Fatal("setup did not retain RC7 congestion")
			}
			if expired {
				*now = now.Add(10 * time.Second)
				e.runCongestionAutomation(*now)
			}
			beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
			a.removeRequesterRoutingKeyVersion(7, a.dynamicASKeyVersionFor(7, false))
			if _, held := a.dynamicASKey(7, false); held {
				t.Fatal("dynamic registration not removed")
			}
			snapshot := e.SSNMKnowledge()
			if len(snapshot.Partitions) != 1 {
				t.Fatalf("RC8 must preserve standalone partition: %+v", snapshot.Partitions)
			}
			survivor := false
			for _, destination := range snapshot.Partitions[0].Destinations {
				for _, source := range destination.CongestionSources {
					if source.AS == key {
						t.Errorf("retired registration retained authoritative validity source: %+v", source)
					}
					survivor = survivor || source.AS.RoutingContext == 8
				}
			}
			if !survivor {
				t.Error("retirement erased surviving RC8 source")
			}
			if e.ssnm.records >= beforeRecords || e.ssnm.bytes >= beforeBytes {
				t.Errorf("retirement did not release source charges: records %d -> %d, bytes %d -> %d", beforeRecords, e.ssnm.records, beforeBytes, e.ssnm.bytes)
			}
			a.addDynamicASKey(key, testRoutingKey(7, 100, params.ServiceIndSCCP), false)
			a.noteRoutingContextsActive([]uint32{7, 8})
			a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
			a.notifyASPRouteStateChanged()
			if err := a.validateMethodData(key, payload); err != nil {
				t.Errorf("new registration inherited retired congestion: %v", err)
			}
			sendSCON(t, a, 7, 7, congestionLevel(3), 100)
			if err := a.validateMethodData(key, payload); err == nil {
				t.Error("new registration failed to accept a fresh congestion report")
			}
		})
	}
}

type cancellationJoinedAuditSender struct {
	entered   chan struct{}
	cancelled chan struct{}
	release   chan struct{}
	once      sync.Once
	attempts  atomic.Int32
}

func (s *cancellationJoinedAuditSender) SendMsg([]byte, sctp.SendOptions) (int, error) {
	return 0, errors.New("unexpected contextless send")
}
func (s *cancellationJoinedAuditSender) SendMsgContext(ctx context.Context, _ []byte, options sctp.SendOptions) (int, error) {
	if s.attempts.Add(1) != 1 {
		return 0, errors.New("stale optional audit reached transport")
	}
	if !options.NoWait {
		return 0, errors.New("audit did not request NoWait")
	}
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.cancelled)
	<-s.release
	return 0, ctx.Err()
}

func TestLateDeregistrationJoinsCancelledOptionalAuditWithoutLockCycle(t *testing.T) {
	e, a, _, _ := newDynamicValidityAssociation(t, true)
	sendSCON(t, a, 7, 7, congestionLevel(2), 100)
	var job *destinationAuditJob
	for _, candidate := range e.ssnm.audits {
		job = candidate
	}
	if job == nil {
		t.Fatal("congestion did not schedule audit")
	}
	sender := &cancellationJoinedAuditSender{entered: make(chan struct{}), cancelled: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(sender.release) }) })
	a.signalWriter = nil
	a.controlTransport = sender
	sent := make(chan error, 1)
	go func() { _, err := a.tryOptionalSSNM(job); sent <- err }()
	select {
	case <-sender.entered:
	case <-time.After(time.Second):
		t.Fatal("optional audit did not enter transport")
	}
	a.rkmCorrelationMu.Lock()
	a.rkmUnresolvedDeregistrationRCs = map[uint32]uint64{7: a.dynamicASKeyVersionFor(7, false)}
	a.rkmCorrelationMu.Unlock()
	response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(params.NewDeregResultPayload(params.NewRoutingContext(7), params.NewDeregistrationStatus(params.SuccessfullyDeregistered))))
	handled := make(chan error, 1)
	go func() { handled <- a.handleDeregistrationResponse(response) }()
	select {
	case <-sender.cancelled:
	case <-time.After(time.Second):
		t.Fatal("retirement did not cancel in-flight optional audit")
	}
	if a.rkmCorrelationMu.TryLock() {
		a.rkmCorrelationMu.Unlock()
		t.Error("late-response correlation fence released before submission completion")
	}
	if a.rkmLifecycleMu.TryLock() {
		a.rkmLifecycleMu.Unlock()
		t.Error("registration lifetime fence released before submission completion")
	}
	if _, held := a.dynamicASKey(7, false); !held {
		t.Error("registration removed before cancelled transport completed")
	}
	select {
	case err := <-handled:
		t.Fatalf("late response completed before transport completion: %v", err)
	default:
	}
	release.Do(func() { close(sender.release) })
	select {
	case err := <-sent:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("optional audit result = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("optional audit completion blocked on retirement locks")
	}
	select {
	case err := <-handled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("late DEREG did not finish after optional audit completion")
	}
	if _, held := a.dynamicASKey(7, false); held {
		t.Error("successful late DEREG retained registration")
	}
	if submitted, _ := a.tryOptionalSSNM(job); submitted {
		t.Error("retired registration submitted stale optional audit")
	}
	if sender.attempts.Load() != 1 {
		t.Error("stale optional audit reached transport")
	}
}
