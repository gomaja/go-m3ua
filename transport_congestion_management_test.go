package m3ua

import (
	"context"
	"github.com/gomaja/go-sctp"
	"sync"
	"testing"
	"time"
)

func TestLocalCongestionReportsUseImplementationNames(t *testing.T) {
	for _, tc := range []struct {
		kind ManagementIndicationKind
		name string
	}{
		{ManagementDataQueueCongestion, "DATA-QUEUE-CONGESTION"},
		{ManagementTransportCongestion, "TRANSPORT-CONGESTION"},
	} {
		if got := tc.kind.String(); got != tc.name {
			t.Errorf("local congestion report name=%q want=%q", got, tc.name)
		}
	}
}

func TestCongestionManagementSourcesRemainIndependent(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	c, sent := overflowAssociation(t)
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	management := c.ManagementIndications()
	producer := &TransportCongestionState{Association: c.ID(), Epoch: c.Epoch(), Level: 2, Known: true}
	c.notifyManagement(&ManagementIndication{Kind: ManagementTransportCongestion, TransportCongestion: producer})
	producer.Level = 1
	select {
	case indication := <-management:
		if indication.TransportCongestion == nil || indication.TransportCongestion.Level != 2 {
			t.Fatalf("transport management retained the producer's mutable snapshot: %+v", indication)
		}
		indication.TransportCongestion.Level = 3
		if producer.Level != 1 {
			t.Fatal("delivered transport management snapshot changed the producer")
		}
	default:
		t.Fatal("missing transport management snapshot")
	}
	now := time.Unix(100, 0)
	charged := 95
	e.transport.now = func() time.Time { return now }
	e.transport.readStatus = nil
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) {
		return &sctp.SendBacklog{Charged: charged, Limit: 100}, nil
	}
	job := transportCongestionJob{c, c.ID(), c.Epoch()}
	e.transport.sample(e, job)
	deliver(t, c, 1, inboundData(1, "discarded"))
	for range 3 {
		now = now.Add(100 * time.Millisecond)
		e.transport.sample(e, job)
	}
	queueOnset, transportOnset := false, false
	for len(management) > 0 {
		ind := <-management
		switch ind.Kind {
		case ManagementDataQueueCongestion:
			if ind.TransportCongestion != nil || ind.DataQueueCongestion == nil {
				t.Fatalf("queue indication mixes sources: %+v", ind)
			}
			queueOnset = ind.DataQueueCongestion.Congested
		case ManagementTransportCongestion:
			if ind.DataQueueCongestion != nil || ind.TransportCongestion == nil {
				t.Fatalf("transport indication mixes sources: %+v", ind)
			}
			if ind.TransportCongestion.Level == 3 {
				transportOnset = true
				ind.TransportCongestion.Level = 0
				if c.TransportCongestion().Level != 3 {
					t.Fatal("delivered management snapshot aliases detector state")
				}
			}
		}
	}
	if !queueOnset || !transportOnset {
		t.Fatalf("management onset queue=%v transport=%v", queueOnset, transportOnset)
	}
	charged = 0
	for range 11 {
		now = now.Add(100 * time.Millisecond)
		e.transport.sample(e, job)
	}
	if c.TransportCongestion().Level != 0 || !c.DataQueueStats().Congested {
		t.Fatal("transport recovery ended inbound queue congestion")
	}
	for len(management) > 0 {
		<-management
	}
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(func() bool { return !c.DataQueueStats().Congested }, 2*time.Second) {
		t.Fatal("queue did not recover")
	}
	queueEnd := false
	for len(management) > 0 {
		ind := <-management
		if ind.Kind == ManagementDataQueueCongestion && ind.DataQueueCongestion != nil && !ind.DataQueueCongestion.Congested {
			queueEnd = true
		}
	}
	if !queueEnd || c.TransportCongestion().Level != 0 {
		t.Fatal("queue recovery changed or failed to report its independent state")
	}
	if len(*sent) != 0 {
		t.Fatal("transport detector originated SCON")
	}
}

func TestQueueCongestionConsumerFiltersManagementSources(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	c, _ := overflowAssociation(t)
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	c.ManagementIndications()
	e.transport.readStatus = nil
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return &sctp.SendBacklog{Charged: 0, Limit: 100}, nil }
	e.transport.sample(e, transportCongestionJob{c, c.ID(), c.Epoch()})
	deliver(t, c, 1, inboundData(1, "discarded"))
	if !nextQueueCongestion(t, c).Congested {
		t.Fatal("missing queue onset after transport observation")
	}
}

func TestTransportCongestionWaitsForSubscriptionReturn(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	e.transport.readStatus = nil
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return &sctp.SendBacklog{Charged: 0, Limit: 100}, nil }
	c.sctpConn = &sctp.Conn{}
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(func() {
		unblock()
		<-done
		e.cancel(context.Canceled)
		e.transport.workers.Wait()
		c.sctpConn = nil
		_ = e.Close()
	})
	c.cfg.Subscribe = func(*Association) { close(entered); <-release }
	go func() { c.subscribeBeforeHandshake(); close(done) }()
	<-entered
	if jobs := e.transport.snapshot(e); len(jobs) != 0 {
		t.Errorf("sampler admitted %d associations before Subscribe returned", len(jobs))
	}
	unblock()
	<-done
	if jobs := e.transport.snapshot(e); len(jobs) != 1 {
		t.Fatalf("sampler did not admit the subscribed association: %d", len(jobs))
	}
}
