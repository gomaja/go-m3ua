package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPDownCommitsAssociationAndMembershipBeforeAck(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	asp.recvStream.Store(0)
	asp.as = registry
	applicationServer := registry.get(1)
	applicationServer.setASPState(asp, StateAspActive, time.Hour)

	var stateAtAck State
	var membershipAtAck State
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspDownAck); ok {
			stateAtAck = asp.State()
			applicationServer.mu.Lock()
			membershipAtAck = applicationServer.asps[asp]
			applicationServer.mu.Unlock()
		}
		return message.MarshalLen(), nil
	}

	if err := asp.handleAspDown(messages.NewAspDown(nil)); err != nil {
		t.Fatal(err)
	}
	if stateAtAck != StateAspDown {
		t.Errorf("association state at ASP Down Ack = %v, want ASP-DOWN", stateAtAck)
	}
	if membershipAtAck != StateAspDown {
		t.Errorf("AS membership at ASP Down Ack = %v, want ASP-DOWN", membershipAtAck)
	}
	select {
	case state := <-asp.StateChanges():
		t.Fatalf("Layer Management saw %v before the dispatcher published ASP-DOWN", state)
	default:
	}

	if _, err := asp.WriteSignal(distributionData(1, 1, "after Ack")); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("post-Ack direct DATA error = %v, want ErrNotEstablished", err)
	}
}

func TestASPDownRejectsEarlierActivePublicationAfterAck(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	asp.recvStream.Store(0)
	asp.as = registry
	applicationServer := registry.get(1)
	applicationServer.setASPState(asp, StateAspActive, time.Hour)

	if err := asp.handleAspDown(messages.NewAspDown(nil)); err != nil {
		t.Fatal(err)
	}

	// This is the head of an ASP-ACTIVE publication that stateChan handed to
	// monitor before the dispatcher received ASP Down, but monitor did not apply
	// until after the Ack.
	if err := asp.handlePublishedStateUpdate(StateAspActive); err != nil {
		t.Fatalf("stale ASP-ACTIVE publication: %v", err)
	}
	if got := asp.State(); got != StateAspDown {
		t.Fatalf("stale publication resurrected association state %v, want ASP-DOWN", got)
	}

	// This is the other possible interleaving: the old entry action finished
	// before ASP Down, but its registry update was delayed until afterwards.
	registry.aspStateChangedPublished(asp, StateAspActive)
	applicationServer.mu.Lock()
	membership := applicationServer.asps[asp]
	applicationServer.mu.Unlock()
	if membership != StateAspDown {
		t.Fatalf("stale publication resurrected AS membership %v, want ASP-DOWN", membership)
	}
	if active := applicationServer.activeASPs(); len(active) != 0 {
		t.Fatalf("stale publication made %d ASPs eligible after ASP Down Ack", len(active))
	}
}

func TestASPDownAckWaitsForAllApplicationServerTraffic(t *testing.T) {
	listener := &Listener{Config: NewServerConfig(
		&HeartbeatInfo{Enabled: false}, 1, 2, 0,
		params.TrafficModeLoadshare, 0, 0, []uint32{1, 2},
		params.ServiceIndSCCP, 0, 0, 1,
	)}
	listener.Config.CorrelationID = nil
	listener.as = newApplicationServers(time.Hour, listener.Config)

	asp, _ := addDistributionASP(t, listener, StateAspActive, 1, 2)
	observer, _ := addDistributionASP(t, listener, StateAspInactive, 1, 2)
	servers := map[uint32]*applicationServer{
		1: listener.as.get(1),
		2: listener.as.get(2),
	}
	for _, applicationServer := range servers {
		applicationServer.setTrafficMode(params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateAspActive, time.Hour)
		applicationServer.setASPState(observer, StateAspInactive, time.Hour)
	}

	var eventsMu sync.Mutex
	var events []string
	record := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	snapshot := func() []string {
		eventsMu.Lock()
		defer eventsMu.Unlock()
		return append([]string(nil), events...)
	}

	started := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{})}
	startedOnce := map[uint32]*sync.Once{1: {}, 2: {}}
	release := map[uint32]chan struct{}{1: make(chan struct{}), 2: make(chan struct{})}
	releaseOnce := map[uint32]*sync.Once{1: {}, 2: {}}
	releaseData := func(routingContext uint32) {
		releaseOnce[routingContext].Do(func() { close(release[routingContext]) })
	}
	t.Cleanup(func() {
		releaseData(1)
		releaseData(2)
	})
	ackSeen := make(chan struct{})
	var ackOnce sync.Once
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch message := message.(type) {
		case *messages.Data:
			routingContext := message.RoutingContext.RoutingContexts()[0]
			record(fmt.Sprintf("data-%d-start", routingContext))
			startedOnce[routingContext].Do(func() { close(started[routingContext]) })
			<-release[routingContext]
			record(fmt.Sprintf("data-%d-finish", routingContext))
		case *messages.AspDownAck:
			record("ack")
			ackOnce.Do(func() { close(ackSeen) })
		}
		return message.MarshalLen(), nil
	}
	observer.signalWriter = func(message messages.M3UA) (int, error) {
		if notification, ok := message.(*messages.Notify); ok {
			routingContext := notification.RoutingContext.RoutingContexts()[0]
			record(fmt.Sprintf("notify-%d", routingContext))
		}
		return message.MarshalLen(), nil
	}

	deliveryDone := make(map[uint32]chan error, len(servers))
	for routingContext := range servers {
		deliveryDone[routingContext] = make(chan error, 1)
		go func() {
			_, err := listener.DistributeData(distributionData(routingContext, uint8(routingContext), "in flight"))
			deliveryDone[routingContext] <- err
		}()
	}
	for routingContext := range servers {
		select {
		case <-started[routingContext]:
		case <-time.After(time.Second):
			t.Fatalf("Routing Context %d DATA never reached the ASP", routingContext)
		}
	}

	downDone := make(chan struct{})
	go func() {
		asp.handleSignals(context.Background(), messages.NewAspDown(nil))
		close(downDone)
	}()

	select {
	case <-ackSeen:
		t.Fatalf("ASP Down Ack escaped while DATA was blocked: events %v", snapshot())
	case <-time.After(50 * time.Millisecond):
	}
	for routingContext, applicationServer := range servers {
		applicationServer.mu.Lock()
		state := applicationServer.asps[asp]
		applicationServer.mu.Unlock()
		if state != StateAspDown {
			t.Errorf("Routing Context %d membership = %v before Ack, want ASP-DOWN", routingContext, state)
		}
		result, err := listener.DistributeData(distributionData(
			routingContext, uint8(routingContext), "while Down is draining"))
		if err != nil {
			t.Fatalf("Routing Context %d distribution during Down drain: %v", routingContext, err)
		}
		if !result.Queued || result.Delivered != 0 {
			t.Errorf("Routing Context %d distribution during Down drain = %+v, want queued with no delivery",
				routingContext, result)
		}
	}
	if len(asp.stateChan) != 0 {
		t.Fatalf("ASP state was published before Ack: %v", <-asp.stateChan)
	}
	for _, event := range snapshot() {
		if event == "notify-1" || event == "notify-2" {
			t.Fatalf("AS Notify preceded ASP Down Ack: events %v", snapshot())
		}
	}

	releaseData(1)
	if err := <-deliveryDone[1]; err != nil {
		t.Fatalf("Routing Context 1 delivery: %v", err)
	}
	select {
	case <-ackSeen:
		t.Fatalf("ASP Down Ack ignored blocked Routing Context 2: events %v", snapshot())
	case <-time.After(50 * time.Millisecond):
	}

	releaseData(2)
	if err := <-deliveryDone[2]; err != nil {
		t.Fatalf("Routing Context 2 delivery: %v", err)
	}
	select {
	case <-downDone:
	case <-time.After(time.Second):
		t.Fatal("ASP Down handling did not finish after all DATA completed")
	}

	gotEvents := snapshot()
	ackIndex := eventIndex(gotEvents, "ack")
	if ackIndex < 0 {
		t.Fatalf("no ASP Down Ack was sent: events %v", gotEvents)
	}
	for _, event := range []string{"data-1-finish", "data-2-finish"} {
		if index := eventIndex(gotEvents, event); index < 0 || index > ackIndex {
			t.Errorf("%s index = %d, Ack index = %d; DATA completed after Ack: %v",
				event, index, ackIndex, gotEvents)
		}
	}
	for _, event := range []string{"notify-1", "notify-2"} {
		if index := eventIndex(gotEvents, event); index < ackIndex {
			t.Errorf("%s index = %d, Ack index = %d; Notify preceded Ack: %v",
				event, index, ackIndex, gotEvents)
		}
	}
	select {
	case state := <-asp.stateChan:
		if state != StateAspDown {
			t.Fatalf("published state = %v, want ASP-DOWN", state)
		}
	default:
		t.Fatal("ASP-DOWN was not published after the Ack")
	}

	for routingContext := range servers {
		result, err := listener.DistributeData(distributionData(routingContext, uint8(routingContext), "after Ack"))
		if err != nil {
			t.Fatalf("Routing Context %d post-Ack distribution: %v", routingContext, err)
		}
		if !result.Queued || result.Delivered != 0 {
			t.Errorf("Routing Context %d post-Ack distribution = %+v, want queued with no delivery",
				routingContext, result)
		}
	}
	time.Sleep(25 * time.Millisecond)
	if got := snapshot(); len(got) != len(gotEvents) {
		t.Fatalf("DATA or control traffic reached the down ASP after Ack: before %v, after %v", gotEvents, got)
	}
}

func TestASPDownMarksActiveAndInactiveMembershipsDown(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
	asp.recvStream.Store(0)
	asp.as = registry
	active := registry.get(1)
	inactive := registry.get(2)
	active.setASPState(asp, StateAspActive, time.Hour)
	inactive.setASPState(asp, StateAspInactive, time.Hour)
	*sent = nil

	if err := asp.handleAspDown(messages.NewAspDown(nil)); err != nil {
		t.Fatal(err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Down Ack" {
		t.Fatalf("signals = %v, want [ASP Down Ack]", got)
	}
	for routingContext, applicationServer := range map[uint32]*applicationServer{1: active, 2: inactive} {
		applicationServer.mu.Lock()
		state := applicationServer.asps[asp]
		applicationServer.mu.Unlock()
		if state != StateAspDown {
			t.Errorf("Routing Context %d membership = %v, want ASP-DOWN", routingContext, state)
		}
	}
}

func eventIndex(events []string, target string) int {
	for index, event := range events {
		if event == target {
			return index
		}
	}
	return -1
}
