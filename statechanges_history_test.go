package m3ua

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// These tests use control-writer substitutes and never start a monitor or open
// an SCTP socket. The unbuffered publication reproduces the monitor hand-off.
func TestStateChangesKeepsAnOvertakenTransition(t *testing.T) {
	for _, overtake := range []bool{false, true} {
		t.Run(fmt.Sprintf("overtake=%v", overtake), func(t *testing.T) {
			cfg := newDoubleExchangeAssociationConfigForTest()
			cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
			cfg.ASPProcedures.ASPUp = ASPProcedureExplicit
			cfg.ASPProcedures.ASPActive = ASPProcedureExplicit
			c, _ := newDoubleExchangeIPSPWithConfigForTest(t, cfg)
			c.stateChan = make(chan State)
			states := c.StateChanges()
			c.setState(StateASPActive)
			c.stateEntered = true
			if err := c.handleStateUpdate(StateASPDown); err != nil {
				t.Fatal(err)
			}
			down := <-states
			if down != StateASPDown {
				t.Fatalf("initial withdrawal = %v, want ASP-DOWN", down)
			}
			release := make(chan struct{})
			finished := make(chan struct{})
			go func() {
				defer close(finished)
				c.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
				select {
				case <-release:
				case <-c.done:
					return
				}
				c.handleReceivedSignals(context.Background(), messages.NewAspActive(
					params.NewTrafficModeType(params.TrafficModeLoadshare),
					params.NewRoutingContext(22), nil), nil)
			}()
			t.Cleanup(func() { _ = c.Close(); <-finished })
			inactivePublication := <-c.stateChan
			if c.State() != StateASPInactive {
				t.Fatalf("Up Ack did not commit inactive: %v", c.State())
			}
			if overtake {
				close(release)
				deadline := time.Now().Add(5 * time.Second)
				for c.State() != StateASPActive {
					if time.Now().After(deadline) {
						t.Fatal("ASPAC did not commit ACTIVE")
					}
					runtime.Gosched()
				}
			}
			applied, err := c.applyPublishedStateUpdate(inactivePublication)
			if err != nil {
				t.Fatal(err)
			}
			if applied == overtake {
				t.Fatalf("inactive applied=%v overtake=%v", applied, overtake)
			}
			if !overtake {
				close(release)
			}
			activePublication := <-c.stateChan
			if err := c.handlePublishedStateUpdate(activePublication); err != nil {
				t.Fatal(err)
			}
			<-finished
			requireStateHistory(t, states, StateASPInactive, StateASPActive)
		})
	}
}

func TestStateChangesEarlySubscriberKeepsOvertakenHandshake(t *testing.T) {
	cfg := newDoubleExchangeAssociationConfigForTest()
	cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
	cfg.ASPProcedures.ASPActive = ASPProcedureExplicit
	var states <-chan State
	var calls int
	cfg.Subscribe = func(c *Association) {
		calls++
		if c.stateEntered {
			t.Error("Subscribe ran after the opening entry")
		}
		states = c.StateChanges()
	}
	c := newAssociation(RoleIPSP, snapshotAssociationConfig(cfg))
	c.signalWriter = func(message messages.M3UA) (int, error) { return message.MarshalLen(), nil }
	t.Cleanup(func() { _ = c.Close() })
	if c.stateEventSubscribed {
		t.Fatal("state delivery started before Subscribe")
	}
	c.subscribeBeforeHandshake()
	if calls != 1 || states == nil {
		t.Fatal("Subscribe did not install the state subscription")
	}
	if err := c.handleStateUpdate(StateASPDown); err != nil {
		t.Fatal(err)
	}
	requireStateHistory(t, states)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
		c.handleSignals(context.Background(), messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(22), nil))
	}()
	t.Cleanup(func() { _ = c.Close(); <-finished })
	inactive := <-c.stateChan
	if inactive != StateASPInactive {
		t.Fatalf("Up Ack publication = %v, want ASP-INACTIVE", inactive)
	}
	deadline := time.Now().Add(5 * time.Second)
	for c.State() != StateASPActive {
		if time.Now().After(deadline) {
			t.Fatal("peer activation did not overtake the opening INACTIVE entry")
		}
		runtime.Gosched()
	}
	if c.appliedState != StateASPDown {
		t.Fatalf("entry already advanced to %v", c.appliedState)
	}
	if applied, err := c.applyPublishedStateUpdate(inactive); err != nil || applied {
		t.Fatalf("overtaken entry: applied=%v err=%v", applied, err)
	}
	// Both handshake edges reach the early subscriber before even receiving
	// the ACTIVE publication; no later message or readiness event is needed.
	requireStateHistory(t, states, StateASPInactive, StateASPActive)
	if err := c.handlePublishedStateUpdate(<-c.stateChan); err != nil {
		t.Fatal(err)
	}
	<-finished
	requireStateHistory(t, states)
}

func TestStateChangesOvertakenHandlerHistory(t *testing.T) {
	for _, role := range []string{"ASP", "SGP", "IPSP single", "IPSP double"} {
		t.Run(role, func(t *testing.T) {
			c := newStateHistoryAssociation(t, role)
			states := c.StateChanges()
			if err := c.handleStateUpdate(StateASPDown); err != nil {
				t.Fatal(err)
			}
			// Discard the opening entry on the old implementation so this test
			// isolates committed transitions lost to the publication guard.
			for len(states) > 0 {
				<-states
			}
			up := messages.M3UA(messages.NewAspUp(nil, nil))
			active := messages.M3UA(messages.NewAspActive(
				params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil))
			if role == "ASP" || role == "IPSP single" {
				up = messages.NewAspUpAck(nil, nil)
			}
			if role == "ASP" {
				active = messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil)
			}
			c.handleSignals(context.Background(), up)
			c.handleSignals(context.Background(), active)
			if c.State() != StateASPActive {
				t.Fatalf("handlers did not commit ASP-ACTIVE: %v", c.State())
			}
			if applied, err := c.applyPublishedStateUpdate(<-c.stateChan); err != nil || applied {
				t.Fatalf("stale entry: applied=%v err=%v", applied, err)
			}
			if err := c.handlePublishedStateUpdate(<-c.stateChan); err != nil {
				t.Fatal(err)
			}
			requireStateHistory(t, states, StateASPInactive, StateASPActive)
			if role == "IPSP double" {
				c.commitLocalIPSPState(StateASPActive)
				c.commitLocalIPSPState(StateASPDown)
				requireStateHistory(t, states)
				if c.IPSPState().TrafficToPeer != StateASPActive {
					t.Fatal("local IPSP transitions changed TrafficToPeer")
				}
			}
		})
	}
}

func TestStateChangesPendingQueueSharesCapacity(t *testing.T) {
	for _, delivered := range []int{0, 8, 16} {
		t.Run(fmt.Sprint(delivered), func(t *testing.T) {
			c := newIndicationAssociation(t)
			states := c.StateChanges()
			for i := range cap(states) {
				state := StateASPInactive
				if i%2 != 0 {
					state = StateASPActive
				}
				c.commitState(state)
				if i < delivered {
					if _, err := c.applyPublishedStateUpdate(state); err != nil {
						t.Fatal(err)
					}
				}
			}
			if c.indicationOverflow.Load() {
				t.Fatal("queue overflowed before the sixteenth committed edge")
			}
			c.commitState(StateASPInactive)
			select {
			case <-c.Done():
			case <-time.After(time.Second):
				t.Fatal("seventeenth undrained committed edge did not close association")
			}
			if !errors.Is(c.Err(), ErrIndicationQueueFull) {
				t.Fatalf("overflow cause = %v", c.Err())
			}
		})
	}
}

func TestStateChangesUnchangedPublicationDrainsCommittedHistory(t *testing.T) {
	c := newStateHistoryAssociation(t, "SGP")
	c.setState(StateASPActive)
	c.stateEntered = true
	states := c.StateChanges()
	c.signalWriter = func(messages.M3UA) (int, error) { return 0, errors.New("control write failed") }
	// The handler commits INACTIVE before writing Up Ack. A failed write
	// makes the dispatcher publish unchanged, but does not undo that commit.
	c.handleSignals(context.Background(), messages.NewAspUp(nil, nil))
	publication := <-c.stateChan
	if publication != stateUnchanged {
		t.Fatalf("publication = %v, want unchanged", publication)
	}
	if err := c.handlePublishedStateUpdate(publication); err != nil {
		t.Fatal(err)
	}
	requireStateHistory(t, states, StateASPInactive)
	if c.appliedState != StateASPActive {
		t.Fatalf("unchanged publication ran an entry: %v", c.appliedState)
	}
}

func TestStateChangesEventsRemainHistoryWhenStateAdvances(t *testing.T) {
	_, _, c, _ := distributionFixtureForContexts(t, params.TrafficModeLoadshare, []uint32{1}, nil)
	c.setState(StateASPDown)
	states := c.StateChanges()
	c.commitState(StateASPInactive)
	c.commitPeerRoutingContextsActive([]uint32{1})
	if applied, err := c.applyPublishedStateUpdate(StateASPInactive); err != nil || applied {
		t.Fatalf("stale entry: applied=%v err=%v", applied, err)
	}
	if len(states) != 2 {
		t.Fatalf("queued history has %d edges, want 2", len(states))
	}
	if first := <-states; first != StateASPInactive {
		t.Fatalf("first historical event = %v, want ASP-INACTIVE", first)
	}
	// The event describes a committed edge, not a promise that a query or
	// traffic gate still reflects it. The current peer activation must win.
	if c.State() != StateASPActive || c.ApplicationServerState(1) != ASActive || !c.activeForRoutingContext(1) {
		t.Fatal("historical INACTIVE event rolled back the current state or AS traffic gate")
	}
	if err := c.handlePublishedStateUpdate(StateASPActive); err != nil {
		t.Fatal(err)
	}
	requireStateHistory(t, states, StateASPActive)
}

func TestStateChangesCommittedHistory(t *testing.T) {
	for _, role := range []string{"ASP", "SGP", "IPSP single", "IPSP double"} {
		for _, path := range []string{"dispatcher", "peer down", "override", "peer activation", "direct apply", "restart"} {
			t.Run(role+"/"+path, func(t *testing.T) {
				c := newStateHistoryAssociation(t, role)
				c.setState(StateASPActive)
				c.stateEntered = true
				states := c.StateChanges()
				first, last := StateASPInactive, StateASPDown
				switch path {
				case "dispatcher":
					c.sendState(first)
					c.sendState(last)
				case "peer down":
					first, last = StateASPDown, StateASPInactive
					c.commitPeerDownState()
					c.commitState(last)
				case "override":
					c.sendOverrideInactiveState()
					c.commitState(last)
				case "peer activation":
					c.setState(StateASPInactive)
					first = StateASPActive
					c.commitPeerRoutingContextsActive(nil)
					c.commitState(last)
				case "direct apply":
					c.commitState(first)
					if err := c.handleStateUpdate(last); err != nil {
						t.Fatal(err)
					}
				case "restart":
					first, last = StateASPDown, StateASPInactive
					c.sendRestartState()
					c.commitState(last)
				}
				if path != "direct apply" {
					requireStateHistory(t, states)
					if applied, err := c.applyPublishedStateUpdate(first); err != nil || applied {
						t.Fatalf("stale publication: applied=%v err=%v", applied, err)
					}
					// The history must be delivered even before the current
					// publication is applied, without running the stale entry.
					if c.appliedState != StateASPActive && path != "peer activation" {
						t.Fatalf("stale entry changed appliedState to %v", c.appliedState)
					}
				}
				requireStateHistory(t, states, first, last)
				// A publication, a restatement, and an unchanged token cannot
				// report the committed edges a second time.
				if err := c.handlePublishedStateUpdate(last); err != nil {
					t.Fatal(err)
				}
				c.sendState(last)
				if err := c.handlePublishedStateUpdate(last); err != nil {
					t.Fatal(err)
				}
				if err := c.handleStateUpdate(stateUnchanged); err != nil {
					t.Fatal(err)
				}
				requireStateHistory(t, states)
			})
		}
	}
}

func TestStateChangesOpeningEntryAndSubscription(t *testing.T) {
	c := newStateHistoryAssociation(t, "SGP")
	states := c.StateChanges()
	if err := c.handleStateUpdate(StateASPDown); err != nil {
		t.Fatal(err)
	}
	requireStateHistory(t, states)
	if !c.stateEntered {
		t.Fatal("opening ASP-DOWN entry was skipped")
	}
	// A decision made before subscription must not be replayed when its
	// publication arrives after subscription.
	unsubscribed := newAssociation(RoleSGP, c.cfg)
	t.Cleanup(func() { _ = unsubscribed.Close() })
	for range 20 {
		unsubscribed.commitState(StateASPInactive)
		unsubscribed.commitState(StateASPDown)
	}
	unsubscribed.commitState(StateASPInactive)
	future := unsubscribed.StateChanges()
	if err := unsubscribed.handlePublishedStateUpdate(StateASPInactive); err != nil {
		t.Fatal(err)
	}
	requireStateHistory(t, future)
	if unsubscribed.indicationOverflow.Load() {
		t.Fatal("unsubscribed commits overflowed")
	}
}

func TestStateChangesPendingHistorySurvivesTeardown(t *testing.T) {
	t.Run("already committed Down", func(t *testing.T) {
		c := newIndicationAssociation(t)
		states := c.StateChanges()
		c.commitState(StateASPInactive)
		c.commitState(StateASPDown)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		requireStateHistory(t, states, StateASPInactive, StateASPDown)
	})
	for _, cause := range []error{ErrAssociationClosed, ErrHeartbeatExpired} {
		t.Run(cause.Error(), func(t *testing.T) {
			c := newIndicationAssociation(t)
			states := c.StateChanges()
			c.commitState(StateASPInactive)
			c.commitState(StateASPActive)
			release := installBlockingRelease(t, c)
			historyAtRelease := make(chan int, 1)
			hold := c.transportCloser
			c.transportCloser = func() error {
				historyAtRelease <- len(states)
				return hold()
			}
			finished := make(chan error, 1)
			c.muStateEvent.Lock()
			go func() { finished <- c.closeWith(cause) }()
			deadline := time.Now().Add(5 * time.Second)
			for c.Err() == nil {
				if time.Now().After(deadline) {
					c.muStateEvent.Unlock()
					t.Fatal("teardown did not start")
				}
				runtime.Gosched()
			}
			// A drain owns this lock across its done check and delivery. The
			// beginning of teardown must wait for that delivery to finish.
			select {
			case <-c.Done():
				c.muStateEvent.Unlock()
				t.Fatal("Done closed while state-history delivery held its lock")
			case <-time.After(10 * time.Millisecond):
			}
			// Teardown is waiting on the delivery lock. Let an entry pass its
			// outer done check and wait there too, exercising the inner guard
			// if teardown wins the lock first.
			applied := make(chan error, 1)
			go func() {
				_, err := c.applyPublishedStateUpdate(StateASPActive)
				applied <- err
			}()
			for c.muState.TryLock() {
				c.muState.Unlock()
				if time.Now().After(deadline) {
					c.muStateEvent.Unlock()
					t.Fatal("publication did not acquire the state lock")
				}
				runtime.Gosched()
			}
			c.muStateEvent.Unlock()
			<-release.reached
			if err := <-applied; err != nil {
				t.Fatal(err)
			}
			if before := <-historyAtRelease; len(states) != before {
				t.Fatalf("pending history escaped during release: before=%d now=%d", before, len(states))
			}
			if _, err := c.applyPublishedStateUpdate(StateASPActive); err != nil {
				t.Fatal(err)
			}
			release.open()
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
			requireStateHistory(t, states, StateASPInactive, StateASPActive, StateASPDown)
			if _, ok := <-states; ok {
				t.Fatal("state stream did not close")
			}
		})
	}
}

func newStateHistoryAssociation(t *testing.T, role string) *Association {
	t.Helper()
	var c *Association
	switch role {
	case "IPSP double":
		c, _ = newDoubleExchangeIPSPForTest(t)
	case "IPSP single":
		c, _ = newTestConn(t, StateASPDown, RoleIPSP)
		c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	case "ASP":
		c, _ = newTestConn(t, StateASPDown, RoleASP)
	default:
		c, _ = newTestConn(t, StateASPDown, RoleSGP)
	}
	c.cfg.ASPProcedures = &ASPProcedurePolicy{
		ASPUp: ASPProcedureExplicit, ASPActive: ASPProcedureExplicit,
		ASPDown: ASPProcedureExplicit, ASPInactive: ASPProcedureExplicit,
	}
	return c
}

func requireStateHistory(t *testing.T, states <-chan State, want ...State) {
	t.Helper()
	var got []State
	for len(states) > 0 {
		got = append(got, <-states)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("state history = %v, want %v", got, want)
	}
}
