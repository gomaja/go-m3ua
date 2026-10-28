package m3ua

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Use the production constructor without starting a monitor or opening SCTP.
func newIndicationAssociation(t *testing.T) *Association {
	t.Helper()
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	association := newAssociation(RoleSGP, config)
	association.setState(StateASPActive)
	t.Cleanup(func() { _ = association.Close() })
	return association
}

func TestIndicationsAreDiscardedUntilSubscription(t *testing.T) {
	for _, test := range []struct {
		name       string
		states     bool
		management bool
	}{
		{"neither", false, false},
		{"state only", true, false},
		{"management only", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			association := newIndicationAssociation(t)
			var states <-chan State
			var management <-chan *ManagementIndication
			if test.states {
				states = association.StateChanges()
			}
			if test.management {
				management = association.ManagementIndications()
			}
			for range 140 {
				association.notifyStateChange(StateASPInactive)
				association.notifyManagement(&ManagementIndication{Kind: ManagementNotify})
				if states != nil {
					select {
					case state, ok := <-states:
						if !ok || state != StateASPInactive {
							t.Fatalf("state indication = %v (open %v), want ASP-INACTIVE", state, ok)
						}
					default:
						t.Fatal("subscribed state indication was not queued")
					}
				}
				if management != nil {
					select {
					case indication, ok := <-management:
						if !ok || indication.Kind != ManagementNotify {
							t.Fatalf("management indication = %+v (open %v), want M-NOTIFY", indication, ok)
						}
					default:
						t.Fatal("subscribed management indication was not queued")
					}
				}
			}
			if association.indicationOverflow.Load() {
				t.Error("an unsubscribed indication stream triggered overflow")
			}
			if got := len(association.stateEventChan); got != 0 {
				t.Errorf("queued state indications = %d, want 0", got)
			}
			if got := len(association.mgmtChan); got != 0 {
				t.Errorf("queued management indications = %d, want 0", got)
			}
			select {
			case <-association.Done():
				t.Fatalf("association ended without an unread subscription: %v", association.Err())
			default:
			}
			if got := association.State(); got != StateASPActive {
				t.Errorf("State = %v, want ASP-ACTIVE", got)
			}
		})
	}
}

func TestIndicationSubscriptionStartsWithFutureEvents(t *testing.T) {
	association := newIndicationAssociation(t)
	association.notifyStateChange(StateASPDown)
	association.notifyManagement(&ManagementIndication{Kind: ManagementError})
	states := association.StateChanges()
	management := association.ManagementIndications()
	if states == nil || states != association.StateChanges() || cap(states) != 16 {
		t.Fatal("StateChanges must return the same non-nil, 16-slot channel")
	}
	if management == nil || management != association.ManagementIndications() || cap(management) != 64 {
		t.Fatal("ManagementIndications must return the same non-nil, 64-slot channel")
	}
	if len(states) != 0 || len(management) != 0 {
		t.Fatalf("subscription replayed earlier indications: states=%d management=%d", len(states), len(management))
	}
	if got := association.State(); got != StateASPActive {
		t.Fatalf("current State = %v, want ASP-ACTIVE", got)
	}
	association.notifyStateChange(StateASPInactive)
	association.notifyManagement(&ManagementIndication{Kind: ManagementNotify, Description: "after subscription"})
	if got := <-states; got != StateASPInactive {
		t.Errorf("first subscribed state = %v, want ASP-INACTIVE", got)
	}
	if got := <-management; got.Kind != ManagementNotify || got.Description != "after subscription" {
		t.Errorf("first subscribed management indication = %+v", got)
	}
}

func TestIndicationSubscriptionAfterCloseReturnsClosedChannels(t *testing.T) {
	association := newIndicationAssociation(t)
	if err := association.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case state, ok := <-association.StateChanges():
		if ok {
			t.Errorf("late state subscription replayed %v", state)
		}
	default:
		t.Error("late state subscription returned an open channel")
	}
	select {
	case indication, ok := <-association.ManagementIndications():
		if ok {
			t.Errorf("late management subscription replayed %+v", indication)
		}
	default:
		t.Error("late management subscription returned an open channel")
	}
}

func TestIndicationSubscriptionDuringTransportRelease(t *testing.T) {
	association := newIndicationAssociation(t)
	release := installBlockingRelease(t, association)
	finished := make(chan error, 1)
	go func() { finished <- association.Close() }()
	<-release.reached
	states := association.StateChanges()
	management := association.ManagementIndications()
	if len(states) != 0 || len(management) != 0 {
		t.Fatal("terminal indications arrived before transport release")
	}
	release.open()
	if err := <-finished; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got, ok := <-states; !ok || got != StateASPDown {
		t.Errorf("final state = %v (open %v), want ASP-DOWN", got, ok)
	}
	if _, ok := <-states; ok {
		t.Error("state channel did not close after final indication")
	}
	if got, ok := <-management; !ok || got.Kind != ManagementSCTPRelease {
		t.Errorf("final management indication = %+v (open %v), want M-SCTP_RELEASE", got, ok)
	}
	if _, ok := <-management; ok {
		t.Error("management channel did not close after release indication")
	}
}

func TestIndicationSubscriptionIsSafeDuringConcurrentPublishAndClose(t *testing.T) {
	association := newIndicationAssociation(t)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for range 8 {
		workers.Go(func() {
			<-start
			states := association.StateChanges()
			management := association.ManagementIndications()
			if states != association.StateChanges() || management != association.ManagementIndications() {
				t.Error("concurrent subscription returned different channels")
			}
		})
	}
	workers.Go(func() {
		<-start
		for range 140 {
			association.notifyStateChange(StateASPInactive)
			association.notifyManagement(&ManagementIndication{Kind: ManagementNotify})
		}
	})
	workers.Go(func() {
		<-start
		_ = association.Close()
	})
	close(start)
	joined := make(chan struct{})
	go func() { workers.Wait(); close(joined) }()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("subscription, publication or close blocked")
	}
	for range association.StateChanges() {
	}
	for range association.ManagementIndications() {
	}
}
