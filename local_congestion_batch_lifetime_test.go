package m3ua

import (
	"testing"
	"time"
)

func TestRegistrationBatchRetiresLocalScopesAtItsFence(t *testing.T) {
	e, a, rcs := newGroupedLifecycleFixture(t, 4)
	e.congestionAutomationOnce.Do(func() {})
	keys := make([]ASKey, len(rcs))
	for i, rc := range rcs {
		keys[i] = *staticASKey(7, rc)
	}
	// The last key has an existing registration, but the removal below
	// carries a stale version. Its local evidence must remain usable.
	a.addDynamicASKey(keys[3], testRoutingKey(7, 2000, 3), false)
	if err := a.noteCanonicalRemoteAS(rcs[3], "as-03"); err != nil {
		t.Fatal(err)
	}
	a.syncSSNMBindings()
	a.reconcileQueueCongestionLifetimes()
	tokens := make([]*localQueueLifetime, len(keys))
	for i, key := range keys {
		if err := e.ReportLocalCongestion(localRequest(a, key, "service", 2)); err != nil {
			t.Fatal("local fixture admission", key, err)
		}
		tokens[i] = a.captureQueueCongestionLifetime(a.queueCongestionBinding(key, localCongestionScope(key)))
		if tokens[i] == nil {
			t.Fatal("local fixture has no live token", key)
		}
	}
	stale := a.dynamicASKeyVersionFor(rcs[3], false) + 1
	before := a.localQueueLifetimeRevision.Load()
	release, admitted := a.beginSCONBindingChanges([]sconBindingChange{
		{id: sconBindingID{rcs[0], false}},
		{id: sconBindingID{rcs[1], false}},
		{id: sconBindingID{rcs[2], true}},
		{id: sconBindingID{rcs[3], false}, removing: true, expectedVersion: &stale},
	})
	defer release()
	if !admitted[sconBindingID{rcs[0], false}] || !admitted[sconBindingID{rcs[1], false}] || admitted[sconBindingID{rcs[3], false}] {
		t.Fatal("fixture did not select the intended current registrations")
	}
	for i, key := range keys {
		selected := i < 2
		if tokens[i].live.Load() == selected {
			t.Error("registration batch failed to fence exactly its local lifetimes", key)
		}
		if count := len(e.ssnm.localCongestionSnapshot(key)); (count == 0) != selected {
			t.Error("registration batch retained retired local evidence or removed a survivor", key, count)
		}
	}
	for _, p := range e.CongestionPublications() {
		for _, rc := range p.Scope.RoutingContexts {
			if rc == rcs[0] || rc == rcs[1] {
				t.Error("registration batch retained a retired publication", rc)
			}
		}
	}
	if got := a.localQueueLifetimeRevision.Load() - before; got != 1 {
		t.Error("registration batch did not retire local lifetimes once per event", got)
	}
}

func TestPartitionBatchRetiresReservedClearsInOnePass(t *testing.T) {
	e, a, rcs := newGroupedLifecycleFixture(t, 3)
	e.congestionAutomationOnce.Do(func() {})
	// Remove unrelated audit work before measuring the publication retirement.
	e.ssnm.mu.Lock()
	e.ssnm.retireDestinationAuditsLocked(a.ID(), nil)
	e.ssnm.auditing = nil
	e.ssnm.mu.Unlock()
	now := time.Unix(100, 0)
	keys := make([]ASKey, len(rcs))
	for i, rc := range rcs {
		keys[i] = *staticASKey(7, rc)
		scope := localCongestionScope(keys[i])
		e.enqueueCongestionPublication(a, scope, a.queueCongestionBinding(keys[i], scope), 2, 0, false, nationalPublication(2), uint64(i+1), false)
		e.runCongestionAutomation(now.Add(time.Duration(i) * time.Second))
	}
	for i := range 2 {
		scope := localCongestionScope(keys[i])
		e.enqueueCongestionPublication(a, scope, a.queueCongestionBinding(keys[i], scope), 2, 0, false, nationalPublication(0), uint64(i+10), false)
	}
	before := e.CongestionPublications()
	pending := 0
	for _, p := range before {
		if p.Pending && !p.DesiredCongestion.Congested && p.SubmittedRevision != 0 {
			pending++
		}
	}
	if len(before) != 3 || pending != 2 {
		t.Fatal("fixture did not reserve two submitted national clears", before)
	}
	w := attachLifecycleWorkProbe(e.ssnm)
	e.ssnm.retirePartitions([]SSNMPartition{a.ssnmPartitionFor(keys[0]), a.ssnmPartitionFor(keys[1])}, a.ID())
	got := e.CongestionPublications()
	if len(got) != 1 || len(got[0].Scope.RoutingContexts) != 1 || got[0].Scope.RoutingContexts[0] != rcs[2] {
		t.Error("partition batch retained retired clear reservations or lost its survivor", got)
	}
	// No audit jobs remain in this fixture: these are the three retained
	// publication rows. A per-partition sweep repeats shrinking table work.
	if w.jobRows != 3 {
		t.Error("partition batch did not visit each publication exactly once", w.jobRows)
	}
}

func TestDoubleExchangeRegistrationBatchKeepsLocalDirectionSeparate(t *testing.T) {
	for _, scenario := range []string{"peer", "local", "both"} {
		t.Run(scenario, func(t *testing.T) {
			c, _ := newDoubleExchangeIPSPForTest(t)
			c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
			e := c.endpoint
			e.congestionAutomationOnce.Do(func() {})
			if !e.trackAssociation(c) {
				t.Fatal("attach DE local source")
			}
			key := *staticASKey(10, 11)
			if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
				t.Fatal(err)
			}
			token := c.captureQueueCongestionLifetime(c.queueCongestionBinding(key, localCongestionScope(key)))
			if token == nil {
				t.Fatal("DE fixture has no local lifetime")
			}
			var changes []sconBindingChange
			if scenario != "local" {
				changes = append(changes, sconBindingChange{id: sconBindingID{11, false}})
			}
			if scenario != "peer" {
				changes = append(changes, sconBindingChange{id: sconBindingID{11, true}})
			}
			before := c.localQueueLifetimeRevision.Load()
			release, _ := c.beginSCONBindingChanges(changes)
			defer release()
			retired := scenario != "peer"
			if token.live.Load() == retired || (len(e.ssnm.localCongestionSnapshot(key)) == 0) != retired {
				t.Error("DE registration batch did not retire only its local direction", scenario)
			}
			if retired && len(e.CongestionPublications()) != 0 {
				t.Error("DE local registration retained optional publication")
			}
			want := uint64(0)
			if retired {
				want = 1
			}
			if got := c.localQueueLifetimeRevision.Load() - before; got != want {
				t.Error("DE registration batch multiplied or crossed local lifetime marks", scenario, got)
			}
		})
	}
}
