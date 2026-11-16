package m3ua

import (
	"context"
	"errors"
	"github.com/gomaja/go-m3ua/messages"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func localCongestionFixture(t *testing.T) (*Endpoint, *Association, ASKey) {
	t.Helper()
	l, as, c, _ := distributionFixture(t, params.TrafficModeLoadshare)
	c.muState.Lock()
	c.state = StateASPActive
	c.muState.Unlock()
	as.setASPState(c, StateASPActive, time.Hour)
	c.syncSSNMBindings()
	return l.endpoint, c, as.key
}

func localRequest(c *Association, key ASKey, source string, level uint8) LocalCongestionRequest {
	return LocalCongestionRequest{Source: source, Association: c.ID(), Epoch: c.Epoch(), AS: key, Destination: 2, Congestion: CongestionState{Congested: level != 0, Level: level, LevelSet: true}}
}

func TestLocalCongestionRetainsOwnedIndependentSources(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	a, b := localRequest(c, key, "service", 2), localRequest(c, key, "cpu", 3)
	if err := e.ReportLocalCongestion(a); err != nil {
		t.Fatal(err)
	}
	if err := e.ReportLocalCongestion(b); err != nil {
		t.Fatal(err)
	}
	got := e.ssnm.localCongestionSnapshot(key)
	if len(got) != 2 {
		t.Fatalf("retained contributions=%+v, want two independently identified sources", got)
	}
	if got[0].Revision == 0 || got[0].Revision == got[1].Revision {
		t.Fatal("library did not assign distinct revisions")
	}
	got[0].Scope.RoutingContexts[0] = 999
	again := e.ssnm.localCongestionSnapshot(key)
	if again[0].Scope.RoutingContexts[0] != 1 {
		t.Fatal("snapshot aliases retained wire scope")
	}
	if err := e.WithdrawLocalCongestion(c.ID(), c.Epoch(), key, 2, "cpu"); err != nil {
		t.Fatal(err)
	}
	got = e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Source != "service" || got[0].Request.Congestion.Level != 2 {
		t.Fatalf("withdrawing cpu cleared independent source: %+v", got)
	}
	a.Congestion = CongestionState{LevelSet: true}
	if err := e.ReportLocalCongestion(a); err != nil {
		t.Fatal(err)
	}
	got = e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Congestion.Congested || !got[0].Request.Congestion.LevelSet {
		t.Fatalf("explicit source clear lost: %+v", got)
	}
}

func TestLocalCongestionRejectsInvalidIdentifiersAndStates(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	for _, source := range []string{"", strings.Repeat("x", 65), "two words", "$queue", "bad\xff", "bad\nname"} {
		request := localRequest(c, key, source, 1)
		if err := e.ReportLocalCongestion(request); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("source %q error=%v", source, err)
		}
	}
	for _, state := range []CongestionState{{Congested: false, Level: 1, LevelSet: true}, {Congested: true, Level: 0, LevelSet: true}, {Congested: true, Level: 1}, {Congested: false}, {Congested: true, Level: 4, LevelSet: true}} {
		request := localRequest(c, key, "source", 1)
		request.Congestion = state
		if err := e.ReportLocalCongestion(request); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("state %+v error=%v", state, err)
		}
	}
	for _, mutate := range []func(*LocalCongestionRequest){func(r *LocalCongestionRequest) { r.Epoch++ }, func(r *LocalCongestionRequest) { r.Association++ }, func(r *LocalCongestionRequest) { r.AS.RoutingContext++ }, func(r *LocalCongestionRequest) { r.Destination = 0x1000000 }} {
		request := localRequest(c, key, "source", 1)
		mutate(&request)
		if err := e.ReportLocalCongestion(request); err == nil {
			t.Fatalf("invalid lifetime/scope/destination accepted: %+v", request)
		}
	}
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("invalid requests installed state: %+v", got)
	}
}

func TestLocalCongestionSharedBudgetRefusesWithoutErasingEvidence(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	_, subscription, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := subscription.Close(); err != nil {
			t.Errorf("close SSNM subscription: %v", err)
		}
	}()
	e.ssnm.mu.Lock()
	e.ssnm.limits.MaxRecords = 1
	e.ssnm.mu.Unlock()
	if err := e.ReportLocalCongestion(localRequest(c, key, "first", 2)); err != nil {
		t.Fatal(err)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "second", 3)); !errors.Is(err, ErrSSNMResourceLoss) {
		t.Fatalf("budget error=%v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil || event.Kind != SSNMResourceLossEvent || event.Reason == "" {
		t.Fatalf("local retention refusal lost subscription diagnostic: %+v, %v", event, err)
	}
	got := e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Source != "first" || e.SSNMKnowledge().ReportsRefused == 0 {
		t.Fatalf("budget lost evidence or diagnostic: %+v", got)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "first", 1)); err != nil {
		t.Fatalf("same record update consumes extra capacity: %v", err)
	}
}

func TestLocalCongestionRetiresWithASAndEpoch(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
		t.Fatal(err)
	}
	c.noteRoutingContextsInactive([]uint32{1})
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("withdrawn AS retains local contribution: %+v", got)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err == nil {
		t.Fatal("inactive scope accepts contribution")
	}
	c.noteRoutingContextsActive([]uint32{1})
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
		t.Fatal(err)
	}
	e.ssnm.retireReceivedEpoch(c.ID(), c.Epoch()+1)
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("retired epoch retains contribution: %+v", got)
	}
}

func TestQueueAndApplicationOverloadClearIndependently(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	app := localRequest(c, key, "service", 2)
	if err := e.ReportLocalCongestion(app); err != nil {
		t.Fatal(err)
	}
	message := &DataMessage{AS: key, Scope: localCongestionScope(key), ProtocolData: &params.ProtocolDataPayload{DestinationPointCode: 2, OriginatingPointCode: 7}}
	if overflow := c.recordDataOverflow(message); overflow == nil || overflow.DestinationPointCode != 2 || overflow.OriginatingPointCode != 7 {
		t.Fatalf("current DATA discard did not identify its queue scope: %+v", overflow)
	}
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 2 {
		t.Fatalf("queue/app not separately inspectable: %+v", got)
	}
	c.muDataOverflow.Lock()
	now := time.Now()
	c.observeDataQueueLocked(now, 0)
	ended := c.observeDataQueueLocked(now.Add(2*time.Second), 0)
	c.muDataOverflow.Unlock()
	if len(ended) != 1 {
		t.Fatalf("queue drain ended %d scopes", len(ended))
	}
	c.clearQueueLocalCongestion(ended[0])
	got := e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Source != "service" || got[0].Request.Congestion.Level != 2 {
		t.Fatalf("queue drain erased application: %+v", got)
	}
	if overflow := c.recordDataOverflow(message); overflow == nil || overflow.DestinationPointCode != 2 || overflow.OriginatingPointCode != 7 {
		t.Fatalf("current DATA discard did not identify its queue scope: %+v", overflow)
	}
	if err := e.WithdrawLocalCongestion(c.ID(), c.Epoch(), key, 2, "service"); err != nil {
		t.Fatal(err)
	}
	got = e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || !strings.HasPrefix(got[0].Request.Source, "$queue:") || !got[0].Request.Congestion.Congested {
		t.Fatalf("app withdrawal erased queue: %+v", got)
	}
}

func TestLocalCongestionInternationalHasNoNumericLevel(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: key.NetworkAppearance, NetworkAppearanceSet: key.NetworkAppearanceSet, Profile: CongestionProfile{Method: CongestionInternational}}}
	for _, state := range []CongestionState{{Congested: true, LevelSet: true}, {Congested: false, LevelSet: true}, {Congested: true, Level: 1}} {
		r := localRequest(c, key, "host", 1)
		r.Congestion = state
		if err := e.ReportLocalCongestion(r); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("international numeric input %+v accepted: %v", state, err)
		}
	}
	for _, congested := range []bool{true, false} {
		r := localRequest(c, key, "host", 0)
		r.Congestion = CongestionState{Congested: congested}
		if err := e.ReportLocalCongestion(r); err != nil {
			t.Fatal(err)
		}
		got := e.ssnm.localCongestionSnapshot(key)
		if len(got) != 1 || got[0].Request.Congestion.Congested != congested || got[0].Request.Congestion.LevelSet {
			t.Fatalf("international state lost: %+v", got)
		}
	}
}

func TestLocalCongestionUnknownAndClearDoNotExpire(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	e.ssnm.validity = time.Nanosecond
	r := localRequest(c, key, "host", 0)
	r.Congestion = CongestionState{Congested: true}
	if err := e.ReportLocalCongestion(r); err != nil {
		t.Fatal(err)
	}
	e.ssnm.expireCongestion(time.Now().Add(time.Hour), e.ctx.Done())
	got := e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Congestion.LevelSet || !got[0].Request.Congestion.Congested {
		t.Fatalf("local contribution acquired received TTL: %+v", got)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.ReportLocalCongestion(r); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("closed report error=%v", err)
	}
	if got = e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("closed endpoint retains local input: %+v", got)
	}
}

func TestLocalCongestionAggregateUnknownDominatesNumericMaximum(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	known := localRequest(c, key, "numeric", 3)
	if err := e.ReportLocalCongestion(known); err != nil {
		t.Fatal(err)
	}
	unknown := localRequest(c, key, "unknown", 0)
	unknown.Congestion = CongestionState{Congested: true}
	if err := e.ReportLocalCongestion(unknown); err != nil {
		t.Fatal(err)
	}
	var revision uint64
	for _, record := range e.ssnm.localCongestionSnapshot(key) {
		revision = max(revision, record.Revision)
	}
	current, acquired := e.ssnm.tryLocalCongestionCurrent(c.ID(), c.Epoch(), key, 2, revision, CongestionState{Congested: true}, "", 0, false)
	if !acquired || !current {
		t.Fatal("unknown contributor did not dominate numeric maximum at final publication gate")
	}
	state, exists := observedLocalAggregate(e.ssnm, c.ID(), key, 2)
	if !exists || state != (CongestionState{Congested: true}) {
		t.Fatalf("numeric maximum erased unknown source: %+v, present=%t", state, exists)
	}
}

func TestLocalCongestionAllSharedRetentionBudgets(t *testing.T) {
	for _, limit := range []string{"partition", "peer", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			e, c, key := localCongestionFixture(t)
			e.ssnm.mu.Lock()
			switch limit {
			case "partition":
				e.ssnm.limits.MaxRecordsPerPartition = 1
			case "peer":
				e.ssnm.limits.MaxRecordsPerPeer = 1
			case "bytes":
				e.ssnm.limits.MaxBytes = e.ssnm.bytes + 350
			}
			e.ssnm.mu.Unlock()
			if err := e.ReportLocalCongestion(localRequest(c, key, "one", 2)); err != nil {
				t.Fatal(err)
			}
			if err := e.ReportLocalCongestion(localRequest(c, key, "two", 3)); !errors.Is(err, ErrSSNMResourceLoss) {
				t.Fatalf("%s budget error=%v", limit, err)
			}
			if got := e.ssnm.localCongestionSnapshot(key); len(got) != 1 || got[0].Request.Source != "one" {
				t.Fatalf("%s refused update changed retained source: %+v", limit, got)
			}
		})
	}
}

func TestLocalCongestionFullIntegerBudgetsRefuseNewSource(t *testing.T) {
	maxInt := int(^uint(0) >> 1)
	config := &SSNMStateConfig{MaxRecords: maxInt, MaxRecordsPerPeer: maxInt, MaxRecordsPerPartition: maxInt, MaxBytes: maxInt}
	for _, dimension := range []string{"store-records", "peer-records", "partition-records", "bytes", "bytes-new-partition"} {
		t.Run(dimension, func(t *testing.T) {
			s, err := newSSNMState(config)
			if err != nil {
				t.Fatalf("positive MaxInt budgets are invalid: %v", err)
			}
			if s.limits.MaxRecords != maxInt || s.limits.MaxRecordsPerPeer != maxInt || s.limits.MaxRecordsPerPartition != maxInt || s.limits.MaxBytes != maxInt {
				t.Fatalf("valid MaxInt budgets were changed: %+v", s.limits)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			const association AssociationID = 1
			s.liveSources = map[AssociationID]uint64{association: 1}
			partition := canonicalSSNMPartition("peer", "first")
			key := ASKey{RoutingContext: 1, RoutingContextSet: true}
			binding := queueCongestionBinding{key: key}
			lifetime := &localQueueLifetime{binding: binding}
			lifetime.live.Store(true)
			record := localCongestionRecord{LocalCongestionContribution: LocalCongestionContribution{
				Request: LocalCongestionRequest{Source: "held", Association: association, Epoch: 1, AS: key, Destination: 2, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}},
				Scope:   localCongestionScope(key),
			}, lifetime: lifetime}
			record.scopeKey = localCongestionScopeKey(record.Scope)
			if _, err := s.commitLocalCongestionLocked(partition, binding, record, false, true, true); err != nil {
				t.Fatalf("retain initial source: %v", err)
			}
			state := s.partitions[partition]
			bucket := s.localByAssociation[association][key]
			heldKey := localCongestionRecordKey{record.Request.Destination, record.Request.Source, record.scopeKey}
			held := bucket.records[heldKey]
			switch dimension {
			case "store-records":
				s.records = maxInt
			case "peer-records":
				s.peerRecords[partition.peerKey()] = maxInt
			case "partition-records":
				state.localRecords = maxInt
			case "bytes", "bytes-new-partition":
				s.bytes = maxInt
			}
			before := [4]int{s.records, s.peerRecords[partition.peerKey()], state.records(), s.bytes}
			beforeBucketBytes, beforeStateBytes := bucket.bytes, state.bytes
			record.Request.Source = "new-source"
			if dimension == "bytes-new-partition" {
				partition.ApplicationServer = "second"
				binding.key.RoutingContext++
				record.Request.AS = binding.key
				record.Scope = localCongestionScope(binding.key)
				record.scopeKey = localCongestionScopeKey(record.Scope)
			}
			update, err := s.commitLocalCongestionLocked(partition, binding, record, false, true, true)
			if !errors.Is(err, ErrSSNMStateLimit) || !errors.Is(err, ErrSSNMResourceLoss) {
				t.Errorf("full %s budget accepted a new source or lost its typed refusal: %v", dimension, err)
			}
			after := [4]int{s.records, s.peerRecords[partition.peerKey()], state.records(), s.bytes}
			if after != before || bucket.bytes != beforeBucketBytes || state.bytes != beforeStateBytes {
				t.Errorf("full %s budget mutated retention counters: before=%v after=%v", dimension, before, after)
			}
			for _, count := range after {
				if count < 0 {
					t.Errorf("full %s budget wrapped a retention counter negative: %v", dimension, after)
				}
			}
			if len(s.partitions) != 1 || len(s.localByAssociation[association]) != 1 || len(bucket.records) != 1 || bucket.records[heldKey].Revision != held.Revision || bucket.records[heldKey].Request != held.Request || update.revision != 0 {
				t.Errorf("full %s budget changed retained evidence or admitted a partial update", dimension)
			}
			if s.reportsRefused != 1 || s.recordsRefused != 1 || s.lastResourceLoss == "" {
				t.Errorf("full %s budget lost its bounded resource diagnostic", dimension)
			}
		})
	}
}

func TestLocalCongestionFailedPublicationPreservesContribution(t *testing.T) {
	rig := newAuditTestRig(t)
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	var attempts atomic.Uint64
	rig.a.signalWriter = func(messages.M3UA) (int, error) { attempts.Add(1); return 0, syscall.EAGAIN }
	request := localRequest(rig.a, key, "service", 2)
	if err := rig.e.ReportLocalCongestion(request); err != nil {
		t.Fatal(err)
	}
	rig.e.runCongestionAutomation(rig.now)
	got := rig.e.ssnm.localCongestionSnapshot(key)
	if len(got) != 1 || got[0].Request.Congestion.Level != 2 || !got[0].Request.Congestion.Congested {
		t.Fatalf("failed publication erased local evidence: %+v", got)
	}
	if attempts.Load() != 1 {
		t.Fatalf("optional EAGAIN seam attempts=%d, want one bounded attempt", attempts.Load())
	}
}

func TestLocalCongestionQueueLifetimeCannotReuseRetiredStaticBinding(t *testing.T) {
	_, c, key := localCongestionFixture(t)
	c.reconcileQueueCongestionLifetimes()
	binding := c.queueCongestionBinding(key, WireScope{})
	before := c.captureQueueCongestionLifetime(binding)
	if before == nil {
		t.Fatal("active binding has no captured queue lifetime")
	}
	c.noteRoutingContextsInactive([]uint32{1})
	if observedQueueLifetimeCurrent(before) {
		t.Fatal("withdrawal left original queue trigger authorized")
	}
	c.noteRoutingContextsActive([]uint32{1})
	after := c.captureQueueCongestionLifetime(binding)
	if after == nil || after == before || !observedQueueLifetimeCurrent(after) {
		t.Fatal("reactivation reused old queue lifetime instead of minting current binding evidence")
	}
	if observedQueueLifetimeCurrent(before) {
		t.Fatal("reactivation resurrected old queue trigger")
	}
}

func TestLocalASPInputsRemainInspectableWithoutSharedASRegistry(t *testing.T) {
	rig := newAuditTestRig(t)
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	if err := rig.e.ReportLocalCongestion(localRequest(rig.a, key, "service", 2)); err != nil {
		t.Fatal(err)
	}
	snapshot, err := rig.e.CongestionInputs(key)
	if err != nil {
		t.Fatalf("configured active ASP local inputs unavailable: %v", err)
	}
	local := snapshot.Local
	if len(local) != 1 || local[0].Request.Source != "service" || local[0].Request.Congestion.Level != 2 || !snapshot.Consistent {
		t.Fatalf("ASP input snapshot lost independent local evidence: %+v", snapshot)
	}
	local[0].Scope.RoutingContexts[0] = 999
	again, err := rig.e.CongestionInputs(key)
	if err != nil || again.Local[0].Scope.RoutingContexts[0] != 1 {
		t.Fatal("ASP local snapshot aliases stored scope")
	}
	key.RoutingContext = 999
	if _, err = rig.e.CongestionInputs(key); !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("unknown ASP AS error=%v", err)
	}
}

func localQueueObservation(t *testing.T, c *Association, key ASKey) dataOverflowScope {
	t.Helper()
	message := &DataMessage{AS: key, Scope: localCongestionScope(key), ProtocolData: &params.ProtocolDataPayload{DestinationPointCode: 2, OriginatingPointCode: 7}}
	if overflow := c.recordDataOverflow(message); overflow == nil || overflow.DestinationPointCode != 2 || overflow.OriginatingPointCode != 7 {
		t.Fatalf("current DATA discard did not identify its queue scope: %+v", overflow)
	}
	c.muDataOverflow.Lock()
	defer c.muDataOverflow.Unlock()
	return c.dataOverflowScopes[overflowKey(message.Scope, 2, 7)]
}
func TestLocalQueueTicketRepairsOnlyUnmarkedCurrentObservation(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	c.localQueueLifetimes.Store(nil)
	ind := localQueueObservation(t, c, key)
	if ind.lifetime != nil {
		t.Fatal("fixture captured a live token instead of an unresolved ticket")
	}
	c.reconcileQueueCongestionLifetimes()
	repaired, ok := c.repairQueueLocalLifetime(ind)
	if !ok || repaired.lifetime == nil {
		t.Fatal("current unresolved queue observation was not repaired from the current configured index")
	}
	c.reportQueueLocalCongestion(repaired)
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 1 {
		t.Fatalf("repaired current queue observation remains unreported: %+v", got)
	}
}
func TestLocalQueueTicketCannotCrossWithdrawalOrEndedEpisode(t *testing.T) {
	for _, change := range []string{"withdrawal", "end", "inactive"} {
		t.Run(change, func(t *testing.T) {
			e, c, key := localCongestionFixture(t)
			if change == "inactive" {
				c.noteRoutingContextsInactive([]uint32{1})
			}
			c.localQueueLifetimes.Store(nil)
			ind := localQueueObservation(t, c, key)
			switch change {
			case "withdrawal":
				c.noteRoutingContextsInactive([]uint32{1})
				c.noteRoutingContextsActive([]uint32{1})
			case "inactive":
				c.noteRoutingContextsActive([]uint32{1})
			default:
				c.muDataOverflow.Lock()
				now := time.Now()
				c.observeDataQueueLocked(now, 0)
				c.observeDataQueueLocked(now.Add(2*time.Second), 0)
				c.muDataOverflow.Unlock()
			}
			c.reconcileQueueCongestionLifetimes()
			repaired, ok := c.repairQueueLocalLifetime(ind)
			if ok {
				c.reportQueueLocalCongestion(repaired)
			}
			if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
				t.Fatalf("old queue ticket crossed %s: %+v", change, got)
			}
			if ok {
				t.Fatalf("retired %s queue ticket was relabelled", change)
			}
		})
	}
}

func localQueueDrainScopes(c *Association) []dataOverflowScope {
	c.muDataOverflow.Lock()
	defer c.muDataOverflow.Unlock()
	now := time.Now()
	c.observeDataQueueLocked(now, 0)
	return c.observeDataQueueLocked(now.Add(2*time.Second), 0)
}
func TestLocalQueueEndedSnapshotCannotReinstallSource(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	e.ssnm.mu.Lock()
	e.ssnm.limits.MaxRecords = 1
	e.ssnm.mu.Unlock()
	ind := localQueueObservation(t, c, key)
	ended := localQueueDrainScopes(c)
	if len(ended) != 1 {
		t.Fatalf("queue episode did not end: %d", len(ended))
	}
	c.clearQueueLocalCongestion(ended[0])
	c.reportQueueLocalCongestion(ind)
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("stale worker snapshot reinstalled ended queue source: %+v", got)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "current", 1)); err != nil {
		t.Fatalf("discarded ended report leaked retention capacity: %v", err)
	}
}
func TestLocalQueueEndedEpisodeFencesFinalAdmissionBeforeCleanup(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	localQueueObservation(t, c, key)
	records := e.ssnm.localCongestionSnapshot(key)
	if len(records) != 1 {
		t.Fatalf("missing initial current queue source: %+v", records)
	}
	ended := localQueueDrainScopes(c)
	if len(ended) != 1 {
		t.Fatalf("queue episode did not end: %d", len(ended))
	}
	current, acquired := e.ssnm.tryLocalCongestionCurrent(c.ID(), c.Epoch(), key, 2, records[0].Revision, CongestionState{Congested: true, Level: 3, LevelSet: true}, "", 0, false)
	if !acquired || current {
		t.Fatal("ended queue episode still authorizes final optional onset before cleanup")
	}
	if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
		t.Fatalf("ended queue evidence remains current before cleanup: %+v", got)
	}
}

func TestLocalInputVersionsObserveMarksBeforeStoreCleanup(t *testing.T) {
	for _, change := range []string{"scope", "episode"} {
		t.Run(change, func(t *testing.T) {
			e, c, key := localCongestionFixture(t)
			if change == "scope" {
				if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
					t.Fatal(err)
				}
			} else {
				localQueueObservation(t, c, key)
			}
			before, err := e.CongestionInputs(key)
			if err != nil {
				t.Fatal(err)
			}
			if change == "scope" {
				c.retireQueueCongestionLifetimes(func(candidate ASKey) bool { return candidate == key })
			} else {
				localQueueDrainScopes(c)
			}
			after, err := e.CongestionInputs(key)
			if err != nil {
				t.Fatal(err)
			}
			if len(after.Local) != 0 {
				t.Fatalf("%s retired local evidence remains visible before cleanup: %+v", change, after.Local)
			}
			if sameCongestionVersions(before.VersionsAfter, after.VersionsBefore) {
				t.Fatalf("%s local evidence mark left component version unchanged before store cleanup", change)
			}
		})
	}
}
func TestLocalInputVersionsRejectOverlappingChanges(t *testing.T) {
	e, c, key := localCongestionFixture(t)
	c.beginLocalInputChange()
	c.beginLocalInputChange()
	both, err := e.CongestionInputs(key)
	if err != nil {
		t.Fatal(err)
	}
	c.finishLocalInputChange()
	one, err := e.CongestionInputs(key)
	if err != nil {
		t.Fatal(err)
	}
	c.finishLocalInputChange()
	settled, err := e.CongestionInputs(key)
	if err != nil {
		t.Fatal(err)
	}
	if both.Consistent || one.Consistent || !settled.Consistent {
		t.Fatalf("overlapping local changes coherence: both=%t one=%t settled=%t", both.Consistent, one.Consistent, settled.Consistent)
	}
}

func TestLocalInputsUseIPSPDoubleExchangeLocalDirection(t *testing.T) {
	c, _ := newDoubleExchangeIPSPForTest(t)
	c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
	e := c.endpoint
	if !e.trackAssociation(c) {
		t.Fatal("attach local-direction IPSP")
	}
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 11, RoutingContextSet: true}
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
		t.Fatalf("active DE local contribution refused by peer-facing direction: %v", err)
	}
	snapshot, err := e.CongestionInputs(key)
	if err != nil || len(snapshot.Local) != 1 || !snapshot.Consistent || len(snapshot.Peers) != 1 || !snapshot.Peers[0].Active {
		t.Fatalf("DE local scope lost or labelled by inactive peer direction: %+v, %v", snapshot, err)
	}
	wrong := localRequest(c, ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 22, RoutingContextSet: true}, "service", 2)
	if err := e.ReportLocalCongestion(wrong); !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("peer-only AS accepted as local evidence: %v", err)
	}
	c.commitLocalIPSPState(StateASPInactive)
	after, err := e.CongestionInputs(key)
	if err != nil || len(after.Local) != 0 || after.Peers[0].Active {
		t.Fatalf("local-direction withdrawal left input current: %+v, %v", after, err)
	}
}

func TestLocalQueueDrainDoesNotReplayConcernedDestinationForApplicationSeverity(t *testing.T) {
	rig := newAuditTestRig(t)
	c, e := rig.a, rig.e
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	frames := make(chan *messages.SignallingCongestion, 16)
	var captureOverflow atomic.Uint64
	c.signalWriter = func(message messages.M3UA) (int, error) {
		raw, err := message.MarshalBinary()
		if err != nil {
			return 0, err
		}
		decoded, err := messages.Parse(raw)
		if err != nil {
			return 0, err
		}
		if scon, ok := decoded.(*messages.SignallingCongestion); ok {
			select {
			case frames <- scon:
			default:
				captureOverflow.Add(1)
				return 0, errors.New("capture capacity exceeded")
			}
		}
		return len(raw), nil
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 2)); err != nil {
		t.Fatal(err)
	}
	localQueueObservation(t, c, key)
	for i := 0; i < 4; i++ {
		e.runCongestionAutomation(rig.now.Add(time.Duration(i) * time.Second))
	}
	foundTrigger := false
	for len(frames) > 0 {
		message := <-frames
		if message.ConcernedDestination != nil && message.ConcernedDestination.ConcernedDestination() == 7 && message.CongestionIndications != nil && message.CongestionIndications.CongestionLevel() == 3 {
			foundTrigger = true
		}
	}
	if !foundTrigger {
		t.Fatal("queue onset with its actual triggering OPC never submitted")
	}
	ended := localQueueDrainScopes(c)
	if len(ended) != 1 {
		t.Fatalf("queue end scopes=%d", len(ended))
	}
	c.clearQueueLocalCongestion(ended[0])
	for i := 4; i < 8; i++ {
		e.runCongestionAutomation(rig.now.Add(time.Duration(i) * time.Second))
	}
	lower := 0
	for len(frames) > 0 {
		message := <-frames
		if message.CongestionIndications == nil || message.CongestionIndications.CongestionLevel() != 2 {
			t.Fatalf("queue end erased current application severity: %+v", message)
		}
		if message.ConcernedDestination != nil {
			t.Fatal("application-only lower severity replayed ended queue OPC")
		}
		lower++
	}
	if lower == 0 {
		t.Fatal("lower current application severity never submitted after queue drain")
	}
	if captureOverflow.Load() != 0 {
		t.Fatal("one report/pass exceeded bounded capture capacity")
	}
}

func TestLocalQueueSourcesPreserveWireNetworksOnAllAppearanceAS(t *testing.T) {
	l, as, c, _ := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *distributionFixtureConfig) {
		setInventoryNetworkAppearance(&config.AssociationConfig.ApplicationServers, nil)
		config.Congestion = &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 3}, Networks: []NetworkCongestionProfile{
			{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionInternational}},
			{NetworkAppearance: 8, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 3}},
		}}
	})
	c.muState.Lock()
	c.state = StateASPActive
	c.muState.Unlock()
	as.setASPState(c, StateASPActive, time.Hour)
	c.syncSSNMBindings()
	for _, na := range []uint32{7, 8} {
		scope := localCongestionScope(as.key)
		scope.NetworkAppearance = na
		scope.NetworkAppearanceSet = true
		if overflow := c.recordDataOverflow(&DataMessage{AS: as.key, Scope: scope, ProtocolData: &params.ProtocolDataPayload{DestinationPointCode: 2, OriginatingPointCode: 7}}); overflow == nil || !overflow.Scope.NetworkAppearanceSet || overflow.Scope.NetworkAppearance != na {
			t.Fatalf("discarded DATA lost its exact wire network %d: %+v", na, overflow)
		}
	}
	snapshot, err := l.endpoint.CongestionInputs(as.key)
	if err != nil || len(snapshot.Local) != 2 {
		t.Fatalf("all-appearance AS merged distinct wire networks: %+v, %v", snapshot, err)
	}
	for _, source := range snapshot.Local {
		if source.Request.AS.NetworkAppearanceSet || source.Request.Source != "$queue:7" {
			t.Fatalf("wire scope relabelled binding/source: %+v", source)
		}
		switch source.Scope.NetworkAppearance {
		case 7:
			if !source.Request.Congestion.Congested || source.Request.Congestion.LevelSet {
				t.Fatalf("international queue source gained numeric level: %+v", source)
			}
		case 8:
			if source.Request.Congestion.Level != 3 || !source.Request.Congestion.LevelSet {
				t.Fatalf("national queue source lost numeric level: %+v", source)
			}
		default:
			t.Fatalf("wire appearance lost: %+v", source)
		}
	}
	if state, held := observedLocalAggregate(l.endpoint.ssnm, c.ID(), as.key, 2); !held || state != (CongestionState{Congested: true}) {
		t.Fatalf("wire-source unknown did not dominate numeric maximum: %+v", state)
	}
}

func observedQueueLifetimeCurrent(token *localQueueLifetime) bool {
	return token != nil && token.live.Load()
}
func observedLocalAggregate(s *ssnmState, id AssociationID, key ASKey, dpc uint32) (CongestionState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := s.localByAssociation[id][key]
	if b == nil || b.groups[dpc] == nil {
		return CongestionState{}, false
	}
	return b.groups[dpc].desired, true
}

func TestLocalSingleExchangeInputsRequireActiveASMember(t *testing.T) {
	for _, producer := range []string{"application", "queue"} {
		t.Run(producer, func(t *testing.T) {
			c, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
			e := c.endpoint
			c.as = e.as
			c.as.register(c.configuredASKeys())
			key := c.configuredLocalASKeysForStatus()[0]
			server := c.as.get(key)
			server.setASPState(c, StateASPActive, time.Hour)
			if !e.trackAssociation(c) {
				t.Fatal("attach SE association")
			}
			if err := e.ReportLocalCongestion(localRequest(c, key, "before", 2)); err != nil {
				t.Fatal(err)
			}
			server.setASPState(c, StateASPInactive, time.Hour)
			if !c.activeForASKey(key) {
				t.Fatal("fixture unexpectedly changed association ledger")
			}
			if producer == "application" {
				if err := e.ReportLocalCongestion(localRequest(c, key, "after", 2)); !errors.Is(err, ErrRoutingContextNotActive) {
					t.Fatalf("inactive SE AS member accepted local application evidence: %v", err)
				}
			} else {
				c.reconcileQueueCongestionLifetimes()
				ind := localQueueObservation(t, c, key)
				server.setASPState(c, StateASPActive, time.Hour)
				c.reconcileQueueCongestionLifetimes()
				c.reportQueueLocalCongestion(ind)
				if got := e.ssnm.localCongestionSnapshot(key); len(got) != 0 {
					t.Fatalf("inactive SE member queue trigger survived reactivation: %+v", got)
				}
			}
		})
	}
}

func localOnlyDoubleExchangeFixture(t *testing.T) (*Endpoint, *Association, ASKey) {
	t.Helper()
	c, _ := newDoubleExchangeIPSPForTest(t)
	c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
	e := c.endpoint
	c.as = e.as
	c.as.register(c.configuredASKeys())
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 11, RoutingContextSet: true}
	e.as.register([]ASKey{key}) // The key exists as a peer-facing AS with no member.
	if !e.trackAssociation(c) {
		t.Fatal("attach local-only DE source")
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "local", 2)); err != nil {
		t.Fatal(err)
	}
	return e, c, key
}
func TestLocalInputVersionsIncludeDELocalOnlySources(t *testing.T) {
	e, c, key := localOnlyDoubleExchangeFixture(t)
	before, err := e.CongestionInputs(key)
	if err != nil || !before.Consistent || len(before.Local) != 1 || len(before.Peers) != 0 {
		t.Fatalf("local-only source changed peer membership semantics: %+v, %v", before, err)
	}
	c.retireQueueCongestionLifetimes(func(candidate ASKey) bool { return candidate == key })
	after, err := e.CongestionInputs(key)
	if err != nil || len(after.Local) != 0 || sameCongestionVersions(before.VersionsAfter, after.VersionsBefore) {
		t.Fatalf("DE local source absent from peer membership was not versioned: before=%+v after=%+v, %v", before.VersionsAfter, after.VersionsBefore, err)
	}
}
func TestLocalInputVersionsExposeUnownedRetainedSource(t *testing.T) {
	e, c, key := localOnlyDoubleExchangeFixture(t)
	e.mu.Lock()
	delete(e.associationsByID, c.ID())
	e.mu.Unlock()
	defer func() { e.mu.Lock(); e.associationsByID[c.ID()] = c; e.mu.Unlock() }()
	snapshot, err := e.CongestionInputs(key)
	if err != nil || snapshot.Consistent {
		t.Fatalf("unowned retained local source falsely yielded consistent snapshot: %+v, %v", snapshot, err)
	}
}

func TestLocalContributionDoesNotPromoteReceivedKnowledgeBinding(t *testing.T) {
	rig := newAuditTestRig(t)
	c, e := rig.a, rig.e
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	partition := c.ssnmPartitionFor(key)
	if err := e.ssnm.bind(partition, c.ID(), true); err != nil {
		t.Fatal(err)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, "local", 2)); err != nil {
		t.Fatal(err)
	}
	for _, part := range e.SSNMKnowledge().Partitions {
		if part.Partition == partition {
			for _, binding := range part.Bindings {
				if binding.Association == c.ID() {
					if !binding.Pending {
						t.Fatal("local contribution promoted received-knowledge pending binding")
					}
					return
				}
			}
		}
	}
	t.Fatal("local contribution removed received knowledge binding")
}

func TestEndedQueueTriggerCannotPublishAtUnchangedApplicationSeverity(t *testing.T) {
	rig := newAuditTestRig(t)
	c, e := rig.a, rig.e
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	frames := make(chan *messages.SignallingCongestion, 16)
	var captureOverflow atomic.Uint64
	c.signalWriter = func(message messages.M3UA) (int, error) {
		raw, err := message.MarshalBinary()
		if err != nil {
			return 0, err
		}
		decoded, err := messages.Parse(raw)
		if err != nil {
			return 0, err
		}
		if scon, ok := decoded.(*messages.SignallingCongestion); ok {
			select {
			case frames <- scon:
			default:
				captureOverflow.Add(1)
				return 0, errors.New("capture capacity exceeded")
			}
		}
		return len(raw), nil
	}
	// Drive the existing episode/end seams manually, keeping the watcher from
	// cleaning the source between the episode mark and final writer admission.
	c.muDataOverflow.Lock()
	c.dataOverflowWorker = true
	c.muDataOverflow.Unlock()
	c.sendMu.Lock()
	locked := true
	defer func() {
		if locked {
			c.sendMu.Unlock()
		}
	}()
	if err := e.ReportLocalCongestion(localRequest(c, key, "service", 3)); err != nil {
		t.Fatal(err)
	}
	localQueueObservation(t, c, key)
	pendingTrigger := false
	for _, state := range e.CongestionPublications() {
		if state.Pending && state.ConcernedDestinationSet && state.ConcernedDestination == 7 {
			pendingTrigger = true
		}
	}
	if !pendingTrigger {
		t.Fatal("fixture did not retain an unsubmitted queue-origin onset")
	}
	ended := localQueueDrainScopes(c)
	if len(ended) != 1 {
		t.Fatalf("queue end scopes=%d", len(ended))
	}
	c.sendMu.Unlock()
	locked = false
	for i := 0; i < 4; i++ {
		e.runCongestionAutomation(rig.now.Add(time.Duration(i) * time.Second))
	}
	currentApplicationReports := 0
	for len(frames) > 0 {
		message := <-frames
		if message.ConcernedDestination != nil {
			t.Fatal("ended queue trigger reached the writer while equal application severity kept aggregate unchanged")
		}
		if message.CongestionIndications == nil || message.CongestionIndications.CongestionLevel() != 3 {
			t.Fatal("unchanged current application severity was lost before queue-source cleanup")
		}
		currentApplicationReports++
	}
	c.clearQueueLocalCongestion(ended[0])
	for i := 4; i < 8; i++ {
		e.runCongestionAutomation(rig.now.Add(time.Duration(i) * time.Second))
	}
	for len(frames) > 0 {
		message := <-frames
		if message.ConcernedDestination != nil {
			t.Fatal("current application report replayed an ended queue trigger after source cleanup")
		}
		if message.CongestionIndications == nil || message.CongestionIndications.CongestionLevel() != 3 {
			t.Fatal("queue cleanup erased the equal-severity application contribution")
		}
		currentApplicationReports++
	}
	if currentApplicationReports == 0 {
		t.Fatal("current application congestion never reached the writer without Concerned Destination")
	}
	if captureOverflow.Load() != 0 {
		t.Fatal("unchanged aggregate exceeded bounded report capture")
	}
}
