package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestRetiredReceiveAdmissionCannotRecreateHistoryOrAudit(t *testing.T) {
	e, a, key, _ := newDynamicValidityAssociation(t, true)
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: a.ID(), Scope: testWireScope(7, true, 7), Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 2, CongestionLevelSet: true}
	admission := a.captureReceivedSSNMAdmission(report.Scope)
	if !admission.valid {
		t.Fatal("setup has no admission")
	}
	a.removeRequesterRoutingKeyVersion(7, a.dynamicASKeyVersionFor(7, false))
	a.addDynamicASKey(key, testRoutingKey(7, 100, params.ServiceIndSCCP), false)
	entered := false
	err := a.withReceivedSSNMAdmission(admission, func() error {
		entered = true
		scoped := report
		scoped.Partition = admission.partitions[0]
		if err := e.ssnm.applyPrepared(scoped, admission.resolution.byPartition[scoped.Partition], admission.epoch); err != nil {
			return err
		}
		a.scheduleDestinationAuditPrepared(report, false, "", admission)
		return nil
	})
	if err != nil || entered || e.ssnm.records != 0 || len(e.ssnm.audits) != 0 {
		t.Fatal("retired receive lifetime recreated history or audits", entered, e.ssnm.records, len(e.ssnm.audits), err)
	}
}

func TestRestartEpochFencesAlreadyPreparedReceivedReports(t *testing.T) {
	e, a, _, _ := newDynamicValidityAssociation(t, true)
	scope := testWireScope(7, true, 7)
	admission := a.captureReceivedSSNMAdmission(scope)
	a.resetTransportCongestionEpoch()
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: a.ID(), Partition: admission.partitions[0], Scope: scope, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 2, CongestionLevelSet: true}
	// The caller can have passed its association check before being delayed at
	// the store. The store's own epoch must reject this old admission, including0.
	if err := e.ssnm.applyPrepared(report, admission.resolution.byPartition[report.Partition], admission.epoch); err != nil {
		t.Fatal(err)
	}
	a.scheduleDestinationAuditPrepared(report, false, "", admission)
	if e.ssnm.records != 0 || len(e.ssnm.audits) != 0 {
		t.Fatal("old source epoch recreated received history/audit", e.ssnm.records, len(e.ssnm.audits))
	}
	sendSCON(t, a, 7, 7, congestionLevel(3), 100)
	before := e.ssnm.records
	e.ssnm.retireReceivedEpoch(a.ID(), a.Epoch())
	if e.ssnm.records != before {
		t.Fatal("equal epoch retired a fresh report")
	}
	id := a.ID()
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	e.ssnm.retireReceivedEpoch(id, a.Epoch()+10)
	if _, held := e.ssnm.liveSources[id]; held {
		t.Fatal("late epoch retirement revived departed source")
	}
}

func TestStaticMembershipKeepsReceivedHistoryAfterDynamicRemoval(t *testing.T) {
	r := newScopedAutomationRig(t)
	key := *staticASKey(7, 1)
	r.a.addDynamicASKey(key, testRoutingKey(1, 100, params.ServiceIndSCCP), false)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	before := r.e.ssnm.records
	r.a.removeRequesterRoutingKeyVersion(1, r.a.dynamicASKeyVersionFor(1, false))
	status := r.e.ssnm.destinationKnowledge(r.a.ssnmPartitionFor(key), 100, key)
	if !status.congested || status.receivedUnknown || r.e.ssnm.records != before {
		t.Fatal("dynamic removal erased static AS knowledge", status)
	}
}

func TestHistoryOnlyDestinationsSurviveSnapshotAndInvalidationReplay(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, r.a, 7, 2, congestionLevel(3), 100)
	before, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(2), params.NewAffectedPointCode(200, 201), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	assertSSNMScopeAccounting(t, r.e)
	after := r.e.SSNMKnowledge()
	if len(after.Partitions[0].Destinations) != 1 {
		t.Fatal("history-only destination disappeared from snapshot")
	}
	got := after.Partitions[0].Destinations[0]
	if got.CongestionSet || len(got.CongestionSources) != 1 || got.CongestionSources[0].AS.RoutingContext != 1 {
		t.Fatal("history-only survivor changed", got)
	}
	replica := before.Partitions[0]
	for revision := before.Revision; revision < after.Revision; {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		event, err := sub.Next(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		revision = event.Revision
		if event.Kind == SSNMDestinationsInvalidatedEvent {
			event.ApplyInvalidation(&replica)
			event.ApplyInvalidation(&replica)
		}
	}
	if !reflect.DeepEqual(replica, after.Partitions[0]) {
		t.Fatal("history-only invalidation replay mismatch", replica, after.Partitions[0])
	}
}

func TestValidityRetentionMatchesResolvedSourceByteReservations(t *testing.T) {
	for _, limit := range []int{551, 552} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			e := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxBytes: limit})
			now := time.Now()
			configureAutomationForTest(e, &now)
			e.ssnm.auditing = nil
			a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			a.noteRoutingContextsAcked(params.NewRoutingContext(1))
			if !e.trackAssociation(a) {
				t.Fatal("track")
			}
			t.Cleanup(func() { _ = a.Close() })
			err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil))
			if limit == 552 {
				if err != nil || e.ssnm.records != 2 || e.ssnm.bytes != 552 {
					t.Fatal("resolved validity exact-fit reservation disagrees", err, e.ssnm.records, e.ssnm.bytes)
				}
			} else if !errors.Is(err, ErrSSNMStateLimit) || e.ssnm.records != 0 || e.ssnm.bytes != 256 {
				t.Fatal("undersized source reservation admitted", err, e.ssnm.records, e.ssnm.bytes)
			}
		})
	}
	for _, limit := range []int{399, 400} {
		_, err := newSSNMState(&SSNMStateConfig{MaxBytes: limit})
		if (err == nil) != (limit == 400) {
			t.Fatal("minimum budget disagrees with scope retention", limit, err)
		}
	}
}

// This consumer deliberately does not call ApplyInvalidation.
func applySourceRemovalIndependently(partition *SSNMPartitionKnowledge, event SSNMEvent) {
	if event.Kind != SSNMDestinationsInvalidatedEvent || event.Epoch != partition.Epoch {
		return
	}
	var kept []SSNMDestinationKnowledge
	for _, value := range partition.Destinations {
		for _, removed := range event.Invalidated {
			if removed.Destination == value.Destination {
				if removed.AvailabilityRemoved {
					value.AvailabilitySet = false
					value.Availability = SSNMAvailability{}
				}
				if removed.CongestionRemoved {
					value.CongestionSet = false
					value.Congestion = SSNMCongestion{}
				}
			}
		}
		var sources []SSNMCongestion
		for _, source := range value.CongestionSources {
			remove := false
			for _, id := range event.CongestionSourcesInvalidated {
				if id.Destination == value.Destination && id.Revision == source.Revision && id.AS == source.AS {
					remove = true
				}
			}
			if !remove {
				sources = append(sources, source)
			}
		}
		value.CongestionSources = sources
		if value.AvailabilitySet || value.CongestionSet || len(sources) > 0 {
			kept = append(kept, value)
		}
	}
	partition.Destinations = kept
}

func TestSourceInvalidationChunksAreBoundedAndReplayIndependently(t *testing.T) {
	state, err := newSSNMState(&SSNMStateConfig{MaxRecords: 10000, MaxRecordsPerPartition: 10000, MaxRecordsPerPeer: 10000})
	if err != nil {
		t.Fatal(err)
	}
	state.validity = time.Hour
	state.registerSource(1)
	partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: 1}
	if err = state.bind(partition, 1, false); err != nil {
		t.Fatal(err)
	}
	scope := testWireScope(7, true, 1)
	as := *staticASKey(7, 1)
	for start := 0; start < 4000; start += 500 {
		destinations := make([]PointCodeRange, 500)
		for i := range destinations {
			destinations[i].PointCode = uint32(start + i)
		}
		if err = state.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: 1, Partition: partition, Scope: scope, Destinations: destinations, CongestionLevel: 2, CongestionLevelSet: true}, []ASKey{as}); err != nil {
			t.Fatal(err)
		}
	}
	state.mu.Lock()
	before := state.snapshotLocked()
	sub := newSSNMSubscription(state, state.limits.SubscriptionQueueSize, state.limits.SubscriptionQueueBytes)
	state.subscribers[sub] = struct{}{}
	state.mu.Unlock()
	defer func() { _ = sub.Close() }()
	independent := state.snapshot().Partitions[0]
	helper := before.Partitions[0]
	err = state.oversized([]SSNMPartition{partition}, 2, 1, 1, ssnmDestinationRefusal{resolved: true, keys: []ASKey{as}})
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := state.snapshot()
	chunks := 0
	dimensionCount, sourceCount := 0, 0
	for revision := before.Revision; revision < after.Revision; {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		event, err := sub.Next(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		revision = event.Revision
		if event.Kind != SSNMDestinationsInvalidatedEvent {
			continue
		}
		chunks++
		if len(event.Invalidated) > 2048 || len(event.CongestionSourcesInvalidated) > 2048 || event.Partition != (SSNMPartition{}) {
			t.Fatal("source removal publication exceeded compact bound")
		}
		bytes, ok := ssnmEventAccountedBytes(event, 131744)
		if !ok || bytes > 131744 {
			t.Fatal("source removal byte bound exceeded", bytes, ok)
		}
		dimensionCount += len(event.Invalidated)
		sourceCount += len(event.CongestionSourcesInvalidated)
		if !event.ApplyInvalidation(&helper) {
			t.Fatal("helper refused matching epoch")
		}
		applySourceRemovalIndependently(&independent, event)
	}
	if chunks != 2 || dimensionCount != 4000 || sourceCount != 4000 || !reflect.DeepEqual(helper, after.Partitions[0]) || !reflect.DeepEqual(independent, after.Partitions[0]) {
		t.Fatal("chunk replay mismatched exact source removal", chunks, dimensionCount, sourceCount)
	}
	sub.mu.Lock()
	lost := sub.continuityLost
	sub.mu.Unlock()
	if lost {
		t.Fatal("bounded invalidation lost default continuity")
	}
	if state.records != 0 || state.bytes != 256 {
		t.Fatal("source removal failed to release charges", state.records, state.bytes)
	}
}

func TestSourceRemovalIDsAreOwnedAcrossSubscribers(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	_, one, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = one.Close() }()
	_, two, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = two.Close() }()
	err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(200, 201), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	next := func(sub *SSNMSubscription) SSNMEvent {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			event, err := sub.Next(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == SSNMDestinationsInvalidatedEvent {
				return event
			}
		}
	}
	a := next(one)
	b := next(two)
	a.CongestionSourcesInvalidated[0].AS.RoutingContext = 999
	if b.CongestionSourcesInvalidated[0].AS.RoutingContext != 1 {
		t.Fatal("source removal IDs alias another subscriber")
	}
}

func TestOptionalAuditSubmissionObservationSurvivesReportRefresh(t *testing.T) {
	r := newAuditTestRig(t)
	sendDUNA(t, r.a, 7, 1, 100)
	_, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	r.a.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.DestinationStateAudit); ok {
			sendDUNA(t, r.a, 7, 1, 100)
		}
		return 0, nil
	}
	r.advance(30 * time.Second)
	local := 0
	sub.mu.Lock()
	for _, queued := range sub.queue {
		if queued.event.Kind == SSNMReportEvent && queued.event.Report.Kind == SSNMDestinationStateAuditReport && queued.event.Report.Source == SSNMLocalReport {
			local++
		}
	}
	sub.mu.Unlock()
	if local != 1 {
		t.Fatal("accepted audit lost local observation during report refresh", local)
	}
}

func TestReceiveAdmissionPrecedesLegacyRouteResolution(t *testing.T) {
	for _, kind := range []string{"congestion", "availability"} {
		t.Run(kind, func(t *testing.T) {
			e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
			now := time.Now()
			configureAutomationForTest(e, &now)
			a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1, 3)
			key := *staticASKey(7, 9)
			a.addDynamicASKey(key, testRoutingKey(9, 100, params.ServiceIndSCCP), false)
			a.noteCanonicalRemoteAS(9, "as-core")
			a.noteRoutingContextsActive([]uint32{1, 3, 9})
			a.noteRoutingContextsAcked(params.NewRoutingContext(1, 3, 9))
			resolved := false
			e.aspRoutes.applyResolved = func() {
				resolved = true
				a.removeRequesterRoutingKeyVersion(9, a.dynamicASKeyVersionFor(9, false))
				a.addDynamicASKey(key, testRoutingKey(9, 100, params.ServiceIndSCCP), false)
				a.noteCanonicalRemoteAS(9, "as-core")
				a.noteRoutingContextsActive([]uint32{1, 3, 9})
				a.noteRoutingContextsAcked(params.NewRoutingContext(1, 3, 9))
			}
			send := func() error {
				if kind == "congestion" {
					return a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(9), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil))
				}
				return a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(9), params.NewAffectedPointCode(100), nil))
			}
			if err := send(); err != nil {
				t.Fatal(err)
			}
			e.aspRoutes.applyResolved = nil
			if !resolved {
				t.Fatal("report did not reach existing route-resolution seam")
			}
			for _, partition := range e.SSNMKnowledge().Partitions {
				if len(partition.Destinations) != 0 {
					t.Fatal("old receive report was relabeled as replacement registration", partition)
				}
			}
			if len(e.ssnm.audits) != 0 {
				t.Fatal("old receive report scheduled a replacement-registration audit")
			}
			if err := send(); err != nil {
				t.Fatal(err)
			}
			if len(e.ssnm.audits) != 1 {
				t.Fatal("replacement registration did not accept fresh report")
			}
		})
	}
}

func TestOversizedReceiveAdmissionCannotInvalidateReplacement(t *testing.T) {
	e, a, key, _ := newDynamicValidityAssociation(t, true)
	scope := testWireScope(7, true, 7)
	admission := a.captureReceivedSSNMAdmission(scope)
	a.removeRequesterRoutingKeyVersion(7, a.dynamicASKeyVersionFor(7, false))
	a.addDynamicASKey(key, testRoutingKey(7, 100, params.ServiceIndSCCP), false)
	a.noteRoutingContextsActive([]uint32{7, 8})
	a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
	sendSCON(t, a, 7, 7, congestionLevel(3), 100)
	before := e.SSNMKnowledge()
	jobs, records, bytes := len(e.ssnm.audits), e.ssnm.records, e.ssnm.bytes
	err := a.refuseOversizedReceivedSSNM(scope, 1025, 1024, SSNMDestinationUnavailableReport, admission)
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(e.SSNMKnowledge(), before) || len(e.ssnm.audits) != jobs || e.ssnm.records != records || e.ssnm.bytes != bytes {
		t.Fatal("stale oversized receive invalidated replacement lifetime")
	}
}

func TestScopedInvalidationRefreshesIPSPValidityProjection(t *testing.T) {
	for _, expired := range []bool{false, true} {
		for _, transport := range []bool{false, true} {
			t.Run(strconv.FormatBool(expired)+"/transport="+strconv.FormatBool(transport), func(t *testing.T) {
				first, second := *staticASKey(7, 1), *staticASKey(7, 2)
				one, two := MTPDestination{MTPRoute: "one", PointCode: 100}, MTPDestination{MTPRoute: "two", PointCode: 100}
				e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, SSNMState: &SSNMStateConfig{MaxAffectedPointCodes: 1}, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}, IPSPDestinations: []IPSPDestinationConfig{{Destination: one, AS: first}, {Destination: two, AS: second}}})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				now := time.Now()
				e.ssnm.validity = 10 * time.Second
				e.ssnm.now = func() time.Time { return now }
				a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1, 2)
				a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
				setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
				a.noteRoutingContextsActive([]uint32{1, 2})
				a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				if !e.trackAssociation(a) {
					t.Fatal("track")
				}
				t.Cleanup(func() { _ = a.Close() })
				level := uint8(0)
				if transport {
					level = 1
					a.transportSnapshot.Store(&TransportCongestionState{Association: a.ID(), Epoch: a.Epoch(), Known: true, SampleValid: true, Level: level})
				}
				sendSCON(t, a, 7, 1, congestionLevel(2), 100)
				now = now.Add(time.Second)
				sendSCON(t, a, 7, 2, congestionLevel(3), 100)
				if expired {
					now = now.Add(9 * time.Second)
					e.runCongestionAutomation(now)
				}
				before, _ := e.MTPDestinationStatus(one)
				otherBefore, _ := e.MTPDestinationStatus(two)
				if before.ReceivedCongestionUnknown != expired || !expired && (!before.Congested || before.CongestionLevel != 2) {
					t.Fatal("setup did not retain target source", before)
				}
				err = a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(200, 201), nil, params.NewCongestionIndications(2), nil))
				if !errors.Is(err, ErrSSNMOversizedReport) {
					t.Fatal(err)
				}
				immediate, _ := e.MTPDestinationStatus(one)
				otherAfter, _ := e.MTPDestinationStatus(two)
				e.refreshIPSPDestinations()
				recomputed, _ := e.MTPDestinationStatus(one)
				if otherAfter != otherBefore {
					t.Fatal("scoped invalidation changed unrelated AS projection", otherBefore, otherAfter)
				}
				if immediate != recomputed {
					t.Fatal("invalidation left stale cached IPSP received projection", immediate, recomputed)
				}
				if immediate.ReceivedCongestionUnknown || immediate.Congested != transport || immediate.TransportCongestionLevel != level || transport && immediate.CongestionLevel != level {
					t.Fatal("invalidation erased transport or retained removed received source", immediate)
				}
				assertSSNMScopeAccounting(t, e)
			})
		}
	}
}
