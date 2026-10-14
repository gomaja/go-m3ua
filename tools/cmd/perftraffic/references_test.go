package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

func routeReferenceArguments(extra ...string) []string {
	return append([]string{
		"-role=asp", "-transport=dial", "-mode=routed-direct", "-sctp-address=127.0.0.1:2905", "-local-address=127.0.0.1:0",
		"-peer-control=http://127.0.0.1:8080", "-associations=8", "-payload=mix", "-cohort=f6", "-same-host-clock",
	}, extra...)
}

func TestRouteReferenceFlagsAreOptInAndBounded(testContext *testing.T) {
	churn, err := parseConfig(routeReferenceArguments("-route-references=churn"))
	if err != nil || churn.RouteReferences != (routeReferenceConfig{Mode: routeReferencesChurn, Rate: routeReferenceDefaultRate}) {
		testContext.Fatalf("churn: %+v %v", churn.RouteReferences, err)
	}
	static, err := parseConfig(routeReferenceArguments("-route-references=static"))
	if err != nil || static.RouteReferences != (routeReferenceConfig{Mode: routeReferencesStatic}) {
		testContext.Fatalf("static: %+v %v", static.RouteReferences, err)
	}
	plain, err := parseConfig(routeReferenceArguments())
	if err != nil || plain.RouteReferences.enabled() {
		testContext.Fatalf("routed-direct without the flag changed: %+v %v", plain.RouteReferences, err)
	}
	if *churn.RouteReferences.spec() != (routeReferenceSpec{Mode: "churn", Rate: 1000, Peak: 1000, Stable: 1000, Cycle: "0-1-1000-0"}) ||
		*static.RouteReferences.spec() != (routeReferenceSpec{Mode: "static", Stable: 1000, Cycle: "static"}) {
		testContext.Fatalf("specs %+v %+v", *churn.RouteReferences.spec(), *static.RouteReferences.spec())
	}
	for name, arguments := range map[string][]string{
		"unknown mode":        routeReferenceArguments("-route-references=sometimes"),
		"routed MTPTransfer":  routeReferenceArguments("-mode=routed", "-route-references=churn"),
		"no shared clock":     {"-role=asp", "-transport=dial", "-mode=routed-direct", "-sctp-address=127.0.0.1:2905", "-local-address=127.0.0.1:0", "-peer-control=http://127.0.0.1:8080", "-associations=8", "-payload=mix", "-cohort=f6", "-route-references=churn"},
		"SGP":                 {"-role=sgp", "-mode=routed-direct", "-sctp-address=127.0.0.1:2905", "-associations=8", "-same-host-clock", "-route-references=churn"},
		"zero rate":           routeReferenceArguments("-route-references=churn", "-route-reference-rate=0"),
		"excessive rate":      routeReferenceArguments("-route-references=churn", "-route-reference-rate=100001"),
		"rate on the control": routeReferenceArguments("-route-references=static", "-route-reference-rate=1000"),
		"rate without mode":   routeReferenceArguments("-route-reference-rate=1000"),
		"with SSNM load":      routeReferenceArguments("-route-references=churn", "-ssnm-rate=10"),
	} {
		if _, err := parseConfig(arguments); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
}

func routeReferenceSpecFixture(mode string) runSpec {
	specification := routedSpec(modeRoutedDirect)
	specification.Clock = &sharedClockWindow{Domain: sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "b", TimeNamespace: "t", Resolution: 1}, Start: 10, End: 10 + int64(specification.Duration)}
	specification.RouteReferences = routeReferenceConfig{Mode: mode, Rate: 1000}.spec()
	return specification
}

func TestRouteReferenceSpecIsValidatedAndComparedByValue(testContext *testing.T) {
	encoded, err := json.Marshal(routedSpec(modeRoutedDirect))
	if err != nil || strings.Contains(string(encoded), "route_references") {
		testContext.Fatalf("a nominal spec serializes route references: %s", encoded)
	}
	for _, mode := range []string{routeReferencesChurn, routeReferencesStatic} {
		if err := validateRouteReferenceSpec(routeReferenceSpecFixture(mode)); err != nil {
			testContext.Fatalf("%s refused: %v", mode, err)
		}
	}
	churn := routeReferenceSpecFixture(routeReferencesChurn)
	copied := copyRunSpec(churn)
	if copied.RouteReferences == churn.RouteReferences || !sameRunSpec(copied, churn) {
		testContext.Fatal("copyRunSpec shares or loses the route-reference declaration")
	}
	if static := routeReferenceSpecFixture(routeReferencesStatic); sameRunSpec(static, churn) {
		testContext.Fatal("sameRunSpec equates the churned cohort with its control")
	}
	for name, mutate := range map[string]func(*runSpec){
		"routed mode": func(specification *runSpec) { specification.Mode = modeRouted },
		"no clock":    func(specification *runSpec) { specification.Clock = nil },
		"peak":        func(specification *runSpec) { specification.RouteReferences.Peak = 999 },
		"cycle":       func(specification *runSpec) { specification.RouteReferences.Cycle = "0-1000" },
		"zero rate":   func(specification *runSpec) { specification.RouteReferences.Rate = 0 },
		"static rate": func(specification *runSpec) {
			*specification.RouteReferences = routeReferenceSpec{Mode: "static", Rate: 5, Stable: 1000, Cycle: "static"}
		},
		"unknown mode":    func(specification *runSpec) { specification.RouteReferences.Mode = "some" },
		"excessive rate":  func(specification *runSpec) { specification.RouteReferences.Rate = routeReferenceMaxRate + 1 },
		"stable coverage": func(specification *runSpec) { specification.RouteReferences.Stable = 999 },
	} {
		specification := copyRunSpec(churn)
		mutate(&specification)
		if err := validateRouteReferenceSpec(specification); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
	control, _ := routedReadyControl(testContext, modeRouted)
	specification := routedSpec(modeRouted)
	specification.RouteReferences = routeReferenceConfig{Mode: routeReferencesChurn, Rate: 1000}.spec()
	if err := control.reset(specification); err == nil {
		testContext.Fatal("a routed MTPTransfer receiver accepted route references")
	}
}

func TestApplicationRouteTableResolvesStableReferencesAndCountsChurn(testContext *testing.T) {
	_, _, _, paths := routedPeerPathsFixture(testContext)
	table, err := newApplicationRouteTable(paths)
	if err != nil {
		testContext.Fatal(err)
	}
	for route := range routingRouteCount {
		got, err := table.resolve(uint16(route))
		want, _ := paths.path(uint16(route))
		if err != nil || got != want {
			testContext.Fatalf("route %d resolved %+v, want %+v", route, got, want)
		}
	}
	if _, err := table.resolve(routingRouteCount); err == nil {
		testContext.Fatal("resolved a route outside the table")
	}
	first, size := table.add(applicationRouteReference{id: 1, association: 7})
	second, _ := table.add(applicationRouteReference{id: 2, association: 7})
	third, size3 := table.add(applicationRouteReference{id: 3, association: 8})
	if !first || second || !third || size != 1 || size3 != 3 {
		testContext.Fatalf("first-reference flags %v %v %v sizes %d %d", first, second, third, size, size3)
	}
	for _, want := range []struct {
		id   uint64
		last bool
		size int
	}{{3, true, 2}, {2, false, 1}, {1, true, 0}} {
		reference, last, size, removed := table.remove()
		if !removed || reference.id != want.id || last != want.last || size != want.size {
			testContext.Fatalf("remove = %+v last %v size %d, want %+v", reference, last, size, want)
		}
	}
	if _, _, _, removed := table.remove(); removed {
		testContext.Fatal("removed from an empty churned set")
	}
}

// TestRoutedDirectWriterResolvesThroughTheApplicationTable proves the DATA
// path consults the application table, not the frozen map, once a table is
// installed.
func TestRoutedDirectWriterResolvesThroughTheApplicationTable(testContext *testing.T) {
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	paths, writer, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, &fakeRoutingDataRemote{receipts: receipts})
	if err != nil {
		testContext.Fatal(err)
	}
	table, err := newApplicationRouteTable(paths)
	if err != nil {
		testContext.Fatal(err)
	}
	route := uint16(0)
	frozen, _ := paths.path(route)
	other, _ := paths.path(route + 1)
	if frozen.Target.Association == other.Target.Association {
		testContext.Fatal("fixture routes share an association")
	}
	table.stable[route] = other
	writer.table = table
	payload, err := buildRoutePayload(planRouteMessage("cohort", 7, uint64(route)), 128)
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err := writer.Write(context.Background(), route, payload); err != nil {
		testContext.Fatal(err)
	}
	moved := plane.associations[other.Target.Association].(*fakeRoutingDataAssociation)
	unused := plane.associations[frozen.Target.Association].(*fakeRoutingDataAssociation)
	if len(moved.writes) != 1 || len(unused.writes) != 0 || moved.writes[0].AS != other.Target.AS {
		testContext.Fatalf("writes: table target %d, frozen target %d", len(moved.writes), len(unused.writes))
	}
}

// fakeReferenceTarget is a live or ended association for the churn.
type fakeReferenceTarget struct {
	id    m3ua.AssociationID
	epoch uint64
	done  chan struct{}
}

func (target *fakeReferenceTarget) ID() m3ua.AssociationID { return target.id }
func (target *fakeReferenceTarget) Epoch() uint64          { return target.epoch }
func (target *fakeReferenceTarget) Done() <-chan struct{}  { return target.done }

func referenceTargets(count int) []routeReferenceTarget {
	targets := make([]routeReferenceTarget, count)
	for index := range targets {
		targets[index] = &fakeReferenceTarget{id: m3ua.AssociationID(index + 1), epoch: uint64(10 + index), done: make(chan struct{})}
	}
	return targets
}

func TestRouteReferenceChurnCyclesOpenLoopAndReportsItsWindow(testContext *testing.T) {
	_, _, _, paths := routedPeerPathsFixture(testContext)
	table, _ := newApplicationRouteTable(paths)
	// The churn runs on a stepped clock, so its lag is at most one step
	// however the host schedules it: on the wall clock a shared CI runner has
	// held it off the CPU past the 100 ms tolerance.
	clock := &fakeMeasurementClock{}
	clock.now.Store(int64(time.Hour))
	const rate = 5000
	churner, err := newRouteReferenceChurner(table, referenceTargets(8), clock, rate, time.Minute)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := churner.start(context.Background()); err != nil {
		testContext.Fatal(err)
	}
	completed := func() int {
		churner.mutex.Lock()
		defer churner.mutex.Unlock()
		return len(churner.completed)
	}
	const step = 10 * time.Millisecond
	for elapsed := step; elapsed <= 1200*time.Millisecond; elapsed += step {
		clock.now.Add(int64(step))
		// Operation k is due at k/rate from the anchor, so these are due.
		due := int(uint64(elapsed)*rate/uint64(time.Second)) + 1
		deadline := time.Now().Add(10 * time.Second)
		for completed() < due {
			if time.Now().After(deadline) {
				testContext.Fatalf("%d of %d operations due at %v completed", completed(), due, elapsed)
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
	churner.stop()
	// A cycle is 2,000 operations, 400 ms at this rate: the window holds the
	// cycle ends at 400 ms and 800 ms.
	start, end := churner.anchor+int64(100*time.Millisecond), churner.anchor+int64(time.Second)
	evidence := churner.evidence(start, end)
	if evidence.Scheduled != 4500 || evidence.PeakReached != routeReferencePeak || evidence.InvalidTargets != 0 || evidence.ClockFailure != "" ||
		evidence.Completed != evidence.Scheduled || evidence.CompletedCycles != 2 || evidence.FirstReferences != 16 || evidence.LastReferences != 16 {
		testContext.Fatalf("evidence %+v", evidence)
	}
	record := &routeReferenceRecord{Churn: &evidence}
	criteria := judgeRouteReferences(routeReferenceSpec{Mode: routeReferencesChurn, Rate: rate, Peak: routeReferencePeak}, record)
	if criteria[0].Name != "churn_intensity" || criteria[0].Outcome != failoverPass || criteria[1].Outcome != failoverPass {
		testContext.Fatalf("criteria %+v", criteria[:2])
	}
	future := churner.evidence(start, churner.anchor+int64(2*time.Second))
	if future.Completed == future.Scheduled {
		testContext.Fatalf("a window past the churn's end looks held: %+v", future)
	}
	if outcome := judgeRouteReferences(routeReferenceSpec{Mode: routeReferencesChurn, Rate: rate, Peak: routeReferencePeak}, &routeReferenceRecord{Churn: &future})[0].Outcome; outcome != failoverNotMeasured {
		testContext.Fatalf("unheld intensity outcome = %s", outcome)
	}
	if before := churner.evidence(churner.anchor-1, end); before.ClockFailure == "" {
		testContext.Fatal("a window before the anchor was accepted")
	}
}

func TestRouteReferenceChurnRefusesDeadTargets(testContext *testing.T) {
	_, _, _, paths := routedPeerPathsFixture(testContext)
	table, _ := newApplicationRouteTable(paths)
	targets := referenceTargets(8)
	close(targets[3].(*fakeReferenceTarget).done)
	targets[5].(*fakeReferenceTarget).epoch++
	churner, err := newRouteReferenceChurner(table, targets, wallMeasurementClock{origin: time.Now()}, 1000, time.Minute)
	if err != nil {
		testContext.Fatal(err)
	}
	targets[5].(*fakeReferenceTarget).epoch++
	for operation := range uint64(16) {
		churner.operate(operation)
	}
	evidence := churner.evidence(churner.anchor, churner.anchor+1)
	if evidence.InvalidTargets != 4 || evidence.InvalidDetail == "" {
		testContext.Fatalf("invalid targets %d (%s), want the two dead and two changed-epoch operations", evidence.InvalidTargets, evidence.InvalidDetail)
	}
	record := &routeReferenceRecord{Churn: &evidence}
	if criterion := judgeRouteReferences(routeReferenceSpec{Mode: routeReferencesChurn}, record)[1]; criterion.Outcome != failoverFail {
		testContext.Fatalf("dead-target criterion = %+v", criterion)
	}
}

func activeReferenceState(sender bool) routeReferenceState {
	var state routeReferenceState
	for index := range routedAssociations {
		state.Associations = append(state.Associations, routeReferenceAssociationState{Association: m3ua.AssociationID(index + 1), Epoch: uint64(index + 20), State: m3ua.StateASPActive.String()})
		status := m3ua.ASPStatus{Key: m3ua.ASPStatusKey{Association: m3ua.AssociationID(index + 1)}}
		if sender {
			status.LocalState, status.LocalStateSet = m3ua.StateASPActive, true
		} else {
			status.PeerState, status.PeerStateSet = m3ua.StateASPActive, true
		}
		state.ASPStatuses = append(state.ASPStatuses, status)
	}
	if !sender {
		state.ApplicationServers = []m3ua.ApplicationServerStatus{{State: m3ua.ASActive, ActiveASPs: []m3ua.AssociationID{1, 2}}}
	}
	return state
}

func routeReferenceRecordFixture() *routeReferenceRecord {
	snapshot := func() routeReferenceSnapshot {
		return routeReferenceSnapshot{Sender: activeReferenceState(true), Receiver: activeReferenceState(false)}
	}
	return &routeReferenceRecord{Before: snapshot(), After: snapshot()}
}

func TestRouteReferenceJudgementGatesLibraryActivity(testContext *testing.T) {
	static := routeReferenceSpec{Mode: routeReferencesStatic, Stable: routingRouteCount, Cycle: routeReferenceStaticCycle}
	criteria := judgeRouteReferences(static, routeReferenceRecordFixture())
	if len(criteria) != 3 {
		testContext.Fatalf("static criteria %+v", criteria)
	}
	for _, criterion := range criteria {
		if criterion.Outcome != failoverPass {
			testContext.Fatalf("clean static cohort: %+v", criterion)
		}
	}
	for _, test := range []struct {
		name, criterion, outcome string
		mutate                   func(*routeReferenceRecord)
	}{
		{name: "association ended", criterion: "no_association_reconnection", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.Events = []routeReferenceEvent{{Kind: "association-ended"}}
		}},
		{name: "sender epoch changed", criterion: "no_association_reconnection", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.After.Sender.Associations[2].Epoch++
		}},
		{name: "receiver association replaced", criterion: "no_association_reconnection", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.After.Receiver.Associations[7].Association = 99
		}},
		{name: "ASP state change", criterion: "no_as_deactivation", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.Events = []routeReferenceEvent{{Kind: "state-change", Detail: "ASP-INACTIVE"}}
		}},
		{name: "AS inactive afterwards", criterion: "no_as_deactivation", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.After.Receiver.ApplicationServers[0].State = m3ua.ASInactive
		}},
		{name: "ASP not active before", criterion: "no_as_deactivation", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.Before.Sender.ASPStatuses[0].LocalState = m3ua.StateASPInactive
			record.After.Sender.ASPStatuses[0].LocalState = m3ua.StateASPInactive
		}},
		{name: "active set changed", criterion: "no_as_deactivation", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.After.Receiver.ApplicationServers[0].ActiveASPs = []m3ua.AssociationID{1}
		}},
		{name: "management indication", criterion: "no_library_indications", outcome: failoverFail, mutate: func(record *routeReferenceRecord) {
			record.Events = []routeReferenceEvent{{Kind: "management-indication"}}
		}},
		{name: "receiver unreadable", criterion: "no_association_reconnection", outcome: failoverNotMeasured, mutate: func(record *routeReferenceRecord) {
			record.After.Error = "connection refused"
		}},
	} {
		record := routeReferenceRecordFixture()
		test.mutate(record)
		found := false
		for _, criterion := range judgeRouteReferences(static, record) {
			if criterion.Name == test.criterion {
				found = true
				if criterion.Outcome != test.outcome {
					testContext.Errorf("%s: %s = %s (%s)", test.name, criterion.Name, criterion.Outcome, criterion.Detail)
				}
			}
		}
		if !found {
			testContext.Errorf("%s: criterion %s missing", test.name, test.criterion)
		}
	}
}

func TestRouteReferenceRecordsStayOutOfNominalRecords(testContext *testing.T) {
	record, err := json.Marshal(runRecord{Spec: routedSpec(modeRoutedDirect)})
	if err != nil || strings.Contains(string(record), "route_references") {
		testContext.Fatalf("a nominal record serializes route references: %v", err)
	}
}
