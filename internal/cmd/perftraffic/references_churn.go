package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

// applicationRouteTable is the application's own route table: a stable
// reference per route, which the DATA path resolves every message through,
// and a churned set of references to live associations. Nothing here calls
// the library. Adding a reference validates its association with read-only
// public state; removing one, including an association's last reference, only
// deletes it from the table.
type applicationRouteTable struct {
	mutex   sync.RWMutex
	stable  [routingRouteCount]routingResolvedPath
	churned []applicationRouteReference
	counts  map[m3ua.AssociationID]int
}

// applicationRouteReference is one churned reference to an association.
type applicationRouteReference struct {
	id          uint64
	association m3ua.AssociationID
}

func newApplicationRouteTable(paths routingPathMap) (*applicationRouteTable, error) {
	if !paths.ready {
		return nil, errors.New("the application route table needs the frozen paths")
	}
	table := &applicationRouteTable{counts: make(map[m3ua.AssociationID]int), churned: make([]applicationRouteReference, 0, routeReferencePeak)}
	table.stable = paths.paths
	return table, nil
}

// resolve returns a route's stable reference under the table's read lock,
// which the churn takes for writing.
func (table *applicationRouteTable) resolve(route uint16) (routingResolvedPath, error) {
	if route >= routingRouteCount {
		return routingResolvedPath{}, errors.New("routing path is not frozen")
	}
	table.mutex.RLock()
	path := table.stable[route]
	table.mutex.RUnlock()
	return path, nil
}

// add inserts a reference and reports whether it is its association's first.
func (table *applicationRouteTable) add(reference applicationRouteReference) (bool, int) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	table.churned = append(table.churned, reference)
	table.counts[reference.association]++
	return table.counts[reference.association] == 1, len(table.churned)
}

// remove deletes the most recently added reference and reports whether it
// was its association's last one and how many references remain.
func (table *applicationRouteTable) remove() (applicationRouteReference, bool, int, bool) {
	table.mutex.Lock()
	defer table.mutex.Unlock()
	if len(table.churned) == 0 {
		return applicationRouteReference{}, false, 0, false
	}
	reference := table.churned[len(table.churned)-1]
	table.churned = table.churned[:len(table.churned)-1]
	table.counts[reference.association]--
	last := table.counts[reference.association] == 0
	if last {
		delete(table.counts, reference.association)
	}
	return reference, last, len(table.churned), true
}

// routeReferenceTarget is what the churn reads to validate a new reference:
// public, read-only association state.
type routeReferenceTarget interface {
	ID() m3ua.AssociationID
	Epoch() uint64
	Done() <-chan struct{}
}

// routeReferenceTransition is an association gaining its first or losing its
// last churned reference.
type routeReferenceTransition struct {
	at          int64
	association m3ua.AssociationID
	first       bool
}

// routeReferenceChurner runs the churn open-loop on the shared clock:
// operation k is due at anchor + floor(k*1s/rate); a late churner catches up
// in order and never skips. The churned set cycles 0 -> 1 -> peak -> 0.
type routeReferenceChurner struct {
	table   *applicationRouteTable
	targets []routeReferenceTarget
	epochs  []uint64
	clock   measurementClock
	rate    uint64
	peak    int
	anchor  int64
	cancel  context.CancelFunc
	done    chan struct{}

	mutex         sync.Mutex
	completed     []int64
	transitions   []routeReferenceTransition
	cycleEnds     []int64
	peakReached   int
	invalidTarget uint64
	invalidDetail string
	clockFailure  string
	operation     *durationHistogram
}

func newRouteReferenceChurner(table *applicationRouteTable, targets []routeReferenceTarget, clock measurementClock, rate uint64, horizon time.Duration) (*routeReferenceChurner, error) {
	if table == nil || len(targets) == 0 || clock == nil || rate == 0 {
		return nil, errors.New("route-reference churn needs the table, live targets, the shared clock and a rate")
	}
	churner := &routeReferenceChurner{
		table: table, targets: targets, epochs: make([]uint64, len(targets)), clock: clock, rate: rate, peak: routeReferencePeak,
		done: make(chan struct{}), operation: newDurationHistogram(),
		completed: make([]int64, 0, min(uint64(horizon.Seconds()+1)*rate, 1<<22)),
	}
	for index, target := range targets {
		churner.epochs[index] = target.Epoch()
	}
	return churner, nil
}

// start anchors the schedule at the current shared-clock instant.
func (churner *routeReferenceChurner) start(ctx context.Context) error {
	anchor, err := churner.clock.Now()
	if err != nil || anchor <= 0 {
		return errors.New("route-reference churn cannot read the shared clock")
	}
	churner.anchor = anchor
	lifetime, cancel := context.WithCancel(ctx)
	churner.cancel = cancel
	go churner.run(lifetime)
	return nil
}

func (churner *routeReferenceChurner) stop() {
	if churner == nil || churner.cancel == nil {
		return
	}
	churner.cancel()
	<-churner.done
}

func (churner *routeReferenceChurner) run(ctx context.Context) {
	defer close(churner.done)
	for operation := uint64(0); ; operation++ {
		due := churner.anchor + int64(operation*uint64(time.Second)/churner.rate)
		if err := waitSharedInstant(ctx, churner.clock, due); err != nil {
			if ctx.Err() == nil {
				churner.mutex.Lock()
				churner.clockFailure = err.Error()
				churner.mutex.Unlock()
			}
			return
		}
		started, startErr := churner.clock.Now()
		churner.operate(operation)
		finished, finishErr := churner.clock.Now()
		churner.mutex.Lock()
		if startErr != nil || finishErr != nil {
			churner.clockFailure = "shared clock read around a reference operation failed"
		}
		churner.completed = append(churner.completed, finished)
		churner.operation.record(time.Duration(finished - started))
		churner.mutex.Unlock()
	}
}

// operate performs operation k: the first peak operations of each cycle add
// a reference, the next peak remove them.
func (churner *routeReferenceChurner) operate(operation uint64) {
	if operation%uint64(2*churner.peak) < uint64(churner.peak) {
		index := int(operation % uint64(len(churner.targets)))
		target := churner.targets[index]
		live := true
		select {
		case <-target.Done():
			live = false
		default:
		}
		if target.Epoch() != churner.epochs[index] {
			live = false
		}
		first, size := churner.table.add(applicationRouteReference{id: operation, association: target.ID()})
		now, _ := churner.clock.Now()
		churner.mutex.Lock()
		defer churner.mutex.Unlock()
		if !live {
			churner.invalidTarget++
			if churner.invalidDetail == "" {
				churner.invalidDetail = fmt.Sprintf("operation %d referenced association %d, which had ended or changed epoch", operation, target.ID())
			}
		}
		churner.peakReached = max(churner.peakReached, size)
		if first {
			churner.transitions = append(churner.transitions, routeReferenceTransition{at: now, association: target.ID(), first: true})
		}
		return
	}
	reference, last, size, removed := churner.table.remove()
	now, _ := churner.clock.Now()
	churner.mutex.Lock()
	defer churner.mutex.Unlock()
	if !removed {
		churner.invalidTarget++
		return
	}
	if last {
		churner.transitions = append(churner.transitions, routeReferenceTransition{at: now, association: reference.association})
	}
	if size == 0 {
		churner.cycleEnds = append(churner.cycleEnds, now)
	}
}

// routeReferenceChurnEvidence is the churn over one cohort window.
type routeReferenceChurnEvidence struct {
	Anchor             int64               `json:"anchor_ns"`
	Rate               uint64              `json:"rate"`
	Peak               int                 `json:"peak"`
	Scheduled          uint64              `json:"scheduled_in_window"`
	Completed          uint64              `json:"completed_in_window"`
	MaximumLag         int64               `json:"maximum_lag_ns"`
	LagTolerance       int64               `json:"lag_tolerance_ns"`
	CompletedCycles    int                 `json:"completed_cycles_in_window"`
	PeakReached        int                 `json:"peak_reached"`
	FirstReferences    int                 `json:"first_reference_transitions_in_window"`
	LastReferences     int                 `json:"last_reference_removals_in_window"`
	InvalidTargets     uint64              `json:"invalid_targets"`
	InvalidDetail      string              `json:"invalid_detail,omitempty"`
	ClockFailure       string              `json:"clock_failure,omitempty"`
	OperationDurations durationPercentiles `json:"operation_duration"`
}

// routeReferenceLagTolerance bounds how late one operation may complete
// before the churn is not held at its declared intensity: one scheduling
// interval, and at least 100 ms.
func routeReferenceLagTolerance(rate uint64) int64 {
	return max(int64(time.Second)/int64(rate), int64(100*time.Millisecond))
}

// evidence reports the operations due in [start, end): how many completed,
// the latest completion relative to its due instant, and the cycles and
// last-reference removals inside the window. An operation due just before
// end may complete just after it; lateness, not the boundary, decides
// whether the churn held its intensity. One never completed is unbounded
// lateness and leaves Completed short of Scheduled.
func (churner *routeReferenceChurner) evidence(start, end int64) routeReferenceChurnEvidence {
	churner.mutex.Lock()
	defer churner.mutex.Unlock()
	evidence := routeReferenceChurnEvidence{
		Anchor: churner.anchor, Rate: churner.rate, Peak: churner.peak, PeakReached: churner.peakReached,
		InvalidTargets: churner.invalidTarget, InvalidDetail: churner.invalidDetail, ClockFailure: churner.clockFailure,
		OperationDurations: churner.operation.percentiles(), LagTolerance: routeReferenceLagTolerance(churner.rate),
	}
	if start < churner.anchor || end <= start {
		evidence.ClockFailure = "the churn schedule does not cover the cohort window"
		return evidence
	}
	first := failoverScheduledIn(churner.rate, ^uint64(0), 0, time.Duration(start-churner.anchor))
	last := failoverScheduledIn(churner.rate, ^uint64(0), 0, time.Duration(end-churner.anchor))
	evidence.Scheduled = last - first
	for operation := first; operation < last && operation < uint64(len(churner.completed)); operation++ {
		due := churner.anchor + int64(operation*uint64(time.Second)/churner.rate)
		evidence.Completed++
		evidence.MaximumLag = max(evidence.MaximumLag, churner.completed[operation]-due)
	}
	for _, at := range churner.cycleEnds {
		if at >= start && at < end {
			evidence.CompletedCycles++
		}
	}
	for _, transition := range churner.transitions {
		if transition.at >= start && transition.at < end {
			if transition.first {
				evidence.FirstReferences++
			} else {
				evidence.LastReferences++
			}
		}
	}
	return evidence
}

// routeReferenceEvent is one library indication observed on the sender.
type routeReferenceEvent struct {
	Association m3ua.AssociationID `json:"association"`
	Kind        string             `json:"kind"`
	Detail      string             `json:"detail"`
	At          int64              `json:"at_ns"`
}

// routeReferenceObserver drains every sender association's StateChanges and
// ManagementIndications and watches Done for the whole run, timestamping each
// on the shared clock. The same observer runs for churn and for the static
// control. Its goroutines end with their association: the library closes both
// channels and Done when it ends, and they must be drained until then, since
// an unread indication channel closes the association.
type routeReferenceObserver struct {
	clock  measurementClock
	mutex  sync.Mutex
	events []routeReferenceEvent
}

func newRouteReferenceObserver(clock measurementClock, associations []*m3ua.Association) *routeReferenceObserver {
	observer := &routeReferenceObserver{clock: clock}
	for _, association := range associations {
		go func(association *m3ua.Association) {
			for state := range association.StateChanges() {
				observer.record(association.ID(), "state-change", state.String())
			}
		}(association)
		go func(association *m3ua.Association) {
			for indication := range association.ManagementIndications() {
				observer.record(association.ID(), "management-indication", fmt.Sprintf("kind %d: %s", indication.Kind, indication.Description))
			}
		}(association)
		go func(association *m3ua.Association) {
			<-association.Done()
			observer.record(association.ID(), "association-ended", fmt.Sprint(association.Err()))
		}(association)
	}
	return observer
}

func (observer *routeReferenceObserver) record(association m3ua.AssociationID, kind, detail string) {
	now, _ := observer.clock.Now()
	observer.mutex.Lock()
	observer.events = append(observer.events, routeReferenceEvent{Association: association, Kind: kind, Detail: detail, At: now})
	observer.mutex.Unlock()
}

func (observer *routeReferenceObserver) between(start, end int64) []routeReferenceEvent {
	observer.mutex.Lock()
	defer observer.mutex.Unlock()
	var events []routeReferenceEvent
	for _, event := range observer.events {
		if event.At >= start && event.At <= end {
			events = append(events, event)
		}
	}
	return events
}

// routeReferenceAssociationState is one association's identity and state.
type routeReferenceAssociationState struct {
	Association m3ua.AssociationID `json:"association"`
	Epoch       uint64             `json:"epoch"`
	State       string             `json:"state"`
}

// routeReferenceState is one side's association, ASP and AS state.
type routeReferenceState struct {
	Associations       []routeReferenceAssociationState `json:"associations"`
	ASPStatuses        []m3ua.ASPStatus                 `json:"asp_statuses"`
	ApplicationServers []m3ua.ApplicationServerStatus   `json:"application_servers"`
}

// routeReferenceStatusSource is the read-only Endpoint status an ASP or SGP
// Endpoint exposes.
type routeReferenceStatusSource interface {
	ASPStatuses() []m3ua.ASPStatus
	ApplicationServerStatuses() []m3ua.ApplicationServerStatus
}

type routeReferenceStateAssociation interface {
	ID() m3ua.AssociationID
	Epoch() uint64
	State() m3ua.State
}

func captureRouteReferenceState(sources []routeReferenceStatusSource, associations []routeReferenceStateAssociation) routeReferenceState {
	var state routeReferenceState
	for _, association := range associations {
		state.Associations = append(state.Associations, routeReferenceAssociationState{Association: association.ID(), Epoch: association.Epoch(), State: association.State().String()})
	}
	sort.Slice(state.Associations, func(first, second int) bool {
		return state.Associations[first].Association < state.Associations[second].Association
	})
	for _, source := range sources {
		state.ASPStatuses = append(state.ASPStatuses, source.ASPStatuses()...)
		state.ApplicationServers = append(state.ApplicationServers, source.ApplicationServerStatuses()...)
	}
	return state
}

// active reports whether every association and ASP status is ASP-ACTIVE and,
// when requireAS is set, every Application Server is AS-ACTIVE.
func (state routeReferenceState) active(requireAS bool) bool {
	if len(state.Associations) != routedAssociations || len(state.ASPStatuses) == 0 {
		return false
	}
	for _, association := range state.Associations {
		if association.State != m3ua.StateASPActive.String() {
			return false
		}
	}
	for _, status := range state.ASPStatuses {
		if (!status.LocalStateSet || status.LocalState != m3ua.StateASPActive) && (!status.PeerStateSet || status.PeerState != m3ua.StateASPActive) {
			return false
		}
	}
	if requireAS {
		for _, server := range state.ApplicationServers {
			if server.State != m3ua.ASActive {
				return false
			}
		}
		return len(state.ApplicationServers) != 0
	}
	return true
}

const routedPeerStatePath = "/routing/peer-state"

// peerStateHandler serves the SGP side's association, ASP and AS state so
// the sender can compare it across a reference cohort. It only reads.
func peerStateHandler(capture func() (routeReferenceState, error)) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			http.Error(writer, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		state, err := capture()
		if err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(writer, http.StatusOK, state)
	})
}

// capturePeerRouteReferenceState reads the four SGP Endpoints and their
// eight associations.
func capturePeerRouteReferenceState(peers *routingPeerSet) (routeReferenceState, error) {
	entries, err := peers.inventoryEntries()
	if err != nil {
		return routeReferenceState{}, err
	}
	var sources []routeReferenceStatusSource
	seen := make(map[routeReferenceStatusSource]bool)
	associations := make([]routeReferenceStateAssociation, 0, len(entries))
	for _, entry := range entries {
		endpoint, valid := entry.endpoint.(routingM3UAEndpoint)
		association, stateful := entry.association.(routeReferenceStateAssociation)
		if !valid || !stateful {
			return routeReferenceState{}, errors.New("routed peer does not expose its Endpoint status")
		}
		if !seen[endpoint] {
			seen[endpoint] = true
			sources = append(sources, endpoint)
		}
		associations = append(associations, association)
	}
	return captureRouteReferenceState(sources, associations), nil
}

func getPeerRouteReferenceState(ctx context.Context, baseURL string) (routeReferenceState, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+routedPeerStatePath, nil)
	if err != nil {
		return routeReferenceState{}, err
	}
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return routeReferenceState{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return routeReferenceState{}, fmt.Errorf("peer state: %s", response.Status)
	}
	var state routeReferenceState
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&state); err != nil {
		return routeReferenceState{}, err
	}
	return state, nil
}

// routeReferenceRun is the ASP side of the workload for the whole run.
type routeReferenceRun struct {
	spec         routeReferenceSpec
	churner      *routeReferenceChurner
	observer     *routeReferenceObserver
	endpoint     routeReferenceStatusSource
	associations []*m3ua.Association
	peerControl  string
}

// routeReferenceSnapshot is both sides' state at a cohort boundary.
type routeReferenceSnapshot struct {
	Sender   routeReferenceState `json:"sender"`
	Receiver routeReferenceState `json:"receiver"`
	Error    string              `json:"error,omitempty"`
}

func (run *routeReferenceRun) snapshot(ctx context.Context) routeReferenceSnapshot {
	associations := make([]routeReferenceStateAssociation, len(run.associations))
	for index, association := range run.associations {
		associations[index] = association
	}
	snapshot := routeReferenceSnapshot{Sender: captureRouteReferenceState([]routeReferenceStatusSource{run.endpoint}, associations)}
	requestContext, cancel := context.WithTimeout(ctx, routingControlTimeout)
	defer cancel()
	receiver, err := getPeerRouteReferenceState(requestContext, run.peerControl)
	if err != nil {
		snapshot.Error = err.Error()
	}
	snapshot.Receiver = receiver
	return snapshot
}

// routeReferenceCriterion is one judgement with the numbers behind it.
type routeReferenceCriterion struct {
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Detail  string `json:"detail"`
}

// routeReferenceRecord is the sender record's reference evidence.
type routeReferenceRecord struct {
	Workload routeReferenceSpec           `json:"workload"`
	Window   [2]int64                     `json:"window_ns"`
	Churn    *routeReferenceChurnEvidence `json:"churn,omitempty"`
	Before   routeReferenceSnapshot       `json:"before"`
	After    routeReferenceSnapshot       `json:"after"`
	Events   []routeReferenceEvent        `json:"events_in_window,omitempty"`
	Criteria []routeReferenceCriterion    `json:"criteria"`
	Verdict  string                       `json:"verdict"`
}

// evaluate judges one cohort. The activity window runs from the declared
// start through the drain.
func (run *routeReferenceRun) evaluate(specification runSpec, before, after routeReferenceSnapshot) *routeReferenceRecord {
	record := &routeReferenceRecord{Workload: run.spec, Before: before, After: after, Verdict: failoverPass}
	if specification.Clock == nil {
		record.Criteria = []routeReferenceCriterion{{Name: "window", Outcome: failoverNotMeasured, Detail: "the cohort has no shared-clock window"}}
		record.Verdict = verdictInconclusive
		return record
	}
	start, end := specification.Clock.Start, specification.Clock.End
	record.Window = [2]int64{start, end + int64(specification.Drain)}
	record.Events = run.observer.between(record.Window[0], record.Window[1])
	if run.churner != nil {
		churn := run.churner.evidence(start, end)
		record.Churn = &churn
	}
	record.Criteria = judgeRouteReferences(run.spec, record)
	for _, criterion := range record.Criteria {
		switch {
		case criterion.Outcome == failoverFail:
			record.Verdict = failoverFail
		case criterion.Outcome == failoverNotMeasured && record.Verdict == failoverPass:
			record.Verdict = verdictInconclusive
		}
	}
	return record
}

func judgeRouteReferences(spec routeReferenceSpec, record *routeReferenceRecord) []routeReferenceCriterion {
	var criteria []routeReferenceCriterion
	if churn := record.Churn; spec.Mode == routeReferencesChurn {
		intensity := routeReferenceCriterion{Name: "churn_intensity", Outcome: failoverNotMeasured}
		switch {
		case churn == nil || churn.ClockFailure != "":
			intensity.Detail = "the churn schedule was not observed over the window"
			if churn != nil {
				intensity.Detail += ": " + churn.ClockFailure
			}
		case churn.Completed != churn.Scheduled || churn.MaximumLag > churn.LagTolerance:
			intensity.Detail = fmt.Sprintf("%d of %d operations due in the window completed; latest %d ns after its due instant (tolerance %d ns)",
				churn.Completed, churn.Scheduled, churn.MaximumLag, churn.LagTolerance)
		case churn.PeakReached != spec.Peak || churn.CompletedCycles == 0 || churn.LastReferences == 0:
			intensity.Detail = fmt.Sprintf("peak %d of %d, %d completed cycles, %d last-reference removals in the window", churn.PeakReached, spec.Peak, churn.CompletedCycles, churn.LastReferences)
		default:
			intensity.Outcome = failoverPass
			intensity.Detail = fmt.Sprintf("%d add/remove operations due in the window (%d/s), all completed, latest %d ns after its due instant; %d completed 0-1-%d-0 cycles; %d first-reference additions and %d last-reference removals; operation p99 %d ns, max %d ns",
				churn.Completed, churn.Rate, churn.MaximumLag, churn.CompletedCycles, spec.Peak, churn.FirstReferences, churn.LastReferences,
				churn.OperationDurations.P99, churn.OperationDurations.Max)
		}
		criteria = append(criteria, intensity)
		targets := routeReferenceCriterion{Name: "references_to_live_associations", Outcome: failoverPass, Detail: "every added reference named a live association with an unchanged epoch"}
		if churn != nil && churn.InvalidTargets != 0 {
			targets.Outcome, targets.Detail = failoverFail, fmt.Sprintf("%d invalid reference operations: %s", churn.InvalidTargets, churn.InvalidDetail)
		}
		criteria = append(criteria, targets)
	}
	reconnection := routeReferenceCriterion{Name: "no_association_reconnection", Outcome: failoverFail}
	activation := routeReferenceCriterion{Name: "no_as_deactivation", Outcome: failoverFail}
	indications := routeReferenceCriterion{Name: "no_library_indications", Outcome: failoverFail}
	var ended, stateChanges, management int
	for _, event := range record.Events {
		switch event.Kind {
		case "association-ended":
			ended++
		case "state-change":
			stateChanges++
		default:
			management++
		}
	}
	sameAssociations := func(first, second []routeReferenceAssociationState) bool {
		if len(first) != routedAssociations || len(first) != len(second) {
			return false
		}
		for index := range first {
			if first[index].Association != second[index].Association || first[index].Epoch != second[index].Epoch {
				return false
			}
		}
		return true
	}
	switch {
	case record.Before.Error != "" || record.After.Error != "":
		reconnection.Outcome = failoverNotMeasured
		reconnection.Detail = "the receiver state could not be read: " + record.Before.Error + record.After.Error
	case ended != 0 || !sameAssociations(record.Before.Sender.Associations, record.After.Sender.Associations) ||
		!sameAssociations(record.Before.Receiver.Associations, record.After.Receiver.Associations):
		reconnection.Detail = fmt.Sprintf("%d associations ended in the window, or an association identity or epoch changed across it", ended)
	default:
		reconnection.Outcome = failoverPass
		reconnection.Detail = "the same 8 association identities and epochs at both ends before and after the cohort, and none ended in the window"
	}
	switch {
	case record.Before.Error != "" || record.After.Error != "":
		activation.Outcome = failoverNotMeasured
		activation.Detail = "the receiver state could not be read"
	case stateChanges != 0:
		activation.Detail = fmt.Sprintf("%d ASP state changes in the window", stateChanges)
	case !record.Before.Sender.active(false) || !record.Before.Receiver.active(true) ||
		!reflect.DeepEqual(record.Before.Sender, record.After.Sender) || !reflect.DeepEqual(record.Before.Receiver, record.After.Receiver):
		activation.Detail = "the ASP or AS state was not ASP-ACTIVE/AS-ACTIVE before the cohort or differs after it"
	default:
		activation.Outcome = failoverPass
		activation.Detail = fmt.Sprintf("%d ASP statuses and %d SGP-side AS statuses active and identical before and after; no ASP state change in the window",
			len(record.Before.Sender.ASPStatuses)+len(record.Before.Receiver.ASPStatuses), len(record.Before.Receiver.ApplicationServers))
	}
	if management == 0 {
		indications.Outcome = failoverPass
		indications.Detail = "no management indication (Notify, Error, SCTP restart or release) on any sender association in the window"
	} else {
		indications.Detail = fmt.Sprintf("%d management indications in the window", management)
	}
	return append(criteria, reconnection, activation, indications)
}
