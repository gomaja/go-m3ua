package m3ua

import (
	"slices"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RC65 is static. RC1..64 are dynamic; RC64 is a surviving dynamic AS.
// One real grouped DUNA creates 64 jobs, each with 65 independent origins,
// and 4160 received source rows. A static declaration for RC1..63 would
// invalidate the work test by making registration retirement preserve them.
func newRequesterLifecycleWorkFixture(t *testing.T) (*Endpoint, *Association, []uint32) {
	t.Helper()
	e := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxRecordsPerPartition: 8192})
	e.ssnm.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
	e.ssnm.now = func() time.Time { return time.Unix(100, 0) }
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 65)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	rcs := make([]uint32, 65)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
		if i < 64 {
			a.addDynamicASKey(*staticASKey(7, rcs[i]), testRoutingKey(7, 1000+uint32(i), params.ServiceIndSCCP), false)
		}
	}
	a.noteRoutingContextsActive(rcs)
	a.noteRoutingContextsAcked(params.NewRoutingContext(rcs...))
	if !e.trackAssociation(a) {
		t.Fatal("track requester")
	}
	t.Cleanup(func() { _ = a.Close() })
	pcs := make([]uint32, 64)
	for i := range pcs {
		pcs[i] = 1000 + uint32(i)
	}
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(rcs...), params.NewAffectedPointCode(pcs...), nil)); err != nil {
		t.Fatal(err)
	}
	if len(e.ssnm.audits) != 64 {
		t.Fatalf("setup jobs=%d want64", len(e.ssnm.audits))
	}
	sources := 0
	for _, state := range e.ssnm.partitions {
		sources += len(state.receivedAvailability)
	}
	if sources != 4160 {
		t.Fatalf("setup sources=%d want4160", sources)
	}
	return e, a, rcs
}

func lifecycleDeregistrationResponse(rcs []uint32) *messages.DeregistrationResponse {
	results := make([]*params.Param, len(rcs))
	for i, rc := range rcs {
		results[i] = params.NewDeregistrationResult(params.NewDeregResultPayload(params.NewRoutingContext(rc), params.NewDeregistrationStatus(params.SuccessfullyDeregistered)))
	}
	return messages.NewDeregistrationResponse(results...)
}

// Measurement covers only the event. Setup and checking are outside it.
// Each source should be inspected a bounded number of times, and a grouped
// job should be cloned at most three times for the whole event. The 3*4160
// bound allows more than a complete input/output pass; baseline instead
// scans shrinking history per RC and repeatedly clones all 65 origins.
// jobRows allows four visits per initial source, because source-to-job
// candidates are real work even when they identify the same grouped job.
// asRows deliberately is not asserted: static-index construction occurs
// before the store lock and shares that probe with locked AS loops.
func checkRequesterLifecycleWork(t *testing.T, e *Endpoint, a *Association, w *ssnmWorkCount, replaced bool, beforeRecords, beforeBytes int) {
	t.Helper()
	e.ssnm.mu.Lock()
	work := *w
	// Detach before behavioral verification so it cannot affect work counts.
	e.ssnm.work = nil
	for _, state := range e.ssnm.partitions {
		state.work = nil
	}
	for _, job := range e.ssnm.audits {
		job.work = nil
	}
	sources := 0
	for _, state := range e.ssnm.partitions {
		for _, source := range state.receivedAvailability {
			sources++
			if source.AS.RoutingContext < 64 {
				t.Errorf("retired source survived: %+v", source.AS)
			}
		}
	}
	if sources != 128 {
		t.Errorf("surviving source count=%d want128", sources)
	}
	if len(e.ssnm.audits) != 64 {
		t.Errorf("surviving jobs=%d want64", len(e.ssnm.audits))
	}
	for _, job := range e.ssnm.audits {
		needed := 0
		for i, origin := range job.origins {
			if origin.needed() {
				needed++
				if job.bindings[i].key.RoutingContext < 64 {
					t.Errorf("retired origin survived: %+v", job.bindings[i])
				}
			}
		}
		if needed != 2 || !job.live.Load() {
			t.Errorf("surviving dynamic/static origins lost: needed=%d live=%v", needed, job.live.Load())
		}
	}
	if e.ssnm.records >= beforeRecords || e.ssnm.bytes >= beforeBytes {
		t.Errorf("retirement failed to release charges: %d/%d %d/%d", e.ssnm.records, beforeRecords, e.ssnm.bytes, beforeBytes)
	}
	e.ssnm.mu.Unlock()
	for rc := uint32(1); rc <= 63; rc++ {
		_, held := a.dynamicASKey(rc, false)
		if held != replaced {
			t.Errorf("RC%d registration held=%v want%v", rc, held, replaced)
		}
	}
	if _, held := a.dynamicASKey(64, false); !held {
		t.Error("surviving dynamic registration removed")
	}
	t.Logf("store-locked source/job/origin work: sources=%d jobs=%d copiedOrigins=%d", work.sourceRows, work.jobRows, work.copiedOrigins)
	if work.sourceRows > 3*4160 || work.copiedOrigins > 3*4160 || work.jobRows > 4*4160 {
		t.Fatalf("multi-RC event multiplied work by retained history: %+v", work)
	}
}

func TestRequesterMultiContextDeregistrationBatchesStoreWork(t *testing.T) {
	for _, late := range []bool{false, true} {
		name := "completed"
		if late {
			name = "late"
		}
		t.Run(name, func(t *testing.T) {
			e, a, rcs := newRequesterLifecycleWorkFixture(t)
			pending := make(map[uint32]int, 63)
			for i, rc := range rcs[:63] {
				pending[rc] = i
			}
			if _, err := a.beginDeregistrationResponseCorrelation(pending); err != nil {
				t.Fatal(err)
			}
			response := lifecycleDeregistrationResponse(rcs[:63])
			if late {
				// Capture versions through the real timeout/unresolved path.
				a.endDeregistrationResponseCorrelation(true)
			} else if err := a.handleDeregistrationResponse(response); err != nil {
				t.Fatal(err)
			}
			beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
			w := attachLifecycleWorkProbe(e.ssnm)
			if late {
				if err := a.handleDeregistrationResponse(response); err != nil {
					t.Fatal(err)
				}
			} else {
				a.endDeregistrationResponseCorrelation(true)
			}
			checkRequesterLifecycleWork(t, e, a, w, false, beforeRecords, beforeBytes)
		})
	}
}

func TestRegistrationResponseReplacementsBatchStoreWork(t *testing.T) {
	// The late-response entrypoint applies all results in one actual handler.
	// Direct calls to applyRegistrationResults would bypass correlation, and
	// calling addDynamicASKey directly would conceal the production item loop.
	e, a, rcs := newRequesterLifecycleWorkFixture(t)
	pending := make(map[uint32]int, 63)
	requests := make(map[uint32]RoutingKeyRegistrationRequest, 63)
	results := make([]*params.Param, 63)
	versions := make(map[uint32]uint64, 63)
	for i, rc := range rcs[:63] {
		id := a.nextLocalRoutingKeyIdentifier()
		pending[id] = i
		requests[id] = RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: id, RoutingKey: testRoutingKey(7, 1000+uint32(i), params.ServiceIndSCCP)}
		results[i] = params.NewRegistrationResult(params.NewRegistrationResultPayload(params.NewLocalRoutingKeyIdentifier(id), params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(rc)))
		versions[rc] = a.dynamicASKeyVersionFor(rc, false)
	}
	if _, err := a.beginRegistrationResponseCorrelation(pending, requests); err != nil {
		t.Fatal(err)
	}
	a.endRegistrationResponseCorrelation(true, false)
	beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := a.handleRegistrationResponse(messages.NewRegistrationResponse(results...)); err != nil {
		t.Fatal(err)
	}
	checkRequesterLifecycleWork(t, e, a, w, true, beforeRecords, beforeBytes)
	for rc, version := range versions {
		if got := a.dynamicASKeyVersionFor(rc, false); got <= version {
			t.Errorf("RC%d replacement version=%d old=%d", rc, got, version)
		}
	}
}

func TestLateDeregistrationVersionFenceSkipsReplacementStoreWork(t *testing.T) {
	e, a, _, _ := newDynamicValidityAssociation(t, true)
	if _, err := a.beginDeregistrationResponseCorrelation(map[uint32]int{7: 0}); err != nil {
		t.Fatal(err)
	}
	a.endDeregistrationResponseCorrelation(true)
	a.addDynamicASKey(*staticASKey(7, 7), testRoutingKey(7, 100, params.ServiceIndSCCP), false)
	a.noteRoutingContextsActive([]uint32{7, 8})
	a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
	sendDUNA(t, a, 7, 7, 100)
	version := a.dynamicASKeyVersionFor(7, false)
	beforeRecords, beforeBytes, beforeJobs := e.ssnm.records, e.ssnm.bytes, len(e.ssnm.audits)
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := a.handleDeregistrationResponse(lifecycleDeregistrationResponse([]uint32{7})); err != nil {
		t.Fatal(err)
	}
	if a.dynamicASKeyVersionFor(7, false) != version || e.ssnm.records != beforeRecords || e.ssnm.bytes != beforeBytes || len(e.ssnm.audits) != beforeJobs {
		t.Fatal("late old version altered replacement")
	}
	if w.sourceRows != 0 || w.jobRows != 0 || w.copiedOrigins != 0 {
		t.Fatalf("obsolete version entered store retirement: %+v", *w)
	}
}

func TestRequesterDeregistrationBatchesLegacyDestinationRetirement(t *testing.T) {
	_, a, rcs := newRequesterLifecycleWorkFixture(t)
	var rows uint64
	a.destinations.mu.Lock()
	records := len(a.destinations.state)
	a.destinations.retirementRows = &rows
	a.destinations.mu.Unlock()
	if records != 64 {
		t.Fatalf("legacy setup records=%d want64", records)
	}
	pending := make(map[uint32]int, 63)
	for _, rc := range rcs[:63] {
		pending[rc] = int(rc)
	}
	if _, err := a.beginDeregistrationResponseCorrelation(pending); err != nil {
		t.Fatal(err)
	}
	if err := a.handleDeregistrationResponse(lifecycleDeregistrationResponse(rcs[:63])); err != nil {
		t.Fatal(err)
	}
	a.endDeregistrationResponseCorrelation(true)
	a.destinations.mu.Lock()
	visited := rows
	a.destinations.retirementRows = nil
	for _, record := range a.destinations.state {
		if !slices.Equal(record.routingContexts, []uint32{64, 65}) {
			t.Errorf("remaining legacy contexts=%v", record.routingContexts)
		}
	}
	a.destinations.mu.Unlock()
	t.Logf("destination-mutex retirement rows=%d", visited)
	if visited > 2*uint64(records) {
		t.Fatalf("multi-RC event rescanned legacy destination records: %d > %d", visited, 2*records)
	}
}
