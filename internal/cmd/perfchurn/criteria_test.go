package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestRetainedHeapToleranceIsTheLargerOfEightMiBAndFivePercent(t *testing.T) {
	cases := []struct {
		baseline, want uint64
	}{
		{0, 8 * mebibyte},
		{100 * mebibyte, 8 * mebibyte},
		{160 * mebibyte, 8 * mebibyte}, // 5% is exactly 8 MiB
		{161 * mebibyte, 161 * mebibyte / 20},
		{400 * mebibyte, 20 * mebibyte},
	}
	for _, testCase := range cases {
		if got := retainedHeapTolerance(testCase.baseline); got != testCase.want {
			t.Errorf("tolerance(%d MiB) = %d, want %d", testCase.baseline/mebibyte, got, testCase.want)
		}
	}
}

func TestCeilingPassesAtTheLimitAndFailsAboveIt(t *testing.T) {
	limit := 256 * mebibyte
	if got := evaluateCeiling("x", "F8", "r", []uint64{1, limit, 2}, limit); got.Status != statusPass {
		t.Fatalf("at limit: %+v", got)
	}
	got := evaluateCeiling("x", "F8", "r", []uint64{1, limit + 1, 2}, limit)
	if got.Status != statusFail || !strings.Contains(got.Observed, fmt.Sprint(limit+1)) {
		t.Fatalf("above limit: %+v", got)
	}
	if got := evaluateCeiling("x", "F8", "r", nil, limit); got.Status != statusNotEvaluated {
		t.Fatalf("no samples must not pass: %+v", got)
	}
}

func TestFinalThreeBlocksRule(t *testing.T) {
	baseline := 100 * mebibyte
	limit := baseline + 8*mebibyte
	over := limit + 1
	cases := []struct {
		name   string
		finals []uint64
		want   string
	}{
		{"all within", []uint64{limit, limit, limit, limit, limit}, statusPass},
		{"early blocks may exceed", []uint64{over, over, limit, limit, 90 * mebibyte}, statusPass},
		{"third from last exceeds", []uint64{limit, limit, over, limit, limit}, statusFail},
		{"last exceeds", []uint64{limit, limit, limit, limit, over}, statusFail},
		{"too few blocks", []uint64{limit, limit}, statusNotEvaluated},
		{"too few blocks but last fails", []uint64{limit, over}, statusFail},
		{"no blocks", nil, statusNotEvaluated},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got := evaluateFinalBlocks(baseline, testCase.finals, 5, 3)
			if got.Status != testCase.want {
				t.Fatalf("status %s (%s), want %s", got.Status, got.Observed, testCase.want)
			}
		})
	}
}

func TestRetentionSampleComparesEveryResourceWithTheBaseline(t *testing.T) {
	baseline := passingRetained(0)
	if pass, failures := evaluateRetention(baseline, passingRetained(4*mebibyte), stableAssociations); !pass {
		t.Fatalf("within tolerance failed: %v", failures)
	}
	mutations := map[string]func(*retainedSample){
		"heap":                func(sample *retainedSample) { sample.LiveHeapBytes += 9 * mebibyte },
		"goroutines":          func(sample *retainedSample) { sample.Goroutines++ },
		"fewer goroutines":    func(sample *retainedSample) { sample.Goroutines-- },
		"descriptors":         func(sample *retainedSample) { sample.FDs.Total++ },
		"descriptor error":    func(sample *retainedSample) { sample.FDs.Error = "unavailable" },
		"registry extra":      func(sample *retainedSample) { sample.Associations++; sample.Unexpected = []uint64{99} },
		"registry missing":    func(sample *retainedSample) { sample.MissingStable = []uint64{3} },
		"kernel owned":        func(sample *retainedSample) { sample.Kernel.OwnedEstablished++ },
		"kernel unowned live": func(sample *retainedSample) { sample.Kernel.UnownedByState = map[string]int{"ESTABLISHED": 1} },
		"kernel error":        func(sample *retainedSample) { sample.Kernel.Error = "unavailable" },
	}
	for name, mutate := range mutations {
		sample := passingRetained(0)
		mutate(&sample)
		if pass, failures := evaluateRetention(baseline, sample, stableAssociations); pass || len(failures) == 0 {
			t.Errorf("%s: retention passed", name)
		}
	}
	transient := passingRetained(0)
	transient.Kernel.UnownedByState = map[string]int{"SHUTDOWN_ACK_SENT": 2}
	if pass, failures := evaluateRetention(baseline, transient, stableAssociations); !pass {
		t.Fatalf("protocol-required transient states were counted as live resources: %v", failures)
	}
}

func passingRetained(extraHeap uint64) retainedSample {
	return retainedSample{
		LiveHeapBytes: 100*mebibyte + extraHeap,
		Goroutines:    200,
		FDs:           fdSnapshot{Total: 40, Sockets: 34, SCTPAssociationSockets: 32, SCTPEndpointSockets: 1},
		Associations:  stableAssociations,
		Kernel:        kernelAssociations{Total: 32, OwnedEstablished: 32},
	}
}

// passingRecord is a synthetic full-contract run: every criterion holds.
func passingRecord() *aspRecord {
	record := &aspRecord{Config: commandConfig{Blocks: 5, BlockCycles: 200, ChurnRate: 4, ChurnGroup: 2}}
	for index, phase := range []string{phaseWarm, phaseSteady, phaseOverload, phaseOverloadDrain, "churn-1", "retain-5"} {
		value := uint64(100+index) * mebibyte
		if overloadPhase(phase) {
			value = 700 * mebibyte
		}
		record.RSSSeries = append(record.RSSSeries, rssSample{Phase: phase, RSSBytes: value})
		heap := uint64(80) * mebibyte
		if overloadPhase(phase) {
			heap = 400 * mebibyte
		}
		record.HeapSeries = append(record.HeapSeries, heapSample{Phase: phase, LiveHeapBytes: heap})
	}
	record.BaselineFinal = passingRetained(0)
	record.Steady = []ledgerResult{{SentTotal: 100, UniqueTotal: 100}, {SentTotal: 100, UniqueTotal: 100}}
	record.Overload = overloadResult{QueueCapacity: dataQueueSize, FullAssociations: stableAssociations, MaxQueued: dataQueueSize,
		QueuedSamples: 10, Discarded: 256 * stableAssociations, StateRecords: stateRecords}
	for subscriber := 0; subscriber < subscriberCount; subscriber++ {
		record.Overload.Subscribers = append(record.Overload.Subscribers,
			subscriberOverload{Subscriber: subscriber, DeliveredBeforeLoss: subscriptionQueueSize, LossObserved: true, Resynced: true})
	}
	for block := 1; block <= 5; block++ {
		final := passingRetained(2 * mebibyte)
		final.SinceDrainMillis = 5000
		final.Pass = true
		record.Blocks = append(record.Blocks, blockResult{
			Block: block, Cycles: 200,
			Peer: churnStats{Attempted: 200, Completed: 200, MaxEstablishing: 2, MaxStartLatenessMillis: 3, AchievedRate: 4,
				ByMode: map[string]int{"asp-graceful": 67, "asp-abrupt": 67, "peer": 66}},
			ASP:     aspChurnStats{Accepted: 200, Released: 200, ByMode: map[string]int{"asp-graceful": 67, "asp-abrupt": 67, "peer": 66}},
			Ledgers: []ledgerResult{{SentTotal: 500, UniqueTotal: 500}, {SentTotal: 500, UniqueTotal: 500}},
			Final:   final,
		})
	}
	return record
}

func criterionByID(t *testing.T, record *aspRecord, id string) criterion {
	t.Helper()
	for _, item := range record.Criteria {
		if item.ID == id {
			return item
		}
	}
	t.Fatalf("criterion %s missing from %+v", id, record.Criteria)
	return criterion{}
}

func TestEvaluateRunPassesTheSyntheticContractRun(t *testing.T) {
	record := passingRecord()
	evaluateRun(record)
	for _, item := range record.Criteria {
		if item.Status != statusPass {
			t.Errorf("%s: %s (%s)", item.ID, item.Status, item.Observed)
		}
	}
	if record.Verdict != verdictPass || len(record.Criteria) < 15 {
		t.Fatalf("verdict %s with %d criteria", record.Verdict, len(record.Criteria))
	}
}

func TestEvaluateRunFailsEachCriterionIndependently(t *testing.T) {
	cases := []struct {
		id     string
		mutate func(*aspRecord)
		want   string
	}{
		{"f7.cycles", func(record *aspRecord) { record.Blocks[2].Peer.Failed, record.Blocks[2].Peer.Completed = 1, 199 }, statusFail},
		{"f7.cycles", func(record *aspRecord) { record.Config.BlockCycles = 40; trimBlocks(record, 2, 40) }, statusNotEvaluated},
		{"f7.rate", func(record *aspRecord) { record.Blocks[0].Peer.MaxStartLatenessMillis = 300 }, statusFail},
		{"f7.rate", func(record *aspRecord) { record.Config.ChurnRate = 2 }, statusNotEvaluated},
		{"f7.concurrent-accepts", func(record *aspRecord) {
			for index := range record.Blocks {
				record.Blocks[index].Peer.MaxEstablishing = 1
			}
		}, statusFail},
		{"f7.child-close", func(record *aspRecord) { record.Blocks[1].ASP.StillOpen = 1 }, statusFail},
		{"f7.child-close", func(record *aspRecord) { record.Blocks[1].ASP.Accepted = 199 }, statusFail},
		{"f7.child-close", func(record *aspRecord) {
			for index := range record.Blocks {
				delete(record.Blocks[index].ASP.ByMode, "asp-abrupt")
			}
		}, statusFail},
		{"f7.surviving-no-loss", func(record *aspRecord) { record.Blocks[4].Ledgers[1].Missing = 1 }, statusFail},
		{"f7.surviving-no-loss", func(record *aspRecord) { record.Blocks[4].StableEnded = 1 }, statusFail},
		{"f7.surviving-no-loss", func(record *aspRecord) { record.Steady[0].Late = 1 }, statusFail},
		{"f7.surviving-no-loss", func(record *aspRecord) { record.Blocks[0].Ledgers[0].SentTotal = 0 }, statusFail},
		{"f8.steady-heap", func(record *aspRecord) { record.HeapSeries[1].LiveHeapBytes = 257 * mebibyte }, statusFail},
		{"f8.steady-rss", func(record *aspRecord) { record.RSSSeries[4].RSSBytes = 513 * mebibyte }, statusFail},
		{"f8.overload-heap", func(record *aspRecord) { record.HeapSeries[2].LiveHeapBytes = 513 * mebibyte }, statusFail},
		{"f8.overload-rss", func(record *aspRecord) { record.RSSSeries[3].RSSBytes = 1025 * mebibyte }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.MaxQueued = dataQueueSize + 1 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.FullAssociations = 31 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.Subscribers[3].DeliveredBeforeLoss = 257 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.Subscribers[3].LossObserved = false }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.StateRecords = stateRecords + 1 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.OOMKills = 1 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.PeerWriteErrors = 1 }, statusFail},
		{"f8.overload-bounds", func(record *aspRecord) { record.Overload.PeerRefused = 5000 }, statusPass},
		{"f8.retained-heap", func(record *aspRecord) { record.Blocks[3].Final.LiveHeapBytes = 109 * mebibyte }, statusFail},
		{"f8.retained-heap", func(record *aspRecord) { record.Blocks[0].Final.LiveHeapBytes = 200 * mebibyte }, statusPass},
		{"f8.retained-resources", func(record *aspRecord) { record.Blocks[0].Final.Goroutines++ }, statusFail},
		{"f8.retained-resources", func(record *aspRecord) { record.Blocks[1].Final.FDs.Total++ }, statusFail},
		{"f8.retained-registry", func(record *aspRecord) { record.Blocks[2].Final.Unexpected = []uint64{77} }, statusFail},
		{"f8.retained-kernel", func(record *aspRecord) { record.Blocks[4].Final.Kernel.OwnedEstablished = 33 }, statusFail},
		{"f8.retain-window", func(record *aspRecord) { record.Blocks[4].Final.SinceDrainMillis = 60001 }, statusFail},
		{"f8.blocks", func(record *aspRecord) { trimBlocks(record, 4, 200) }, statusNotEvaluated},
	}
	for _, testCase := range cases {
		t.Run(testCase.id+"/"+testCase.want, func(t *testing.T) {
			record := passingRecord()
			testCase.mutate(record)
			evaluateRun(record)
			got := criterionByID(t, record, testCase.id)
			if got.Status != testCase.want {
				t.Fatalf("%s: %s (%s), want %s", testCase.id, got.Status, got.Observed, testCase.want)
			}
			wantVerdict := verdictFail
			switch testCase.want {
			case statusNotEvaluated:
				wantVerdict = verdictIncomplete
			case statusPass:
				wantVerdict = verdictPass
			}
			if record.Verdict != wantVerdict {
				t.Fatalf("verdict %s, want %s", record.Verdict, wantVerdict)
			}
		})
	}
}

func trimBlocks(record *aspRecord, blocks, cycles int) {
	record.Blocks = record.Blocks[:blocks]
	record.Config.Blocks, record.Config.BlockCycles = blocks, cycles
	for index := range record.Blocks {
		block := &record.Blocks[index]
		block.Cycles, block.Peer.Attempted, block.Peer.Completed, block.ASP.Accepted, block.ASP.Released = cycles, cycles, cycles, cycles, cycles
	}
}

func TestEvaluateRunIsInvalidAfterAFatalError(t *testing.T) {
	record := passingRecord()
	record.Error = "peer control: connection refused"
	evaluateRun(record)
	if record.Verdict != verdictInvalid {
		t.Fatalf("verdict %s", record.Verdict)
	}
}
