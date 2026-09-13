package main

import (
	"errors"
	"testing"
)

func TestWarmupFailurePreservesBothRawRecords(testContext *testing.T) {
	sender := runRecord{Side: "sender", Capped: 7, Verdict: verdictInvalid}
	receiver := runRecord{Side: "receiver", Delivery: deliveryResult{Missing: 7}, Verdict: verdictInvalid}
	result := failedCohortResult("warmup", sender, receiver, errors.New("warmup cohort is invalid"))
	if result.Phase != "warmup" || result.Warmup == nil || result.Measurement != nil {
		testContext.Fatalf("phase result = %+v", result)
	}
	if result.Sender.Capped != 7 || result.Receiver.Delivery.Missing != 7 || result.Error == "" {
		testContext.Fatalf("raw warmup evidence was not preserved: %+v", result)
	}
}

func TestGrowingBacklogIsDiagnosticNotDeliveryInvalidation(testContext *testing.T) {
	record := runRecord{
		Expected:          1,
		Delivery:          deliveryResult{UniqueMeasurement: 1, Unique: 1},
		BacklogAssessment: "growing",
		CPU: CPUObservation{
			Scope:  wholeProcessScope,
			Before: map[string]uint64{"usage_usec": 10, "nr_throttled": 0},
			After:  map[string]uint64{"usage_usec": 20, "nr_throttled": 0},
		},
	}
	record.evaluate()
	if record.FixtureVerdict != verdictPass || record.Verdict != verdictInconclusive {
		testContext.Fatalf("record = %+v", record)
	}
}
