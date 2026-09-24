package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// cohortValidityError is the whole error of a cohort that failed only its own
// validity rules, and warmupValidityError the error a run whose warm-up
// failed that way ends with, which internal/cmd/perfcapacity accepts as
// evidence against the rate.
const (
	cohortValidityError = "cohort is invalid; inspect machine-readable reasons"
	warmupValidityError = "warmup did not drain cleanly: " + cohortValidityError
)

// directionRecords returns a direction's sender and receiver records: invalid
// with 7 cap refusals when overloaded, otherwise loss-free.
func directionRecords(overloaded bool) (runRecord, runRecord) {
	if overloaded {
		return runRecord{Side: "sender", Capped: 7, Verdict: verdictInvalid, FixtureVerdict: verdictInvalid},
			runRecord{Side: "receiver", Delivery: deliveryResult{Missing: 7}, Verdict: verdictInvalid, FixtureVerdict: verdictInvalid}
	}
	return runRecord{Side: "sender", Verdict: verdictInconclusive, FixtureVerdict: verdictPass},
		runRecord{Side: "receiver", Verdict: verdictInconclusive, FixtureVerdict: verdictPass}
}

// bidirectionalRunner runs a cohort the way runSender does in bidirectional
// mode: the forward records and error of runSenderCohort, then the reverse
// cohort collected from the SGP, which serves peer.
func bidirectionalRunner(testContext *testing.T, forwardOverloaded bool, forwardErr error, peer runRecord) cohortRunner {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, peer)
	}))
	testContext.Cleanup(server.Close)
	return func(_ commandConfig, phase, _ string, _ time.Duration) (cohortResult, error) {
		sender, receiver := directionRecords(forwardOverloaded)
		result := newCohortResult(phase, sender, receiver, forwardErr)
		collectReverse(context.Background(), commandConfig{PeerControl: server.URL, Drain: time.Second}, &result)
		return result, forwardErr
	}
}

// reversePeer is the SGP's result after its reverse cohort: the reverse
// records and, when the cohort failed, its error.
func reversePeer(overloaded bool, reverseError string) runRecord {
	sender, receiver := directionRecords(overloaded)
	return runRecord{Side: "receiver", Verdict: verdictInconclusive, Reverse: &sender, ReverseReceiver: &receiver, ReverseError: reverseError}
}

// A bidirectional warm-up that failed keeps both directions' records. When
// every direction failed only its own validity rules, the run ends with the
// warm-up validity error perfcapacity accepts as overload evidence, whichever
// direction failed. Any fixture fault, in either direction, keeps its own
// text so the warm-up is refused rather than mistaken for overload.
func TestFailedBidirectionalWarmupKeepsBothDirections(testContext *testing.T) {
	validity := errors.New(cohortValidityError)
	for _, scenario := range []struct {
		name              string
		forwardOverloaded bool
		forwardErr        error
		peer              runRecord
		wantError         string
		wantFault         string
	}{
		{name: "both directions overloaded", forwardOverloaded: true, forwardErr: validity,
			peer: reversePeer(true, cohortValidityError), wantError: warmupValidityError},
		{name: "only the reverse direction overloaded", peer: reversePeer(true, cohortValidityError), wantError: warmupValidityError},
		{name: "only the forward direction overloaded", forwardOverloaded: true, forwardErr: validity,
			peer: reversePeer(false, ""), wantError: warmupValidityError},
		{name: "reverse direction fault", forwardOverloaded: true, forwardErr: validity,
			peer: reversePeer(true, "stop receiver: connection refused\n"+cohortValidityError), wantFault: "reverse cohort: stop receiver: connection refused"},
		{name: "reverse receiver fault", forwardOverloaded: true, forwardErr: validity,
			peer:      runRecord{Side: "receiver", Verdict: verdictInvalid, FatalError: "association 0 ReadData in receiver phase measuring: EOF"},
			wantFault: "reverse cohort receiver: association 0 ReadData"},
		{name: "forward direction fault", forwardOverloaded: true, forwardErr: errors.Join(errors.New("stop receiver: connection refused"), validity),
			peer: reversePeer(true, cohortValidityError), wantFault: "warmup did not drain cleanly: stop receiver: connection refused"},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			config := commandConfig{Warmup: time.Second, Duration: time.Second, Cohort: "bidi"}
			result, err := runWarmupAndMeasurement(config, bidirectionalRunner(testContext, scenario.forwardOverloaded, scenario.forwardErr, scenario.peer), nil)
			if err == nil || result.Phase != "warmup" || result.Measurement != nil || result.Warmup == nil || result.Verdict != verdictInvalid {
				testContext.Fatalf("result phase %q verdict %q warmup %v measurement %v err %v", result.Phase, result.Verdict, result.Warmup != nil, result.Measurement != nil, err)
			}
			warmup := result.Warmup
			if warmup.Phase != "warmup" || warmup.Verdict != verdictInvalid || warmup.Error != result.Error {
				testContext.Fatalf("warm-up cohort phase %q verdict %q error %q, combined error %q", warmup.Phase, warmup.Verdict, warmup.Error, result.Error)
			}
			if scenario.peer.Reverse != nil && (warmup.ReverseSender == nil || warmup.ReverseReceiver == nil ||
				warmup.ReverseSender.Capped != scenario.peer.Reverse.Capped) {
				testContext.Fatalf("reverse records were not kept: sender %+v receiver %+v", warmup.ReverseSender, warmup.ReverseReceiver)
			}
			if scenario.wantError != "" && result.Error != scenario.wantError {
				testContext.Fatalf("error %q, want %q", result.Error, scenario.wantError)
			}
			if scenario.wantFault != "" && (result.Error == warmupValidityError || !strings.Contains(result.Error, scenario.wantFault)) {
				testContext.Fatalf("error %q, want the fault %q named and not the validity error", result.Error, scenario.wantFault)
			}
		})
	}
}

// A unidirectional warm-up is unchanged: its records and error are the
// cohort's own.
func TestFailedUnidirectionalWarmupIsUnchanged(testContext *testing.T) {
	for name, forwardErr := range map[string]error{
		"validity": errors.New(cohortValidityError),
		"fault":    errors.Join(errors.New("stop receiver: connection refused"), errors.New(cohortValidityError)),
	} {
		testContext.Run(name, func(testContext *testing.T) {
			runner := func(_ commandConfig, phase, _ string, _ time.Duration) (cohortResult, error) {
				sender, receiver := directionRecords(true)
				return newCohortResult(phase, sender, receiver, forwardErr), forwardErr
			}
			result, err := runWarmupAndMeasurement(commandConfig{Warmup: time.Second, Duration: time.Second}, runner, nil)
			if !errors.Is(err, forwardErr) || result.Error != "warmup did not drain cleanly: "+forwardErr.Error() || result.Warmup == nil ||
				result.Warmup.ReverseSender != nil || result.Warmup.Error != result.Error || result.Sender.Capped != 7 {
				testContext.Fatalf("result %+v err %v", result, err)
			}
		})
	}
}
