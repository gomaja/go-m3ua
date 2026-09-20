package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

func echoRunJSON() string {
	return echoRunAtRateJSON(10)
}

func echoRunAtRateJSON(rate int) string {
	expected := rate * 120
	sender := strings.Replace(runAtRateJSON(passingRunJSON(), rate), `"mode":"throughput"`, `"mode":"echo"`, 1)
	return strings.Replace(sender, `"sender_window"`,
		fmt.Sprintf(`"echo":{"scope":"round-trip","deadline_ns":2000000000,"outstanding_limit":8192,"requests":%d,"validated":%d,"capped":0,"deadline_exceeded":0,"invalid":0,"outstanding_after_drain":0},"sender_window"`, expected, expected), 1)
}

func TestRunRejectsContradictoryDuplicatedSettings(testContext *testing.T) {
	for _, field := range []struct {
		name string
		old  string
		new  string
	}{
		{"outstanding", `"outstanding_limit":8192`, `"outstanding_limit":4096`},
		{"initiation", `"initiation":"asp-dial"`, `"initiation":"sgp-dial"`},
	} {
		testContext.Run(field.name, func(testContext *testing.T) {
			run := replaceManifestField(passingRunJSON(), field.old, field.new)
			if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
				testContext.Fatal("accepted contradictory settings within one run")
			}
		})
	}
}

func TestRunAcceptsConsistentNondefaultDuplicatedSettings(testContext *testing.T) {
	run := strings.ReplaceAll(passingRunJSON(), `"initiation":"asp-dial"`, `"initiation":"sgp-dial"`)
	run = strings.ReplaceAll(run, `:8192`, `:4096`)
	if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err != nil {
		testContext.Fatal(err)
	}
}

func TestRunRejectsContradictoryExpectedCount(testContext *testing.T) {
	for _, replacement := range []string{`},"expected":1000`, `},"expected":null`, `}`} {
		run := strings.Replace(passingRunJSON(), `},"expected":1200`, replacement, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
			testContext.Fatalf("accepted missing or contradictory expected count: %s", replacement)
		}
	}
}

func TestPassingRunRequiresExpectedValidatedDeliveries(testContext *testing.T) {
	for _, replacement := range []string{`"unique":1000`, `"unique":1201`, `"unique":null`} {
		run := strings.Replace(passingRunJSON(), `"unique":1200`, replacement, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
			testContext.Fatalf("accepted invalid delivered count: %s", replacement)
		}
	}
}

func TestFixtureVerdictMustMatchAllValidityInputs(testContext *testing.T) {
	tests := []struct {
		name string
		run  string
	}{
		{name: "fatal error", run: strings.Replace(passingRunJSON(), `"fixture_verdict"`, `"fatal_error":"fatal read","fixture_verdict"`, 1)},
		{name: "outstanding at start", run: strings.Replace(passingRunJSON(), `"outstanding_at_window_start":0`, `"outstanding_at_window_start":1`, 1)},
		{name: "outstanding after drain", run: strings.Replace(passingRunJSON(), `"outstanding_after_drain":0`, `"outstanding_after_drain":1`, 1)},
		{name: "invalid verdict without failure", run: strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fixture_verdict":"invalid"`, 1)},
		{name: "unknown verdict", run: strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fixture_verdict":"unknown"`, 1)},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if _, _, err := evidenceFromFixture(json.RawMessage(test.run), 10); err == nil {
				testContext.Fatal("accepted a fixture verdict that contradicts its validity inputs")
			}
		})
	}
}

func TestPassingFixtureRejectsEveryFailureCounter(testContext *testing.T) {
	capped := strings.Replace(passingRunJSON(), `"sent":1200,"submitted":1200`, `"sent":1199,"submitted":1199`, 1)
	capped = strings.Replace(capped, `"capped":0`, `"capped":1`, 1)
	sendError := strings.Replace(passingRunJSON(), `"sent":1200,"submitted":1200`, `"sent":1199,"submitted":1199`, 1)
	sendError = strings.Replace(sendError, `"send_errors":0`, `"send_errors":1`, 1)
	missing := strings.Replace(passingRunJSON(), `"unique":1200,"unique_measurement":1200`, `"unique":1199,"unique_measurement":1199`, 1)
	missing = strings.Replace(missing, `"missing":0`, `"missing":1`, 1)
	tests := []struct {
		name string
		run  string
	}{
		{name: "capped", run: capped},
		{name: "send error", run: sendError},
		{name: "missing", run: missing},
		{name: "duplicate", run: strings.Replace(passingRunJSON(), `"duplicate":0`, `"duplicate":1`, 1)},
		{name: "invalid", run: strings.Replace(passingRunJSON(), `"invalid":0`, `"invalid":1`, 1)},
		{name: "reordered", run: strings.Replace(passingRunJSON(), `"reordered":0`, `"reordered":1`, 1)},
		{name: "late", run: strings.Replace(passingRunJSON(), `"late_after_stop":0`, `"late_after_stop":1`, 1)},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if _, _, err := evidenceFromFixture(json.RawMessage(test.run), 10); err == nil {
				testContext.Fatal("accepted a passing fixture with a failure counter")
			}
		})
	}
}

func TestSenderTotalsMustReconcile(testContext *testing.T) {
	for _, test := range []struct {
		name string
		run  string
	}{
		{name: "scheduled", run: strings.Replace(passingRunJSON(), `"scheduled":1200`, `"scheduled":1199`, 1)},
		{name: "sent", run: strings.Replace(passingRunJSON(), `"sent":1200`, `"sent":1199`, 1)},
		{name: "submitted", run: strings.Replace(passingRunJSON(), `"sent":1200,"submitted":1200`, `"sent":1199,"submitted":1199`, 1)},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			if _, _, err := evidenceFromFixture(json.RawMessage(test.run), 10); err == nil {
				testContext.Fatal("accepted sender totals that do not reconcile")
			}
		})
	}
}

func TestInvalidFixtureMayRetainIncompleteFailureCounters(testContext *testing.T) {
	run := strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fatal_error":"receiver result unavailable","fixture_verdict":"invalid"`, 1)
	run = strings.Replace(run, `"unique":1200`, `"unique":0`, 1)
	run = strings.Replace(run, `"unique_measurement":1200`, `"unique_measurement":0`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err != nil {
		testContext.Fatalf("rejected a legitimate invalid fixture record: %v", err)
	}
}

func TestCompleteInvalidFixtureRequiresReconciledDelivery(testContext *testing.T) {
	complete := strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fixture_verdict":"invalid"`, 1)
	complete = strings.Replace(complete, `"duplicate":0`, `"duplicate":1`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(complete), 10); err != nil {
		testContext.Fatalf("rejected complete failed probe: %v", err)
	}
	for _, missing := range []string{"0", "1199", "1201", "18446744073709551615"} {
		broken := strings.Replace(complete, `"unique":1200,"unique_measurement":1200`, `"unique":0,"unique_measurement":0`, 1)
		broken = strings.Replace(broken, `"missing":0`, `"missing":`+missing, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(broken), 10); err == nil {
			testContext.Fatalf("accepted inconsistent completed failure with missing=%s", missing)
		}
	}
}

func TestEchoRequestsMustEqualSubmissions(testContext *testing.T) {
	for _, replacement := range []string{"", `"requests":null,`, `"requests":0,`, `"requests":1199,`, `"requests":1201,`} {
		run := strings.Replace(echoRunJSON(), `"requests":1200,`, replacement, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
			testContext.Fatalf("accepted contradictory echo request count: %s", replacement)
		}
	}
}

func TestZeroSubmissionFailedProbeRemainsValidEvidence(testContext *testing.T) {
	run := strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fixture_verdict":"invalid"`, 1)
	run = strings.Replace(run, `"sent":1200,"submitted":1200`, `"sent":0,"submitted":0`, 1)
	run = strings.Replace(run, `"send_errors":0`, `"send_errors":1200`, 1)
	run = strings.Replace(run, `"unique":1200,"unique_measurement":1200`, `"unique":0,"unique_measurement":0`, 1)
	run = strings.Replace(run, `"missing":0`, `"missing":1200`, 1)
	evidence, _, err := evidenceFromFixture(json.RawMessage(run), 10)
	if err != nil || evidence.FixtureValid || evidence.Counters.SendErrors != 1200 {
		testContext.Fatalf("all-failed probe rejected or credited: %+v, %v", evidence, err)
	}
}

func TestRunMeasurementMetadataMustMatchSpecification(testContext *testing.T) {
	complete := passingRunJSON()
	if _, _, err := evidenceFromFixture(json.RawMessage(complete), 10); err != nil {
		testContext.Fatal(err)
	}
	for _, mutation := range []struct{ name, old, replacement string }{
		{"duration mismatch", `"measurement_duration_ns":120000000000`, `"measurement_duration_ns":60000000000`},
		{"duration missing", `"measurement_duration_ns":120000000000,`, ""},
		{"duration null", `"measurement_duration_ns":120000000000`, `"measurement_duration_ns":null`},
		{"window mismatch", `"sender_window":{"duration_ns":120000000000,`, `"sender_window":{"duration_ns":60000000000,`},
		{"window missing", `"sender_window":{"duration_ns":120000000000,`, `"sender_window":{`},
		{"streams mismatch", `"negotiated_outbound_streams":[8,8,8,8,8,8,8,8]`, `"negotiated_outbound_streams":[8]`},
		{"streams missing", `"negotiated_outbound_streams":[8,8,8,8,8,8,8,8],`, ""},
		{"streams null", `"negotiated_outbound_streams":[8,8,8,8,8,8,8,8]`, `"negotiated_outbound_streams":null`},
	} {
		testContext.Run(mutation.name, func(testContext *testing.T) {
			run := strings.Replace(complete, mutation.old, mutation.replacement, 1)
			if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
				testContext.Fatal("accepted contradictory or missing measurement metadata")
			}
		})
	}
}

func TestDeliveryPartitionMustEqualUniqueWithoutOverflow(testContext *testing.T) {
	for _, replacement := range []string{
		`"unique_measurement":0,"unique_drain":0`,
		`"unique_measurement":` + fmt.Sprint(uint64(math.MaxUint64)) + `,"unique_drain":1201`,
	} {
		run := strings.Replace(passingRunJSON(), `"unique_measurement":1200,"unique_drain":0`, replacement, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
			testContext.Fatalf("accepted contradictory delivery partition: %s", replacement)
		}
	}
}

func TestEchoWorkloadRequiresEchoEvidence(testContext *testing.T) {
	run := strings.Replace(passingRunJSON(), `"mode":"throughput"`, `"mode":"echo"`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
		testContext.Fatal("accepted echo workload without echo counters")
	}
}

func TestEchoEvidenceMustMatchWorkload(testContext *testing.T) {
	echo := strings.Replace(passingRunJSON(), `"sender_window"`, `"echo":{"requests":1200,"validated":1200,"capped":0,"deadline_exceeded":0,"invalid":0,"outstanding_after_drain":0},"sender_window"`, 1)
	for _, mode := range []string{"throughput", "bidirectional", "unknown"} {
		run := strings.Replace(echo, `"mode":"throughput"`, `"mode":"`+mode+`"`, 1)
		if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
			testContext.Fatalf("accepted echo evidence for %s", mode)
		}
	}
	echo = strings.Replace(echo, `"mode":"throughput"`, `"mode":"echo"`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(echo), 10); err != nil {
		testContext.Fatal(err)
	}
}

func TestEchoRunRequiresEveryReplyToValidate(testContext *testing.T) {
	if _, _, err := evidenceFromFixture(json.RawMessage(echoRunJSON()), 10); err != nil {
		testContext.Fatalf("rejected complete echo sender record: %v", err)
	}
	for _, mutation := range []struct {
		name string
		old  string
		new  string
	}{
		{name: "validated", old: `"validated":1200`, new: `"validated":0`},
		{name: "capped", old: `"validated":1200,"capped":0`, new: `"validated":1200,"capped":1`},
		{name: "deadline", old: `"deadline_exceeded":0`, new: `"deadline_exceeded":1`},
		{name: "invalid", old: `"deadline_exceeded":0,"invalid":0`, new: `"deadline_exceeded":0,"invalid":1`},
		{name: "outstanding", old: `"invalid":0,"outstanding_after_drain":0}`, new: `"invalid":0,"outstanding_after_drain":1}`},
	} {
		testContext.Run(mutation.name, func(testContext *testing.T) {
			run := strings.Replace(echoRunJSON(), mutation.old, mutation.new, 1)
			if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
				testContext.Fatal("accepted contradictory echo evidence")
			}
		})
	}
	withReceiverEvidence := strings.Replace(echoRunJSON(), `"sender_window"`,
		`"receiver_echo":{"replies":1200,"reply_errors":0,"replies_dropped":0},"sender_window"`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(withReceiverEvidence), 10); err == nil {
		testContext.Fatal("accepted receiver-only evidence on a sender record")
	}
}

func TestBidirectionalCapacityRequiresASeparateCohortContract(testContext *testing.T) {
	direct := strings.Replace(passingRunJSON(), `"mode":"throughput"`, `"mode":"bidirectional"`, 1)
	if _, _, err := evidenceFromFixture(json.RawMessage(direct), 10); err == nil {
		testContext.Fatal("accepted a bidirectional sender without cohort evidence")
	}
}
