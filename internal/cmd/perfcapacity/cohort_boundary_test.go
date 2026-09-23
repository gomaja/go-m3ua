package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestSharedClockStandaloneSenderRequiresCompleteCohort(testContext *testing.T) {
	var cohort map[string]json.RawMessage
	if err := json.Unmarshal([]byte(bidirectionalRunJSON(10, -1, 0, -2, -1)), &cohort); err != nil {
		testContext.Fatal(err)
	}
	for _, record := range []struct {
		name string
		run  string
	}{
		{name: "forward sender", run: string(cohort["sender"])},
		{name: "reverse sender", run: string(cohort["reverse_sender"])},
		{name: "unidirectional sender", run: strings.Replace(string(cohort["sender"]), `"mode":"bidirectional"`, `"mode":"throughput"`, 1)},
	} {
		testContext.Run(record.name, func(testContext *testing.T) {
			if _, _, err := evidenceFromFixture(json.RawMessage(record.run), 10); err == nil || !strings.Contains(err.Error(), "complete cohort") {
				testContext.Fatalf("standalone shared-clock sender accepted without a complete cohort: %v", err)
			}
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, record.run)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "complete cohort") {
				testContext.Fatalf("standalone shared-clock sender earned capacity credit: status=%d result=%+v", status, decoded)
			}
		})
	}
}

func TestExtractedReverseSendersCannotCertifyCapacity(testContext *testing.T) {
	var request strings.Builder
	request.WriteString(`{"initial":10,"maximum":100,"max_probes":24,"probes":[`)
	for index, probe := range capacity37Schedule {
		if index > 0 {
			request.WriteByte(',')
		}
		lower, upper := 1.0, 2.0
		if probe.passing {
			lower, upper = -1, 0
		}
		fmt.Fprintf(&request, `{"rate":%d,"run":%s}`, probe.rate, bidirectionalSenderJSON(probe.rate, true, lower, upper))
	}
	request.WriteString(`],"repetitions":[`)
	for index := 0; index < 5; index++ {
		if index > 0 {
			request.WriteByte(',')
		}
		fmt.Fprintf(&request, `{"rate":37,"run":%s}`, bidirectionalSenderJSON(37, true, -1, 0))
	}
	request.WriteString(`]}`)
	status, decoded := runRequest(testContext, request.String())
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "complete cohort") {
		testContext.Fatalf("extracted reverse-sender campaign earned capacity credit: status=%d result=%+v", status, decoded)
	}
}

func TestCapacityPreservesSupportedCohortShapes(testContext *testing.T) {
	for _, record := range []struct {
		name string
		run  string
	}{
		{name: "unaligned throughput", run: passingRunJSON()},
		{name: "unaligned echo", run: echoRunJSON()},
		{name: "complete bidirectional", run: bidirectionalRunJSON(10, -1, 0, -2, -1)},
	} {
		testContext.Run(record.name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, record.run)
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "pass" {
				testContext.Fatalf("supported cohort shape rejected: status=%d result=%+v", status, decoded)
			}
		})
	}
}

func TestSharedClockUnidirectionalCohortRemainsUnsupported(testContext *testing.T) {
	cohort := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
		delete(cohort, "reverse_sender")
		delete(cohort, "reverse_receiver")
		for _, side := range []string{"sender", "receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["mode"] = "throughput"
		}
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, cohort)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "requires sender, receiver, reverse_sender and reverse_receiver") {
		testContext.Fatalf("unsupported two-record shared-clock cohort accepted: status=%d result=%+v", status, decoded)
	}
}
