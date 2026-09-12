package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestRunExitStatusesAndJSONResults(testContext *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantStatus   int
		wantDecision string
	}{
		{name: "pass", input: requestJSON("upper", 1.1, 1), wantStatus: passingExitStatus, wantDecision: "pass"},
		{name: "fail", input: requestJSON("upper", 1.1, 1.2), wantStatus: failingExitStatus, wantDecision: "fail"},
		{name: "inconclusive", input: alternatingRequestJSON("upper", 1.1, 0.9, 1.3), wantStatus: inconclusiveExitStatus, wantDecision: "inconclusive"},
		{name: "invalid", input: `{}`, wantStatus: invalidInputExitStatus, wantDecision: "invalid-input"},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			var output bytes.Buffer
			status := run(strings.NewReader(test.input), &output)
			if status != test.wantStatus {
				testContext.Fatalf("run() status = %d, want %d; output = %s", status, test.wantStatus, output.String())
			}

			var response struct {
				Decision string `json:"decision"`
			}
			if err := json.Unmarshal(output.Bytes(), &response); err != nil {
				testContext.Fatalf("output is not JSON: %v; output = %q", err, output.String())
			}
			if response.Decision != test.wantDecision {
				testContext.Fatalf("decision = %q, want %q; output = %s", response.Decision, test.wantDecision, output.String())
			}
		})
	}
}

func TestRunRejectsNonStrictJSON(testContext *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "unknown field", input: strings.TrimSuffix(requestJSON("upper", 1.1, 1), "}") + `,"unexpected":true}`},
		{name: "trailing value", input: requestJSON("upper", 1.1, 1) + `{}`},
		{name: "duplicate top-level field", input: strings.Replace(requestJSON("upper", 1.1, 1), `"boundary":1.1`, `"boundary":1.1,"boundary":1.2`, 1)},
		{name: "duplicate pair field", input: strings.Replace(requestJSON("upper", 1.1, 1), `"baseline":1`, `"baseline":1,"baseline":2`, 1)},
		{name: "case alias top-level field", input: strings.Replace(requestJSON("upper", 1.1, 1), `"boundary":1.1`, `"boundary":1.1,"Boundary":1.2`, 1)},
		{name: "case alias pair field", input: strings.Replace(requestJSON("upper", 1.1, 1), `"baseline":1`, `"baseline":1,"Baseline":2`, 1)},
		{name: "null boundary", input: `{"direction":"upper","boundary":null,"pairs":[]}`},
		{name: "missing candidate", input: missingCandidateRequestJSON()},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			var output bytes.Buffer
			status := run(strings.NewReader(test.input), &output)
			if status != invalidInputExitStatus {
				testContext.Fatalf("run() status = %d, want %d; output = %s", status, invalidInputExitStatus, output.String())
			}
			if !json.Valid(output.Bytes()) {
				testContext.Fatalf("output is not JSON: %q", output.String())
			}
		})
	}
}

func TestRunRejectsOversizedInput(testContext *testing.T) {
	input := requestJSON("upper", 1.1, 1) + strings.Repeat(" ", maximumJSONInputBytes)

	var output bytes.Buffer
	status := run(strings.NewReader(input), &output)
	if status != invalidInputExitStatus {
		testContext.Fatalf("run() status = %d, want %d; output = %s", status, invalidInputExitStatus, output.String())
	}
	if !strings.Contains(output.String(), "exceeds") {
		testContext.Fatalf("output = %s, want bounded-input error", output.String())
	}
}

func TestRunEmitsJSONSafeExtremeResult(testContext *testing.T) {
	input := requestJSON("upper", 1.7976931348623157e308, 1.7976931348623157e308)
	input = strings.ReplaceAll(input, `"baseline":1`, `"baseline":5e-324`)

	var output bytes.Buffer
	status := run(strings.NewReader(input), &output)
	if status != failingExitStatus {
		testContext.Fatalf("run() status = %d, want %d; output = %s", status, failingExitStatus, output.String())
	}
	if !json.Valid(output.Bytes()) {
		testContext.Fatalf("output is not valid JSON: %q", output.String())
	}
	if !strings.Contains(output.String(), `"range":"above-float64"`) || !strings.Contains(output.String(), `"ratio":null`) {
		testContext.Fatalf("output = %s, want explicit JSON-safe overflow", output.String())
	}
}

func FuzzRunNeverPanicsAndAlwaysWritesJSON(fuzz *testing.F) {
	fuzz.Add([]byte(`{}`))
	fuzz.Add([]byte(requestJSON("upper", 1.1, 1)))
	fuzz.Add([]byte(`{"direction":"upper","boundary":1.1,"pairs":null}`))
	fuzz.Add([]byte{0xff, 0x00, '{', '}'})

	fuzz.Fuzz(func(testContext *testing.T, input []byte) {
		var output bytes.Buffer
		status := run(bytes.NewReader(input), &output)
		if status < passingExitStatus || status > invalidInputExitStatus {
			testContext.Fatalf("run() status = %d, want %d..%d", status, passingExitStatus, invalidInputExitStatus)
		}
		if !json.Valid(output.Bytes()) {
			testContext.Fatalf("output is not JSON: %q", output.String())
		}
	})
}

func requestJSON(direction string, boundary float64, ratio float64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, `{"direction":%q,"boundary":%g,"pairs":[`, direction, boundary)
	for index := 0; index < 20; index++ {
		if index > 0 {
			builder.WriteByte(',')
		}
		fmt.Fprintf(&builder, `{"id":"pair-%d","baseline":1,"candidate":%g}`, index+1, ratio)
	}
	builder.WriteString(`]}`)
	return builder.String()
}

func alternatingRequestJSON(direction string, boundary float64, first float64, second float64) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, `{"direction":%q,"boundary":%g,"pairs":[`, direction, boundary)
	for index := 0; index < 20; index++ {
		if index > 0 {
			builder.WriteByte(',')
		}
		ratio := first
		if index%2 == 1 {
			ratio = second
		}
		fmt.Fprintf(&builder, `{"id":"pair-%d","baseline":1,"candidate":%g}`, index+1, ratio)
	}
	builder.WriteString(`]}`)
	return builder.String()
}

func missingCandidateRequestJSON() string {
	request := requestJSON("upper", 1.1, 1)
	return strings.Replace(request, `,"candidate":1`, ``, 1)
}
