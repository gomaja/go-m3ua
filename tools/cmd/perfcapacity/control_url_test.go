package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func fixtureControlURLCases() []struct {
	name  string
	value string
	valid bool
} {
	return []struct {
		name  string
		value string
		valid bool
	}{
		{name: "not configured", value: "", valid: true},
		{name: "HTTP IPv4", value: "http://127.0.0.1:8080", valid: true},
		{name: "HTTPS hostname", value: "https://control.example:8443", valid: true},
		{name: "HTTP default port", value: "http://control.example", valid: true},
		{name: "IPv6 literal", value: "http://[::1]:8080", valid: true},
		{name: "empty query", value: "http://control.example?", valid: true},
		{name: "empty fragment", value: "http://control.example#", valid: true},
		{name: "not a URL", value: "not-a-url"},
		{name: "missing scheme", value: "//control.example:8080"},
		{name: "non HTTP scheme", value: "ftp://control.example:8080"},
		{name: "missing host", value: "http://"},
		{name: "opaque URL", value: "http:control.example"},
		{name: "credentials", value: "http://user:password@control.example"},
		{name: "empty credentials", value: "http://@control.example"},
		{name: "operation path", value: "http://control.example/reset"},
		{name: "trailing slash", value: "http://control.example/"},
		{name: "escaped path", value: "http://control.example/%2F"},
		{name: "query", value: "http://control.example?next=reset"},
		{name: "fragment", value: "http://control.example#reset"},
		{name: "invalid escape", value: "http://%zz"},
		{name: "invalid port", value: "http://control.example:invalid"},
		{name: "invalid IPv6 literal", value: "http://[::1"},
	}
}

func TestBidirectionalControlURLMatchesProducer(testContext *testing.T) {
	for _, controlURL := range fixtureControlURLCases() {
		testContext.Run(controlURL.name, func(testContext *testing.T) {
			run := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
				for _, side := range []string{"sender", "receiver"} {
					cohort[side].(map[string]any)["spec"].(map[string]any)["peer_control"] = controlURL.value
				}
			})
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			status, decoded := runRequest(testContext, input)
			if !controlURL.valid || controlURL.value == "" {
				if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "peer_control") {
					testContext.Fatalf("bidirectional control URL %q: status=%d decision=%s error=%s", controlURL.value, status, decoded.Decision, decoded.Error)
				}
				return
			}
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "pass" {
				testContext.Fatalf("producer-compatible control URL %q rejected: status=%d result=%+v", controlURL.value, status, decoded)
			}
		})
	}
}

func TestSingleDirectionControlURLMatchesProducer(testContext *testing.T) {
	for _, mode := range []struct {
		name string
		run  string
	}{{name: "throughput", run: passingRunJSON()}, {name: "echo", run: echoRunJSON()}} {
		for _, controlURL := range fixtureControlURLCases() {
			testContext.Run(mode.name+"/"+controlURL.name, func(testContext *testing.T) {
				encoded, err := json.Marshal(controlURL.value)
				if err != nil {
					testContext.Fatal(err)
				}
				run := strings.Replace(mode.run, `"peer_control":"http://127.0.0.1:8080"`, `"peer_control":`+string(encoded), 1)
				_, _, err = evidenceFromFixture(json.RawMessage(run), 10)
				if controlURL.valid {
					if err != nil {
						testContext.Fatalf("optional producer control URL %q rejected: %v", controlURL.value, err)
					}
					return
				}
				if err == nil || !strings.Contains(err.Error(), "peer_control") {
					testContext.Fatalf("producer-impossible control URL %q: %v", controlURL.value, err)
				}
			})
		}
	}
}

func TestBidirectionalReverseControlRemainsUnconfigured(testContext *testing.T) {
	run := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
		for _, side := range []string{"reverse_sender", "reverse_receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["peer_control"] = "https://control.example:8443"
		}
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "reverse peer_control must be omitted") {
		testContext.Fatalf("configured reverse control accepted: status=%d result=%+v", status, decoded)
	}
}
