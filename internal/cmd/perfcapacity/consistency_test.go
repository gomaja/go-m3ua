package main

import (
	"encoding/json"
	"strings"
	"testing"
)

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
