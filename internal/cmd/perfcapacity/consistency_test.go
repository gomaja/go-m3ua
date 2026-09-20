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
