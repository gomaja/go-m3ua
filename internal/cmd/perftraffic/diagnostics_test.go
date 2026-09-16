package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestReadFatalErrorNamesAssociationPhaseAndCause(testContext *testing.T) {
	cause := errors.New("M3UA association not established")
	err := readFatalError(7, "idle", cause)
	if !errors.Is(err, cause) {
		testContext.Fatalf("readFatalError does not wrap the cause: %v", err)
	}
	message := err.Error()
	for _, fragment := range []string{"association 7", "idle", "not established"} {
		if !strings.Contains(message, fragment) {
			testContext.Fatalf("readFatalError = %q, want fragment %q", message, fragment)
		}
	}
}

func TestWriteStartupDiagnosticEmitsOneStructuredLine(testContext *testing.T) {
	var captured bytes.Buffer
	original := startupDiagnosticWriter
	defer func() { startupDiagnosticWriter = original }()
	startupDiagnosticWriter = &captured

	writeStartupDiagnostic("read-fatal", 7, "armed", errors.New("M3UA association not established"))
	writeStartupDiagnostic("read-fatal", 3, "idle", errors.New("second failure"))

	lines := strings.Split(strings.TrimSpace(captured.String()), "\n")
	if len(lines) != 2 {
		testContext.Fatalf("diagnostic lines = %d, want 2: %q", len(lines), captured.String())
	}
	var first map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		testContext.Fatalf("first diagnostic is not JSON: %v", err)
	}
	if first["startup_diagnostic"] != "read-fatal" || first["association"] != float64(7) ||
		first["receiver_phase"] != "armed" || first["error"] != "M3UA association not established" {
		testContext.Fatalf("first diagnostic = %v", first)
	}
	var second map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		testContext.Fatalf("second diagnostic is not JSON: %v", err)
	}
	if second["association"] != float64(3) || second["receiver_phase"] != "idle" {
		testContext.Fatalf("diagnostics did not preserve failure order: %v", second)
	}
}

func TestReceiverPhaseNameDistinguishesStartupFromCohort(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	if phase := control.phaseName(); phase != string(receiverIdle) {
		testContext.Fatalf("phaseName = %q, want %q", phase, receiverIdle)
	}
	control.setAssociationReady(0, 15)
	if err := control.reset(runSpec{
		Cohort: "phase", Seed: 1, Associations: 1, Expected: 1, Duration: 1_000_000_000,
		Rate: 1, Payload: workload128,
	}); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if phase := control.phaseName(); phase != string(receiverArmed) {
		testContext.Fatalf("phaseName = %q, want %q", phase, receiverArmed)
	}
}
