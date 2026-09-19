package main

import (
	"encoding/json"
	"testing"
)

// The acceptance CLI at internal/cmd/perfcapacity treats a run record without
// capped or send_errors as invalid input rather than as a run whose counters
// happened to be zero: evidenceFromFixture rejects it with "capped and
// send_errors are required". A loss-free run is exactly the run whose counters
// are zero, so omitting them on zero makes the fixture's own passing records
// unusable by the tool that has to decide them. These fields carry a counted
// quantity, and a counted zero is evidence; it is not an absent measurement.
func TestSenderRecordKeepsTheAcceptanceRequiredCountersAtZero(t *testing.T) {
	encoded, err := json.Marshal(runRecord{Side: "sender", FixtureVerdict: verdictPass})
	if err != nil {
		t.Fatalf("marshal run record: %v", err)
	}

	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unmarshal run record: %v", err)
	}

	for _, field := range []string{"capped", "send_errors"} {
		raw, ok := decoded[field]
		if !ok {
			t.Errorf("run record omits %q when it is zero; internal/cmd/perfcapacity requires it and rejects the record as invalid input", field)
			continue
		}
		if string(raw) != "0" {
			t.Errorf("run record %q = %s, want 0", field, raw)
		}
	}
}
