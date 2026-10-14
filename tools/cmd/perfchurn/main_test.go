package main

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestExitCodeContract(t *testing.T) {
	cases := map[string]int{verdictPass: 0, verdictFail: 1, verdictIncomplete: 3, verdictInvalid: 4, "": 4, "unknown": 4}
	for verdict, want := range cases {
		if got := exitCodeFor(verdict); got != want {
			t.Errorf("verdict %q exits %d, want %d", verdict, got, want)
		}
	}
	for _, arguments := range [][]string{{"-role=sgp"}, {"-role=asp"}, {"-no-such-flag"}, {"-role=peer", "-local-ip=0.0.0.0"}} {
		var stdout, stderr bytes.Buffer
		if code := run(arguments, &stdout, &stderr); code != exitUsage {
			t.Errorf("%v exits %d, want %d", arguments, code, exitUsage)
		}
		var report map[string]string
		if err := json.Unmarshal(stderr.Bytes(), &report); err != nil || report["verdict"] != verdictInvalid || report["error"] == "" {
			t.Errorf("%v usage report %q: %v", arguments, stderr.String(), err)
		}
		if stdout.Len() != 0 {
			t.Errorf("%v wrote a record for a usage error: %q", arguments, stdout.String())
		}
	}
}
