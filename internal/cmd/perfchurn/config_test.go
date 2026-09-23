package main

import (
	"strings"
	"testing"
	"time"
)

func aspArguments(extra ...string) []string {
	return append([]string{"-role=asp", "-peer-control=http://peer:8080"}, extra...)
}

func TestConfigDefaultsAreTheContractWorkload(t *testing.T) {
	config, err := parseConfig(aspArguments())
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if config.Routes != 1000 || config.Blocks != 5 || config.BlockCycles != 200 || config.totalCycles() != 1000 ||
		config.ChurnRate != 4 || config.DataRate != 320 {
		t.Fatalf("defaults %+v are not the contract workload: 1,000 routes, 5 blocks of 200 cycles at 4/s", config)
	}
	if config.ChurnGroup < 2 || config.AcceptConcurrency < 2 {
		t.Fatalf("defaults %+v do not exercise concurrent accepts", config)
	}
}

func TestConfigAcceptsZeroRouteRetention(t *testing.T) {
	config, err := parseConfig(aspArguments("-routes=0", "-blocks=2", "-block-cycles=40", "-steady=20s"))
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if config.Routes != 0 || config.totalCycles() != 80 || config.Steady != 20*time.Second {
		t.Fatalf("config %+v", config)
	}
}

func TestConfigPeerNeedsOneSpecificLocalIP(t *testing.T) {
	if _, err := parseConfig([]string{"-role=peer", "-sctp-address=10.0.0.1:2905", "-local-ip=10.0.0.2"}); err != nil {
		t.Fatalf("valid peer rejected: %v", err)
	}
	for _, localIP := range []string{"", "0.0.0.0", "not-an-ip", "10.0.0.2:30000"} {
		if _, err := parseConfig([]string{"-role=peer", "-local-ip=" + localIP}); err == nil {
			t.Errorf("peer local-ip %q accepted", localIP)
		}
	}
}

func TestConfigRejectsInvalidRuns(t *testing.T) {
	cases := []struct {
		name      string
		arguments []string
		want      string
	}{
		{"no role", []string{"-peer-control=http://peer:8080"}, "role"},
		{"unknown role", []string{"-role=sgp"}, "role"},
		{"missing peer control", []string{"-role=asp"}, "peer-control"},
		{"peer control with path", aspArguments("-peer-control=http://peer:8080/x"), "peer-control"},
		{"peer control https", aspArguments("-peer-control=https://peer:8080"), "peer-control"},
		{"routes neither 0 nor 1000", aspArguments("-routes=10"), "routes"},
		{"steady too short", aspArguments("-steady=500ms"), "steady"},
		{"data rate not a multiple", aspArguments("-data-rate=100"), "data-rate"},
		{"data rate zero", aspArguments("-data-rate=0"), "data-rate"},
		{"overload hold zero", aspArguments("-overload-hold=0s"), "overload-hold"},
		{"overload extra zero", aspArguments("-overload-extra=0"), "overload-extra"},
		{"toggles cannot overflow", aspArguments("-ssnm-toggles=128"), "ssnm-toggles"},
		{"no blocks", aspArguments("-blocks=0"), "blocks"},
		{"no cycles", aspArguments("-block-cycles=0"), "block-cycles"},
		{"cycles exceed ports", aspArguments("-blocks=5", "-block-cycles=700"), "port space"},
		{"zero rate", aspArguments("-churn-rate=0"), "churn-rate"},
		{"NaN rate", aspArguments("-churn-rate=NaN"), "churn-rate"},
		{"group zero", aspArguments("-churn-group=0"), "churn-group"},
		{"negative hold", aspArguments("-churn-hold=-1s"), "churn-hold"},
		{"no accepts", aspArguments("-accept-concurrency=0"), "accept-concurrency"},
		{"positional", aspArguments("extra"), "positional"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := parseConfig(testCase.arguments)
			if err == nil || !strings.Contains(err.Error(), testCase.want) {
				t.Fatalf("error %v, want one naming %q", err, testCase.want)
			}
		})
	}
}
