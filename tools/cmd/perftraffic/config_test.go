package main

import (
	"os"
	"testing"
	"time"
)

func TestParseConfigAcceptsCoreSender(testContext *testing.T) {
	config, err := parseConfig([]string{
		"-role=asp",
		"-transport=dial",
		"-sctp-address=10.0.0.2:2905",
		"-peer-control=http://10.0.0.2:8080",
		"-associations=8",
		"-payload=mix",
		"-rate=40000",
		"-warmup=10s",
		"-duration=2m",
		"-drain=2s",
		"-cohort=baseline-mix-01",
	})
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.Role != "asp" || config.Transport != "dial" {
		testContext.Fatalf("role/transport = %q/%q, want asp/dial", config.Role, config.Transport)
	}
	if config.Associations != 8 || config.Workload != workloadMix || config.Rate != 40000 {
		testContext.Fatalf("unexpected sender config: %+v", config)
	}
	if config.Warmup != 10*time.Second || config.Duration != 2*time.Minute || config.Drain != 2*time.Second {
		testContext.Fatalf("unexpected durations: %+v", config)
	}
}

func TestParseConfigAcceptsCoreReceiver(testContext *testing.T) {
	config, err := parseConfig([]string{
		"-role=sgp",
		"-transport=listen",
		"-sctp-address=0.0.0.0:2905",
		"-control-address=0.0.0.0:8080",
		"-associations=1",
	})
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.Role != "sgp" || config.Transport != "listen" {
		testContext.Fatalf("role/transport = %q/%q, want sgp/listen", config.Role, config.Transport)
	}
}

func TestParseConfigAcceptsEchoModeWithSameBounds(testContext *testing.T) {
	config, err := parseConfig([]string{
		"-role=asp", "-transport=dial", "-mode=echo",
		"-sctp-address=10.0.0.2:2905", "-peer-control=http://10.0.0.2:8080",
		"-cohort=latency-echo-01", "-rate=17500",
	})
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.Mode != modeEcho || config.Direction != directionASPToSGP {
		testContext.Fatalf("mode/direction = %q/%q, want echo/asp-to-sgp", config.Mode, config.Direction)
	}
	for _, args := range [][]string{
		{"-mode=echo", "-rate=0"},
		{"-mode=echo", "-rate=1000001"},
		{"-mode=echo", "-associations=33"},
		{"-mode=echo", "-outstanding=8193"},
		{"-mode=echo", "-duration=11m"},
	} {
		if _, err := parseConfig(args); err == nil {
			testContext.Fatalf("parseConfig(%v) unexpectedly succeeded", args)
		}
	}
}

func TestParseConfigDerivesInitiationDirection(testContext *testing.T) {
	tests := []struct {
		role       string
		transport  string
		initiation string
	}{
		{role: "asp", transport: "dial", initiation: initiationASPDial},
		{role: "sgp", transport: "listen", initiation: initiationASPDial},
		{role: "sgp", transport: "dial", initiation: initiationSGPDial},
		{role: "asp", transport: "listen", initiation: initiationSGPDial},
	}
	for _, test := range tests {
		testContext.Run(test.role+"/"+test.transport, func(testContext *testing.T) {
			args := []string{
				"-role=" + test.role, "-transport=" + test.transport,
				"-sctp-address=10.0.0.2:2905",
			}
			if test.role == "asp" {
				args = append(args, "-peer-control=http://10.0.0.2:8080", "-cohort=cohort-01")
			}
			config, err := parseConfig(args)
			if err != nil {
				testContext.Fatalf("parseConfig: %v", err)
			}
			if config.Initiation != test.initiation {
				testContext.Fatalf("initiation = %q, want %q", config.Initiation, test.initiation)
			}
		})
	}
}

func TestParseConfigAcceptsBidirectionalWithAdvertisedControlURL(testContext *testing.T) {
	config, err := parseConfig([]string{
		"-role=asp", "-transport=dial", "-mode=bidirectional",
		"-sctp-address=10.0.0.2:2905", "-peer-control=http://10.0.0.2:8080",
		"-control-address=0.0.0.0:8080", "-control-url=http://10.0.0.3:8080",
		"-associations=8", "-rate=20000", "-cohort=bidi-8-01",
	})
	if err != nil {
		testContext.Fatalf("parseConfig: %v", err)
	}
	if config.Mode != modeBidirectional || config.ControlURL != "http://10.0.0.3:8080" {
		testContext.Fatalf("unexpected bidirectional config: %+v", config)
	}
}

func TestParseConfigRejectsUnsupportedAndUnboundedInputs(testContext *testing.T) {
	testCases := []struct {
		name string
		args []string
	}{
		{name: "zero associations", args: []string{"-associations=0"}},
		{name: "too many associations", args: []string{"-associations=33"}},
		{name: "zero rate", args: []string{"-rate=0"}},
		{name: "runtime over ten minutes", args: []string{"-warmup=5m", "-duration=5m", "-drain=1ns"}},
		{name: "outstanding over bound", args: []string{"-outstanding=8193"}},
		{name: "unknown transport", args: []string{"-role=sgp", "-transport=accept"}},
		{name: "bidirectional ASP without advertised control URL", args: []string{"-mode=bidirectional", "-role=asp", "-transport=dial", "-peer-control=http://10.0.0.2:8080", "-cohort=bidi-01"}},
		{name: "unknown workload", args: []string{"-payload=129"}},
		{name: "peer control with a path", args: []string{"-peer-control=http://10.0.0.2:8080/reset"}},
		{name: "peer control with a query", args: []string{"-peer-control=http://10.0.0.2:8080?a=b"}},
		{name: "peer control with a fragment", args: []string{"-peer-control=http://10.0.0.2:8080#f"}},
		{name: "peer control with credentials", args: []string{"-peer-control=http://user:pass@10.0.0.2:8080"}},
		{name: "peer control with a file scheme", args: []string{"-peer-control=file:///etc/passwd"}},
		{name: "peer control with a non-HTTP scheme and a host", args: []string{"-peer-control=ftp://10.0.0.2:21"}},
		{name: "peer control with a gopher scheme and a host", args: []string{"-peer-control=gopher://10.0.0.2:70"}},
		{name: "peer control without a scheme", args: []string{"-peer-control=10.0.0.2:8080"}},
		{name: "peer control without a host", args: []string{"-peer-control=http://"}},
		{name: "advertised control URL with a path", args: []string{"-role=sgp", "-control-url=http://10.0.0.3:8080/x"}},
	}
	for _, testCase := range testCases {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			if _, err := parseConfig(testCase.args); err == nil {
				testContext.Fatal("parseConfig unexpectedly succeeded")
			}
		})
	}
}

func TestParseConfigDoesNotWriteUsageToProcessOutput(testContext *testing.T) {
	for _, argument := range []string{"-unknown-flag", "-associations=not-a-number"} {
		testContext.Run(argument, func(testContext *testing.T) {
			stdout, err := os.CreateTemp(testContext.TempDir(), "stdout")
			if err != nil {
				testContext.Fatal(err)
			}
			defer func() { _ = stdout.Close() }()
			stderr, err := os.CreateTemp(testContext.TempDir(), "stderr")
			if err != nil {
				testContext.Fatal(err)
			}
			defer func() { _ = stderr.Close() }()
			originalStdout, originalStderr := os.Stdout, os.Stderr
			defer func() { os.Stdout, os.Stderr = originalStdout, originalStderr }()
			os.Stdout, os.Stderr = stdout, stderr
			_, parseErr := parseConfig([]string{argument})
			os.Stdout, os.Stderr = originalStdout, originalStderr
			if parseErr == nil {
				testContext.Fatal("parseConfig unexpectedly succeeded")
			}
			for _, capture := range []*os.File{stdout, stderr} {
				contents, readErr := os.ReadFile(capture.Name())
				if readErr != nil {
					testContext.Fatal(readErr)
				}
				if len(contents) != 0 {
					testContext.Errorf("unexpected process output: %q", contents)
				}
			}
		})
	}
}
