// Command perfchurn is the association lifecycle churn (F7) and retained
// memory (F8) fixture of performance-budgets.md section 4.
//
// It runs as two processes on Linux, each in its own container. The ASP
// process (-role=asp) is the library process under measurement: one ASP
// Endpoint holding the reference memory inventory (32 stable associations
// accepted on one listener, 128 SSNM partitions, 16,384 retained records, 8
// subscriptions and, with -routes=1000, a 1,000-route inventory). The peer
// process (-role=peer) hosts four SGP Endpoints that dial it, populate the
// store, carry ledgered DATA and run the churn cycles. The ASP drives every
// phase over the peer's HTTP control:
//
//	warm      establish the 32 stable associations, populate the store
//	steady    ledgered DATA both ways; RSS every 1 s, live heap every 10 s
//	baseline  drain, then the warmed baseline after two forced GCs
//	overload  pause readers and subscribers, fill every bounded queue with
//	          4,096-byte payloads, hold, resume and drain
//	churn-N   establish/activate/close cycles at -churn-rate with concurrent
//	          accepts and independent child close, while the stable
//	          associations carry ledgered DATA
//	retain-N  after drain, samples after two forced GCs within 60 s
//
// Each process writes one JSON record to stdout; the ASP record embeds the
// peer's and carries the time series, phase boundaries, per-block retained
// samples, baseline, limits, manifest and a pass/fail/not-evaluated status
// per criterion.
//
// Exit status: 0 the run passed every criterion; 1 a criterion failed (or the
// peer process failed); 2 the command line was invalid and nothing ran; 3 the
// run was incomplete, some criterion not evaluated; 4 the run was invalid.
//
//	perfchurn -role=peer -sctp-address=ASP_IP:2905 -local-ip=PEER_IP
//	perfchurn -role=asp -peer-control=http://PEER_IP:8080 -routes=1000
package main

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/signal"
	"syscall"
)

const (
	exitPass       = 0
	exitFail       = 1
	exitUsage      = 2
	exitIncomplete = 3
	exitInvalid    = 4
)

// exitCodeFor maps the ASP verdict to the process exit status, so a caller
// that only checks the status cannot mistake a failed or incomplete run for a
// passing one.
func exitCodeFor(verdict string) int {
	switch verdict {
	case verdictPass:
		return exitPass
	case verdictFail:
		return exitFail
	case verdictIncomplete:
		return exitIncomplete
	default:
		return exitInvalid
	}
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(arguments []string, stdout, stderr io.Writer) int {
	config, err := parseConfig(arguments)
	if err != nil {
		_ = json.NewEncoder(stderr).Encode(map[string]string{"verdict": verdictInvalid, "error": err.Error()})
		return exitUsage
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	if config.Role == rolePeer {
		record, err := runPeer(ctx, config)
		if err != nil && record.Error == "" {
			record.Error = err.Error()
		}
		_ = encoder.Encode(record)
		if err != nil {
			return exitFail
		}
		return exitPass
	}
	record := runASP(ctx, config)
	_ = encoder.Encode(record)
	return exitCodeFor(record.Verdict)
}
