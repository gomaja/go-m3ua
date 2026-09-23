package main

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// A live overload cohort over loopback SCTP. A one-message outstanding cap
// against 40,000 messages/s forces fixture refusals, so the trial exercises
// the refusal classes as well as acceptance; whatever mix of outcomes the
// scheduler produces, every offered message must end in exactly one class,
// the per-message ledgers must reconcile with the receiver's, and no bound
// may be exceeded. Recovery depends on the host's speed and is only logged.
func TestOverloadCohortAccountsForEveryMessageOverLoopbackSCTP(testContext *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	address, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := endpoint.Listen("m3ua", address, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		testContext.Fatal(err)
	}
	control := newReceiverControl(1, maxOutstanding)
	fatal := make(chan error, 1)
	go acceptAndRead(ctx, listener, 1, control, fatal)
	server := httptest.NewServer(control.handler())
	defer server.Close()
	profile, err := parseOverloadProfile("2x:1s,0.5x:12s", 20_000)
	if err != nil {
		testContext.Fatal(err)
	}
	config := commandConfig{
		Role: "asp", Mode: modeThroughput, Direction: directionASPToSGP, Initiation: initiationASPDial,
		SCTPAddress: listener.Addr().String(), Associations: 1, PeerControl: server.URL, Cohort: "overload-live", Seed: 7,
		Rate: 20_000, Workload: workloadMix, Duration: profile.duration(), Drain: overloadRequestDeadline + overloadDrainMargin,
		Outstanding: 1, OverloadProfile: profile.text, overload: profile,
	}
	result, runErr := runSender(ctx, config)
	select {
	case err := <-fatal:
		testContext.Fatal(err)
	default:
	}
	sender := result.Sender
	if sender.Overload == nil || sender.Overload.Acceptance == nil || sender.Spec.Overload == nil || result.Receiver.Overload == nil {
		testContext.Fatalf("overload evidence missing: %v %+v", runErr, sender.Overload)
	}
	overload := sender.Overload
	status := map[string]string{}
	for _, criterion := range overload.Acceptance.Criteria {
		status[criterion.Name] = criterion.Status
		testContext.Logf("%s: %s: %s", criterion.Name, criterion.Status, criterion.Detail)
	}
	for _, name := range []string{"fixture", "accounting", "bounds"} {
		if status[name] != overloadCriterionPass {
			testContext.Errorf("criterion %s = %s", name, status[name])
		}
	}
	totals := overload.Totals
	reconciliation := overload.Reconciliation
	if totals.Offered != profile.expected() || totals.classified() != totals.Offered || totals.Unresolved != 0 ||
		totals.CapRefusedOutstanding+totals.CapRefusedQueue == 0 || totals.Accepted == 0 {
		testContext.Errorf("totals = %+v", totals)
	}
	if reconciliation.Phantom != 0 || reconciliation.DeliveredLedger != result.Receiver.Delivery.Unique ||
		reconciliation.AcceptedLedger != totals.Accepted || reconciliation.UnexplainedMissingLower != 0 {
		testContext.Errorf("reconciliation = %+v", reconciliation)
	}
	if overload.Bounds.MaxOutstanding > 1 || len(overload.Bounds.ReceiverAssociations) != 1 || !overload.Bounds.ReceiverAssociations[0].Active {
		testContext.Errorf("bounds = %+v", overload.Bounds)
	}
	if len(overload.Recovery) != 1 {
		testContext.Fatalf("recovery = %+v", overload.Recovery)
	}
	testContext.Logf("recovery %s in %s; totals %+v; verdict %s (%v)", overload.Recovery[0].Status, overload.Recovery[0].RecoveryTime, *totals, overload.Acceptance.Verdict, runErr)
}
