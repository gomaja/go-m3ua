package main

import (
	"context"
	"errors"
	"slices"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// A cohort offered far above what one association can submit, with a 1 ms
// drain, ends with its fixture queue still full when the drain deadline
// passes. That is an overloaded probe, not a fixture fault: the workers fail
// the work they hold at once on the expired write deadline, the sender record
// names the backlog in sender_drain_timeout and has no fatal error, and the
// associations stay up, so the next cohort on the same associations runs and
// passes. Before, the fixture reported the backlog as a fatal error and closed
// every association.
func TestSenderBacklogAtTheDrainDeadlineIsAFailedProbeOverLoopbackSCTP(testContext *testing.T) {
	for _, sharedClock := range []bool{false, true} {
		name := "http-interval"
		if sharedClock {
			name = "same-host-clock"
		}
		testContext.Run(name, func(testContext *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			receiver := startWithholdingReceiver(testContext, ctx, sharedClock, -1, false)
			config := withholdingSenderConfig(receiver, sharedClock)
			endpoint, err := m3ua.NewEndpoint(senderEndpointConfig(config))
			if err != nil {
				testContext.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			associations, release, err := establishSenderAssociations(ctx, config, m3uaConnector{endpoint: endpoint})
			if err != nil {
				testContext.Fatal(err)
			}
			defer release()
			if err := waitForReady(ctx, config.PeerControl, config.Associations); err != nil {
				testContext.Fatal(err)
			}

			overloaded := config
			overloaded.Rate, overloaded.Drain = 1_000_000, time.Millisecond
			sender, peer, err := runSenderCohort(ctx, overloaded, associations, nil, "backlog", 200*time.Millisecond)
			if err == nil || err.Error() != cohortValidityError {
				testContext.Fatalf("overloaded cohort error %v, want only the validity error", err)
			}
			if sender.FatalError != "" || peer.FatalError != "" {
				testContext.Fatalf("fatal errors: sender %q receiver %q", sender.FatalError, peer.FatalError)
			}
			timeout := sender.SenderDrainTimeout
			if timeout == nil || timeout.Cause != senderDrainTimeoutCause || timeout.Drain != time.Millisecond ||
				timeout.OutstandingAtDeadline == 0 || timeout.Unsubmitted == 0 || timeout.Unsubmitted != sender.SendErrors {
				testContext.Fatalf("sender drain timeout %+v, send errors %d", timeout, sender.SendErrors)
			}
			if sender.DrainTimeout != nil || sender.FixtureVerdict != verdictInvalid ||
				!slices.Contains(sender.Reasons, "scheduled traffic was still unsubmitted at the sender when the drain deadline passed") ||
				sender.Scheduled != sender.Submitted+sender.SendErrors+sender.Capped {
				testContext.Fatalf("sender record: drain timeout %+v verdict %s reasons %q scheduled %d submitted %d send errors %d capped %d",
					sender.DrainTimeout, sender.FixtureVerdict, sender.Reasons, sender.Scheduled, sender.Submitted, sender.SendErrors, sender.Capped)
			}
			testContext.Logf("backlog: %d outstanding at the deadline, %d unsubmitted, %d capped, %d submitted, %d delivered",
				timeout.OutstandingAtDeadline, timeout.Unsubmitted, sender.Capped, sender.Submitted, peer.Delivery.Unique)

			for index, association := range associations {
				if state := association.State(); state != m3ua.StateASPActive {
					testContext.Fatalf("association state after the failed probe = %v, want ASP-ACTIVE", state)
				}
				// The cohort's expired drain deadline no longer bounds a
				// write: left in place it would fail this one at once, and
				// the next write the library makes on its own behalf would
				// close the association. The backlog may still fill the send
				// buffer, so a refusal for that is waited out; a deadline
				// error is the defect.
				identity := planMessage("after-backlog-stray", 1, 0, 1)
				request := tupleFor(identity.Flow, identity.Association).dataRequest(buildPayload(identity, 128))
				var writeErr error
				for attempt := 0; attempt < 1000; attempt++ {
					if _, writeErr = association.WriteData(request); writeErr == nil || !errors.Is(writeErr, syscall.EAGAIN) {
						break
					}
					time.Sleep(10 * time.Millisecond)
				}
				if writeErr != nil {
					testContext.Fatalf("association %d write after the failed probe: %v", index, writeErr)
				}
			}
			// Messages submitted just before the deadline may still be in
			// flight; let them land on the stopped cohort before the next one.
			time.Sleep(300 * time.Millisecond)
			nominal := config
			nominal.Rate, nominal.Drain = 1_000, 500*time.Millisecond
			sender, peer, err = runSenderCohort(ctx, nominal, associations, nil, "after-backlog", 300*time.Millisecond)
			if err != nil || sender.FixtureVerdict != verdictPass || peer.FixtureVerdict != verdictPass {
				testContext.Fatalf("cohort after the failed probe: %v sender %s %q receiver %s %q", err, sender.FixtureVerdict, sender.Reasons, peer.FixtureVerdict, peer.Reasons)
			}
		})
	}
}
