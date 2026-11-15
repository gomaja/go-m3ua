package main

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
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
	const overloadedRate = 1_000_000
	const overloadedDuration = 200 * time.Millisecond
	const overloadedMessages = int(overloadedRate * overloadedDuration / time.Second)
	for _, sharedClock := range []bool{false, true} {
		name := "http-interval"
		if sharedClock {
			name = "same-host-clock"
		}
		testContext.Run(name, func(testContext *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			// Observe markers only after the receiver has accounted for them.
			var recordedFlows atomic.Uint32
			strayHash := cohortHash("after-backlog-stray")
			// Reserve room for the entire overloaded offer and its markers.
			// Receiver queue loss must not discard this sender-backlog test's
			// synchronization markers while its reader catches up.
			receiver := startWithholdingReceiverWithQueue(testContext, ctx, sharedClock, -1, false, overloadedMessages+flowCount, func(message receivedMessage) {
				identity, err := parsePayload(message.ProtocolData.Data)
				if err == nil && identity.CohortHash == strayHash {
					recordedFlows.Or(1 << identity.Flow)
				}
			})
			config := withholdingSenderConfig(receiver, sharedClock)
			endpointConfig := senderEndpointConfig(config)
			endpointConfig.TransportCongestion = &m3ua.TransportCongestionConfig{Disabled: true}
			endpoint, err := m3ua.NewEndpoint(endpointConfig)
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
			overloaded.Rate, overloaded.Drain = overloadedRate, time.Millisecond
			sender, peer, err := runSenderCohort(ctx, overloaded, associations, nil, "backlog", overloadedDuration)
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

			drainContext, cancelDrain := context.WithTimeout(ctx, 10*time.Second)
			defer cancelDrain()
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
				for flow := range flowCount {
					identity := planMessage("after-backlog-stray", 1, uint64(flow), len(associations))
					if int(identity.Association) != index {
						continue
					}
					request := tupleFor(identity.Flow, identity.Association).dataRequest(buildPayload(identity, 128))
					var writeErr error
					for attempt := 0; attempt < 1000; attempt++ {
						if _, writeErr = association.WriteDataContext(drainContext, request); writeErr == nil || !errors.Is(writeErr, syscall.EAGAIN) {
							break
						}
						timer := time.NewTimer(10 * time.Millisecond)
						select {
						case <-timer.C:
						case <-drainContext.Done():
							timer.Stop()
						}
					}
					if writeErr != nil {
						testContext.Fatalf("association %d flow %d write after the failed probe: %v; recorded flow mask %#x; submitted %d; delivery %+v",
							index, flow, writeErr, recordedFlows.Load(), sender.Submitted, receiver.control.result().Delivery)
					}
				}
			}
			// Each marker follows that flow's earlier DATA on the same ordered
			// stream (RFC 4666 Section 1.4.7). Wait for every flow to be recorded
			// before resetting the receiver for the nominal cohort. This also
			// covers writes submitted but completed after the shared deadline,
			// which the failed probe counts as unsubmitted.
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			const allFlows = uint32(1<<flowCount - 1)
			for recordedFlows.Load() != allFlows {
				select {
				case <-ticker.C:
				case <-drainContext.Done():
					testContext.Fatalf("receiver drain after the failed probe: %v; recorded flow mask %#x, want %#x; submitted %d; delivery %+v",
						drainContext.Err(), recordedFlows.Load(), allFlows, sender.Submitted, receiver.control.result().Delivery)
				}
			}
			nominal := config
			nominal.Rate, nominal.Drain = 1_000, 500*time.Millisecond
			sender, peer, err = runSenderCohort(ctx, nominal, associations, nil, "after-backlog", 300*time.Millisecond)
			if err != nil || sender.FixtureVerdict != verdictPass || peer.FixtureVerdict != verdictPass {
				testContext.Fatalf("cohort after the failed probe: %v sender %s %q receiver %s %q", err, sender.FixtureVerdict, sender.Reasons, peer.FixtureVerdict, peer.Reasons)
			}
		})
	}
}
