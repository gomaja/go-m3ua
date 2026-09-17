package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

// acceptAndRead and dialAndRead register every accepted/dialed association
// for the bidirectional reverse cohort. Non-bidirectional receivers and
// test-constructed controls have no reverse driver; registration there must
// be a no-op, not the nil dereference that panicked the Linux suite.
func TestRegisterReverseAssociationWithoutDriverIsANoOp(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.registerReverseAssociation(nil)
	control.registerReverseAssociation(nil)

	withDriver := newReceiverControl(1, 16)
	withDriver.driver = &reverseDriver{}
	withDriver.registerReverseAssociation(nil)
	withDriver.registerReverseAssociation(nil)
	if len(withDriver.driver.associations) != 2 {
		testContext.Fatalf("driver holds %d associations, want 2", len(withDriver.driver.associations))
	}
}

// runSender creates the echo reply registry only in echo mode; every other
// runSenderCohort caller passes nil. An echo cohort without a registry must
// fail with a named error before touching the network, not panic on the nil
// registry deep in the cohort.
func TestRunSenderCohortEchoModeWithoutRegistryIsANamedError(testContext *testing.T) {
	config := commandConfig{Mode: modeEcho, Rate: 1, Workload: workload128, Outstanding: 8}
	_, _, err := runSenderCohort(context.Background(), config, nil, nil, "echo-no-registry", time.Second)
	if err == nil || !strings.Contains(err.Error(), "echo reply registry") {
		testContext.Fatalf("runSenderCohort error = %v, want the named missing-registry error", err)
	}
}

// A control without a reverse driver must never spawn a reverse cohort, even
// when a bidirectional spec arrives: only the SGP's runReceiver control owns
// a driver.
func TestBidirectionalStartWithoutReverseDriverDoesNotSpawn(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.reverseControl = "http://127.0.0.1:1"
	specification := runSpec{
		Cohort: "bidi-no-driver", Seed: 1, Associations: 1, Expected: 1, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 1, Payload: workload128, Mode: modeBidirectional,
		Direction: directionASPToSGP, PeerControl: control.reverseControl,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	record := control.result()
	if record.Reverse != nil || record.ReverseReceiver != nil || record.ReverseError != "" {
		testContext.Fatalf("driverless control spawned reverse evidence: %+v", record)
	}
}
