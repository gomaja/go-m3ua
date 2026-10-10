// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPMultiSGCanonicalPublicationGatesRoutingReadiness(t *testing.T) {
	endpoint, associations, _ := newASPTransferFixture(t, integrationASPConfig())
	first := associations["sg-a/sgp-a1"]
	second := associations["sg-b/sgp-b1"]
	const pointCode = uint32(0x123456)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPDRST(t, second, 9, 42, pointCode, 0)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 5, []byte("through-sg-b"))}
	result, err := endpoint.MTPTransfer(request)
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-b" {
		t.Fatalf("initial transfer: result=%+v err=%v", result, err)
	}
	before := endpoint.SSNMKnowledge().Revision
	// Hold publication after the private cache changes. RFC 4666 Section
	// 4.5.2.2 requires per-SG availability; routing reads canonical knowledge.
	// The hook holds the pending-request lock after DAVA scope validation,
	// so publishSSNMReport waits in ssnmPendingPartitions.
	locked := make(chan struct{})
	endpoint.aspRoutes.applyResolved = func() {
		first.tack.mu.Lock()
		close(locked)
	}
	finished := make(chan error, 1)
	go func() {
		finished <- first.handleDestinationAvailable(messages.NewDestinationAvailable(
			params.NewNetworkAppearance(7), params.NewRoutingContext(1),
			params.NewAffectedPointCode(pointCode), nil,
		))
	}()
	select {
	case <-locked:
	case <-time.After(10 * time.Second):
		t.Fatal("DAVA did not reach route-resolution hook")
	}
	held := true
	defer func() {
		if held {
			first.tack.mu.Unlock()
			<-finished
		}
	}()
	if !waitFor(func() bool {
		return retainedAvailabilityForNetworkAndRoutingContext(first, 7, 1, pointCode) == DestinationAvailable
	}, 5*time.Second) {
		t.Fatal("private availability did not advance while publication was paused")
	}
	partition := canonicalSSNMPartition("sg-a", "as-core")
	status := endpoint.ssnm.destinationKnowledge(partition, pointCode)
	if !status.availabilitySet || status.availability != DestinationUnavailable || endpoint.ssnm.currentRevision() != before {
		t.Fatalf("canonical state advanced before publication: status=%+v revision=%d", status, endpoint.ssnm.currentRevision())
	}
	if integrationAvailabilityReady(endpoint, "sg-a", pointCode, DestinationAvailable) {
		t.Fatal("routing readiness advanced before canonical DAVA publication")
	}
	request.ProtocolData = transferProtocolData(pointCode, 5, []byte("through-sg-a"))
	result, err = endpoint.MTPTransfer(request)
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-b" {
		t.Fatalf("transfer before canonical DAVA publication: result=%+v err=%v", result, err)
	}
	first.tack.mu.Unlock()
	held = false
	if err := <-finished; err != nil {
		t.Fatalf("complete DAVA: %v", err)
	}
	endpoint.aspRoutes.applyResolved = nil
	status = endpoint.ssnm.destinationKnowledge(partition, pointCode)
	if !status.availabilitySet || status.availability != DestinationAvailable {
		t.Fatalf("completed DAVA did not advance canonical state: %+v", status)
	}
	result, err = endpoint.MTPTransfer(request)
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-a" {
		t.Fatalf("transfer after canonical DAVA completion: result=%+v err=%v", result, err)
	}
	if !integrationAvailabilityReady(endpoint, "sg-a", pointCode, DestinationAvailable) {
		t.Fatal("routing readiness did not advance after canonical DAVA publication")
	}
}

func TestASPMultiSGReadinessRequiresRetainedGatewayAvailability(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	const pointCode = uint32(0x123456)
	for _, state := range []DestinationAvailability{
		DestinationAvailable, DestinationUnavailable, DestinationRestricted,
	} {
		if integrationAvailabilityReady(endpoint, "sg-a", pointCode, state) {
			t.Fatalf("unreported destination is ready with state %v", state)
		}
	}
	applyASPDAVA(t, second, 9, 42, pointCode, 0)
	if integrationAvailabilityReady(endpoint, "sg-a", pointCode, DestinationAvailable) {
		t.Fatal("sg-b availability made sg-a ready")
	}
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	if !integrationAvailabilityReady(endpoint, "sg-a", pointCode, DestinationUnavailable) {
		t.Fatal("canonical sg-a DUNA did not make readiness true")
	}
	if integrationAvailabilityReady(endpoint, "sg-a", pointCode+1, DestinationUnavailable) {
		t.Fatal("another destination's DUNA made readiness true")
	}
}
