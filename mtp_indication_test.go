// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestEndpointMTPDestinationStatus(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	drainMTPIndications(endpoint.MTPIndications())

	const pointCode = uint32(0x123456)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPSCON(t, second, 9, 42, pointCode, 0, params.NewCongestionIndications(2))

	status, known := endpoint.MTPDestinationStatus(MTPDestination{
		MTPRoute:  "sccp-a",
		PointCode: pointCode,
	})
	if !known {
		t.Fatal("MTPDestinationStatus reported the provisioned destination unknown")
	}
	if status.Destination != (MTPDestination{MTPRoute: "sccp-a", PointCode: pointCode}) ||
		status.Availability != DestinationAvailable || !status.Congested ||
		status.CongestionLevel != 2 || !status.CongestionLevelSet {
		t.Fatalf("MTP destination status = %#v", status)
	}
}

func TestEndpointMTPDestinationStatusRejectsMixedRange(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	drainMTPIndications(endpoint.MTPIndications())
	const pointCode = uint32(0x123456)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPDUNA(t, second, 9, 42, pointCode, 0)

	if status, known := endpoint.MTPDestinationStatus(MTPDestination{
		MTPRoute: "sccp-a", PointCode: 0x120000, Mask: 16,
	}); known {
		t.Fatalf("mixed MTP Route range returned one status: %#v", status)
	}
	if status, known := endpoint.MTPDestinationStatus(MTPDestination{
		MTPRoute: "sccp-a", PointCode: pointCode,
	}); !known || status.Availability != DestinationUnavailable {
		t.Fatalf("unavailable exact destination = %#v, known %v", status, known)
	}
	if status, known := endpoint.MTPDestinationStatus(MTPDestination{
		MTPRoute: "sccp-a", PointCode: pointCode + 1,
	}); !known || status.Availability != DestinationAvailable {
		t.Fatalf("available exact destination = %#v, known %v", status, known)
	}
}

func TestEndpointMTPDestinationStatusesReturnsCanonicalSnapshot(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	drainMTPIndications(endpoint.MTPIndications())
	const pointCode = uint32(0x123456)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPDUNA(t, second, 9, 42, pointCode, 0)

	statuses := endpoint.MTPDestinationStatuses()
	if len(statuses) != 17 {
		t.Fatalf("canonical status count = %d, want 17: %#v", len(statuses), statuses)
	}
	foundUnavailable := false
	for firstIndex, firstStatus := range statuses {
		if firstStatus.Destination.MTPRoute != "sccp-a" {
			t.Fatalf("snapshot contains unexpected MTP Route: %#v", firstStatus)
		}
		if firstStatus.Destination.PointCode == pointCode && firstStatus.Destination.Mask == 0 {
			foundUnavailable = firstStatus.Availability == DestinationUnavailable
		} else if firstStatus.Availability != DestinationAvailable {
			t.Fatalf("unaffected destination is not available: %#v", firstStatus)
		}
		for secondIndex := firstIndex + 1; secondIndex < len(statuses); secondIndex++ {
			secondStatus := statuses[secondIndex]
			if aspRangesOverlap(
				firstStatus.Destination.PointCode, firstStatus.Destination.Mask,
				secondStatus.Destination.PointCode, secondStatus.Destination.Mask,
			) {
				t.Fatalf("snapshot ranges overlap: %#v and %#v", firstStatus, secondStatus)
			}
		}
	}
	if !foundUnavailable {
		t.Fatal("canonical snapshot omitted the unavailable exact destination")
	}
}

func TestEndpointMTPPauseOnlyAfterLastSignallingGatewayRouteFails(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)
	const pointCode = uint32(0x123456)

	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	requireNoMTPIndication(t, indications)

	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", pointCode, 0,
		DestinationUnavailable, false, 0, false)

	applyASPDAVA(t, first, 7, 1, pointCode, 0)
	requireMTPIndication(t, indications, MTPResumeIndication, "sccp-a", pointCode, 0,
		DestinationAvailable, false, 0, false)
}

func TestEndpointMTPStatusReportsRestrictionAndCongestion(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)
	const pointCode = uint32(0x123456)

	applyASPDRST(t, first, 7, 1, pointCode, 0)
	requireNoMTPIndication(t, indications)
	applyASPDRST(t, second, 9, 42, pointCode, 0)
	requireMTPIndication(t, indications, MTPStatusIndication, "sccp-a", pointCode, 0,
		DestinationRestricted, false, 0, false)

	applyASPSCON(t, first, 7, 1, pointCode, 0, params.NewCongestionIndications(2))
	requireNoMTPIndication(t, indications)
	applyASPSCON(t, second, 9, 42, pointCode, 0, params.NewCongestionIndications(3))
	requireMTPIndication(t, indications, MTPStatusIndication, "sccp-a", pointCode, 0,
		DestinationRestricted, true, 2, true)
}

func TestEndpointMTPSuppressesUnchangedDerivedStatus(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)
	const pointCode = uint32(0x123456)

	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	requireNoMTPIndication(t, indications)
	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", pointCode, 0,
		DestinationUnavailable, false, 0, false)
	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	requireNoMTPIndication(t, indications)
}

func TestEndpointMTPIndicationsPartitionOverlappingRanges(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	applyASPDUNA(t, first, 7, 1, 0x120000, 16)
	requireNoMTPIndication(t, indications)
	applyASPDUNA(t, second, 9, 42, 0x123456, 0)
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", 0x123456, 0,
		DestinationUnavailable, false, 0, false)
	requireNoMTPIndication(t, indications)

	applyASPDAVA(t, first, 7, 1, 0x123456, 0)
	requireMTPIndication(t, indications, MTPResumeIndication, "sccp-a", 0x123456, 0,
		DestinationAvailable, false, 0, false)
	requireNoMTPIndication(t, indications)
}

func TestClosingOneASPAssociationDoesNotPauseReachableDestination(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	if err := first.Close(); err != nil {
		t.Fatalf("close first Association: %v", err)
	}
	requireNoMTPIndication(t, indications)

	if err := second.Close(); err != nil {
		t.Fatalf("close second Association: %v", err)
	}
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", 0x120000, 16,
		DestinationUnavailable, false, 0, false)

	select {
	case indication, open := <-indications:
		if !open {
			t.Fatal("Association close closed the Endpoint MTP indication channel")
		}
		t.Fatalf("unexpected MTP indication after Association close: %#v", indication)
	default:
	}
}

func TestClosingPartitionedSignallingGatewayDoesNotEmitSpuriousResume(t *testing.T) {
	endpoint, first, _ := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	requireNoMTPIndication(t, indications)
	if err := first.Close(); err != nil {
		t.Fatalf("close partitioned SG Association: %v", err)
	}
	requireNoMTPIndication(t, indications)
}

func TestClosingLastASPAssociationReportsEveryCoalescedPause(t *testing.T) {
	config := validASPConfig()
	config.SignallingGateways = config.SignallingGateways[:1]
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association := attachASPRouteAssociation(t, endpoint, SGPIdentity{
		SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	applyASPDUNA(t, association, 7, 1, 0x123456, 0)
	drainMTPIndications(indications)
	expected := make(map[MTPDestination]struct{})
	for _, status := range endpoint.MTPDestinationStatuses() {
		if status.Availability != DestinationUnavailable {
			expected[status.Destination] = struct{}{}
		}
	}
	if len(expected) == 0 {
		t.Fatal("pre-close route snapshot has no available ranges")
	}

	if err := association.Close(); err != nil {
		t.Fatalf("close last Association: %v", err)
	}
	for range len(expected) {
		select {
		case indication := <-indications:
			if indication == nil || indication.ResyncRequired || indication.Kind != MTPPauseIndication ||
				indication.Destination.Availability != DestinationUnavailable {
				t.Fatalf("coalesced transition indication = %#v, want MTP-PAUSE", indication)
			}
			if _, exists := expected[indication.Destination.Destination]; !exists {
				t.Fatalf("unexpected coalesced MTP-PAUSE destination %#v", indication.Destination.Destination)
			}
			delete(expected, indication.Destination.Destination)
		default:
			t.Fatalf("missing MTP-PAUSE indications for %v", expected)
		}
	}
	if len(expected) != 0 {
		t.Fatalf("missing MTP-PAUSE indications for %v", expected)
	}
	requireNoMTPIndication(t, indications)
	statuses := endpoint.MTPDestinationStatuses()
	if len(statuses) != 1 || statuses[0].Destination != (MTPDestination{
		MTPRoute: "sccp-a", PointCode: 0x120000, Mask: 16,
	}) || statuses[0].Availability != DestinationUnavailable {
		t.Fatalf("post-detach MTP destination statuses = %#v, want configured unavailable baseline", statuses)
	}
}

func TestEndpointMTPIndicationsFollowCommittedAssociationState(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	if !first.commitState(StateASPDown) {
		t.Fatal("first ASP-DOWN state was not committed")
	}
	requireNoMTPIndication(t, indications)
	if !second.commitState(StateASPDown) {
		t.Fatal("second ASP-DOWN state was not committed")
	}
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", 0x120000, 16,
		DestinationUnavailable, false, 0, false)

	if !first.commitState(StateASPActive) {
		t.Fatal("first ASP-ACTIVE state was not committed")
	}
	requireMTPIndication(t, indications, MTPResumeIndication, "sccp-a", 0x120000, 16,
		DestinationAvailable, false, 0, false)
}

func TestEndpointMTPIndicationsFollowActiveRoutingContextScope(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	first.noteRoutingContextsUnacked(params.NewRoutingContext(1))
	requireNoMTPIndication(t, indications)
	second.noteRoutingContextsUnacked(params.NewRoutingContext(42))
	requireMTPIndication(t, indications, MTPPauseIndication, "sccp-a", 0x120000, 16,
		DestinationUnavailable, false, 0, false)

	first.noteRoutingContextsAcked(params.NewRoutingContext(1))
	requireMTPIndication(t, indications, MTPResumeIndication, "sccp-a", 0x120000, 16,
		DestinationAvailable, false, 0, false)
}

func TestEndpointCloseClosesMTPIndications(t *testing.T) {
	endpoint, _, _ := newASPMultiSGFixture(t)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	for {
		select {
		case _, open := <-indications:
			if !open {
				return
			}
		case <-time.After(time.Second):
			t.Fatal("Endpoint.Close did not close MTPIndications")
		}
	}
}

func TestEndpointMTPIndicationOverflowRequiresResynchronization(t *testing.T) {
	config := validASPConfig()
	config.MTPIndicationQueueSize = 1
	endpoint, first, second := newASPMultiSGFixtureWithConfig(t, config)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)
	const pointCode = uint32(0x123456)

	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	applyASPDAVA(t, first, 7, 1, pointCode, 0)

	select {
	case indication := <-indications:
		if indication == nil || !indication.ResyncRequired {
			t.Fatalf("overflow indication = %#v, want ResyncRequired", indication)
		}
	default:
		t.Fatal("overflow did not publish a resynchronization marker")
	}
}

func TestEndpointMTPIndicationRepeatedOverflowKeepsSingleResyncMarker(t *testing.T) {
	config := validASPConfig()
	config.MTPIndicationQueueSize = 3
	endpoint, _, _ := newASPMultiSGFixtureWithConfig(t, config)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	for index := 0; index < 7; index++ {
		endpoint.aspRoutes.publishOne(&MTPIndication{Kind: MTPStatusIndication})
	}

	var got []*MTPIndication
	for len(indications) > 0 {
		got = append(got, <-indications)
	}
	if len(got) != 1 || got[0] == nil || !got[0].ResyncRequired {
		t.Fatalf("overflow queue = %#v, want one ResyncRequired marker", got)
	}
}

func TestEndpointMTPIndicationDropsDeltasWhileResyncIsPending(t *testing.T) {
	config := validASPConfig()
	config.MTPIndicationQueueSize = 3
	endpoint, _, _ := newASPMultiSGFixtureWithConfig(t, config)
	indications := endpoint.MTPIndications()
	drainMTPIndications(indications)

	for index := 0; index < 5; index++ {
		endpoint.aspRoutes.publishOne(&MTPIndication{Kind: MTPStatusIndication})
	}
	if len(indications) != 1 {
		t.Fatalf("pending resynchronization queue length = %d, want 1", len(indications))
	}
	marker := <-indications
	if marker == nil || !marker.ResyncRequired {
		t.Fatalf("overflow indication = %#v, want ResyncRequired", marker)
	}

	endpoint.aspRoutes.publishOne(&MTPIndication{Kind: MTPStatusIndication})
	select {
	case indication := <-indications:
		if indication == nil || indication.ResyncRequired || indication.Kind != MTPStatusIndication {
			t.Fatalf("post-resynchronization indication = %#v", indication)
		}
	default:
		t.Fatal("indication stream did not resume after the resynchronization marker was consumed")
	}
}

func TestASPRoutesPublishesEachMutationBatchAtomically(t *testing.T) {
	routes := &aspRoutes{}
	const firstBatchSize = 100_000
	firstIndication := &MTPIndication{Kind: MTPPauseIndication}
	firstBatch := make([]*MTPIndication, firstBatchSize)
	for index := range firstBatch {
		firstBatch[index] = firstIndication
	}
	laterBatch := []*MTPIndication{{ResyncRequired: true}}
	routes.indications = make(chan *MTPIndication, len(firstBatch)+len(laterBatch))

	routes.indicationMu.Lock()
	firstStarted := make(chan struct{})
	laterStarted := make(chan struct{})
	var publishers sync.WaitGroup
	publishers.Add(2)
	go func() {
		defer publishers.Done()
		close(firstStarted)
		routes.publish(firstBatch)
	}()
	<-firstStarted
	for range 100 {
		runtime.Gosched()
	}
	go func() {
		defer publishers.Done()
		close(laterStarted)
		routes.publish(laterBatch)
	}()
	<-laterStarted
	for range 100 {
		runtime.Gosched()
	}
	routes.indicationMu.Unlock()
	publishers.Wait()

	if len(routes.indications) != firstBatchSize+1 {
		t.Fatalf("published %d indications, want %d", len(routes.indications), firstBatchSize+1)
	}
	firstPublished := <-routes.indications
	switch firstPublished {
	case firstIndication:
		for index := 1; index < firstBatchSize; index++ {
			if got := <-routes.indications; got != firstIndication {
				t.Fatalf("indication %d = %#v, want an item from the same mutation batch", index, got)
			}
		}
		if got := <-routes.indications; got != laterBatch[0] {
			t.Fatalf("last indication = %#v, want other mutation batch %#v", got, laterBatch[0])
		}
	case laterBatch[0]:
		for index := range firstBatchSize {
			if got := <-routes.indications; got != firstIndication {
				t.Fatalf("indication %d after the single-item batch = %#v, want an item from the same mutation batch", index, got)
			}
		}
	default:
		t.Fatalf("first indication = %#v, want an item from either mutation batch", firstPublished)
	}
}

func TestASPRoutesKeepsMutationOrderedThroughIndicationPublication(t *testing.T) {
	endpoint, association, alternate := newASPMultiSGFixture(t)
	if err := alternate.Close(); err != nil {
		t.Fatalf("close alternate SG Association: %v", err)
	}
	routes := endpoint.aspRoutes
	drainMTPIndications(routes.indications)

	routes.indicationMu.Lock()
	routes.mu.Lock()
	started := make(chan struct{})
	completed := make(chan error, 1)
	go func() {
		close(started)
		completed <- association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			params.NewNetworkAppearance(7),
			params.NewRoutingContext(1),
			params.NewAffectedPointCode(0x123456),
			nil,
		))
	}()
	<-started
	for range 100 {
		runtime.Gosched()
	}
	routes.mu.Unlock()

	stateVisibleBeforePublication := false
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		if routes.mu.TryLock() {
			stateVisibleBeforePublication = len(routes.availability) != 0
			routes.mu.Unlock()
			if stateVisibleBeforePublication {
				break
			}
		}
		runtime.Gosched()
	}
	routes.indicationMu.Unlock()
	if err := <-completed; err != nil {
		t.Fatalf("handleDestinationUnavailable: %v", err)
	}
	if stateVisibleBeforePublication {
		t.Fatal("route mutation became visible before its indication batch was published")
	}
	requireMTPIndication(t, routes.indications, MTPPauseIndication, "sccp-a", 0x123456, 0,
		DestinationUnavailable, false, 0, false)
}

func drainMTPIndications(indications <-chan *MTPIndication) {
	for {
		select {
		case <-indications:
		default:
			return
		}
	}
}

func requireNoMTPIndication(t *testing.T, indications <-chan *MTPIndication) {
	t.Helper()
	select {
	case indication, open := <-indications:
		if !open {
			t.Fatal("MTP indication channel closed")
		}
		t.Fatalf("unexpected MTP indication: %#v", indication)
	default:
	}
}

func requireMTPIndication(
	t *testing.T,
	indications <-chan *MTPIndication,
	kind MTPIndicationKind,
	mtpRoute MTPRouteID,
	pointCode uint32,
	mask uint8,
	availability DestinationState,
	congested bool,
	congestionLevel uint8,
	congestionLevelSet bool,
) {
	t.Helper()
	select {
	case indication, open := <-indications:
		if !open {
			t.Fatal("MTP indication channel closed")
		}
		wantDestination := MTPDestination{MTPRoute: mtpRoute, PointCode: pointCode, Mask: mask}
		if indication == nil || indication.ResyncRequired || indication.Kind != kind ||
			indication.Destination.Destination != wantDestination ||
			indication.Destination.Availability != availability ||
			indication.Destination.Congested != congested ||
			indication.Destination.CongestionLevel != congestionLevel ||
			indication.Destination.CongestionLevelSet != congestionLevelSet {
			t.Fatalf("MTP indication = %#v, want kind=%v destination=%#v availability=%v congested=%v level=%d set=%v",
				indication, kind, wantDestination, availability, congested, congestionLevel, congestionLevelSet)
		}
	default:
		t.Fatalf("missing %v indication", kind)
	}
}
