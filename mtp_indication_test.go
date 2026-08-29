// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"
	"time"

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
