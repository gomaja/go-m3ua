package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func newSSNMRouteOwnershipEndpoint(testContext *testing.T) (*Endpoint, *Association) {
	testContext.Helper()
	config := validASPConfig()
	useSignallingGateways(config, "sg-a")
	config.Routing.MTPRoutes[0].Mask = 17
	endpoint := newSSNMStateEndpoint(testContext, config, nil)
	association := attachASPRouteAssociation(testContext, endpoint, SGPIdentity{
		SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	return endpoint, association
}

func ssnmRouteOwnershipRanges(count int) []PointCodeRange {
	ranges := ssnmOwnershipReport(count, SSNMDestinationUnavailableReport).Destinations
	for index := range ranges {
		ranges[index].PointCode -= 0xe0000
	}
	return ranges
}

func requireSSNMRouteOwnershipRecords(testContext *testing.T, endpoint *Endpoint, ranges []PointCodeRange) {
	testContext.Helper()
	routes := endpoint.aspRoutes
	routes.mu.RLock()
	availabilityRecords := len(routes.availability)
	congestionRecords := len(routes.congestion)
	routes.mu.RUnlock()
	if availabilityRecords != len(ranges) || congestionRecords != len(ranges) {
		testContext.Fatalf("route retained availability=%d congestion=%d, want %d of each", availabilityRecords, congestionRecords, len(ranges))
	}
	for _, destination := range ranges {
		requireASPRouteStatus(testContext, endpoint, "sccp-a", destination.PointCode,
			DestinationUnavailable, true, 2, true)
	}
}

func TestSSNMRouteApplyDoesNotRetainTransientStatuses(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		testContext.Run(fmt.Sprintf("APC%d", count), func(testContext *testing.T) {
			endpoint, association := newSSNMRouteOwnershipEndpoint(testContext)
			ranges := ssnmRouteOwnershipRanges(count)
			routingContexts := []uint32{1}
			values := make([]destinationStatus, count)
			statuses := make([]*destinationStatus, count)
			for index, destination := range ranges {
				values[index] = destinationStatus{
					PointCode: destination.PointCode, Mask: destination.Mask,
					NetworkAppearance: 7, NetworkAppearanceSet: true,
					RoutingContexts: routingContexts, RoutingContextSet: true,
				}
				statuses[index] = &values[index]
			}
			for _, update := range []aspRouteUpdate{
				{kind: aspRouteAvailabilityUpdate, availability: DestinationUnavailable},
				{kind: aspRouteCongestionUpdate, congested: true, congestionLevel: 2, congestionLevelSet: true},
			} {
				if err := endpoint.aspRoutes.apply(association, statuses, update); err != nil {
					testContext.Fatal(err)
				}
			}
			requireSSNMRouteOwnershipRecords(testContext, endpoint, ranges)
			want := ssnmOwnershipValue(testContext, endpoint.aspRoutes.mtpDestinationStatuses())
			routingContexts[0] = 999
			for index := range values {
				values[index] = destinationStatus{PointCode: 0xffffff, Mask: 24}
				statuses[index] = nil
			}
			endpoint.aspRoutes.mu.Lock()
			endpoint.aspRoutes.recomputeLocked(map[MTPRouteID]struct{}{"sccp-a": {}})
			endpoint.aspRoutes.mu.Unlock()
			requireSSNMRouteOwnershipRecords(testContext, endpoint, ranges)
			requireSSNMOwnershipValue(testContext, "route projection after transient storage reuse", endpoint.aspRoutes.mtpDestinationStatuses(), want)
		})
	}
}

func TestSSNMRoutePublicationOwnsDecodedInput(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		testContext.Run(fmt.Sprintf("APC%d", count), func(testContext *testing.T) {
			endpoint, association := newSSNMRouteOwnershipEndpoint(testContext)
			first := mustSubscribeSSNM(testContext, endpoint)
			second := mustSubscribeSSNM(testContext, endpoint)
			ranges := ssnmRouteOwnershipRanges(count)
			words := make([]uint32, len(ranges))
			for index, destination := range ranges {
				words[index] = uint32(destination.Mask)<<24 | destination.PointCode
			}
			networkAppearance := params.NewNetworkAppearance(7)
			routingContext := params.NewRoutingContext(1)
			affected := params.NewAffectedPointCode(words...)
			congestion := params.NewCongestionIndications(2)
			if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				networkAppearance, routingContext, affected, nil)); err != nil {
				testContext.Fatal(err)
			}
			if err := association.handleSignallingCongestion(messages.NewSignallingCongestion(
				networkAppearance, routingContext, affected, nil, congestion, nil)); err != nil {
				testContext.Fatal(err)
			}
			requireSSNMRouteOwnershipRecords(testContext, endpoint, ranges)
			wantSnapshot := ssnmOwnershipValue(testContext, endpoint.SSNMKnowledge())
			wantRoutes := ssnmOwnershipValue(testContext, endpoint.aspRoutes.mtpDestinationStatuses())
			for _, parameter := range []*params.Param{networkAppearance, routingContext, affected, congestion} {
				clear(parameter.Data)
			}
			clear(words)
			for _, kind := range []SSNMReportKind{SSNMDestinationUnavailableReport, SSNMSignallingCongestionReport} {
				event := nextSSNMOwnershipEvent(testContext, first)
				if event.Kind != SSNMReportEvent || event.Report.Kind != kind || len(event.Updated) != count ||
					len(event.Report.Destinations) != count || event.Report.Scope.NetworkAppearance != 7 ||
					!event.Report.Scope.NetworkAppearanceSet || !event.Report.Scope.RoutingContextSet ||
					len(event.Report.Scope.RoutingContexts) != 1 || event.Report.Scope.RoutingContexts[0] != 1 {
					testContext.Fatal("decoded input mutation changed queued report identity or cardinality")
				}
				for index, destination := range event.Report.Destinations {
					if destination != ranges[index] {
						testContext.Fatalf("decoded input mutation changed destination %d", index)
					}
				}
				wantEvent := ssnmOwnershipValue(testContext, event)
				mutateSSNMOwnershipReport(event.Report)
				mutateSSNMOwnershipStates(event.Updated)
				requireSSNMOwnershipValue(testContext, "route report subscriber isolation", nextSSNMOwnershipEvent(testContext, second), wantEvent)
			}
			requireSSNMRouteOwnershipRecords(testContext, endpoint, ranges)
			requireSSNMOwnershipValue(testContext, "route publication retained state", endpoint.SSNMKnowledge(), wantSnapshot)
			requireSSNMOwnershipValue(testContext, "route publication projection", endpoint.aspRoutes.mtpDestinationStatuses(), wantRoutes)
		})
	}
}
