package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestAssociationCloseRetiresTypedSSNMAndPausesCachedDestinations(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(testContext, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	subscription := observeSSNM(testContext, association)
	for _, pointCode := range []uint32{0x111111, 0x222222} {
		if err := association.handleDestinationAvailable(messages.NewDestinationAvailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), apc(pointCode), nil)); err != nil {
			testContext.Fatal(err)
		}
		report := nextSSNMReport(testContext, association)
		if report.Kind != SSNMDestinationAvailableReport || report.Destinations[0].PointCode != pointCode {
			testContext.Fatalf("report = %+v", report)
		}
	}
	if err := association.Close(); err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil || event.Kind != SSNMPartitionRetiredEvent {
		testContext.Fatalf("close event = %+v, %v", event, err)
	}
	if snapshot := endpoint.SSNMKnowledge(); len(snapshot.Partitions) != 0 {
		testContext.Fatalf("last-binding knowledge survived close: %+v", snapshot)
	}
	for _, pointCode := range []uint32{0x111111, 0x222222} {
		if state := retainedAvailabilityForNetworkAndRoutingContext(association, 7, 1, pointCode); state != DestinationUnavailable {
			testContext.Fatalf("cached availability = %v", state)
		}
	}
	if err := association.Close(); err != nil {
		testContext.Fatal(err)
	}
}

func TestTypedSSNMReaderTerminatesWithEndpoint(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { _, nextErr := subscription.Next(ctx); finished <- nextErr }()
	if err := endpoint.Close(); err != nil {
		testContext.Fatal(err)
	}
	if err := <-finished; !errors.Is(err, ErrEndpointClosed) {
		testContext.Fatalf("Next after Endpoint close = %v", err)
	}
}

func TestSSNMReportsRacingAssociationClose(testContext *testing.T) {
	association, _ := newSSNMTestConn(testContext, StateASPActive, RoleASP)
	var writers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				_ = association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(uint32(iteration)), nil))
			}
		}()
	}
	if err := association.Close(); err != nil {
		testContext.Fatal(err)
	}
	writers.Wait()
}

func TestUnreadTypedSSNMDoesNotBlockClose(testContext *testing.T) {
	association, _ := newSSNMTestConn(testContext, StateASPActive, RoleASP)
	for pointCode := uint32(0); pointCode < 300; pointCode++ {
		if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(pointCode), nil)); err != nil {
			testContext.Fatal(err)
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- association.Close() }()
	select {
	case err := <-finished:
		if err != nil {
			testContext.Fatal(err)
		}
	case <-time.After(time.Second):
		testContext.Fatal("association close blocked on unread typed subscription")
	}
}

func TestClosingAnAssociationDoesNotInventDestinations(testContext *testing.T) {
	association, _ := newSSNMTestConn(testContext, StateASPActive, RoleASP)
	if err := association.Close(); err != nil {
		testContext.Fatal(err)
	}
	if ranges := retainedRanges(association); len(ranges) != 0 {
		testContext.Fatalf("invented cached destinations: %+v", ranges)
	}
	if snapshot := association.endpoint.SSNMKnowledge(); len(snapshot.Partitions) != 0 {
		testContext.Fatalf("invented typed destination knowledge: %+v", snapshot)
	}
}
