// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestSSNMOperationCollectorCleanupWithoutFinish(testContext *testing.T) {
	collector := newSSNMOperationCollector(testContext, 1, func(ctx context.Context) (SSNMEvent, error) {
		<-ctx.Done()
		return SSNMEvent{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	collector.start(ctx, 1)
	cancel()
}

func TestSSNMOperationUpdateFixture(t *testing.T) {
	for _, congestion := range []bool{false, true} {
		t.Run(fmt.Sprintf("congestion=%v", congestion), func(t *testing.T) {
			fixture := newSSNMOperationFixture(t, 1, 1, 2, true)
			partition := fixture.partitions[0]
			for iteration := 0; iteration < 4; iteration++ {
				ranges := partition.ranges[iteration%2 : iteration%2+1]
				fixture.runUntimed(t, partition, ranges, newSSNMOperationInput(partition, ranges, congestion, iteration%2 != 0))
			}
			if err := fixture.verifySnapshot(fixture.endpoint.SSNMKnowledge()); err != nil {
				t.Fatal(err)
			}
			snapshot := fixture.endpoint.SSNMKnowledge()
			snapshot.Partitions[0].Bindings[0].Association++
			snapshot.Partitions[0].Destinations[0].Availability.Scope.RoutingContexts[0]++
			snapshot.Partitions[0].Destinations[0].Congestion.Scope.RoutingContexts[0]++
			if err := fixture.verifySnapshot(fixture.endpoint.SSNMKnowledge()); err != nil {
				t.Fatalf("snapshot aliases retained state: %v", err)
			}
		})
	}
}

func TestSSNMOperationFixturePopulations(t *testing.T) {
	for _, config := range []struct{ peers, partitions, ranges, records int }{{1, 1, 2, 4}, {1, 1, 1024, 2048}, {32, 4, 64, 16384}} {
		t.Run(fmt.Sprintf("records=%d", config.records), func(t *testing.T) {
			fixture := newSSNMOperationFixture(t, config.peers, config.partitions, config.ranges, false)
			snapshot, subscription, err := fixture.endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = subscription.Close() })
			if err := fixture.verifySnapshot(snapshot); err != nil {
				t.Fatal(err)
			}
			records := 0
			peers := make(map[AssociationID]bool)
			for _, partition := range snapshot.Partitions {
				for _, binding := range partition.Bindings {
					peers[binding.Association] = true
				}
				for _, state := range partition.Destinations {
					if state.AvailabilitySet {
						records++
					}
					if state.CongestionSet {
						records++
					}
				}
			}
			if records != config.records || len(peers) != config.peers || len(snapshot.Partitions) != config.peers*config.partitions {
				t.Fatalf("population records=%d peers=%d partitions=%d", records, len(peers), len(snapshot.Partitions))
			}
		})
	}
}

func verifySSNMOperationAtomicBoundary(snapshot SSNMSnapshot, subscription *SSNMSubscription, before, after SSNMSnapshot, wantEvent SSNMEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	switch snapshot.Revision {
	case before.Revision:
		if err := verifySSNMOperationSnapshot(snapshot, before); err != nil {
			return err
		}
		event, err := subscription.Next(ctx)
		if err != nil {
			return fmt.Errorf("snapshot/delta gap: %w", err)
		}
		if err := verifySSNMOperation(event, wantEvent); err != nil {
			return err
		}
	case after.Revision:
		if err := verifySSNMOperationSnapshot(snapshot, after); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unexpected atomic revision %d", snapshot.Revision)
	}
	cancel()
	if _, err := subscription.Next(ctx); err != context.Canceled {
		return fmt.Errorf("duplicate delta or loss: %v", err)
	}
	return nil
}

func TestSSNMOperationAtomicSubscription(t *testing.T) {
	fixture := newSSNMOperationFixture(t, 1, 1, 2, false)
	partition := fixture.partitions[0]
	ranges := partition.ranges[:1]
	for iteration := 0; iteration < 32; iteration++ {
		input := newSSNMOperationInput(partition, ranges, false, iteration%2 != 0)
		before := fixture.expectedSnapshot()
		wantCached, wantEvent := fixture.expected(partition, ranges, input)
		start := make(chan struct{})
		finished := make(chan error, 1)
		go func() { <-start; finished <- input.apply(partition.association) }()
		close(start)
		snapshot, subscription, err := fixture.endpoint.SubscribeSSNM()
		if applyErr := <-finished; applyErr != nil {
			t.Fatal(applyErr)
		}
		validationErr := fixture.finish(partition, wantCached, wantEvent)
		if err != nil {
			t.Fatal(err)
		}
		if validationErr != nil {
			t.Fatal(validationErr)
		}
		boundaryErr := verifySSNMOperationAtomicBoundary(snapshot, subscription, before, fixture.expectedSnapshot(), wantEvent)
		_ = subscription.Close()
		if boundaryErr != nil {
			t.Fatal(boundaryErr)
		}
		if snapshot.Revision == fixture.revision {
			if err := fixture.verifySnapshot(snapshot); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestSSNMOperationAtomicOracleRejectsRegistrationGap(t *testing.T) {
	fixture := newSSNMOperationFixture(t, 1, 1, 2, false)
	partition := fixture.partitions[0]
	ranges := partition.ranges[:1]
	before := fixture.expectedSnapshot()
	input := newSSNMOperationInput(partition, ranges, false, false)
	wantCached, wantEvent := fixture.expected(partition, ranges, input)
	if err := input.apply(partition.association); err != nil {
		t.Fatal(err)
	}
	if err := fixture.finish(partition, wantCached, wantEvent); err != nil {
		t.Fatal(err)
	}
	_, subscription, err := fixture.endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	if err := verifySSNMOperationAtomicBoundary(before, subscription, before, fixture.expectedSnapshot(), wantEvent); err == nil {
		t.Fatal("non-atomic snapshot then registration accepted")
	}
}

func TestSSNMOperationOracleRejectsCorruption(testContext *testing.T) {
	wantEvent := SSNMEvent{Kind: SSNMReportEvent, Revision: 9, ReportSet: true, Report: SSNMReport{Kind: SSNMDestinationUnavailableReport}, Updated: []SSNMDestinationKnowledge{{Destination: PointCodeRange{PointCode: 256, Mask: 8}, AvailabilitySet: true, Availability: SSNMAvailability{State: DestinationUnavailable}}}}
	for _, name := range []string{"missing-typed", "wrong-dimension", "typed-loss", "duplicate-destination", "stale-revision", "wrong-scope", "wrong-association", "wrong-epoch"} {
		testContext.Run(name, func(testContext *testing.T) {
			event := wantEvent
			switch name {
			case "missing-typed":
				event = SSNMEvent{}
			case "wrong-dimension":
				event.Updated = []SSNMDestinationKnowledge{{Destination: PointCodeRange{PointCode: 256, Mask: 8}, CongestionSet: true}}
			case "typed-loss":
				event.ContinuityLost = true
			case "duplicate-destination":
				event.Updated = append(append([]SSNMDestinationKnowledge(nil), event.Updated...), event.Updated[0])
			case "stale-revision":
				event.Revision--
			case "wrong-scope":
				event.Report.Scope.NetworkAppearanceSet = true
			case "wrong-association":
				event.Report.Association++
			case "wrong-epoch":
				event.Epoch++
			}
			if err := verifySSNMOperation(event, wantEvent); err == nil {
				testContext.Fatal("corrupt operation accepted")
			}
		})
	}
}

func TestSSNMOperationCacheOracleRejectsWrongDimension(testContext *testing.T) {
	fixture := newSSNMOperationFixture(testContext, 1, 1, 2, false)
	partition := fixture.partitions[0]
	ranges := partition.ranges[:1]
	wantCached, wantEvent := fixture.expected(partition, ranges, newSSNMOperationInput(partition, ranges, true, false))
	if err := newSSNMOperationInput(partition, ranges, false, false).apply(partition.association); err != nil {
		testContext.Fatal(err)
	}
	if err := fixture.finish(partition, wantCached, wantEvent); err == nil {
		testContext.Fatal("wrong cached dimension accepted")
	}
}

func TestSSNMOperationFixtureRejectsDuplicateTypedReport(testContext *testing.T) {
	fixture := newSSNMOperationFixture(testContext, 1, 1, 2, true)
	partition := fixture.partitions[0]
	ranges := partition.ranges[:1]
	input := newSSNMOperationInput(partition, ranges, false, false)
	wantCached, wantEvent := fixture.expected(partition, ranges, input)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	fixture.typed.start(ctx, 1)
	for iteration := 0; iteration < 2; iteration++ {
		if err := input.apply(partition.association); err != nil {
			testContext.Fatal(err)
		}
	}
	if err := fixture.finish(partition, wantCached, wantEvent); err == nil {
		testContext.Fatal("duplicate typed publication accepted")
	}
}
