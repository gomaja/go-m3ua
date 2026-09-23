package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

type routingTimedWorkerFixture struct {
	routed       *routingTimedSender
	direct       *routingTimedSender
	endpoint     *fakeRoutingDataEndpoint
	associations map[m3ua.AssociationID]*routingTimedGoldenAssociation
	paths        routingPathMap
}

func routingTimedConstructorInputs(testContext *testing.T) (*routingDataSenderPlane, *routingDirectWriter, routingPathMap) {
	testContext.Helper()
	topology, pairs, _, endpoint, _ := routingDataFixture(testContext)
	_, bindings, observations := routingPreflightFixture(testContext, "primary")
	paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	associations := make([]routingDataAssociation, len(pairs))
	byID := make(map[m3ua.AssociationID]*routingTimedGoldenAssociation, len(pairs))
	for index, pair := range pairs {
		association := &routingTimedGoldenAssociation{
			id: pair.Binding.SenderAssociation, epoch: pair.SenderEpoch,
			maximum: pair.Binding.MaxMessageStreamID, written: -1,
		}
		associations[index] = association
		byID[association.id] = association
	}
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	directWriter := &routingDirectWriter{
		paths: paths, associations: make(map[m3ua.AssociationID]routingDataAssociation, len(byID)),
		epochs: make(map[m3ua.AssociationID]uint64, len(byID)), admission: make(chan struct{}, maxOutstanding),
	}
	for associationID, association := range byID {
		directWriter.associations[associationID] = association
		directWriter.epochs[associationID] = association.epoch
	}
	return plane, directWriter, paths
}

func newRoutingTimedWorkerFixture(testContext *testing.T) routingTimedWorkerFixture {
	testContext.Helper()
	plane, directWriter, paths := routingTimedConstructorInputs(testContext)
	routed, err := newRoutingTimedSender(routingTimedRouted, plane, nil, paths, workloadMix)
	if err != nil {
		testContext.Fatal(err)
	}
	direct, err := newRoutingTimedSender(routingTimedDirect, plane, directWriter, paths, workloadMix)
	if err != nil {
		testContext.Fatal(err)
	}
	byID := make(map[m3ua.AssociationID]*routingTimedGoldenAssociation, len(plane.associations))
	for associationID, value := range plane.associations {
		association := value.(*routingTimedGoldenAssociation)
		byID[associationID] = association
		association.accessors = nil
	}
	return routingTimedWorkerFixture{routed: routed, direct: direct, endpoint: plane.endpoint.(*fakeRoutingDataEndpoint), associations: byID, paths: paths}
}

func routingTimedOutcomeCase(testContext *testing.T, scenario string) (*routingDirectWriter, *routingTimedGoldenAssociation, []byte, context.Context) {
	testContext.Helper()
	writer, association, payload := routingTimedGoldenWriter(testContext)
	ctx := context.Background()
	switch scenario {
	case "success":
	case "already-canceled":
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	case "stale-before":
		association.epoch++
	case "write-error":
		association.written = 0
		association.writeErr = errRoutingTimedGoldenWrite
	case "partial":
		association.written = len(payload) - 1
	case "changed-after-epoch":
		association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.epoch++ }
	case "changed-after-maximum":
		association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.maximum++ }
	case "partial-write-error-changed-after-epoch":
		association.written = len(payload) - 1
		association.writeErr = errRoutingTimedGoldenWrite
		association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.epoch++ }
	default:
		testContext.Fatalf("unknown direct outcome scenario %q", scenario)
	}
	return writer, association, payload, ctx
}

func routingTimedErrorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func TestRoutingTimedRawOutcomeValidationMatchesGoldenWrite(testContext *testing.T) {
	for _, scenario := range []string{
		"success", "already-canceled", "stale-before", "write-error", "partial", "changed-after-epoch", "changed-after-maximum", "partial-write-error-changed-after-epoch",
	} {
		testContext.Run(scenario, func(testContext *testing.T) {
			writeWriter, writeAssociation, writePayload, writeContext := routingTimedOutcomeCase(testContext, scenario)
			rawWriter, rawAssociation, rawPayload, rawContext := routingTimedOutcomeCase(testContext, scenario)
			writeCount, writeErr := writeWriter.Write(writeContext, 0, writePayload)
			rawCount, rawErr := validateRoutingDirectOutcome(rawWriter.writeOutcome(rawContext, 0, rawPayload))
			if writeCount != rawCount || routingTimedErrorText(writeErr) != routingTimedErrorText(rawErr) ||
				len(writeAssociation.writes) != len(rawAssociation.writes) || strings.Join(writeAssociation.accessors, ",") != strings.Join(rawAssociation.accessors, ",") {
				testContext.Fatalf("Write=%d/%v/%d/%v raw=%d/%v/%d/%v", writeCount, writeErr, len(writeAssociation.writes), writeAssociation.accessors,
					rawCount, rawErr, len(rawAssociation.writes), rawAssociation.accessors)
			}
		})
	}
}

func TestRoutingTimedVariantsPlanIdenticalJobsAcrossAllRoutes(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	var routedCoverage [8]bool
	var directCoverage [8]bool
	for index := uint64(0); index < routingRouteCount; index++ {
		scheduled := time.Unix(100, int64(index))
		routedJob := fixture.routed.job("timed", 9, index, scheduled, nil, time.Duration(index))
		directJob := fixture.direct.job("timed", 9, index, scheduled, nil, time.Duration(index))
		path, err := fixture.paths.path(uint16(index))
		if err != nil {
			testContext.Fatal(err)
		}
		wantRouteID := m3ua.MTPRouteID(fmt.Sprintf("route-%04d", index))
		if routedJob != directJob || routedJob.identity.Route != uint16(index) || routedJob.identity.Sequence != 0 ||
			routedJob.routeID != wantRouteID || routedJob.path != path || routedJob.size != workloadMix.size(index) || routedJob.queue >= 8 {
			testContext.Fatalf("index %d routed=%+v direct=%+v", index, routedJob, directJob)
		}
		routedCoverage[routedJob.queue] = true
		directCoverage[directJob.queue] = true
	}
	if routedCoverage != [8]bool{true, true, true, true, true, true, true, true} || routedCoverage != directCoverage {
		testContext.Fatalf("coverage routed=%v direct=%v", routedCoverage, directCoverage)
	}
}

func TestRoutingTimedConstructorRejectsIncompleteOrContradictoryInventory(testContext *testing.T) {
	for _, testCase := range []struct {
		name     string
		variant  routingTimedVariant
		workload workload
		change   func(**routingDataSenderPlane, **routingDirectWriter, *routingPathMap)
	}{
		{name: "nil-plane", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) { *plane = nil }},
		{name: "nil-endpoint", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) {
			(*plane).endpoint = nil
		}},
		{name: "missing-association", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) {
			delete((*plane).associations, (*plane).bindings[0].SenderAssociation)
		}},
		{name: "duplicate-association-binding", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) {
			(*plane).bindings[1].SenderAssociation = (*plane).bindings[0].SenderAssociation
		}},
		{name: "missing-epoch", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) {
			delete((*plane).epochs, (*plane).bindings[0].SenderAssociation)
		}},
		{name: "stale-epoch", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, _ *routingPathMap) {
			(*plane).epochs[(*plane).bindings[0].SenderAssociation]++
		}},
		{name: "unready-paths", variant: routingTimedRouted, workload: workloadMix, change: func(_ **routingDataSenderPlane, _ **routingDirectWriter, paths *routingPathMap) { paths.ready = false }},
		{name: "unknown-path-association", variant: routingTimedRouted, workload: workloadMix, change: func(_ **routingDataSenderPlane, _ **routingDirectWriter, paths *routingPathMap) {
			paths.paths[0].Target.Association = 9999
		}},
		{name: "missing-queue-coverage", variant: routingTimedRouted, workload: workloadMix, change: func(plane **routingDataSenderPlane, _ **routingDirectWriter, paths *routingPathMap) {
			missing := (*plane).bindings[0].SenderAssociation
			replacementAssociation := (*plane).bindings[1].SenderAssociation
			var replacement routingResolvedPath
			for _, path := range paths.paths {
				if path.Target.Association == replacementAssociation {
					replacement = path
					break
				}
			}
			for index := range paths.paths {
				if paths.paths[index].Target.Association == missing {
					paths.paths[index] = replacement
				}
			}
		}},
		{name: "nil-direct-writer", variant: routingTimedDirect, workload: workloadMix, change: func(_ **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) { *direct = nil }},
		{name: "missing-direct-admission", variant: routingTimedDirect, workload: workloadMix, change: func(_ **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) {
			(*direct).admission = nil
		}},
		{name: "mismatched-direct-paths", variant: routingTimedDirect, workload: workloadMix, change: func(_ **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) {
			(*direct).paths.paths[0].Target.Association++
		}},
		{name: "missing-direct-association", variant: routingTimedDirect, workload: workloadMix, change: func(plane **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) {
			delete((*direct).associations, (*plane).bindings[0].SenderAssociation)
		}},
		{name: "mismatched-direct-association", variant: routingTimedDirect, workload: workloadMix, change: func(plane **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) {
			first := (*plane).bindings[0].SenderAssociation
			second := (*plane).bindings[1].SenderAssociation
			(*direct).associations[first] = (*plane).associations[second]
		}},
		{name: "mismatched-direct-epoch", variant: routingTimedDirect, workload: workloadMix, change: func(plane **routingDataSenderPlane, direct **routingDirectWriter, _ *routingPathMap) {
			(*direct).epochs[(*plane).bindings[0].SenderAssociation]++
		}},
		{name: "unsupported-variant", variant: 99, workload: workloadMix},
		{name: "unsupported-workload", variant: routingTimedRouted, workload: workload128},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			plane, direct, paths := routingTimedConstructorInputs(testContext)
			if testCase.change != nil {
				testCase.change(&plane, &direct, &paths)
			}
			if _, err := newRoutingTimedSender(testCase.variant, plane, direct, paths, testCase.workload); err == nil {
				testContext.Fatal("contradictory timed sender inventory was accepted")
			}
		})
	}
}

func TestRoutingTimedVariantsSendEqualPayloadWithoutRetry(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	job := fixture.routed.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
	routedCounters := newSenderCounters(1)
	routedCounters.reserve()
	fixture.routed.send(context.Background(), job.queue, job, routedCounters)
	directCounters := newSenderCounters(1)
	directCounters.reserve()
	fixture.direct.send(context.Background(), job.queue, job, directCounters)
	association := fixture.associations[job.path.Target.Association]
	if len(fixture.endpoint.calls) != 1 || len(association.writes) != 1 || fixture.endpoint.calls[0].MTPRoute != job.routeID ||
		!reflect.DeepEqual(fixture.endpoint.calls[0].ProtocolData, &association.writes[0].ProtocolData) {
		testContext.Fatalf("routed calls=%d direct calls=%d routed=%+v direct=%+v", len(fixture.endpoint.calls), len(association.writes), fixture.endpoint.calls, association.writes)
	}
	if routingTimedCountersSnapshot(routedCounters) != (routingTimedCounterSnapshot{submitted: 1}) ||
		routingTimedCountersSnapshot(directCounters) != (routingTimedCounterSnapshot{submitted: 1}) {
		testContext.Fatalf("routed counters=%+v direct counters=%+v", routingTimedCountersSnapshot(routedCounters), routingTimedCountersSnapshot(directCounters))
	}
}

func TestRoutingTimedPayloadFailurePrecedesTimingAndAPI(testContext *testing.T) {
	for _, variant := range []routingTimedVariant{routingTimedRouted, routingTimedDirect} {
		testContext.Run(fmt.Sprint(variant), func(testContext *testing.T) {
			fixture := newRoutingTimedWorkerFixture(testContext)
			sender := fixture.routed
			if variant == routingTimedDirect {
				sender = fixture.direct
			}
			nowCalls := 0
			sender.now = func() time.Time {
				nowCalls++
				return time.Unix(100, 0)
			}
			job := sender.job("timed", 9, 42, time.Unix(99, 0), nil, 0)
			job.size = 1
			counters := newSenderCounters(1)
			counters.reserve()
			sender.send(context.Background(), job.queue, job, counters)
			association := fixture.associations[job.path.Target.Association]
			snapshot := routingTimedCountersSnapshot(counters)
			if nowCalls != 0 || len(fixture.endpoint.calls) != 0 || len(association.writes) != 0 || snapshot.sendErrors != 1 || snapshot.outstanding != 0 {
				testContext.Fatalf("now=%d routed=%d direct=%d counters=%+v", nowCalls, len(fixture.endpoint.calls), len(association.writes), snapshot)
			}
		})
	}
}

func TestRoutingTimedRoutedRequestConstructionBeginsInsideTiming(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	nowCalls := 0
	fixture.routed.now = func() time.Time {
		nowCalls++
		return time.Unix(100, int64(nowCalls))
	}
	job := fixture.routed.job("timed", 9, 42, time.Unix(99, 0), nil, 0)
	job.identity.Route = routingRouteCount
	outcome := fixture.routed.routedSubmission(job, make([]byte, 128))
	if outcome.err == nil || nowCalls != 2 || outcome.started.IsZero() || outcome.ended.IsZero() || !outcome.ended.After(outcome.started) || len(fixture.endpoint.calls) != 0 {
		testContext.Fatalf("now=%d started=%s ended=%s calls=%d error=%v", nowCalls, outcome.started, outcome.ended, len(fixture.endpoint.calls), outcome.err)
	}
}

func TestRoutingTimedOutcomeValidationFollowsSecondTimestamp(testContext *testing.T) {
	for _, variant := range []routingTimedVariant{routingTimedRouted, routingTimedDirect} {
		testContext.Run(fmt.Sprint(variant), func(testContext *testing.T) {
			fixture := newRoutingTimedWorkerFixture(testContext)
			sender := fixture.routed
			if variant == routingTimedDirect {
				sender = fixture.direct
			}
			job := sender.job("timed", 9, 42, time.Unix(99, 0), nil, 0)
			if variant == routingTimedRouted {
				result := fixture.endpoint.results[job.routeID]
				result.UserDataOctets--
				fixture.endpoint.results[job.routeID] = result
			} else {
				fixture.associations[job.path.Target.Association].written = job.size - 1
			}
			times := []time.Time{time.Unix(100, 0), time.Unix(100, 25)}
			nowCalls := 0
			sender.now = func() time.Time {
				if nowCalls >= len(times) {
					testContext.Fatalf("now called %d times", nowCalls+1)
				}
				value := times[nowCalls]
				nowCalls++
				return value
			}
			counters := newSenderCounters(1)
			counters.reserve()
			sender.send(context.Background(), job.queue, job, counters)
			result := counters.result(runSpec{Expected: 1}, time.Second, 0, 0)
			if nowCalls != 2 || result.SendDuration.Count != 1 || result.SendDuration.Max != 25 || result.SendErrors != 1 {
				testContext.Fatalf("now=%d duration=%+v errors=%d", nowCalls, result.SendDuration, result.SendErrors)
			}
		})
	}
}

func TestRoutingTimedErrorsNeverRetry(testContext *testing.T) {
	for _, scenario := range []string{
		"routed-error", "routed-partial", "routed-no-path", "routed-two-paths", "routed-wrong-path",
		"direct-error", "direct-partial", "direct-epoch-changed", "direct-stream-changed", "canceled", "wrong-queue",
	} {
		testContext.Run(scenario, func(testContext *testing.T) {
			fixture := newRoutingTimedWorkerFixture(testContext)
			sender := fixture.routed
			job := sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
			association := fixture.associations[job.path.Target.Association]
			ctx := context.Background()
			queue := job.queue
			wantCalls := 1
			switch scenario {
			case "routed-error":
				fixture.endpoint.errors[job.routeID] = errors.New("routed failure")
			case "routed-partial":
				result := fixture.endpoint.results[job.routeID]
				result.UserDataOctets--
				fixture.endpoint.results[job.routeID] = result
			case "routed-no-path":
				result := fixture.endpoint.results[job.routeID]
				result.SuccessfulPaths = nil
				fixture.endpoint.results[job.routeID] = result
			case "routed-two-paths":
				result := fixture.endpoint.results[job.routeID]
				result.SuccessfulPaths = append(result.SuccessfulPaths, result.SuccessfulPaths[0])
				fixture.endpoint.results[job.routeID] = result
			case "routed-wrong-path":
				result := fixture.endpoint.results[job.routeID]
				result.SuccessfulPaths[0].Association++
				fixture.endpoint.results[job.routeID] = result
			case "direct-error":
				sender = fixture.direct
				job = sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
				association = fixture.associations[job.path.Target.Association]
				association.written = 0
				association.writeErr = errRoutingTimedGoldenWrite
			case "direct-partial":
				sender = fixture.direct
				job = sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
				association = fixture.associations[job.path.Target.Association]
				association.written = job.size - 1
			case "direct-epoch-changed":
				sender = fixture.direct
				job = sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
				association = fixture.associations[job.path.Target.Association]
				association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.epoch++ }
			case "direct-stream-changed":
				sender = fixture.direct
				job = sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
				association = fixture.associations[job.path.Target.Association]
				association.afterWrite = func(changed *routingTimedGoldenAssociation) { changed.maximum++ }
			case "canceled":
				sender = fixture.direct
				job = sender.job("timed", 9, 42, time.Unix(100, 0), nil, 0)
				association = fixture.associations[job.path.Target.Association]
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
				wantCalls = 0
			case "wrong-queue":
				queue = (job.queue + 1) % 8
				wantCalls = 0
			}
			counters := newSenderCounters(1)
			counters.reserve()
			sender.send(ctx, queue, job, counters)
			calls := len(fixture.endpoint.calls)
			if sender == fixture.direct {
				calls = len(association.writes)
			}
			snapshot := routingTimedCountersSnapshot(counters)
			if calls != wantCalls || snapshot.sendErrors != 1 || snapshot.outstanding != 0 || snapshot.fatal == "" {
				testContext.Fatalf("calls=%d want=%d counters=%+v", calls, wantCalls, snapshot)
			}
		})
	}
}
