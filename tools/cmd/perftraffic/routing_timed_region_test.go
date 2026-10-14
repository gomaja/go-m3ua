package main

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// routingTimedRegionContext counts every context consultation, so a test can
// place the timed sender's context checks relative to its two timestamps.
type routingTimedRegionContext struct {
	context.Context
	calls *int
}

func (ctx routingTimedRegionContext) Err() error {
	*ctx.calls++
	return ctx.Context.Err()
}

func (ctx routingTimedRegionContext) Done() <-chan struct{} {
	*ctx.calls++
	return ctx.Context.Done()
}

// routingTimedRegionMark is what had happened when the send clock was read.
type routingTimedRegionMark struct {
	contextCalls int
	accessors    string
	transfers    int
	admitted     int
}

// The two timed regions must hold the same work: Protocol Data construction
// and the one library call. Admission, context checks and the association
// epoch and stream revalidation of routed-direct happen outside them, as the
// routed variant's path check does. Each clock read records what had executed
// so far; the difference between the two reads is the timed region.
func TestRoutingTimedRegionsHoldOnlyProtocolDataAndOneLibraryCall(testContext *testing.T) {
	for _, variant := range []routingTimedVariant{routingTimedRouted, routingTimedDirect} {
		testContext.Run(fmt.Sprint(variant), func(testContext *testing.T) {
			fixture := newRoutingTimedWorkerFixture(testContext)
			sender := fixture.routed
			if variant == routingTimedDirect {
				sender = fixture.direct
			}
			job := sender.job("timed-region", 9, 42, time.Unix(99, 0), nil, 0)
			association := fixture.associations[job.path.Target.Association]
			contextCalls := 0
			ctx := routingTimedRegionContext{Context: context.Background(), calls: &contextCalls}
			var marks []routingTimedRegionMark
			sender.now = func() time.Time {
				marks = append(marks, routingTimedRegionMark{
					contextCalls: contextCalls, accessors: strings.Join(association.accessors, ","),
					transfers: len(fixture.endpoint.calls), admitted: len(fixture.direct.direct.admission),
				})
				return time.Unix(100, int64(len(marks)))
			}
			counters := newSenderCounters(1)
			counters.reserve()
			sender.send(ctx, job.queue, job, counters)
			if snapshot := routingTimedCountersSnapshot(counters); snapshot != (routingTimedCounterSnapshot{submitted: 1}) {
				testContext.Fatalf("counters = %+v", snapshot)
			}
			if len(marks) != 2 {
				testContext.Fatalf("clock read %d times, want 2", len(marks))
			}
			started, ended := marks[0], marks[1]
			if ended.contextCalls != started.contextCalls {
				testContext.Fatalf("the timed region consulted the context %d times", ended.contextCalls-started.contextCalls)
			}
			switch variant {
			case routingTimedRouted:
				if started.transfers != 0 || ended.transfers != 1 || started.accessors != "" || ended.accessors != "" || association.accessors != nil {
					testContext.Fatalf("routed region: start %+v end %+v accessors %v", started, ended, association.accessors)
				}
				if started.contextCalls == 0 {
					testContext.Fatal("routed send did not check its context before timing")
				}
			case routingTimedDirect:
				// The frozen-path revalidation precedes the clock, the write is
				// the only thing inside it, and the after-write revalidation
				// follows it. The admission slot is held across the region and
				// released after it.
				if started.accessors != "epoch,maximum" || ended.accessors != "epoch,maximum,write" ||
					strings.Join(association.accessors, ",") != "epoch,maximum,write,epoch,maximum" {
					testContext.Fatalf("direct region: start %q end %q all %v", started.accessors, ended.accessors, association.accessors)
				}
				if started.admitted != 1 || ended.admitted != 1 || len(fixture.direct.direct.admission) != 0 {
					testContext.Fatalf("admission start=%d end=%d after=%d, want held across the region and released after", started.admitted, ended.admitted, len(fixture.direct.direct.admission))
				}
				if started.contextCalls < 3 || started.transfers != 0 || ended.transfers != 0 {
					testContext.Fatalf("direct region: start %+v end %+v", started, ended)
				}
				protocolData, err := routeProtocolData(job.identity.Route, association.writes[0].ProtocolData.Data)
				if err != nil || len(association.writes) != 1 || !reflect.DeepEqual(association.writes[0].ProtocolData, protocolData) ||
					association.writes[0].AS != job.path.Target.AS || association.writes[0].Stream != uint16(protocolData.SignallingLinkSelection)%job.path.Binding.MaxMessageStreamID+1 {
					testContext.Fatalf("direct write %+v, want the route's Protocol Data on the frozen AS and stream", association.writes)
				}
			}
		})
	}
}

// A direct write that fails its untimed checks never reads the send clock and
// never reaches WriteData, and still returns its admission slot.
func TestRoutingTimedDirectPrecheckFailureIsUntimedAndReleasesAdmission(testContext *testing.T) {
	for _, scenario := range []string{"stale-epoch", "stale-maximum", "canceled-during-admission"} {
		testContext.Run(scenario, func(testContext *testing.T) {
			fixture := newRoutingTimedWorkerFixture(testContext)
			sender := fixture.direct
			job := sender.job("timed-region", 9, 42, time.Unix(99, 0), nil, 0)
			association := fixture.associations[job.path.Target.Association]
			ctx := context.Context(context.Background())
			switch scenario {
			case "stale-epoch":
				association.epoch++
			case "stale-maximum":
				association.maximum++
			case "canceled-during-admission":
				// Fill the admission bound so the write can only be canceled.
				for len(sender.direct.admission) < cap(sender.direct.admission) {
					sender.direct.admission <- struct{}{}
				}
				canceled, cancel := context.WithCancel(context.Background())
				calls := 0
				ctx = routingTimedRegionCancelOnDone{Context: canceled, cancel: cancel, calls: &calls}
			}
			held := len(sender.direct.admission)
			clockReads := 0
			sender.now = func() time.Time {
				clockReads++
				return time.Unix(100, 0)
			}
			counters := newSenderCounters(1)
			counters.reserve()
			sender.send(ctx, job.queue, job, counters)
			snapshot := routingTimedCountersSnapshot(counters)
			if clockReads != 0 || len(association.writes) != 0 || snapshot.sendErrors != 1 || snapshot.outstanding != 0 {
				testContext.Fatalf("clock=%d writes=%d counters=%+v", clockReads, len(association.writes), snapshot)
			}
			if len(sender.direct.admission) != held {
				testContext.Fatalf("admission slots %d after a failed write, want %d", len(sender.direct.admission), held)
			}
		})
	}
}

// routingTimedRegionCancelOnDone cancels itself when a caller starts waiting
// on it, which is the only way a write can be canceled while blocked on the
// admission bound. The first Err check still sees a live context.
type routingTimedRegionCancelOnDone struct {
	context.Context
	cancel context.CancelFunc
	calls  *int
}

func (ctx routingTimedRegionCancelOnDone) Done() <-chan struct{} {
	*ctx.calls++
	ctx.cancel()
	return ctx.Context.Done()
}
