package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// cohortTestClock advances after the test has joined the work due at its current
// instant. It exclusively owns the monitored channels: independent observer
// goroutines must never receive from them.
type cohortTestClock struct {
	mutex    sync.Mutex
	now      int64
	observer *routeReferenceObserver
	cases    []reflect.SelectCase
	sources  []cohortEventSource
	fences   []cohortInboundFence
}

type cohortInboundFence interface {
	ASPActive(context.Context, ...m3ua.ASKey) error
}

type cohortEventSource struct {
	association m3ua.AssociationID
	kind        string
	ended       func() string
}

func newCohortTestClock() *cohortTestClock {
	clock := &cohortTestClock{now: int64(time.Hour)}
	clock.observer = &routeReferenceObserver{clock: clock}
	clock.cases = []reflect.SelectCase{{Dir: reflect.SelectDefault}}
	clock.sources = []cohortEventSource{{}}
	return clock
}

func (clock *cohortTestClock) Now() (int64, error) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.now, nil
}

func (*cohortTestClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "controlled-cohort", TimeNamespace: "controlled-cohort", Resolution: 1}, nil
}

func (clock *cohortTestClock) subscribe(channel any, source cohortEventSource) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.cases = append(clock.cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(channel)})
	clock.sources = append(clock.sources, source)
}

func (clock *cohortTestClock) observe(associations []*m3ua.Association) {
	for _, association := range associations {
		clock.fences = append(clock.fences, association)
		clock.subscribe(association.ManagementIndications(), cohortEventSource{association: association.ID(), kind: "management-indication"})
		clock.subscribe(association.StateChanges(), cohortEventSource{association: association.ID(), kind: "state-change"})
		clock.subscribe(association.Done(), cohortEventSource{association: association.ID(), kind: "association-ended", ended: func() string { return fmt.Sprint(association.Err()) }})
	}
}

// advance drains through one exhaustive select over every channel. Its final
// default decision is the logical boundary: sends completed before that cut
// are recorded at the old instant, and later sends belong to the new instant.
// The select locks all participating channels together; separate per-channel
// drains would leave a delivery race. Now and advance share the same mutex, so
// no clock reader can observe an instant between the cut and the new value.
func (clock *cohortTestClock) advance(next int64) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	if next < clock.now {
		panic("controlled cohort clock regressed")
	}
	for {
		index, value, open := reflect.Select(clock.cases)
		if index == 0 {
			clock.now = next
			return
		}
		source := clock.sources[index]
		if !open {
			clock.cases[index].Chan = reflect.Value{}
			if source.kind != "association-ended" {
				continue
			}
		}
		detail := ""
		switch source.kind {
		case "management-indication":
			indication := value.Interface().(*m3ua.ManagementIndication)
			detail = fmt.Sprintf("%s (kind %d): %s", indication.Kind, indication.Kind, indication.Description)
		case "state-change":
			detail = value.Interface().(m3ua.State).String()
		case "association-ended":
			detail = source.ended()
		}
		clock.observer.mutex.Lock()
		clock.observer.events = append(clock.observer.events, routeReferenceEvent{Association: source.association, Kind: source.kind, Detail: detail, At: clock.now})
		clock.observer.mutex.Unlock()
	}
}

// boundary joins inbound indication production before sealing the current
// instant. All controlled workload producers have already joined. WriteSignal
// and the library's ASP Active Ack use ordered SCTP stream 0 (RFC 9260 §6.6;
// RFC 4666 §1.4.7 permits ASPTM on that stream). A repeated ASP Active is acknowledged even
// when already active (§4.3.4.3). The serial dispatcher handles any earlier
// inbound message, including its synchronous ManagementIndications send,
// before completing the public ASPActive call for that Ack. StateChanges has
// another stage: commits append history, and monitor flushes it after receiving
// a state publication. ASPActive completes before its dispatcher publication.
//
// Use three consecutive exchanges per association. The first may restore
// ACTIVE. Handling the second Ack requires monitor to have received the first
// Ack's publication. Handling the third requires receipt of the second's
// publication, after monitor has finished the first publication's history
// flush. Thus even a withdrawal overtaken by that restoration has reached
// StateChanges before the exhaustive select seals the window. See fsm.go's
// dispatchLoop, publishState, monitor and applyPublishedStateUpdate, and
// association.go's recordStateChange and flushStateChanges. DATA receipts alone
// cannot supply this fence on the ASP's inbound dispatcher.
//
// The fixture uses unchanged loadshare scopes and TAck beyond its overall
// deadline: no retry can leave a stale matching Ack to satisfy a later fence.
// Independent real-time transport sampling and heartbeats are disabled. The
// boundary then drains every queued event before moving logical time.
func (clock *cohortTestClock) boundary(ctx context.Context, next int64) error {
	if err := clock.fence(ctx); err != nil {
		return fmt.Errorf("inbound dispatch fence: %w", err)
	}
	clock.advance(next)
	return nil
}

func (clock *cohortTestClock) fence(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	completed := make(chan error, len(clock.fences))
	for _, association := range clock.fences {
		go func() {
			for range 3 {
				if err := association.ASPActive(ctx); err != nil {
					completed <- err
					return
				}
			}
			completed <- nil
		}()
	}
	for range clock.fences {
		select {
		case err := <-completed:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func waitCohortWork(testContext *testing.T, ctx context.Context, clock *cohortTestClock, description string, completed func() bool) {
	testContext.Helper()
	for {
		now, _ := clock.Now()
		clock.advance(now)
		if completed() {
			return
		}
		select {
		case <-ctx.Done():
			testContext.Fatalf("%s: %v", description, ctx.Err())
		case <-time.After(100 * time.Microsecond):
		}
	}
}

func TestCohortClockAttributesChannelDeliveriesAcrossWindows(testContext *testing.T) {
	clock := newCohortTestClock()
	first := make(chan *m3ua.ManagementIndication, 4)
	second := make(chan *m3ua.ManagementIndication, 4)
	clock.subscribe(first, cohortEventSource{association: 1, kind: "management-indication"})
	clock.subscribe(second, cohortEventSource{association: 2, kind: "management-indication"})
	before, _ := clock.Now()
	start, end := before+10, before+20
	first <- &m3ua.ManagementIndication{Kind: m3ua.ManagementNotify, Description: "before window"}
	second <- &m3ua.ManagementIndication{Kind: m3ua.ManagementError, Description: "also before window"}
	clock.advance(start)
	first <- &m3ua.ManagementIndication{Kind: m3ua.ManagementError, Description: "inside window"}
	second <- &m3ua.ManagementIndication{Kind: m3ua.ManagementNotify, Description: "also inside window"}
	clock.advance(end + 1)
	close(first)
	close(second)
	// Evaluate only after both channels close and logical time moves again.
	clock.advance(end + 100)
	events := clock.observer.between(start, end)
	if len(events) != 2 {
		testContext.Fatalf("window events = %+v", events)
	}
	for _, event := range events {
		if event.At != start || (event.Detail != "M-ERROR (kind 1): inside window" && event.Detail != "M-NOTIFY (kind 0): also inside window") {
			testContext.Errorf("misattributed event: %+v", event)
		}
	}
	if old := clock.observer.between(before, before); len(old) != 2 {
		testContext.Fatalf("pre-window events = %+v", old)
	}
}

// Disabling one real-time producer in the live fixture must not turn into a
// filter: every library management kind delivered inside a logical window fails
// the unchanged evaluator, including both local congestion indication kinds.
func TestCohortClockRejectsEveryManagementIndicationKind(testContext *testing.T) {
	for _, kind := range []m3ua.ManagementIndicationKind{
		m3ua.ManagementNotify, m3ua.ManagementError, m3ua.ManagementSCTPRestart,
		m3ua.ManagementSCTPRelease, m3ua.ManagementSCTPStatus,
		m3ua.ManagementDataQueueCongestion, m3ua.ManagementTransportCongestion, m3ua.ManagementASPWithdrawal,
		m3ua.ManagementASPActivation, m3ua.ManagementCongestionProfileMismatch,
	} {
		testContext.Run(kind.String(), func(testContext *testing.T) {
			clock := newCohortTestClock()
			channel := make(chan *m3ua.ManagementIndication, 1)
			clock.subscribe(channel, cohortEventSource{association: 1, kind: "management-indication"})
			specification := routeReferenceSpecFixture(routeReferencesStatic)
			now, _ := clock.Now()
			specification.Clock.Start = now + 1
			specification.Clock.End = specification.Clock.Start + int64(specification.Duration)
			clock.advance(specification.Clock.Start)
			channel <- &m3ua.ManagementIndication{Kind: kind, Description: "delivered inside the cohort"}
			clock.advance(specification.Clock.End + 1)
			fixture := routeReferenceRecordFixture()
			run := &routeReferenceRun{spec: *specification.RouteReferences, observer: clock.observer}
			record := run.evaluate(specification, fixture.Before, fixture.After)
			if record.Verdict != failoverFail || len(record.Events) != 1 || len(record.Criteria) != 3 {
				testContext.Fatalf("kind %s: %+v", kind, record)
			}
			for _, criterion := range record.Criteria {
				want := failoverPass
				if criterion.Name == "no_library_indications" {
					want = failoverFail
				}
				if criterion.Outcome != want {
					testContext.Errorf("kind %s: %+v", kind, criterion)
				}
			}
		})
	}
}

type waitingCohortFence struct{ entered chan struct{} }

func (f waitingCohortFence) ASPActive(ctx context.Context, _ ...m3ua.ASKey) error {
	close(f.entered)
	<-ctx.Done()
	return ctx.Err()
}

func TestCohortClockKeepsBoundaryOpenUntilInboundFenceCompletes(testContext *testing.T) {
	clock := newCohortTestClock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	clock.fences = []cohortInboundFence{waitingCohortFence{entered: entered}}
	now, _ := clock.Now()
	completed := make(chan error, 1)
	go func() { completed <- clock.boundary(ctx, now+1) }()
	select {
	case <-entered:
	case err := <-completed:
		testContext.Fatalf("boundary sealed before inbound fence: %v", err)
	}
	if observed, _ := clock.Now(); observed != now {
		testContext.Errorf("clock advanced while inbound fence was pending: %d", observed)
	}
	cancel()
	if err := <-completed; !errors.Is(err, context.Canceled) {
		testContext.Fatalf("fence error = %v", err)
	}
	if observed, _ := clock.Now(); observed != now {
		testContext.Errorf("canceled boundary advanced: %d", observed)
	}
}
