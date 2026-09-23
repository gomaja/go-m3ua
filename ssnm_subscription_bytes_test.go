package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
	"unsafe"
)

var (
	_ [ssnmEventBaseBytes - int(unsafe.Sizeof(queuedSSNMEvent{}))]byte
	_ [ssnmEventStateBytes - int(unsafe.Sizeof(SSNMDestinationKnowledge{}))]byte
	_ [ssnmEventDestinationBytes - int(unsafe.Sizeof(PointCodeRange{}))]byte
)

func TestSSNMSubscriptionDefaultBytesRejectsOversizedEvent(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	subscription.enqueue(SSNMEvent{Kind: SSNMReportEvent, States: make([]SSNMDestinationKnowledge, 4097)})
	if len(subscription.queue) != 0 || !subscription.continuityLost {
		testContext.Fatal("subscription retained an event larger than its default byte budget")
	}
}

func TestSSNMSubscriptionByteConfig(testContext *testing.T) {
	for _, config := range []*SSNMStateConfig{nil, {}} {
		resolved, err := resolveSSNMStateConfig(config)
		if err != nil || resolved.SubscriptionQueueBytes != 1<<20 || resolved.SubscriptionQueueSize != 256 {
			testContext.Fatalf("default limits: %+v, %v", resolved, err)
		}
	}
	for _, limit := range []int{-1, 1, 511, 512, 513, int(^uint(0) >> 1)} {
		config := SSNMStateConfig{SubscriptionQueueBytes: limit}
		resolved, err := resolveSSNMStateConfig(&config)
		if limit < 512 {
			if !errors.Is(err, ErrInvalidSSNMStateConfig) {
				testContext.Fatalf("limit %d: %v, want invalid configuration", limit, err)
			}
		} else if err != nil || resolved.SubscriptionQueueBytes != limit {
			testContext.Fatalf("limit %d: %+v, %v", limit, resolved, err)
		}
		if config.SubscriptionQueueBytes != limit {
			testContext.Fatal("configuration was mutated")
		}
	}
}

func TestSSNMSubscriptionCountLimitWithLargeEvent(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueSize: 1})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	subscription.enqueue(SSNMEvent{Revision: 1})
	subscription.enqueue(SSNMEvent{Revision: 2, States: make([]SSNMDestinationKnowledge, 1000)})
	if len(subscription.queue) != 1 || subscription.queuedBytes != 512 || !subscription.continuityLost {
		testContext.Fatal("event-count overflow did not preserve the queued event and mark continuity loss")
	}
	event, ready, _ := subscription.take()
	if !ready || event.Revision != 1 {
		testContext.Fatalf("first queued event = %+v, ready %v", event, ready)
	}
}

func TestSSNMEventAccountedBytesIncludesVariableData(testContext *testing.T) {
	cases := []struct {
		name       string
		event      SSNMEvent
		additional int
	}{
		{"base", SSNMEvent{}, 0},
		{"destinations", SSNMEvent{Report: SSNMReport{Destinations: make([]PointCodeRange, 2)}}, 16},
		{"states", SSNMEvent{States: make([]SSNMDestinationKnowledge, 2)}, 512},
		{"report scope", SSNMEvent{Report: SSNMReport{Scope: WireScope{RoutingContexts: []uint32{1, 2}}}}, 8},
		{"availability scope", SSNMEvent{States: []SSNMDestinationKnowledge{{Availability: SSNMAvailability{Scope: WireScope{RoutingContexts: []uint32{1, 2}}}}}}, 264},
		{"congestion scope", SSNMEvent{States: []SSNMDestinationKnowledge{{Congestion: SSNMCongestion{Scope: WireScope{RoutingContexts: []uint32{1, 2}}}}}}, 264},
		{"reason", SSNMEvent{Reason: "reason"}, 6},
		{"partition SG", SSNMEvent{Partition: SSNMPartition{SignallingGateway: "gateway"}}, 7},
		{"partition AS", SSNMEvent{Partition: SSNMPartition{ApplicationServer: "server"}}, 6},
		{"report partition SG", SSNMEvent{Report: SSNMReport{Partition: SSNMPartition{SignallingGateway: "gateway"}}}, 7},
		{"report partition AS", SSNMEvent{Report: SSNMReport{Partition: SSNMPartition{ApplicationServer: "server"}}}, 6},
	}
	for _, test := range cases {
		testContext.Run(test.name, func(testContext *testing.T) {
			want := 512 + test.additional
			for _, limit := range []int{want - 1, want, want + 1} {
				got, fits := ssnmEventAccountedBytes(test.event, limit)
				if fits != (limit >= want) || fits && got != want {
					testContext.Fatalf("cost with limit %d = %d, %v; want cost %d", limit, got, fits, want)
				}
			}
		})
	}
}

func TestSSNMSubscriptionByteBoundaryAndRelease(testContext *testing.T) {
	for _, limit := range []int{1023, 1024, 1025} {
		endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: limit})
		_, subscription, err := endpoint.SubscribeSSNM()
		if err != nil {
			testContext.Fatal(err)
		}
		subscription.enqueue(SSNMEvent{Revision: 1})
		subscription.enqueue(SSNMEvent{Revision: 2})
		wantEvents := 2
		if limit < 1024 {
			wantEvents = 1
		}
		if len(subscription.queue) != wantEvents || subscription.queuedBytes != wantEvents*512 || subscription.continuityLost != (limit < 1024) {
			testContext.Fatalf("limit %d: count=%d bytes=%d lost=%v", limit, len(subscription.queue), subscription.queuedBytes, subscription.continuityLost)
		}
		for revision := 1; revision <= wantEvents; revision++ {
			event, ready, err := subscription.take()
			if !ready || err != nil || event.Revision != uint64(revision) || subscription.queuedBytes != (wantEvents-revision)*512 {
				testContext.Fatalf("dequeue %d: %+v %v %v, bytes=%d", revision, event, ready, err, subscription.queuedBytes)
			}
		}
		if limit < 1024 {
			event, ready, err := subscription.take()
			if !ready || err != nil || event.Kind != SSNMContinuityLostEvent || !event.ContinuityLost {
				testContext.Fatalf("missing continuity marker: %+v, %v, %v", event, ready, err)
			}
			if _, ready, _ := subscription.take(); ready {
				testContext.Fatal("continuity marker repeated")
			}
		} else {
			subscription.enqueue(SSNMEvent{Revision: 3})
			if subscription.queuedBytes != 512 || len(subscription.queue) != 1 || subscription.continuityLost {
				testContext.Fatal("consumed accounting was not reusable")
			}
		}
	}
}

func TestSSNMSubscriptionRejectsBytesBeforeCloning(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: 512})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	event := SSNMEvent{Report: SSNMReport{Destinations: make([]PointCodeRange, 1024)}, States: make([]SSNMDestinationKnowledge, 1024)}
	allocations := testing.AllocsPerRun(100, func() {
		subscription.continuityLost = false
		subscription.pendingLoss = false
		subscription.enqueue(event)
	})
	if allocations != 0 || subscription.queuedBytes != 0 || len(subscription.queue) != 0 || !subscription.continuityLost {
		testContext.Fatalf("oversized event allocated or was retained: allocations=%v bytes=%d count=%d lost=%v", allocations, subscription.queuedBytes, len(subscription.queue), subscription.continuityLost)
	}
}

func TestSSNMSubscriptionResyncReleasesBytesAndSlots(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: 1024})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	subscription.enqueue(SSNMEvent{Reason: "retained"})
	backing := subscription.queue
	subscription.enqueue(SSNMEvent{States: make([]SSNMDestinationKnowledge, 4)})
	if !subscription.continuityLost {
		testContext.Fatal("expected byte overflow")
	}
	if _, err := subscription.Resync(); err != nil {
		testContext.Fatal(err)
	}
	if subscription.queuedBytes != 0 || len(subscription.queue) != 0 || subscription.continuityLost || subscription.pendingLoss {
		testContext.Fatal("Resync retained accounting or continuity loss")
	}
	if !reflect.DeepEqual(backing[0], queuedSSNMEvent{}) {
		testContext.Fatal("Resync retained queued references")
	}
	subscription.enqueue(SSNMEvent{Revision: 3})
	if subscription.queuedBytes != 512 {
		testContext.Fatal("Resync did not restore the byte allowance")
	}
}

func TestSSNMSubscriptionClosePreservesQueuedBytesUntilDelivery(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: 512})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	subscription.enqueue(SSNMEvent{Revision: 1})
	if err := subscription.Close(); err != nil {
		testContext.Fatal(err)
	}
	if subscription.queuedBytes != 512 {
		testContext.Fatal("Close discarded queued accounting before delivery")
	}
	event, err := subscription.Next(context.Background())
	if err != nil || event.Revision != 1 || subscription.queuedBytes != 0 {
		testContext.Fatalf("queued-before-close delivery: %+v %v, bytes=%d", event, err, subscription.queuedBytes)
	}
	if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrSSNMSubscriptionClosed) {
		testContext.Fatalf("terminal result: %v", err)
	}
}

func TestSSNMByteReservationCannotOverflow(testContext *testing.T) {
	maximum := int(^uint(0) >> 1)
	for _, test := range []struct {
		remaining, count, width int
		fits                    bool
	}{
		{maximum, maximum, 1, true},
		{maximum, maximum, 2, false},
		{maximum, maximum/4 + 1, 4, false},
		{maximum, maximum / 4, 4, true},
		{-1, 0, 1, false},
		{maximum, -1, 1, false},
		{maximum, 1, 0, false},
		{maximum, 1, -1, false},
	} {
		remaining := test.remaining
		if fits := reserveSSNMEventBytes(&remaining, test.count, test.width); fits != test.fits {
			testContext.Fatalf("reservation %+v = %v", test, fits)
		}
		if !test.fits && remaining != test.remaining || test.fits && remaining != test.remaining-test.count*test.width {
			testContext.Fatalf("reservation %+v left %d", test, remaining)
		}
	}
}

func TestSSNMSubscriptionConcurrentByteAccounting(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: 2048})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	var workers sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for iteration := 0; iteration < 100; iteration++ {
				subscription.enqueue(SSNMEvent{Reason: strings.Repeat("x", iteration%16)})
				_, _, _ = subscription.take()
			}
		}()
	}
	workers.Wait()
	subscription.mu.Lock()
	defer subscription.mu.Unlock()
	accounted := 0
	for _, queued := range subscription.queue {
		accounted += queued.bytes
	}
	if accounted != subscription.queuedBytes || accounted > subscription.byteLimit {
		testContext.Fatalf("queue accounting: sum=%d retained=%d limit=%d", accounted, subscription.queuedBytes, subscription.byteLimit)
	}
}

func TestSSNMSubscriptionByteLimitPreservesStoreAndSnapshots(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, ssnmPeerInventoryConfig(), &SSNMStateConfig{SubscriptionQueueBytes: 512})
	association := attachSSNMAssociation(testContext, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	sendDUNA(testContext, association, 7, 1, 0x123456)
	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil || len(snapshot.Partitions) != 1 || len(snapshot.Partitions[0].Destinations) != 1 || subscription.queuedBytes != 0 {
		testContext.Fatalf("snapshot charged to queue: %+v, %v", snapshot, err)
	}
	sendDAVA(testContext, association, 7, 1, 0x123456)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil || !event.ContinuityLost || subscription.queuedBytes != 0 {
		testContext.Fatalf("oversized report: %+v, %v", event, err)
	}
	snapshot, err = subscription.Resync()
	if err != nil || snapshot.RecordsRefused != 0 || snapshot.ReportsRefused != 0 || snapshot.PartitionsInvalidated != 0 {
		testContext.Fatalf("queue overflow damaged store: %+v, %v", snapshot, err)
	}
	if snapshot.Partitions[0].Destinations[0].Availability.State != DestinationAvailable {
		testContext.Fatal("queue overflow discarded authoritative state")
	}
}

func TestSSNMSubscriptionOwnedPayloadAndByteRelease(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	event := SSNMEvent{
		Report: SSNMReport{Scope: WireScope{RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: 7}}},
		States: []SSNMDestinationKnowledge{{
			Availability: SSNMAvailability{Scope: WireScope{RoutingContexts: []uint32{2}}},
			Congestion:   SSNMCongestion{Scope: WireScope{RoutingContexts: []uint32{3}}},
		}},
	}
	subscription.enqueue(event)
	event.Report.Scope.RoutingContexts[0] = 99
	event.Report.Destinations[0].PointCode = 99
	event.States[0].Availability.Scope.RoutingContexts[0] = 99
	event.States[0].Congestion.Scope.RoutingContexts[0] = 99
	if subscription.queuedBytes != 512+8+256+12 {
		testContext.Fatalf("retained byte cost = %d", subscription.queuedBytes)
	}
	owned, ready, err := subscription.take()
	if !ready || err != nil || owned.Report.Scope.RoutingContexts[0] != 1 || owned.Report.Destinations[0].PointCode != 7 ||
		owned.States[0].Availability.Scope.RoutingContexts[0] != 2 || owned.States[0].Congestion.Scope.RoutingContexts[0] != 3 || subscription.queuedBytes != 0 {
		testContext.Fatalf("owned event or byte release: %+v, %v, %v, bytes=%d", owned, ready, err, subscription.queuedBytes)
	}
}

func TestSSNMSubscriptionConcurrentByteResyncAndClose(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, &SSNMStateConfig{SubscriptionQueueBytes: 2048})
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	var workers sync.WaitGroup
	workers.Add(3)
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < 500; iteration++ {
			subscription.enqueue(SSNMEvent{States: make([]SSNMDestinationKnowledge, iteration%8)})
		}
	}()
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < 500; iteration++ {
			_, _ = subscription.Resync()
		}
	}()
	go func() {
		defer workers.Done()
		for iteration := 0; iteration < 500; iteration++ {
			_, _, _ = subscription.take()
		}
		_ = subscription.Close()
	}()
	workers.Wait()
	for range len(subscription.queue) {
		_, _, _ = subscription.take()
	}
	if subscription.queuedBytes != 0 || !subscription.closed {
		testContext.Fatalf("closed queue retains %d accounted bytes", subscription.queuedBytes)
	}
}

func FuzzSSNMEventByteAccounting(fuzzContext *testing.F) {
	fuzzContext.Add([]byte{0, 0, 0}, uint64(511))
	fuzzContext.Add([]byte{3, 4, 5}, uint64(2048))
	fuzzContext.Add([]byte{255, 255, 255}, ^uint64(0))
	fuzzContext.Fuzz(func(testContext *testing.T, input []byte, budget uint64) {
		if len(input) < 3 || len(input) > 32 {
			return
		}
		event := SSNMEvent{
			Reason: string(input),
			Report: SSNMReport{Destinations: make([]PointCodeRange, int(input[0])%32), Scope: WireScope{RoutingContexts: make([]uint32, int(input[1])%16)}},
			States: make([]SSNMDestinationKnowledge, int(input[2])%8),
		}
		want := 512 + len(input) + 8*len(event.Report.Destinations) + 4*len(event.Report.Scope.RoutingContexts) + 256*len(event.States)
		for index := range event.States {
			event.States[index].Availability.Scope.RoutingContexts = make([]uint32, index%4)
			event.States[index].Congestion.Scope.RoutingContexts = make([]uint32, index%3)
			want += 4 * (index%4 + index%3)
		}
		limit := int(budget & uint64(^uint(0)>>1))
		got, fits := ssnmEventAccountedBytes(event, limit)
		if fits != (limit >= want) || fits && got != want {
			testContext.Fatalf("limit=%d cost=%d fits=%v want=%d", limit, got, fits, want)
		}
	})
}

func TestSSNMSubscriptionTakeClearsConsumedSlot(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	subscription.enqueue(SSNMEvent{Revision: 1, Reason: "first", States: make([]SSNMDestinationKnowledge, 1)})
	subscription.enqueue(SSNMEvent{Revision: 2, Reason: "second"})
	backing := subscription.queue
	event, ready, err := subscription.take()
	if err != nil || !ready || event.Revision != 1 {
		testContext.Fatalf("take = %+v, %v, %v", event, ready, err)
	}
	if !reflect.DeepEqual(backing[0], reflect.Zero(reflect.TypeOf(backing[0])).Interface()) {
		testContext.Fatal("consumed queue slot retains event references")
	}
}
