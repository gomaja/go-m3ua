package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// byteCapPeer is one ASP Endpoint of the live byte-cap cross-check: its own
// SSNM store under limits, a reference subscription read in step with every
// report, and a paused subscriber whose subscription is not read until every
// report has been delivered.
type byteCapPeer struct {
	name   string
	limits m3ua.SSNMStateConfig
	// canonical provisions the SGP, so the peer's reports land in a canonical
	// partition whose identity strings the accounting charges.
	canonical bool
	// short marks a byte limit one byte below the exact size of the first
	// retained+1 reports.
	short bool
	// retained and binding are what the peer's paused queue must show, and
	// failure what the F3 judgment makes of it ("" when it holds).
	retained int
	binding  string
	failure  string

	endpoint  *m3ua.Endpoint
	reference *m3ua.SSNMSubscription
	held      *recordingStream
	delivered []m3ua.SSNMEvent
}

// recordingStream keeps every event its subscription delivers.
type recordingStream struct {
	ssnmEventStream
	events []m3ua.SSNMEvent
}

func (stream *recordingStream) Next(ctx context.Context) (m3ua.SSNMEvent, error) {
	event, err := stream.ssnmEventStream.Next(ctx)
	if err == nil {
		stream.events = append(stream.events, event)
	}
	return event, err
}

// The canonical peer's provisioned identity: every event and its report name
// this partition, so each charges 2 x (6 + 8) identity bytes.
const (
	byteCapGateway = m3ua.SignallingGatewayID("sg-cap")
	byteCapServer  = m3ua.RemoteASID("as-rc100")
	byteCapStrings = 2 * (len(byteCapGateway) + len(byteCapServer))
)

// byteCapCounts are the Affected Point Codes of the published reports. The
// sizes vary so a mistaken term in the accounting moves the boundary, and
// the messages at index 5 and 10 name one destination, so an exact limit
// minus one byte leaves the one-APC workload's smallest event unable to fit.
var byteCapCounts = []int{1, 5, 2, 9, 3, 1, 7, 4, 2, 6, 1, 8, 3, 2, 5}

// byteCapModel is the accounted size the documented formula predicts for
// message index on a peer.
func byteCapModel(index int, canonical bool) int {
	size := ssnmWorkloadEventBytes(byteCapCounts[index])
	if canonical {
		size += byteCapStrings
	}
	return size
}

func byteCapPrefix(count int, canonical bool) int {
	total := 0
	for index := 0; index < count; index++ {
		total += byteCapModel(index, canonical)
	}
	return total
}

// TestSSNMByteCapMatchesLibraryAccounting ties the fixture's byte accounting
// to the library's behaviour over real associations. Every peer's byte limit
// is either the exact accounted size of the first k reports or one byte less,
// so a paused queue must lose continuity after exactly k or k-1 events: the
// library refuses the next event exactly when the fixture-computed bytes say
// it would not fit. The fixture's own drain and F3 judgment then classify
// each queue, and a peer with a four-event count limit shows the count cap.
//
// The judgment credits the byte cap only when the workload's smallest event,
// a standalone one-APC report of 784 bytes, would not have fitted. A
// canonical one-APC report is 812 bytes, so the canonical peer one byte
// short, whose refused event is one of those, is not credited: the fixture
// reports an unproven cap rather than guess. Every fixture association is
// standalone, where the bound is exact.
func TestSSNMByteCapMatchesLibraryAccounting(testContext *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	standalone, canonical := byteCapPrefix(6, false), byteCapPrefix(11, true)
	peers := []*byteCapPeer{
		{name: "standalone exact fit", limits: m3ua.SSNMStateConfig{SubscriptionQueueBytes: standalone}, retained: 6, binding: ssnmBindingBytes},
		{name: "standalone one byte short", short: true, limits: m3ua.SSNMStateConfig{SubscriptionQueueBytes: standalone - 1}, retained: 5, binding: ssnmBindingBytes},
		{name: "canonical exact fit", canonical: true, limits: m3ua.SSNMStateConfig{SubscriptionQueueBytes: canonical}, retained: 11, binding: ssnmBindingBytes},
		{name: "canonical one byte short", canonical: true, short: true, limits: m3ua.SSNMStateConfig{SubscriptionQueueBytes: canonical - 1}, retained: 10, failure: "below both caps"},
		{name: "count cap", limits: m3ua.SSNMStateConfig{SubscriptionQueueSize: 4}, retained: 4, binding: ssnmBindingCount},
	}

	sgp, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	address, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := sgp.Listen("m3ua", address, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	accepted := make(chan error, len(peers))
	go func() {
		for range peers {
			_, acceptErr := listener.Accept(ctx)
			accepted <- acceptErr
		}
	}()
	remote := listener.Addr().(*sctp.SCTPAddr)
	for _, peer := range peers {
		endpointConfig, associationConfig := byteCapPeerConfig(peer)
		peer.endpoint, err = m3ua.NewEndpoint(endpointConfig)
		if err != nil {
			testContext.Fatalf("%s: %v", peer.name, err)
		}
		defer func() { _ = peer.endpoint.Close() }()
		if _, err := peer.endpoint.Dial(ctx, "m3ua", nil, remote, associationConfig); err != nil {
			testContext.Fatalf("%s: dial: %v", peer.name, err)
		}
	}
	for range peers {
		if err := <-accepted; err != nil {
			testContext.Fatalf("accept: %v", err)
		}
	}
	for _, peer := range peers {
		_, peer.reference, err = peer.endpoint.SubscribeSSNM()
		if err != nil {
			testContext.Fatal(err)
		}
		_, held, err := peer.endpoint.SubscribeSSNM()
		if err != nil {
			testContext.Fatal(err)
		}
		peer.held = &recordingStream{ssnmEventStream: held}
	}

	// Publish in step: each report reaches every reference subscription
	// before the next one is sent, so every store has published it and the
	// held queues see the reports in order.
	destinations := ssnmDestinations(64)
	first := 0
	for index, count := range byteCapCounts {
		request := m3ua.DestinationAvailabilityRequest{Scope: ssnmScope(), Destinations: destinations[first : first+count], Availability: m3ua.DestinationUnavailable}
		first += count
		if err := sgp.ReportDestinationAvailability(request); err != nil {
			testContext.Fatalf("report %d: %v", index, err)
		}
		for _, peer := range peers {
			nextContext, nextCancel := context.WithTimeout(ctx, 5*time.Second)
			event, err := peer.reference.Next(nextContext)
			nextCancel()
			if err != nil || event.Kind != m3ua.SSNMReportEvent {
				testContext.Fatalf("%s: report %d delivered %s, %v", peer.name, index, event.Kind, err)
			}
			if got, want := ssnmEventBytes(event), byteCapModel(index, peer.canonical); got != want {
				testContext.Fatalf("%s: report %d (%d APCs, %d updated, partition %+v) accounts %d bytes, the documented formula predicts %d",
					peer.name, index, len(event.Report.Destinations), len(event.Updated), event.Partition, got, want)
			}
			peer.delivered = append(peer.delivered, event)
		}
	}

	plan := ssnmPlan{records: 64, apcs: 1}
	for _, peer := range peers {
		testContext.Run(peer.name, func(testContext *testing.T) {
			queueLimit := peer.limits.SubscriptionQueueSize
			if queueLimit == 0 {
				queueLimit = m3ua.DefaultSSNMSubscriptionQueueSize
			}
			queueBytes := peer.limits.SubscriptionQueueBytes
			if queueBytes == 0 {
				queueBytes = m3ua.DefaultSSNMSubscriptionQueueBytes
			}
			subscriber := newSSNMSubscriber(0, true, plan, 1000, 1, queueLimit, queueBytes)
			subscriber.subscription = peer.held
			pause := ssnmPause{Duration: time.Millisecond}
			subscriber.pauseAndRecover(ctx, &steppingMeasurementClock{step: int64(pause.Duration)}, 0, pause)
			record := subscriber.pause
			retainedBytes := 0
			for _, event := range peer.delivered[:peer.retained] {
				retainedBytes += ssnmEventBytes(event)
			}
			next := ssnmEventBytes(peer.delivered[peer.retained])
			testContext.Logf("limit %d events / %d bytes: retained %d events, %d accounted bytes; next event %d bytes; binding %s",
				queueLimit, queueBytes, record.QueuedAtLoss, record.QueuedBytesAtLoss, next, record.BindingCap)
			if record.Error != "" || !record.ContinuityLossObserved || record.QueuedAtLoss != peer.retained || record.QueuedBytesAtLoss != retainedBytes {
				testContext.Fatalf("pause record %+v, want continuity lost after %d events of %d accounted bytes", record, peer.retained, retainedBytes)
			}
			// The library filled an exact limit to the byte and refused the
			// event one byte past a short one, by the fixture's own count.
			switch {
			case peer.limits.SubscriptionQueueBytes == 0:
			case !peer.short && retainedBytes != queueBytes:
				testContext.Fatalf("retained %d bytes by the fixture's accounting, the library filled its %d-byte limit exactly", retainedBytes, queueBytes)
			case peer.short && retainedBytes+next != queueBytes+1:
				testContext.Fatalf("retained %d bytes and refused a %d-byte event by the fixture's accounting, one byte over the %d-byte limit", retainedBytes, next, queueBytes)
			}
			for index, event := range peer.held.events[:peer.retained] {
				if event.Revision != peer.delivered[index].Revision {
					testContext.Fatalf("retained event %d has revision %d, the reference saw %d", index, event.Revision, peer.delivered[index].Revision)
				}
			}
			if marker := peer.held.events[peer.retained]; marker.Kind != m3ua.SSNMContinuityLostEvent {
				testContext.Fatalf("event after the retained queue is %s, want the continuity-loss marker", marker.Kind)
			}
			failure := ssnmCapFailure(record)
			if record.BindingCap != peer.binding || peer.failure == "" && failure != "" || !strings.Contains(failure, peer.failure) {
				testContext.Fatalf("binding %q failure %q, want binding %q and failure %q", record.BindingCap, failure, peer.binding, peer.failure)
			}
		})
	}
}

// byteCapPeerConfig is a peer's ASP Endpoint and Association configuration.
// A standalone peer is the fixture's own configuration; a canonical peer
// provisions the SGP as one Signalling Gateway serving the generator's
// Routing Context.
func byteCapPeerConfig(peer *byteCapPeer) (m3ua.EndpointConfig, *m3ua.AssociationConfig) {
	limits := peer.limits
	endpoint := m3ua.EndpointConfig{Role: m3ua.RoleASP, SSNMState: &limits}
	association := associationConfig("asp")
	if !peer.canonical {
		return endpoint, association
	}
	key := m3ua.ASKey{NetworkAppearance: testNetworkAppearance, NetworkAppearanceSet: true, RoutingContext: ssnmRoutingContext, RoutingContextSet: true}
	endpoint.ASP = &m3ua.ASPConfig{SignallingGateways: []m3ua.SignallingGatewayConfig{{
		ID: byteCapGateway,
		SGPs: []m3ua.SignallingGatewayProcessConfig{{
			ID:                 "sgp-1",
			ApplicationServers: []m3ua.RemoteASConfig{{ID: byteCapServer, ASKey: &key}},
		}},
	}}}
	association.ApplicationServers = association.ApplicationServers[:1]
	association.PeerSGP = &m3ua.SGPIdentity{SignallingGateway: byteCapGateway, SignallingGatewayProcess: "sgp-1"}
	return endpoint, association
}
