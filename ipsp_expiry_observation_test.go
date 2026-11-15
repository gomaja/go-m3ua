package m3ua

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

const transportMaskedExpiryDestinations = 512

func drainExpiryIndications(t *testing.T, e *Endpoint) []MTPIndication {
	t.Helper()
	var events []MTPIndication
	for {
		select {
		case event, open := <-e.MTPIndications():
			if !open || event == nil {
				t.Fatal("MTP stream closed during expiry fixture")
			}
			events = append(events, *event)
		default:
			return events
		}
	}
}

func newTransportMaskedExpiryFixture(t *testing.T, capacity int) (*Endpoint, *Association, *time.Time, []uint32) {
	t.Helper()
	key := routingContextASKey(1)
	mappings := make([]IPSPDestinationConfig, transportMaskedExpiryDestinations)
	points := make([]uint32, len(mappings))
	for i := range mappings {
		points[i] = uint32(i)
		mappings[i] = IPSPDestinationConfig{
			Destination: MTPDestination{MTPRoute: MTPRouteID(fmt.Sprintf("masked-%d", i)), PointCode: points[i]},
			AS:          key,
		}
	}
	e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{
		Role: RoleIPSP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}, TransportCongestion: &TransportCongestionConfig{Disabled: true}, IPSPDestinations: mappings,
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if capacity > 0 {
		// Installed before any association or projection can publish.
		e.ipspMTP.stream.indications = make(chan *MTPIndication, capacity)
	}
	now := time.Unix(1000, 0)
	e.ssnm.now = func() time.Time { return now }
	e.ssnm.validity = time.Second
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track mapped IPSP")
	}
	t.Cleanup(func() { _ = a.Close() })
	_ = drainExpiryIndications(t, e)
	setAssociationTransport(t, e, a, 3)
	_ = drainExpiryIndications(t, e)
	if got := len(e.MTPDestinationStatuses()); got != len(mappings) {
		t.Fatal("mapped fixture destination count", got)
	}
	for _, status := range e.MTPDestinationStatuses() {
		if status.Availability != DestinationAvailable || !status.Congested || status.CongestionLevel != 3 || status.TransportCongestionLevel != 3 || status.TransportCongestionUnknown || status.ReceivedCongestionUnknown {
			t.Fatal("fixture did not retain sustained known transport level 3", status)
		}
	}
	before := e.MTPDestinationStatuses()
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(points...), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	if after := e.MTPDestinationStatuses(); !reflect.DeepEqual(before, after) {
		t.Fatal("transport level 3 did not mask level-2 receipt", before, after)
	}
	if events := drainExpiryIndications(t, e); len(events) != 0 {
		t.Fatal("masked receipt changed exposed status", len(events))
	}
	if err := a.validateMethodData(key, &params.ProtocolDataPayload{DestinationPointCode: points[0], MessagePriority: 3}); err != nil {
		t.Fatal("fresh masked received knowledge rejected maximum national priority", err)
	}
	return e, a, &now, points
}

func requireExpiredTransportMaskedStatuses(t *testing.T, e *Endpoint) map[MTPDestination]MTPDestinationStatus {
	t.Helper()
	statuses := e.MTPDestinationStatuses()
	if len(statuses) != transportMaskedExpiryDestinations {
		t.Fatal("status resync lost mapped destinations", len(statuses))
	}
	indexed := make(map[MTPDestination]MTPDestinationStatus, len(statuses))
	for _, status := range statuses {
		if status.Availability != DestinationAvailable || !status.Congested || status.CongestionLevel != 3 || status.TransportCongestionLevel != 3 || status.TransportCongestionUnknown || !status.ReceivedCongestionUnknown {
			t.Fatal("expiry changed transport severity or hid received uncertainty", status)
		}
		if _, duplicate := indexed[status.Destination]; duplicate {
			t.Fatal("resync repeated a destination", status.Destination)
		}
		indexed[status.Destination] = status
	}
	return indexed
}

func TestTransportMaskedExpiryReportsEachChangedIPSPDestination(t *testing.T) {
	e, a, now, points := newTransportMaskedExpiryFixture(t, 2*transportMaskedExpiryDestinations)
	before := e.MTPDestinationStatuses()
	*now = now.Add(time.Second)
	e.runCongestionAutomation(*now)
	after := requireExpiredTransportMaskedStatuses(t, e)
	changed := make(map[MTPDestination]bool)
	for _, old := range before {
		if old != after[old.Destination] {
			changed[old.Destination] = true
		}
	}
	if len(changed) != transportMaskedExpiryDestinations {
		t.Fatal("expiry did not expose every unknown-bit change", len(changed))
	}
	events := drainExpiryIndications(t, e)
	counts := make(map[MTPDestination]int)
	for _, event := range events {
		if event.ResyncRequired || event.Kind != MTPStatusIndication || !changed[event.Destination.Destination] || event.Destination != after[event.Destination.Destination] {
			t.Fatal("expiry suppressed a real change or emitted an unrelated status", event)
		}
		counts[event.Destination.Destination]++
	}
	if len(events) != len(changed) || len(counts) != len(changed) {
		t.Fatal("expiry must emit exactly one indication per changed destination", len(events), len(changed), len(counts))
	}
	for destination, count := range counts {
		if count != 1 {
			t.Fatal("expiry repeated a destination change", destination, count)
		}
	}
	if err := a.validateMethodData(routingContextASKey(1), &params.ProtocolDataPayload{DestinationPointCode: points[0], MessagePriority: 3}); err == nil {
		t.Fatal("expired unknown did not affect national-priority admission")
	}
	e.runCongestionAutomation(*now)
	e.refreshIPSPDestinations()
	if repeated := drainExpiryIndications(t, e); len(repeated) != 0 {
		t.Fatal("unchanged expired status replayed indications", len(repeated))
	}
	t.Logf("masked receipt indications=0; expiry changed destinations=%d indications=%d", len(changed), len(events))
}

func TestTransportMaskedExpiryOverflowRequiresStatusResync(t *testing.T) {
	e, a, now, points := newTransportMaskedExpiryFixture(t, 0)
	if cap(e.MTPIndications()) != DefaultMTPIndicationQueueSize || cap(e.MTPIndications()) != 256 {
		t.Fatal("fixture does not use the default indication queue", cap(e.MTPIndications()))
	}
	*now = now.Add(time.Second)
	e.runCongestionAutomation(*now)
	events := drainExpiryIndications(t, e)
	if len(events) != 1 || !events[0].ResyncRequired {
		t.Fatal("512 real status changes must signal default indication overflow", events)
	}
	// The marker explicitly replaces the discarded delta burst. The owned
	// status snapshot is the recovery source for every affected destination.
	recovered := requireExpiredTransportMaskedStatuses(t, e)
	e.refreshIPSPDestinations()
	if repeated := drainExpiryIndications(t, e); len(repeated) != 0 {
		t.Fatal("status resync created duplicate changes", repeated)
	}
	// Reading the marker and obtaining the snapshot must leave the stream
	// usable for subsequent real changes; do not accept a silently stuck gate.
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(points[0]), nil, params.NewCongestionIndications(0), nil)); err != nil {
		t.Fatal(err)
	}
	resumed := drainExpiryIndications(t, e)
	if len(resumed) != 1 || resumed[0].ResyncRequired || resumed[0].Kind != MTPStatusIndication || resumed[0].Destination.Destination.PointCode != points[0] || resumed[0].Destination.ReceivedCongestionUnknown || resumed[0].Destination.CongestionLevel != 3 || resumed[0].Destination.TransportCongestionLevel != 3 {
		t.Fatal("MTP stream did not resume exactly one fresh change after status resync", resumed)
	}
	for _, status := range e.MTPDestinationStatuses() {
		prior := recovered[status.Destination]
		if status.Destination.PointCode == points[0] {
			prior.ReceivedCongestionUnknown = false
		}
		if status != prior {
			t.Fatal("resync/fresh clear lost another destination", status, prior)
		}
	}
	t.Logf("masked receipt indications=0; expired destinations=%d; default queue capacity=256; overflow markers=%d; resumed changes=%d", len(recovered), len(events), len(resumed))
}

func TestExpiryIndicationContractSeparatesSubscriptionAndMTPQueues(t *testing.T) {
	// The compact SSNM expiry bound does not bound derived MTP-STATUS changes.
	// Keep the replacement contract next to the full-snapshot replay contract.
	source, err := os.ReadFile("docs/design/endpoint-management-and-ssnm.md")
	if err != nil {
		t.Fatal(err)
	}
	text := string(source)
	const required = "Expiry may produce more MTP indications than receipt"
	start := strings.Index(text, required)
	if start < 0 {
		t.Fatal("expiry indication contract omits transport-masked growth and status resynchronization")
	}
	paragraph := strings.SplitN(text[start:], "\n\n", 2)[0]
	for _, detail := range []string{"ReceivedCongestionUnknown", "one indication per", "ResyncRequired", "MTPDestinationStatuses"} {
		if !strings.Contains(paragraph, detail) {
			t.Fatalf("expiry indication contract omits %s", detail)
		}
	}
}
