package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

func routedSenderArguments(mode string, extra ...string) []string {
	return append([]string{
		"-role=asp", "-transport=dial", "-mode=" + mode,
		"-sctp-address=10.0.0.2:2905", "-local-address=10.0.0.3:0",
		"-peer-control=http://10.0.0.2:8080", "-associations=8", "-payload=mix",
		"-rate=32000", "-warmup=5s", "-duration=20s", "-drain=2s", "-cohort=routed-01",
	}, extra...)
}

func routedReceiverArguments(mode string, extra ...string) []string {
	return append([]string{
		"-role=sgp", "-transport=listen", "-mode=" + mode,
		"-sctp-address=10.0.0.2:2905", "-control-address=0.0.0.0:8080", "-associations=8",
	}, extra...)
}

func TestParseConfigAcceptsRoutedModesWithTheFixedTopology(testContext *testing.T) {
	for _, mode := range []string{modeRouted, modeRoutedDirect} {
		for _, arguments := range [][]string{
			routedSenderArguments(mode),
			routedSenderArguments(mode, "-same-host-clock"),
			routedReceiverArguments(mode),
			routedReceiverArguments(mode, "-same-host-clock"),
		} {
			config, err := parseConfig(arguments)
			if err != nil {
				testContext.Fatalf("parseConfig(%v): %v", arguments, err)
			}
			if config.Mode != mode || config.Direction != directionASPToSGP || config.Initiation != initiationASPDial || config.Associations != 8 {
				testContext.Fatalf("parseConfig(%v) = %+v, want %s asp-to-sgp asp-dial over eight associations", arguments, config, mode)
			}
			if config.Role == "asp" && config.Workload != workloadMix {
				testContext.Fatalf("routed sender workload = %q, want mix", config.Workload)
			}
		}
	}
}

func TestParseConfigRejectsRoutedModesOutsideTheFixedTopology(testContext *testing.T) {
	for _, mode := range []string{modeRouted, modeRoutedDirect} {
		for _, test := range []struct {
			name      string
			arguments []string
			wantError string
		}{
			{name: "sender associations", arguments: routedSenderArguments(mode, "-associations=4"), wantError: "8 associations"},
			{name: "receiver associations", arguments: routedReceiverArguments(mode, "-associations=1"), wantError: "8 associations"},
			{name: "sender payload", arguments: routedSenderArguments(mode, "-payload=128"), wantError: "mix payload"},
			{name: "sender listens", arguments: routedSenderArguments(mode, "-transport=listen"), wantError: "asp-dial"},
			{name: "receiver dials", arguments: routedReceiverArguments(mode, "-transport=dial"), wantError: "asp-dial"},
			{name: "sender without local address", arguments: routedSenderArguments(mode, "-local-address="), wantError: "local-address"},
			{name: "sender fixed local port", arguments: routedSenderArguments(mode, "-local-address=10.0.0.3:2905"), wantError: "ephemeral"},
			{name: "receiver wildcard listen", arguments: routedReceiverArguments(mode, "-sctp-address=0.0.0.0:2905"), wantError: "concrete"},
			{name: "receiver port range", arguments: routedReceiverArguments(mode, "-sctp-address=10.0.0.2:65533"), wantError: "four consecutive"},
			{name: "receiver missing port", arguments: routedReceiverArguments(mode, "-sctp-address=10.0.0.2"), wantError: "sctp-address"},
			{name: "sender multihomed peer", arguments: routedSenderArguments(mode, "-sctp-address=10.0.0.2/10.0.0.4:2905"), wantError: "one address"},
		} {
			testContext.Run(mode+"/"+test.name, func(testContext *testing.T) {
				_, err := parseConfig(test.arguments)
				if err == nil || !strings.Contains(err.Error(), test.wantError) {
					testContext.Fatalf("parseConfig error = %v, want it to mention %q", err, test.wantError)
				}
			})
		}
	}
}

func TestParseConfigStillRejectsUnknownModes(testContext *testing.T) {
	_, err := parseConfig([]string{"-role=sgp", "-mode=routing"})
	if err == nil || !strings.Contains(err.Error(), "routed-direct") {
		testContext.Fatalf("unknown mode error = %v, want the implemented modes listed", err)
	}
}

func TestRoutedPeerAddressesUseFourConsecutivePorts(testContext *testing.T) {
	addresses, err := routedPeerAddresses("127.0.0.1:2905")
	if err != nil {
		testContext.Fatal(err)
	}
	if len(addresses) != 4 {
		testContext.Fatalf("addresses = %d, want 4", len(addresses))
	}
	for index, address := range addresses {
		if len(address.IPAddrs) != 1 || !address.IPAddrs[0].IP.Equal([]byte{127, 0, 0, 1}) || address.Port != 2905+index {
			testContext.Fatalf("address %d = %+v", index, address)
		}
	}
	local, err := routedLocalAddress("127.0.0.1:0")
	if err != nil || local.Port != 0 || len(local.IPAddrs) != 1 {
		testContext.Fatalf("local address = %+v, %v", local, err)
	}
	for _, invalid := range []string{"0.0.0.0:2905", "[::]:2905", "127.0.0.1:65533", "127.0.0.1"} {
		if _, err := routedPeerAddresses(invalid); err == nil {
			testContext.Fatalf("routedPeerAddresses(%q) succeeded", invalid)
		}
	}
}

func routedPeerPathsFixture(testContext *testing.T) (routingTopology, []routingAssociationPair, []routingDataReceiptDTO, routingPathMap) {
	testContext.Helper()
	topology, pairs, _, _, receipts := routingDataFixture(testContext)
	paths, err := freezeRoutingPeerPaths(topology, pairs, receipts, "preflight", 7)
	if err != nil {
		testContext.Fatalf("freezeRoutingPeerPaths: %v", err)
	}
	return topology, pairs, receipts, paths
}

func TestRoutingPeerPathsFreezeTheReceiverView(testContext *testing.T) {
	_, _, _, paths := routedPeerPathsFixture(testContext)
	senderTopology, bindings, observations := routingPreflightFixture(testContext, "primary")
	senderPaths, err := freezeRoutingPaths(senderTopology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	for route := range routingRouteCount {
		peer, err := paths.path(uint16(route))
		if err != nil {
			testContext.Fatal(err)
		}
		sender, _ := senderPaths.path(uint16(route))
		if peer.Binding != sender.Binding || peer.Target.AS != sender.Target.AS || peer.Target.SGP != sender.Target.SGP ||
			peer.Target.Association != sender.Target.Association || peer.Target.Path != sender.Target.Path {
			testContext.Fatalf("route %d receiver path %+v differs from sender path %+v", route, peer, sender)
		}
	}
}

func TestRoutingPeerPathsRejectContradictoryReceipts(testContext *testing.T) {
	topology, pairs, receipts, _ := routedPeerPathsFixture(testContext)
	secondary := topology.Peers[0].ApplicationServers[1].ASKey
	for _, test := range []struct {
		name   string
		mutate func([]routingDataReceiptDTO) []routingDataReceiptDTO
	}{
		{name: "missing route", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO { return values[:len(values)-1] }},
		{name: "duplicate route", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[1].Route = values[0].Route
			return values
		}},
		{name: "unpreferred AS", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			for index := range values {
				if values[index].SGP == topology.Peers[0].Identity {
					values[index].AS = secondary
					values[index].RoutingContext = secondary.RoutingContext
				}
			}
			return values
		}},
		{name: "peer epoch", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[0].Epoch++
			return values
		}},
		{name: "unknown transport", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[0].Association = 99
			return values
		}},
		{name: "payload", mutate: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[0].ProtocolData.Data[len(values[0].ProtocolData.Data)-1] ^= 1
			return values
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			owned := cloneRoutingDataReceipts(receipts)
			if _, err := freezeRoutingPeerPaths(topology, pairs, test.mutate(owned), "preflight", 7); err == nil {
				testContext.Fatal("contradictory receipts froze a receiver path map")
			}
		})
	}
}

func routedReadyControl(testContext *testing.T, mode string) (*receiverControl, routingPathMap) {
	testContext.Helper()
	_, _, _, paths := routedPeerPathsFixture(testContext)
	control := newReceiverControl(8, maxOutstanding)
	control.enableRouted(mode)
	if err := control.freezeRoutes(paths); err != nil {
		testContext.Fatal(err)
	}
	for index := range 8 {
		control.setAssociationReady(index, 16)
	}
	return control, paths
}

func routedSpec(mode string) runSpec {
	return runSpec{
		Cohort: "routed-cohort", Seed: 3, Associations: 8, Expected: 2000, Duration: 2 * time.Second,
		Drain: 2 * time.Second, Rate: 1000, Outstanding: maxOutstanding, Payload: workloadMix,
		Mode: mode, Direction: directionASPToSGP, Initiation: initiationASPDial,
	}
}

func TestRoutedReceiverResetRequiresFrozenRoutesAndTheFixedShape(testContext *testing.T) {
	plain := newReceiverControl(8, maxOutstanding)
	for index := range 8 {
		plain.setAssociationReady(index, 16)
	}
	if err := plain.reset(routedSpec(modeRouted)); err == nil {
		testContext.Fatal("direct receiver accepted a routed cohort")
	}

	unfrozen := newReceiverControl(8, maxOutstanding)
	unfrozen.enableRouted(modeRouted)
	for index := range 8 {
		unfrozen.setAssociationReady(index, 16)
	}
	if err := unfrozen.reset(routedSpec(modeRouted)); err == nil {
		testContext.Fatal("routed receiver accepted a cohort before routes were frozen")
	}

	for _, test := range []struct {
		name   string
		mutate func(*runSpec)
	}{
		{name: "other routed mode", mutate: func(specification *runSpec) { specification.Mode = modeRoutedDirect }},
		{name: "throughput", mutate: func(specification *runSpec) { specification.Mode = modeThroughput }},
		{name: "payload", mutate: func(specification *runSpec) { specification.Payload = workload128 }},
		{name: "direction", mutate: func(specification *runSpec) { specification.Direction = directionSGPToASP }},
		{name: "initiation", mutate: func(specification *runSpec) { specification.Initiation = initiationSGPDial }},
		{name: "associations", mutate: func(specification *runSpec) { specification.Associations = 4 }},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			control, _ := routedReadyControl(testContext, modeRouted)
			specification := routedSpec(modeRouted)
			test.mutate(&specification)
			if err := control.reset(specification); err == nil {
				testContext.Fatalf("routed receiver accepted %s", test.name)
			}
		})
	}
	control, _ := routedReadyControl(testContext, modeRoutedDirect)
	if err := control.reset(routedSpec(modeRoutedDirect)); err != nil {
		testContext.Fatalf("valid routed-direct reset: %v", err)
	}
	if control.ledger != nil {
		testContext.Fatal("routed cohort allocated the 32-flow direct ledger")
	}
}

func routedTimedMessage(testContext *testing.T, paths routingPathMap, specification runSpec, index uint64) (routingTransport, *m3ua.DataMessage) {
	testContext.Helper()
	identity := planRouteMessage(specification.Cohort, specification.Seed, index)
	path, err := paths.path(identity.Route)
	if err != nil {
		testContext.Fatal(err)
	}
	return path.Binding.Peer, routingReceivedMessage(testContext, identity, specification.Payload.size(index), path.Target, path.Binding)
}

func TestRoutedReceiverValidatesEveryRouteFlow(testContext *testing.T) {
	control, paths := routedReadyControl(testContext, modeRouted)
	specification := routedSpec(modeRouted)
	early, earlyMessage := routedTimedMessage(testContext, paths, specification, 0)
	if outcome := control.recordRouted(early, earlyMessage); outcome != recordIgnored {
		testContext.Fatalf("arrival before any cohort = %v, want ignored", outcome)
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	for index := range specification.Expected {
		transport, message := routedTimedMessage(testContext, paths, specification, index)
		if outcome := control.recordRouted(transport, message); outcome != recordUnique {
			testContext.Fatalf("message %d outcome = %v, want unique", index, outcome)
		}
	}
	transport, message := routedTimedMessage(testContext, paths, specification, 5)
	if outcome := control.recordRouted(transport, message); outcome != recordNotUnique {
		testContext.Fatalf("duplicate outcome = %v", outcome)
	}
	otherTransport, _ := routedTimedMessage(testContext, paths, specification, 6)
	if transport == otherTransport {
		testContext.Fatal("fixture routes 5 and 6 share a transport")
	}
	if outcome := control.recordRouted(otherTransport, message); outcome != recordInvalid {
		testContext.Fatalf("misrouted arrival outcome = %v, want invalid", outcome)
	}
	if err := control.stop(); err != nil {
		testContext.Fatal(err)
	}
	record := control.result()
	if record.Delivery.Unique != specification.Expected || record.Delivery.Missing != 0 || record.Delivery.Duplicate != 1 ||
		record.Delivery.Invalid != 1 || record.Delivery.Reordered != 0 {
		testContext.Fatalf("routed delivery = %+v", record.Delivery)
	}
	if progress := control.progress(); progress.Delivery.Unique != specification.Expected {
		testContext.Fatalf("routed progress = %+v", progress.Delivery)
	}
	if _, claimsUnavailable := record.UnsupportedModes["router_or_ssnm_workload"]; claimsUnavailable {
		testContext.Fatalf("routed record still reports the router workload unavailable: %+v", record.UnsupportedModes)
	}
	if record.Manifest.FlowCount != 0 {
		testContext.Fatalf("control result invented a manifest: %+v", record.Manifest)
	}
}

func TestDirectRecordsKeepTheirUnsupportedModeText(testContext *testing.T) {
	record := runRecord{Spec: runSpec{Mode: modeThroughput}}
	record.evaluate()
	if record.UnsupportedModes["router_or_ssnm_workload"] != "unavailable: this fixture does not exercise the existing routing and state APIs" {
		testContext.Fatalf("direct unsupported modes changed: %+v", record.UnsupportedModes)
	}
}

func TestRoutedControlHandlerSeparatesRoutingAndCohortOperations(testContext *testing.T) {
	control, _ := routedReadyControl(testContext, modeRouted)
	var routingPaths []string
	routing := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		routingPaths = append(routingPaths, request.URL.Path)
		writer.WriteHeader(http.StatusTeapot)
	})
	handler := routedControlHandler(control.handler(), routing)
	for _, path := range []string{"/routing/inventory", "/routing/prepare", "/routing/data/start", "/routing/stop"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, path, nil))
		if response.Code != http.StatusTeapot {
			testContext.Fatalf("%s reached the cohort control: %d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/ready", nil))
	var ready readyResult
	if err := json.Unmarshal(response.Body.Bytes(), &ready); err != nil || !ready.Ready || ready.Associations != 8 {
		testContext.Fatalf("/ready = %s (%v)", response.Body.String(), err)
	}
	if len(routingPaths) != 4 {
		testContext.Fatalf("routing handler saw %v", routingPaths)
	}
}

func TestRoutingControlUsesNamespacedReadyAndStop(testContext *testing.T) {
	control, err := newRoutingControl("prep-a", routingControlOperations{
		Inventory: func(context.Context) (routingInventoryDTO, error) { return routingInventoryDTO{}, nil },
		Prepare:   func(context.Context, []routingTransportDTO) error { return nil },
		Publish:   func(context.Context, uint8) error { return nil },
		Stop:      func(context.Context) error { return nil },
	})
	if err != nil {
		testContext.Fatal(err)
	}
	for _, path := range []string{"/ready", "/stop"} {
		response := httptest.NewRecorder()
		control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			testContext.Fatalf("routing control still serves %s: %d", path, response.Code)
		}
	}
	response := httptest.NewRecorder()
	control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/routing/ready", nil))
	if response.Code != http.StatusOK {
		testContext.Fatalf("/routing/ready = %d %s", response.Code, response.Body.String())
	}
}

func TestDispatchRoutedOffersUniformRouteHitsOnFrozenQueues(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	const expected = uint64(2 * routingRouteCount)
	duration := 2 * time.Second
	queues := make([]chan routingTimedJob, 8)
	for index := range queues {
		queues[index] = make(chan routingTimedJob, expected)
	}
	counters := newSenderCounters(int(expected))
	config := commandConfig{Rate: 1000, Seed: 9, Workload: workloadMix}
	dispatchRouted(context.Background(), config, fixture.direct, "routed-dispatch", duration, time.Now().Add(-duration), expected, queues, counters, nil)
	if snapshot := routingTimedCountersSnapshot(counters); snapshot != (routingTimedCounterSnapshot{scheduled: expected, outstanding: expected}) {
		testContext.Fatalf("dispatch counters = %+v", snapshot)
	}
	var hits [routingRouteCount]int
	total := 0
	for queue, jobs := range queues {
		close(jobs)
		previous := make(map[uint16]uint64)
		for job := range jobs {
			index := job.identity.Sequence*routingRouteCount + uint64(job.identity.Route)
			path, err := fixture.paths.path(job.identity.Route)
			if err != nil {
				testContext.Fatal(err)
			}
			if int(job.queue) != queue || job.path != path || job.size != workloadMix.size(index) || job.identity.Seed != config.Seed {
				testContext.Fatalf("queue %d job %+v", queue, job)
			}
			if last, seen := previous[job.identity.Route]; seen && job.identity.Sequence <= last {
				testContext.Fatalf("route %d queued out of order", job.identity.Route)
			}
			previous[job.identity.Route] = job.identity.Sequence
			hits[job.identity.Route]++
			total++
		}
	}
	for route, count := range hits {
		if count != 2 {
			testContext.Fatalf("route %d received %d hits, want 2", route, count)
		}
	}
	if total != int(expected) {
		testContext.Fatalf("queued %d jobs, want %d", total, expected)
	}
}

func TestDispatchRoutedAccountsCapAndFullQueues(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	const expected = uint64(16)
	duration := 16 * time.Millisecond
	config := commandConfig{Rate: 1000, Seed: 9, Workload: workloadMix}

	capped := make([]chan routingTimedJob, 8)
	for index := range capped {
		capped[index] = make(chan routingTimedJob, expected)
	}
	counters := newSenderCounters(1)
	dispatchRouted(context.Background(), config, fixture.direct, "routed-cap", duration, time.Now().Add(-duration), expected, capped, counters, nil)
	if snapshot := routingTimedCountersSnapshot(counters); snapshot != (routingTimedCounterSnapshot{scheduled: expected, capped: expected - 1, outstanding: 1}) {
		testContext.Fatalf("capped dispatch counters = %+v", snapshot)
	}

	full := make([]chan routingTimedJob, 8)
	for index := range full {
		full[index] = make(chan routingTimedJob)
	}
	counters = newSenderCounters(int(expected))
	dispatchRouted(context.Background(), config, fixture.direct, "routed-full", duration, time.Now().Add(-duration), expected, full, counters, nil)
	if snapshot := routingTimedCountersSnapshot(counters); snapshot != (routingTimedCounterSnapshot{scheduled: expected, capped: expected}) {
		testContext.Fatalf("full-queue dispatch counters = %+v", snapshot)
	}
}

func TestRoutedWorkersSubmitEveryJobInRouteOrder(testContext *testing.T) {
	fixture := newRoutingTimedWorkerFixture(testContext)
	const expected = uint64(3 * routingRouteCount)
	duration := 3 * time.Second
	config := commandConfig{Rate: 1000, Seed: 9, Workload: workloadMix, Outstanding: maxOutstanding}
	counters := newSenderCounters(config.Outstanding)
	queues, done := startRoutedSendWorkers(context.Background(), fixture.direct, config, counters)
	dispatchRouted(context.Background(), config, fixture.direct, "routed-workers", duration, time.Now().Add(-duration), expected, queues, counters, nil)
	for _, queue := range queues {
		close(queue)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		testContext.Fatal("routed workers did not finish")
	}
	snapshot := routingTimedCountersSnapshot(counters)
	if snapshot.scheduled != expected || snapshot.submitted != expected || snapshot.sendErrors != 0 || snapshot.capped != 0 || snapshot.outstanding != 0 || snapshot.fatal != "" {
		testContext.Fatalf("worker counters = %+v", snapshot)
	}
	written := 0
	for associationID, association := range fixture.associations {
		previous := make(map[uint16]uint64)
		for _, request := range association.writes {
			identity, err := parseRoutePayload(request.ProtocolData.Data)
			if err != nil {
				testContext.Fatal(err)
			}
			path, _ := fixture.paths.path(identity.Route)
			if path.Target.Association != associationID || request.AS != path.Target.AS {
				testContext.Fatalf("route %d written on association %d scope %+v", identity.Route, associationID, request.AS)
			}
			if last, seen := previous[identity.Route]; seen && identity.Sequence != last+1 {
				testContext.Fatalf("route %d sequence %d follows %d", identity.Route, identity.Sequence, last)
			}
			previous[identity.Route] = identity.Sequence
			written++
		}
	}
	if written != int(expected) || len(fixture.endpoint.calls) != 0 {
		testContext.Fatalf("direct writes = %d, MTPTransfer calls = %d", written, len(fixture.endpoint.calls))
	}
}

func TestRoutedPathSharesCoverEveryAssociation(testContext *testing.T) {
	_, _, _, paths := routedPeerPathsFixture(testContext)
	shares := routedPathShares(&paths)
	total := 0
	senders := make(map[m3ua.AssociationID]bool)
	for _, share := range shares {
		if share.Routes == 0 || senders[share.SenderAssociation] {
			testContext.Fatalf("share %+v is empty or repeated", share)
		}
		senders[share.SenderAssociation] = true
		total += share.Routes
	}
	if len(shares) != 8 || total != routingRouteCount {
		testContext.Fatalf("shares = %+v, want eight associations carrying all %d routes", shares, routingRouteCount)
	}
}
