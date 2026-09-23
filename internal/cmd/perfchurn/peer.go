package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// peerRun is the SGP side: four SGP Endpoints, one per provisioned SGP, that
// dial the ASP listener from their own port ranges. It is load generation and
// counterpart only; nothing about its own memory is measured.
type peerRun struct {
	config    commandConfig
	ctx       context.Context
	finish    context.CancelFunc
	endpoints [sgpCount]*m3ua.Endpoint
	remote    *sctp.SCTPAddr
	localIP   net.IP

	// finishing is set once the ASP has asked for the record: the ASP then
	// closes its Endpoint, and the ends that follow are the teardown.
	finishing atomic.Bool

	mutex  sync.Mutex
	stable []*m3ua.Association
	ledger receiveLedger
	sender *ledgerSender
	record peerRecord
}

func runPeer(ctx context.Context, config commandConfig) (peerRecord, error) {
	ctx, finish := context.WithCancel(ctx)
	defer finish()
	peer := &peerRun{config: config, ctx: ctx, finish: finish, localIP: net.ParseIP(config.LocalIP),
		record: peerRecord{Kind: "perfchurn-peer", Manifest: currentManifest(rolePeer, config)}}
	remote, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return peer.record, fmt.Errorf("resolve ASP address: %w", err)
	}
	peer.remote = remote
	for index := range peer.endpoints {
		endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP, SGP: sgpEndpointConfig()})
		if err != nil {
			return peer.record, fmt.Errorf("SGP endpoint %d: %w", index, err)
		}
		peer.endpoints[index] = endpoint
		defer func() { _ = endpoint.Close() }()
	}
	listener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return peer.record, fmt.Errorf("control listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.Handle(operationReady, handle(func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil }))
	mux.Handle(operationEstablish, handle(peer.establish))
	mux.Handle(operationPopulate, handle(peer.populate))
	mux.Handle(operationTrafficStart, handle(peer.trafficStart))
	mux.Handle(operationTrafficStop, handle(peer.trafficStop))
	mux.Handle(operationOverload, handle(peer.overload))
	mux.Handle(operationChurn, handle(peer.churn))
	mux.Handle(operationFinish, handle(peer.finishRun))
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
	case err = <-served:
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return peer.record, err
	}
	return peer.record, nil
}

func (peer *peerRun) event(format string, arguments ...any) {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	if len(peer.record.Events) < 1000 {
		peer.record.Events = append(peer.record.Events, fmt.Sprintf(format, arguments...))
	}
}

func (peer *peerRun) localAddress(port int) *sctp.SCTPAddr {
	return &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: peer.localIP}}, Port: port}
}

// establish dials the 32 stable associations. The SGP-role Dial returns once
// the ASP has activated every Application Server of the association.
func (peer *peerRun) establish(_ context.Context, _ struct{}) (establishResponse, error) {
	started := time.Now()
	stable := make([]*m3ua.Association, stableAssociations)
	for sgp := 0; sgp < sgpCount; sgp++ {
		for index := 0; index < stablePerSGP; index++ {
			port := stablePort(sgp, index)
			role, err := classifyPort(port)
			if err != nil {
				return establishResponse{}, err
			}
			association, err := peer.endpoints[sgp].Dial(peer.ctx, "m3ua", peer.localAddress(port), peer.remote, sgpAssociationConfig(role))
			if err != nil {
				return establishResponse{}, fmt.Errorf("dial stable association %d from port %d: %w", role.StableIndex, port, err)
			}
			stable[role.StableIndex] = association
			stableIndex := role.StableIndex
			drainChannels(association, func() {
				if peer.ctx.Err() == nil && !peer.finishing.Load() {
					peer.event("stable association %d ended: %v", stableIndex, association.Err())
				}
			})
			go peer.read(stableIndex, association)
		}
	}
	peer.mutex.Lock()
	peer.stable = stable
	peer.record.Stable = len(stable)
	peer.mutex.Unlock()
	return establishResponse{Stable: len(stable), Millis: time.Since(started).Milliseconds()}, nil
}

func (peer *peerRun) read(index int, association *m3ua.Association) {
	for {
		message, err := association.ReadData(peer.ctx)
		if err != nil {
			return
		}
		if message.ProtocolData != nil {
			peer.ledger.record(index, message.ProtocolData.Data)
		}
	}
}

func pointCodeRanges(pointCodes []uint32) []m3ua.PointCodeRange {
	ranges := make([]m3ua.PointCodeRange, len(pointCodes))
	for index, pointCode := range pointCodes {
		ranges[index] = m3ua.PointCodeRange{PointCode: pointCode}
	}
	return ranges
}

func reportScope(sgp, as int) m3ua.WireScope {
	return m3ua.WireScope{NetworkAppearance: networkAppearance, NetworkAppearanceSet: true,
		RoutingContexts: []uint32{routingContext(sgp, as)}, RoutingContextSet: true}
}

// populate reports every SGP's share of the reference store: one DAVA and one
// DUNA per SGP and Application Server, 512 messages carrying 16,384 records.
func (peer *peerRun) populate(_ context.Context, _ struct{}) (populateResponse, error) {
	response := populateResponse{}
	for sgp := 0; sgp < sgpCount; sgp++ {
		for as := 0; as < asPerGateway; as++ {
			available, unavailable := populationShare(sgp, as)
			for _, report := range []struct {
				pointCodes   []uint32
				availability m3ua.DestinationAvailability
			}{{available, m3ua.DestinationAvailable}, {unavailable, m3ua.DestinationUnavailable}} {
				err := peer.endpoints[sgp].ReportDestinationAvailability(m3ua.DestinationAvailabilityRequest{
					Scope: reportScope(sgp, as), Destinations: pointCodeRanges(report.pointCodes), Availability: report.availability})
				response.Messages++
				if err != nil && len(response.Errors) < 20 {
					response.Errors = append(response.Errors, fmt.Sprintf("SGP %d AS %d: %v", sgp, as, err))
				}
			}
		}
	}
	return response, nil
}

func (peer *peerRun) stableAssociations() []*m3ua.Association {
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	return peer.stable
}

func (peer *peerRun) trafficStart(_ context.Context, request trafficStartRequest) (struct{}, error) {
	stable := peer.stableAssociations()
	if len(stable) != stableAssociations || request.PerAssociation < 1 {
		return struct{}{}, errors.New("traffic start before establishment or without a rate")
	}
	peer.ledger.reset(request.Epoch)
	sender := startLedgerSender(peer.ctx, stable, false, request.Epoch, request.PerAssociation)
	peer.mutex.Lock()
	defer peer.mutex.Unlock()
	if peer.sender != nil {
		sender.stop()
		return struct{}{}, errors.New("traffic already running")
	}
	peer.sender = sender
	return struct{}{}, nil
}

// trafficStop ends the peer's epoch and judges the ASP's: it waits, bounded,
// for everything the ASP says it wrote before reading the ledger.
func (peer *peerRun) trafficStop(ctx context.Context, request trafficStopRequest) (trafficStopResponse, error) {
	peer.mutex.Lock()
	sender := peer.sender
	peer.sender = nil
	peer.mutex.Unlock()
	if sender == nil || sender.epoch != request.Epoch {
		return trafficStopResponse{}, fmt.Errorf("no running epoch %d", request.Epoch)
	}
	written := sender.stop()
	waitLedger(ctx, &peer.ledger, request.ASPSent, time.Duration(request.DrainWaitMillis)*time.Millisecond)
	result := peer.ledger.result("asp-to-sgp", request.ASPSent, request.ASPWriteErrors, request.ASPFirstError)
	result.Refused = request.ASPRefused
	peer.mutex.Lock()
	peer.record.Ledgers = append(peer.record.Ledgers, result)
	peer.mutex.Unlock()
	return trafficStopResponse{PeerSent: written.Sent, PeerWriteErrors: written.Errors, PeerFirstError: written.FirstError,
		PeerRefused: written.Refused, Ledger: result}, nil
}

// overload writes 4,096-byte DATA to every stable association as fast as the
// transport takes it, then toggles destination state twice per toggle so the
// paused subscriptions overflow. Toggled destinations end in their original
// state, so the store's record count does not change.
func (peer *peerRun) overload(_ context.Context, request overloadRequest) (overloadResponse, error) {
	stable := peer.stableAssociations()
	if len(stable) != stableAssociations || request.PerAssociation < 1 {
		return overloadResponse{}, errors.New("overload before establishment or without messages")
	}
	var sent, writeErrors, refused atomic.Uint64
	var firstError atomic.Pointer[string]
	var group sync.WaitGroup
	for index, association := range stable {
		group.Add(1)
		go func(index int, association *m3ua.Association) {
			defer group.Done()
			key, protocolData := dataTuple(index, false)
			buffer := make([]byte, overloadPayloadSize)
			for sequence := 0; sequence < request.PerAssociation; sequence++ {
				protocolData.Data = encodePayload(buffer, payloadHeader{Kind: kindOverload, Association: uint16(index), Sequence: uint64(sequence)})
				// The flood outruns the transport by design. A refused send is
				// offered again, so every association really receives
				// PerAssociation messages and its DATA queue fills; the ASP
				// discards the excess, which is what the phase measures.
				refusals, err := writeWithBackpressure(peer.ctx, 30*time.Second, func() error {
					_, err := association.WriteData(m3ua.DataRequest{AS: key, ProtocolData: protocolData})
					return err
				})
				refused.Add(uint64(refusals))
				if err != nil {
					writeErrors.Add(1)
					message := fmt.Sprintf("association %d message %d: %v", index, sequence, err)
					firstError.CompareAndSwap(nil, &message)
					continue
				}
				sent.Add(1)
			}
		}(index, association)
	}
	group.Wait()
	response := overloadResponse{Sent: sent.Load(), WriteErrors: writeErrors.Load(), Refused: refused.Load()}
	if first := firstError.Load(); first != nil {
		response.FirstWriteError = *first
	}
	for toggle := 0; toggle < request.Toggles; toggle++ {
		sgp := toggle % sgpCount
		as := (toggle / sgpCount) % asPerGateway
		available, _ := populationShare(sgp, as)
		destination := pointCodeRanges(available[:1])
		for _, availability := range []m3ua.DestinationAvailability{m3ua.DestinationUnavailable, m3ua.DestinationAvailable} {
			err := peer.endpoints[sgp].ReportDestinationAvailability(m3ua.DestinationAvailabilityRequest{
				Scope: reportScope(sgp, as), Destinations: destination, Availability: availability})
			response.Reports++
			if err != nil && len(response.ReportErrors) < 20 {
				response.ReportErrors = append(response.ReportErrors, err.Error())
			}
		}
	}
	return response, nil
}

func (peer *peerRun) churn(_ context.Context, request churnRequest) (churnResponse, error) {
	plan := request.Plan
	if plan.Cycles < 1 || plan.Group < 1 || !(plan.Rate > 0) || plan.FirstCycle < 0 || plan.FirstCycle+plan.Cycles > maxChurnCycles {
		return churnResponse{}, fmt.Errorf("invalid churn plan %+v", plan)
	}
	hold := time.Duration(request.HoldMillis) * time.Millisecond
	establishing := &gauge{}
	stats := runChurnBlock(peer.ctx, plan, systemClock{}, func(ctx context.Context, cycle int) cycleOutcome {
		return peer.cycle(ctx, cycle, hold, establishing)
	})
	stats.MaxEstablishing = establishing.maximum()
	peer.mutex.Lock()
	peer.record.Blocks = append(peer.record.Blocks, stats)
	peer.mutex.Unlock()
	return churnResponse{Stats: stats}, nil
}

// cycle is one complete establish/activate/close cycle from the SGP side.
func (peer *peerRun) cycle(ctx context.Context, cycle int, hold time.Duration, establishing *gauge) cycleOutcome {
	role, port, err := churnCycle(cycle)
	outcome := cycleOutcome{Mode: role.CloseMode.String()}
	if err != nil {
		outcome.Reason, outcome.Err = "plan", err
		return outcome
	}
	started := time.Now()
	establishing.inc()
	association, err := peer.endpoints[role.SGP].Dial(peer.ctx, "m3ua", peer.localAddress(port), peer.remote, sgpAssociationConfig(role))
	establishing.dec()
	if err != nil {
		outcome.Reason, outcome.Err = "establish", fmt.Errorf("cycle %d port %d: %w", cycle, port, err)
		return outcome
	}
	outcome.Establish = time.Since(started)
	drained := drainChannels(association, nil)
	if role.CloseMode == closePeer {
		select {
		case <-time.After(hold):
		case <-ctx.Done():
		}
		if err := association.Close(); err != nil {
			outcome.Reason, outcome.Err = "peer-close", fmt.Errorf("cycle %d: %w", cycle, err)
		}
	} else {
		select {
		case <-association.Done():
		case <-time.After(hold + 20*time.Second):
			_ = association.Close()
			outcome.Reason, outcome.Err = "asp-close-not-observed", fmt.Errorf("cycle %d: the ASP did not end the association", cycle)
		}
	}
	select {
	case <-drained:
	case <-time.After(10 * time.Second):
		if outcome.Err == nil {
			outcome.Reason, outcome.Err = "channels-open", fmt.Errorf("cycle %d: indication channels still open", cycle)
		}
	}
	return outcome
}

func (peer *peerRun) finishRun(_ context.Context, _ struct{}) (peerRecord, error) {
	peer.finishing.Store(true)
	peer.mutex.Lock()
	record := peer.record
	record.Blocks = append([]churnStats(nil), peer.record.Blocks...)
	record.Ledgers = append([]ledgerResult(nil), peer.record.Ledgers...)
	record.Events = append([]string(nil), peer.record.Events...)
	peer.mutex.Unlock()
	// Answer first, then end the run.
	go func() {
		time.Sleep(200 * time.Millisecond)
		peer.finish()
	}()
	return record, nil
}
