package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func wireMethodConfigForTest(role Role, international, national uint32, max uint8) EndpointConfig {
	return EndpointConfig{Role: role, TransportCongestion: &TransportCongestionConfig{Disabled: true}, Congestion: &CongestionConfig{
		Default: CongestionProfile{Method: CongestionInternational}, Networks: []NetworkCongestionProfile{
			{NetworkAppearance: international, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionInternational}},
			{NetworkAppearance: national, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: max}},
		}}}
}

func newMethodWireEndpoint(t *testing.T, role Role, international, national uint32, max uint8) *Endpoint {
	t.Helper()
	e, err := NewEndpoint(wireMethodConfigForTest(role, international, national, max))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func injectMethodPeer(t *testing.T, c *Association, message messages.M3UA, stream uint16) {
	t.Helper()
	raw, err := message.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	info := *c.sctpInfo
	info.Stream = stream
	if _, err := c.sctpConn.SendMsg(raw, sctp.SendOptions{Info: &info}); err != nil {
		t.Fatal(err)
	}
}

func methodWireSCON(na, rc, pc uint32, level *params.Param) *messages.SignallingCongestion {
	return messages.NewSignallingCongestion(params.NewNetworkAppearance(na), params.NewRoutingContext(rc), params.NewAffectedPointCode(pc), nil, level, nil)
}

func TestMethodProfileTwoNetworksOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	aspEndpoint := newMethodWireEndpoint(t, RoleASP, 7, 9, 2)
	sgpEndpoint := newMethodWireEndpoint(t, RoleSGP, 7, 9, 2)
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 7, []uint32{1, 2})
	config.ApplicationServers[1].ASKey.NetworkAppearance = 9
	listener, err := sgpEndpoint.Listen("m3ua", mcAddr(3541, "127.0.0.2"), NewListenerConfig(config))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan associationResult, 1)
	go func() { c, err := listener.Accept(ctx); accepted <- associationResult{association: c, err: err} }()
	aspConfig := snapshotAssociationConfig(config)
	aspConfig.ASPIdentifier = params.NewAspIdentifier(1)
	aspConfig.DataQueueSize = 1
	asp, err := aspEndpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr), aspConfig)
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	sgp := result.association
	observeSSNM(t, asp)
	observeSSNM(t, sgp)
	management := asp.ManagementIndications()
	// Positive controls for both networks and optional level presence.
	for _, request := range []SignallingCongestionRequest{
		{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 0x601001}}},
		{Scope: testWireScope(9, true, 2), Destinations: []PointCodeRange{{PointCode: 0x602001}}, CongestionLevel: 2, CongestionLevelSet: true},
	} {
		if err := asp.SignallingCongestion(request); err != nil {
			t.Fatal(err)
		}
		_ = nextMethodPeerReport(t, sgp)
	}
	// A valid International DATA is a positive MP-zero control on this same association.
	internationalData := transferProtocolData(0x601002, 1, []byte("international-zero"))
	if _, err := asp.WriteData(DataRequest{AS: *staticASKey(7, 1), ProtocolData: *internationalData}); err != nil {
		t.Fatal(err)
	}
	if _, err := sgp.ReadData(ctx); err != nil {
		t.Fatal(err)
	}
	// Distinct markers make invalid typed/raw sends observable independently.
	if err := asp.SignallingCongestion(SignallingCongestionRequest{Scope: testWireScope(9, true, 2), Destinations: []PointCodeRange{{PointCode: 0x603301}}, CongestionLevel: 3, CongestionLevelSet: true}); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("invalid typed SCON=%v", err)
	}
	generic := messages.New(1, messages.MsgClassSSNM, messages.MsgTypeSignallingCongestion, params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(0x603302), params.NewCongestionIndications(0))
	if n, err := asp.WriteSignal(generic); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("invalid serialized SCON n=%d err=%v", n, err)
	}
	payload := transferProtocolData(0x604401, 1, []byte("invalid-typed-MP"))
	payload.MessagePriority = 1
	if n, err := asp.WriteData(DataRequest{AS: *staticASKey(7, 1), ProtocolData: *payload}); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("invalid typed DATA n=%d err=%v", n, err)
	}
	payload = transferProtocolData(0x604402, 1, []byte("invalid-raw-MP"))
	payload.MessagePriority = 3
	rawData := messages.NewData(params.NewNetworkAppearance(9), params.NewRoutingContext(2), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data), nil)
	if n, err := asp.WriteSignal(rawData); n != 0 || !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("invalid serialized DATA n=%d err=%v", n, err)
	}
	for _, message := range []*messages.SignallingCongestion{methodWireSCON(7, 1, 0x601100, params.NewCongestionIndications(3)), methodWireSCON(9, 2, 0x602200, params.NewCongestionIndications(3))} {
		injectMethodPeer(t, sgp, message, 0)
		report := nextMethodPeerReport(t, asp)
		if report.CongestionLevel != 3 || !report.CongestionLevelSet {
			t.Fatal("mismatched SCON level changed")
		}
		if len(report.ProfileMismatches) != 1 {
			t.Error("missing SCON diagnostic")
		}
	}
	// A below-level local-user offer is refused; equality at capped MaxLevel passes.
	payload = transferProtocolData(0x602200, 1, []byte("priority-equality"))
	payload.MessagePriority = 1
	if n, err := asp.WriteData(DataRequest{AS: *staticASKey(9, 2), ProtocolData: *payload}); n != 0 || err == nil {
		t.Errorf("below-level offer n=%d err=%v", n, err)
	}
	payload.MessagePriority = 2
	if _, err := asp.WriteData(DataRequest{AS: *staticASKey(9, 2), ProtocolData: *payload}); err != nil {
		t.Fatal(err)
	}
	// Drain any baseline-invalid sends before verifying the positive marker.
	for {
		msg, err := sgp.ReadData(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if msg.ProtocolData.DestinationPointCode == 0x602200 && msg.ProtocolData.MessagePriority == 2 {
			break
		}
	}
	// Unrestricted peer DATA keeps the original disallowed MP.
	payload = transferProtocolData(0x605500, 1, []byte("method-mismatch"))
	payload.MessagePriority = 255
	injectMethodPeer(t, sgp, messages.NewData(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data), nil), 1)
	received, err := asp.ReadData(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if received.ProtocolData.MessagePriority != 255 || string(received.ProtocolData.Data) != "method-mismatch" {
		t.Fatal("mismatched DATA was changed")
	}
	// SGP transit carries the original representable MP even when its method
	// does not allow it. Read each arrival before sending the next so this
	// admission proof cannot trigger the separate one-message overflow queue.
	nifManagement := sgp.ManagementIndications()
	for _, raw := range []bool{false, true} {
		pc := uint32(0x609901)
		if raw {
			pc = 0x609902
		}
		payload = transferProtocolData(pc, 1, []byte("nif-profile-mismatch"))
		payload.MessagePriority = 1
		request := DataRequest{AS: *staticASKey(7, 1), ProtocolData: *payload}
		var sendErr error
		if raw {
			message, parseErr := messages.Parse(sgp.encodeDataFrame(&request))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			_, sendErr = sgp.WriteSignal(message)
		} else {
			_, sendErr = sgp.WriteData(request)
		}
		if sendErr != nil {
			t.Errorf("representable SGP transit raw=%v: %v", raw, sendErr)
			continue
		}
		delivered, readErr := asp.ReadData(ctx)
		if readErr != nil || delivered.ProtocolData.DestinationPointCode != pc || delivered.ProtocolData.MessagePriority != 1 || string(delivered.ProtocolData.Data) != "nif-profile-mismatch" {
			t.Fatalf("changed NIF transit: %+v, %v", delivered, readErr)
		}
	}
	if diagnostic := sgp.CongestionProfileDiagnostics(); diagnostic.ReceivedDataMismatches != 2 || diagnostic.LastDataMismatch == nil || diagnostic.LastDataMismatch.MessagePriority != 1 {
		t.Errorf("SGP transit diagnostic: %+v", diagnostic)
	}
	indications := 0
	for len(nifManagement) > 0 {
		if (<-nifManagement).Kind == ManagementCongestionProfileMismatch {
			indications++
		}
	}
	if indications != 1 {
		t.Errorf("SGP transit mismatch indications=%d", indications)
	}
	// Unsupported upper MP bits are refused by both actual SGP send APIs.
	for _, raw := range []bool{false, true} {
		pc := uint32(0x609903)
		if raw {
			pc = 0x609904
		}
		payload = transferProtocolData(pc, 1, []byte("unsupported-nif-priority"))
		payload.MessagePriority = 4
		request := DataRequest{AS: *staticASKey(7, 1), ProtocolData: *payload}
		var sendErr error
		if raw {
			message, parseErr := messages.Parse(sgp.encodeDataFrame(&request))
			if parseErr != nil {
				t.Fatal(parseErr)
			}
			_, sendErr = sgp.WriteSignal(message)
		} else {
			_, sendErr = sgp.WriteData(request)
		}
		if !errors.Is(sendErr, ErrInvalidParameterValue) {
			t.Errorf("unsupported SGP priority raw=%v: %v", raw, sendErr)
		}
	}

	// A synthetic backlog exercises the real detector and projection without
	// depending on kernel pressure. This separate local input originates no SCON.
	now := time.Unix(1000, 0)
	monitor := &transportCongestionMonitor{
		config: sgp.endpoint.transport.config,
		read: func(*Association) (*sctp.SendBacklog, error) {
			return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil
		},
		now: func() time.Time { return now },
	}
	job := transportCongestionJob{sgp, sgp.ID(), sgp.Epoch()}
	monitor.sample(sgp.endpoint, job)
	for range 3 {
		now = now.Add(100 * time.Millisecond)
		monitor.sample(sgp.endpoint, job)
	}
	if state := sgp.TransportCongestion(); !state.Known || state.Level != 3 || state.Samples != 4 {
		t.Fatalf("synthetic backlog onset=%+v", state)
	}
	// SGP transit ignores the local-user priority discard rule. Two arrivals
	// overflow the one-message ASP queue and exercise actual onset and drain.
	payload = transferProtocolData(0x607700, 1, []byte("queue"))
	for range 2 {
		if _, err := sgp.WriteData(DataRequest{AS: *staticASKey(9, 2), ProtocolData: *payload}); err != nil {
			t.Fatal(err)
		}
	}
	// Baseline invalid SCONs can precede the actual queue report.
	for {
		report := nextMethodPeerReport(t, sgp)
		if len(report.Destinations) > 0 && report.Destinations[0].PointCode == 0x607700 {
			if !report.CongestionLevelSet || report.CongestionLevel != 2 {
				t.Error("national queue onset format")
			}
			break
		}
	}
	if _, err := asp.ReadData(ctx); err != nil {
		t.Fatal(err)
	}
	report := nextMethodPeerReport(t, sgp)
	if !report.CongestionLevelSet || report.CongestionLevel != 0 || report.ConcernedDestinationSet {
		t.Errorf("national clear=%+v", report)
	}
	timer := time.NewTimer(150 * time.Millisecond)
	defer timer.Stop()
	for {
		select {
		case ind := <-management:
			if ind != nil && ind.Kind == ManagementError {
				t.Fatalf("profile mismatch generated Error: %+v", ind)
			}
		case <-timer.C:
			return
		}
	}
}

func TestMethodProfileIPSPDirectionsOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	aEndpoint := newMethodWireEndpoint(t, RoleIPSP, 10, 20, 1)
	bEndpoint := newMethodWireEndpoint(t, RoleIPSP, 10, 20, 1)
	cfgA := integrationIPSPDoubleExchangeConfig(IPSPASPSMExchangeDouble, true, ipspIntegrationTraffic(10, 11), ipspIntegrationTraffic(20, 22))
	cfgB := integrationIPSPDoubleExchangeConfig(IPSPASPSMExchangeDouble, true, ipspIntegrationTraffic(20, 22), ipspIntegrationTraffic(10, 11))
	listener, err := bEndpoint.Listen("m3ua", mcAddr(3542, "127.0.0.2"), NewListenerConfig(cfgB))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan associationResult, 1)
	go func() { c, e := listener.Accept(ctx); accepted <- associationResult{association: c, err: e} }()
	a, err := aEndpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr), cfgA)
	if err != nil {
		t.Fatal(err)
	}
	result := <-accepted
	if result.err != nil {
		t.Fatal(result.err)
	}
	b := result.association
	waitForIPSPState(t, a, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	waitForIPSPState(t, b, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	observeSSNM(t, a)
	observeSSNM(t, b)
	// SCON uses the sender's local-directed profile; DATA the peer-directed one.
	if err := a.SignallingCongestion(SignallingCongestionRequest{Scope: testWireScope(10, true, 11), Destinations: []PointCodeRange{{PointCode: 0x608801}}, CongestionLevel: 1, CongestionLevelSet: true}); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("IPSP local International SCON=%v", err)
	}
	if err := b.SignallingCongestion(SignallingCongestionRequest{Scope: testWireScope(20, true, 22), Destinations: []PointCodeRange{{PointCode: 0x608802}}, CongestionLevel: 1, CongestionLevelSet: true}); err != nil {
		t.Fatal(err)
	}
	_ = nextMethodPeerReport(t, a)
	payload := transferProtocolData(0x608803, 1, []byte("IPSP-national"))
	payload.MessagePriority = 1
	if _, err := a.WriteData(DataRequest{AS: *staticASKey(20, 22), ProtocolData: *payload}); err != nil {
		t.Fatal(err)
	}
	msg, err := b.ReadData(ctx)
	if err != nil || msg.ProtocolData.MessagePriority != 1 {
		t.Fatalf("IPSP national DATA=%+v err=%v", msg, err)
	}
	payload = transferProtocolData(0x608804, 1, []byte("IPSP-international"))
	if _, err := b.WriteData(DataRequest{AS: *staticASKey(10, 11), ProtocolData: *payload}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ReadData(ctx); err != nil {
		t.Fatal(err)
	}
}

func nextMethodPeerReport(t *testing.T, c *Association) SSNMReport {
	t.Helper()
	for {
		report := nextSSNMReport(t, c)
		if report.Source == SSNMPeerReport {
			return report
		}
	}
}
