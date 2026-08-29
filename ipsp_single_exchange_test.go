package m3ua

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestIPSPAssociationRequiresAnExplicitExchangeModel(t *testing.T) {
	tests := []struct {
		name    string
		role    Role
		ipsp    *IPSPConfig
		wantErr error
	}{
		{
			name:    "IPSP without model",
			role:    RoleIPSP,
			wantErr: ErrInvalidRoleConfiguration,
		},
		{
			name: "IPSP Single Exchange",
			role: RoleIPSP,
			ipsp: &IPSPConfig{ExchangeModel: IPSPExchangeSingle},
		},
		{
			name:    "IPSP Double Exchange not implemented by this change",
			role:    RoleIPSP,
			ipsp:    &IPSPConfig{ExchangeModel: IPSPExchangeDouble},
			wantErr: ErrUnsupportedIPSPExchangeModel,
		},
		{
			name:    "IPSP unknown exchange model",
			role:    RoleIPSP,
			ipsp:    &IPSPConfig{ExchangeModel: IPSPExchangeModel(255)},
			wantErr: ErrUnsupportedIPSPExchangeModel,
		},
		{
			name:    "ASP cannot carry IPSP policy",
			role:    RoleASP,
			ipsp:    &IPSPConfig{ExchangeModel: IPSPExchangeSingle},
			wantErr: ErrInvalidRoleConfiguration,
		},
		{
			name:    "SGP cannot carry IPSP policy",
			role:    RoleSGP,
			ipsp:    &IPSPConfig{ExchangeModel: IPSPExchangeSingle},
			wantErr: ErrInvalidRoleConfiguration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			config.IPSP = test.ipsp

			err := validateAssociationConfigForRole(test.role, config)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("validateAssociationConfigForRole() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestIPSPAssociationRejectsForeignRolePolicy(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*AssociationConfig)
	}{
		{
			name: "SGP ASP authorization",
			configure: func(config *AssociationConfig) {
				config.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }
			},
		},
		{
			name: "ASP peer SGP identity",
			configure: func(config *AssociationConfig) {
				config.PeerSGP = &SGPIdentity{SignallingGateway: "sg", SignallingGatewayProcess: "sgp"}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
			test.configure(config)

			if err := validateAssociationConfigForRole(RoleIPSP, config); !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("validateAssociationConfigForRole() error = %v, want ErrInvalidRoleConfiguration", err)
			}
		})
	}
}

func TestIPSPSingleExchangeConfigIsDeepSnapshotted(t *testing.T) {
	original := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	original.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeSingle,
		InitiateASPSM: true,
		InitiateASPTM: true,
	}

	snapshot := snapshotAssociationConfig(original)
	original.IPSP.ExchangeModel = IPSPExchangeDouble
	original.IPSP.InitiateASPSM = false
	original.IPSP.InitiateASPTM = false

	if snapshot.IPSP == original.IPSP {
		t.Fatal("snapshot shares its IPSPConfig pointer with the caller")
	}
	if got := *snapshot.IPSP; got != (IPSPConfig{
		ExchangeModel: IPSPExchangeSingle,
		InitiateASPSM: true,
		InitiateASPTM: true,
	}) {
		t.Fatalf("snapshot IPSPConfig = %+v", got)
	}
}

func TestIPSPSingleExchangeSimultaneousASPSMAndASPTM(t *testing.T) {
	first, firstSent := newSingleExchangeIPSPForTest(t, StateASPDown)
	second, secondSent := newSingleExchangeIPSPForTest(t, StateASPDown)
	first.cfg.IPSP.InitiateASPSM, first.cfg.IPSP.InitiateASPTM = true, true
	second.cfg.IPSP.InitiateASPSM, second.cfg.IPSP.InitiateASPTM = true, true

	if err := first.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("first enter ASP-DOWN: %v", err)
	}
	if err := second.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("second enter ASP-DOWN: %v", err)
	}
	firstUp := (*firstSent)[0].(*messages.AspUp)
	secondUp := (*secondSent)[0].(*messages.AspUp)
	if err := first.handleAspUp(secondUp); err != nil {
		t.Fatalf("first receive simultaneous ASP Up: %v", err)
	}
	if err := second.handleAspUp(firstUp); err != nil {
		t.Fatalf("second receive simultaneous ASP Up: %v", err)
	}
	if err := first.handleAspUpAck((*secondSent)[1].(*messages.AspUpAck)); err != nil {
		t.Fatalf("first receive ASP Up Ack: %v", err)
	}
	if err := second.handleAspUpAck((*firstSent)[1].(*messages.AspUpAck)); err != nil {
		t.Fatalf("second receive ASP Up Ack: %v", err)
	}

	if err := first.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("first enter ASP-INACTIVE: %v", err)
	}
	if err := second.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("second enter ASP-INACTIVE: %v", err)
	}
	firstActive := (*firstSent)[2].(*messages.AspActive)
	secondActive := (*secondSent)[2].(*messages.AspActive)
	if err := first.handleAspActive(secondActive); err != nil {
		t.Fatalf("first receive simultaneous ASP Active: %v", err)
	}
	if err := second.handleAspActive(firstActive); err != nil {
		t.Fatalf("second receive simultaneous ASP Active: %v", err)
	}
	if err := first.handleAspActiveAck((*secondSent)[3].(*messages.AspActiveAck)); err != nil {
		t.Fatalf("first receive ASP Active Ack: %v", err)
	}
	if err := second.handleAspActiveAck((*firstSent)[3].(*messages.AspActiveAck)); err != nil {
		t.Fatalf("second receive ASP Active Ack: %v", err)
	}
	if err := first.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("first enter ASP-ACTIVE: %v", err)
	}
	if err := second.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("second enter ASP-ACTIVE: %v", err)
	}

	if first.pendingTAck() != 0 || second.pendingTAck() != 0 {
		t.Fatalf("pending T(ack): first=%d second=%d", first.pendingTAck(), second.pendingTAck())
	}
	if first.State() != StateASPActive || second.State() != StateASPActive {
		t.Fatalf("states after simultaneous exchange: first=%s second=%s", first.State(), second.State())
	}
}

func TestIPSPEndpointSharesApplicationServerStateAcrossAssociations(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint(): %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	listener := newListener(endpoint, NewListenerConfig(config))

	incumbent, incumbentSent := newSingleExchangeIPSPForTest(t, StateASPInactive)
	challenger, _ := newSingleExchangeIPSPForTest(t, StateASPInactive)
	for _, association := range []*Association{incumbent, challenger} {
		association.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		if !listener.promoteAcceptedAssociation(association) {
			t.Fatal("promoteAcceptedAssociation() = false")
		}
	}
	if incumbent.as == nil || incumbent.as != challenger.as {
		t.Fatal("IPSP associations do not share their Endpoint Application Server registry")
	}

	incumbent.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	))
	if got := incumbent.State(); got != StateASPActive {
		t.Fatalf("incumbent state = %v, want ASP-ACTIVE", got)
	}
	before := len(notifies(*incumbentSent))

	challenger.handleSignals(context.Background(), messages.NewAspUp(params.NewAspIdentifier(77), nil))
	challenger.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	))

	if got := incumbent.State(); got != StateASPInactive {
		t.Fatalf("overridden IPSP state = %v, want ASP-INACTIVE", got)
	}
	got := notifies(*incumbentSent)
	if len(got) <= before {
		t.Fatal("overridden IPSP received no Alternate ASP Active Notify")
	}
	last := got[len(got)-1]
	if _, status := statusOf(t, last); status != uint16(params.AlternateAspActive&0xffff) {
		t.Fatalf("Notify Status Information = %d, want Alternate ASP Active", status)
	}
	if last.AspIdentifier == nil || last.AspIdentifier.AspIdentifier() != 77 {
		t.Fatalf("Notify ASP Identifier = %v, want overriding IPSP identifier 77", last.AspIdentifier)
	}
}

func TestIPSPSingleExchangeActiveAckAppliesSharedASOverride(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, incumbentSent := newSingleExchangeIPSPForTest(t, StateASPActive)
	challenger, _ := newSingleExchangeIPSPForTest(t, StateASPInactive)
	for _, association := range []*Association{incumbent, challenger} {
		association.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		association.as = registry
	}
	incumbent.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(incumbent, StateASPActive)
	registry.aspStateChanged(challenger, StateASPInactive)

	if err := challenger.initiateASPActive(params.NewRoutingContext(1)); err != nil {
		t.Fatalf("initiateASPActive(): %v", err)
	}
	if err := challenger.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("handleAspActiveAck(): %v", err)
	}
	if err := challenger.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("challenger enter ASP-ACTIVE: %v", err)
	}

	if got := registry.get(1).activeASPs(); len(got) != 1 || got[0] != challenger {
		t.Fatalf("Override active IPSPs = %v, want only challenger", got)
	}
	if got := incumbent.State(); got != StateASPInactive {
		t.Fatalf("incumbent state = %v, want ASP-INACTIVE", got)
	}
	var alternate bool
	for _, notify := range notifies(*incumbentSent) {
		_, information := statusOf(t, notify)
		if information == uint16(params.AlternateAspActive&0xffff) {
			alternate = true
			break
		}
	}
	if !alternate {
		t.Fatal("incumbent received no Alternate ASP Active Notify")
	}
}

func TestIPSPSingleExchangeOverrideDrainsDisplacedDirectTrafficBeforeNotify(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	challenger, _ := newSingleExchangeIPSPForTest(t, StateASPInactive)
	for _, association := range []*Association{incumbent, challenger} {
		association.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		association.as = registry
	}
	incumbent.noteRoutingContextsActive([]uint32{1})
	registry.aspStateChanged(incumbent, StateASPActive)
	registry.aspStateChanged(challenger, StateASPInactive)

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	notifySeen := make(chan struct{})
	incumbent.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.Data:
			close(dataStarted)
			<-releaseData
		case *messages.Notify:
			close(notifySeen)
		}
		return message.MarshalLen(), nil
	}
	activeAckSeen := make(chan struct{})
	challenger.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspActiveAck); ok {
			close(activeAckSeen)
		}
		return message.MarshalLen(), nil
	}

	data := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte{0x01},
	), nil)
	dataDone := make(chan error, 1)
	go func() {
		_, err := incumbent.WriteSignal(data)
		dataDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("incumbent DATA did not enter its direct traffic write")
	}

	activationDone := make(chan error, 1)
	go func() {
		activationDone <- challenger.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeOverride),
			params.NewRoutingContext(1), nil,
		))
	}()
	select {
	case <-activeAckSeen:
	case <-time.After(time.Second):
		t.Fatal("challenger sent no ASP Active Ack")
	}
	select {
	case err := <-activationDone:
		t.Fatalf("Override completed before admitted DATA drained: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-notifySeen:
		t.Fatal("Alternate ASP Active Notify overtook admitted DATA")
	default:
	}

	close(releaseData)
	select {
	case err := <-dataDone:
		if err != nil {
			t.Fatalf("incumbent DATA write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("incumbent DATA did not finish")
	}
	select {
	case err := <-activationDone:
		if err != nil {
			t.Fatalf("handleAspActive(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Override did not complete after admitted DATA drained")
	}
	select {
	case <-notifySeen:
	case <-time.After(time.Second):
		t.Fatal("incumbent received no Alternate ASP Active Notify")
	}
	if _, err := incumbent.WriteSignal(data); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA after Override error = %v, want ErrNotEstablished", err)
	}
}

func TestIPSPASPUpAckWaitsForActiveTrafficToQuiesce(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.maxMessageStreamID = 4
	association.recvStream.Store(0)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(0)

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	ackSeen := make(chan struct{})
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.Data:
			close(dataStarted)
			<-releaseData
		case *messages.AspUpAck:
			close(ackSeen)
		}
		return message.MarshalLen(), nil
	}
	t.Cleanup(func() {
		select {
		case <-releaseData:
		default:
			close(releaseData)
		}
	})

	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteSignal(distributionData(1, 1, "in flight"))
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case err := <-writeDone:
		t.Fatalf("outbound DATA returned before entering the traffic path: %v", err)
	case <-time.After(time.Second):
		t.Fatal("outbound DATA did not enter the IPSP traffic path")
	}

	handleDone := make(chan struct{})
	go func() {
		association.handleSignals(context.Background(), messages.NewAspUp(nil, nil))
		close(handleDone)
	}()

	select {
	case <-ackSeen:
		t.Fatal("ASP Up Ack overtook outbound DATA admitted while the peer was ASP-ACTIVE")
	case <-time.After(30 * time.Millisecond):
	}

	deadline := time.Now().Add(time.Second)
	for association.State() != StateASPInactive && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := association.State(); got != StateASPInactive {
		t.Fatalf("state while quiescing = %v, want ASP-INACTIVE", got)
	}
	if _, err := association.WriteSignal(distributionData(1, 1, "too late")); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA admitted after ASP Up committed ASP-INACTIVE: %v", err)
	}

	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("in-flight DATA: %v", err)
	}
	select {
	case <-ackSeen:
	case <-time.After(time.Second):
		t.Fatal("ASP Up Ack was not sent after outbound DATA quiesced")
	}
	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("ASP Up handler did not finish after outbound DATA quiesced")
	}
}

func TestIPSPSingleExchangeReceivedASPUpAckQuiescesActiveTraffic(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.maxMessageStreamID = 4
	association.recvStream.Store(0)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(0)
	association.noteRoutingContextsActive([]uint32{1})
	registry := newApplicationServers(time.Hour)
	t.Cleanup(registry.close)
	association.as = registry
	applicationServer := registry.get(associationConfigASKey(association.cfg, 1))
	applicationServer.setASPState(association, StateASPActive, time.Hour)
	association.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	t.Cleanup(association.stopAllTAck)

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			close(dataStarted)
			<-releaseData
		}
		return message.MarshalLen(), nil
	}
	t.Cleanup(func() {
		select {
		case <-releaseData:
		default:
			close(releaseData)
		}
	})

	data := distributionData(1, 1, "in flight")
	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteSignal(data)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case err := <-writeDone:
		t.Fatalf("outbound DATA returned before entering the traffic path: %v", err)
	case <-time.After(time.Second):
		t.Fatal("outbound DATA did not enter the IPSP traffic path")
	}

	handleDone := make(chan struct{})
	go func() {
		association.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
		close(handleDone)
	}()

	deadline := time.Now().Add(time.Second)
	for association.State() != StateASPInactive && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := association.State(); got != StateASPInactive {
		t.Fatalf("state while applying ASP Up Ack = %v, want ASP-INACTIVE", got)
	}
	if active := applicationServer.activeASPs(); len(active) != 0 {
		t.Fatalf("Application Server retained %d active IPSPs while applying ASP Up Ack", len(active))
	}
	if association.activeForRoutingContext(1) {
		t.Fatal("Routing Context 1 remained active while applying ASP Up Ack")
	}
	select {
	case <-handleDone:
		t.Fatal("ASP Up Ack handler returned before admitted DATA drained")
	case <-time.After(30 * time.Millisecond):
	}
	if _, err := association.WriteSignal(data); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA admitted after ASP Up Ack committed ASP-INACTIVE: %v", err)
	}

	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("in-flight DATA: %v", err)
	}
	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("ASP Up Ack handler did not finish after DATA drained")
	}
}

func TestIPSPSingleExchangeReorderedAcknowledgementsChangePeerState(t *testing.T) {
	tests := []struct {
		name    string
		state   State
		message messages.M3UA
		want    State
	}{
		{"ASP Up Ack", StateASPDown, messages.NewAspUpAck(nil, nil), StateASPInactive},
		{"ASP Active Ack", StateASPInactive, messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil), StateASPActive},
		{"ASP Inactive Ack", StateASPActive, messages.NewAspInactiveAck(params.NewRoutingContext(1), nil), StateASPInactive},
		{"ASP Down Ack", StateASPActive, messages.NewAspDownAck(nil), StateASPDown},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association, _ := newSingleExchangeIPSPForTest(t, test.state)
			if test.state == StateASPActive {
				association.noteRoutingContextsActive([]uint32{1})
			}
			association.handleSignals(context.Background(), test.message)
			select {
			case got := <-association.stateChan:
				if got != test.want {
					t.Fatalf("published state = %s, want %s", got, test.want)
				}
			default:
				t.Fatal("no state published")
			}
		})
	}
}

func TestIPSPSingleExchangeDuplicateASPActiveIsAcknowledged(t *testing.T) {
	association, sent := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	active := messages.NewAspActive(nil, params.NewRoutingContext(1), nil)

	for duplicate := 0; duplicate < 2; duplicate++ {
		if err := association.handleAspActive(active); err != nil {
			t.Fatalf("handleAspActive duplicate %d: %v", duplicate, err)
		}
	}
	if got := typeNames(*sent); len(got) != 2 || got[0] != "ASP Active Ack" || got[1] != "ASP Active Ack" {
		t.Fatalf("sent %v, want two ASP Active Acks", got)
	}
}

func TestIPSPSingleExchangeTAckTimeoutIsBounded(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPDown)
	association.cfg.IPSP.InitiateASPSM = true
	association.cfg.TAck = 5 * time.Millisecond
	association.cfg.TAckRetries = 1
	writes := make(chan messages.M3UA, 4)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	if err := association.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("enter ASP-DOWN: %v", err)
	}
	select {
	case err := <-association.errChan:
		if !errors.Is(err, ErrTAckExpired) {
			t.Fatalf("T(ack) error = %v, want ErrTAckExpired", err)
		}
	case <-time.After(time.Second):
		t.Fatal("T(ack) did not report its bounded timeout")
	}
	if got := len(writes); got != 2 {
		t.Fatalf("ASP Up writes = %d, want initial request plus one retry", got)
	}
	if association.pendingTAck() != 0 {
		t.Fatalf("pending T(ack) after timeout = %d, want 0", association.pendingTAck())
	}
}

func TestIPSPSingleExchangeRestartRequiresFreshASPSM(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.resetTAckEpoch()

	activeAck := messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil)
	var unexpected *UnexpectedMessageError
	if err := association.handleAspActiveAck(activeAck); !errors.As(err, &unexpected) {
		t.Fatalf("ASP Active Ack before fresh ASPSM error = %v, want UnexpectedMessageError", err)
	}

	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatalf("fresh peer ASP Up: %v", err)
	}
	if err := association.handleAspActiveAck(activeAck); err != nil {
		t.Fatalf("ASP Active Ack after fresh ASPSM: %v", err)
	}
}

func TestIPSPSingleExchangeRestartWhileDownReinitiatesConfiguredASPSM(t *testing.T) {
	association, sent := newSingleExchangeIPSPForTest(t, StateASPDown)
	association.cfg.IPSP.InitiateASPSM = true
	association.stateEntered = true
	association.appliedState = StateASPDown

	association.handleSCTPRestart()

	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up" {
		t.Fatalf("sent %v, want [ASP Up]", got)
	}
}

func TestIPSPSingleExchangeShutdownUsesASPTMThenASPSM(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.cfg.TAck = time.Second
	association.cfg.TAckRetries = 2

	writes := make(chan messages.M3UA, 4)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- association.Shutdown() }()

	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("IPSP shutdown did not begin with ASP Inactive")
	}
	association.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(1), nil,
	))
	if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
		t.Fatal("IPSP shutdown did not continue with ASP Down")
	}
	association.handleSignals(context.Background(), messages.NewAspDownAck(nil))

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown(): %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("IPSP Shutdown did not finish after ASP Down Ack")
	}
}

func TestIPSPSingleExchangeInactiveAckPublishesPeerInactive(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	if err := association.initiateASPInactive(params.NewRoutingContext(1)); err != nil {
		t.Fatalf("initiateASPInactive(): %v", err)
	}

	association.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(1), nil,
	))

	select {
	case state := <-association.stateChan:
		if state != StateASPInactive {
			t.Fatalf("published state = %s, want ASP-INACTIVE", state)
		}
	default:
		t.Fatal("no state was published for ASP Inactive Ack")
	}
}

func TestIPSPSingleExchangeRejectsDataOutsideActiveRoutingContext(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.cfg.RoutingContexts = params.NewRoutingContext(1, 2)
	association.noteRoutingContextsActive([]uint32{1})
	association.recvStream.Store(1)
	data := messages.NewData(nil, params.NewRoutingContext(2), params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte{0x01},
	), nil)

	association.handleData(context.Background(), data)

	select {
	case delivered := <-association.dataChan:
		t.Fatalf("delivered inactive Routing Context DATA: %+v", delivered)
	default:
	}
	select {
	case err := <-association.errChan:
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("DATA error = %v, want UnexpectedMessageError", err)
		}
	default:
		t.Fatal("inactive Routing Context DATA produced no protocol error")
	}
}

func TestIPSPSingleExchangeAcceptsNotify(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	notify := messages.NewNotify(
		params.NewStatus(params.AsStateActive),
		nil, params.NewRoutingContext(1), nil,
	)

	if err := association.handleNotify(notify); err != nil {
		t.Fatalf("handleNotify(): %v", err)
	}
	select {
	case indication := <-association.mgmtChan:
		if indication.Kind != ManagementNotify {
			t.Fatalf("management indication kind = %v, want ManagementNotify", indication.Kind)
		}
	default:
		t.Fatal("Notify produced no management indication")
	}
}

func TestIPSPSingleExchangeHandlesHeartbeatAndError(t *testing.T) {
	association, sent := newSingleExchangeIPSPForTest(t, StateASPActive)
	heartbeatData := []byte("ipsp-beat")
	if err := association.handleHeartbeat(messages.NewHeartbeat(
		params.NewHeartbeatData(heartbeatData),
	)); err != nil {
		t.Fatalf("handleHeartbeat(): %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "Heartbeat Ack" {
		t.Fatalf("sent %v, want [Heartbeat Ack]", got)
	}

	association.setBeatData(append([]byte(nil), heartbeatData...))
	if err := association.handleHeartbeatAck(messages.NewHeartbeatAck(
		params.NewHeartbeatData(heartbeatData),
	)); err != nil {
		t.Fatalf("handleHeartbeatAck(): %v", err)
	}
	if err := association.handleError(messages.NewError(
		params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil,
	)); err != nil {
		t.Fatalf("handleError(): %v", err)
	}
	select {
	case indication := <-association.mgmtChan:
		if indication.Kind != ManagementError {
			t.Fatalf("Error indication kind = %v, want ManagementError", indication.Kind)
		}
	default:
		t.Fatal("Error produced no management indication")
	}
}

func TestIPSPSingleExchangeRejectsSGPOnlySCONConcernedDestination(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	congestion := messages.NewSignallingCongestion(
		nil,
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456),
		params.NewConcernedDestination(0x654321),
		params.NewCongestionIndications(1),
		nil,
	)

	if err := association.handleSignallingCongestion(congestion); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("handleSignallingCongestion() error = %v, want ErrInvalidParameterValue", err)
	}
}

func TestIPSPSingleExchangeRejectsSCONOutsideActiveRoutingContext(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.cfg.RoutingContexts = params.NewRoutingContext(1, 2)
	association.noteRoutingContextsActive([]uint32{1})
	congestion := messages.NewSignallingCongestion(
		nil,
		params.NewRoutingContext(2),
		params.NewAffectedPointCode(0x123456),
		nil,
		params.NewCongestionIndications(1),
		nil,
	)

	var unexpected *UnexpectedMessageError
	if err := association.handleSignallingCongestion(congestion); !errors.As(err, &unexpected) {
		t.Fatalf("handleSignallingCongestion() error = %v, want UnexpectedMessageError", err)
	}
}

func TestIPSPSingleExchangeRejectsSGPOnlyOutboundSSNM(t *testing.T) {
	tests := []struct {
		name    string
		message messages.M3UA
	}{
		{"DUNA", messages.NewDestinationUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil)},
		{"DAVA", messages.NewDestinationAvailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil)},
		{"DAUD", messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil)},
		{"DUPU", messages.NewDestinationUserPartUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), params.NewUserCause(1, 1), nil)},
		{"DRST", messages.NewDestinationRestricted(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
			association.noteRoutingContextsActive([]uint32{1})

			if _, err := association.WriteSignal(test.message); !errors.Is(err, ErrUnsupportedRole) {
				t.Fatalf("WriteSignal(%s) error = %v, want ErrUnsupportedRole", test.name, err)
			}
		})
	}
}

func TestIPSPSingleExchangeRejectsSS7NetworkManagementMessages(t *testing.T) {
	tests := []struct {
		name   string
		handle func(*Association) error
	}{
		{"DUNA", func(association *Association) error {
			return association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil))
		}},
		{"DAVA", func(association *Association) error {
			return association.handleDestinationAvailable(messages.NewDestinationAvailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil))
		}},
		{"DAUD", func(association *Association) error {
			return association.handleDestinationStateAudit(messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil))
		}},
		{"DUPU", func(association *Association) error {
			return association.handleDestinationUserPartUnavailable(messages.NewDestinationUserPartUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), params.NewUserCause(1, 1), nil))
		}},
		{"DRST", func(association *Association) error {
			return association.handleDestinationRestricted(messages.NewDestinationRestricted(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil))
		}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
			association.noteRoutingContextsActive([]uint32{1})

			var unexpected *UnexpectedMessageError
			if err := test.handle(association); !errors.As(err, &unexpected) {
				t.Fatalf("handle %s error = %v, want UnexpectedMessageError", test.name, err)
			}
		})
	}
}

func TestIPSPSingleExchangeAllowsSCONInBothDirections(t *testing.T) {
	association, sent := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	congestion := messages.NewSignallingCongestion(
		nil,
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456),
		nil,
		params.NewCongestionIndications(2),
		nil,
	)

	if _, err := association.WriteSignal(congestion); err != nil {
		t.Fatalf("WriteSignal(SCON): %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "Signalling Congestion" {
		t.Fatalf("sent %v, want [Signalling Congestion]", got)
	}
	if err := association.handleSignallingCongestion(congestion); err != nil {
		t.Fatalf("handleSignallingCongestion(): %v", err)
	}
	status := nextStatus(t, association)
	if status.State != DestinationCongested || status.CongestionLevel != 2 {
		t.Fatalf("SCON status = %+v, want congested at level 2", status)
	}
}

func TestIPSPSingleExchangeInactiveAckWaitsForAdmittedData(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	ackWritten := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.Data:
			close(dataStarted)
			<-releaseData
		case *messages.AspInactiveAck:
			ackWritten <- struct{}{}
		}
		return message.MarshalLen(), nil
	}
	data := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte{0x01},
	), nil)
	dataDone := make(chan error, 1)
	go func() {
		_, err := association.WriteSignal(data)
		dataDone <- err
	}()
	<-dataStarted

	inactiveDone := make(chan error, 1)
	go func() {
		inactiveDone <- association.handleAspInactive(messages.NewAspInactive(
			params.NewRoutingContext(1), nil,
		))
	}()
	select {
	case <-ackWritten:
		t.Fatal("ASP Inactive Ack overtook admitted IPSP DATA")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseData)
	if err := <-dataDone; err != nil {
		t.Fatalf("DATA write: %v", err)
	}
	if err := <-inactiveDone; err != nil {
		t.Fatalf("handleAspInactive(): %v", err)
	}
	select {
	case <-ackWritten:
	default:
		t.Fatal("ASP Inactive Ack was not written after DATA drained")
	}
}

func TestIPSPSingleExchangeInactiveAckDefersCompletionUntilAdmittedDataDrains(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.cfg.RoutingContexts = nil
	association.cfg.TAck = 20 * time.Millisecond
	association.noteRoutingContextsActive(nil)

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	var inactiveWrites atomic.Int32
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.Data:
			close(dataStarted)
			<-releaseData
		case *messages.AspInactive:
			inactiveWrites.Add(1)
		}
		return message.MarshalLen(), nil
	}
	t.Cleanup(func() {
		select {
		case <-releaseData:
		default:
			close(releaseData)
		}
	})

	data := messages.NewData(nil, nil, params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte{0x01},
	), nil)
	dataDone := make(chan error, 1)
	go func() {
		_, err := association.WriteSignal(data)
		dataDone <- err
	}()
	select {
	case <-dataStarted:
	case err := <-dataDone:
		t.Fatalf("outbound DATA returned before entering the traffic path: %v", err)
	case <-time.After(time.Second):
		t.Fatal("outbound DATA did not enter the IPSP traffic path")
	}

	request, err := association.beginASPInactive(nil)
	if err != nil {
		t.Fatalf("beginASPInactive(): %v", err)
	}
	if got := inactiveWrites.Load(); got != 1 {
		t.Fatalf("initial ASP Inactive writes = %d, want 1", got)
	}
	waitDone := make(chan error, 1)
	go func() { waitDone <- association.waitTAck(context.Background(), request) }()
	handleDone := make(chan struct{})
	go func() {
		association.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
		close(handleDone)
	}()

	deadline := time.Now().Add(time.Second)
	for association.State() != StateASPInactive && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := association.State(); got != StateASPInactive {
		t.Fatalf("state while applying ASP Inactive Ack = %v, want ASP-INACTIVE", got)
	}
	time.Sleep(50 * time.Millisecond)
	if got := inactiveWrites.Load(); got != 1 {
		t.Fatalf("ASP Inactive retransmitted %d times after its Ack was claimed", got-1)
	}
	select {
	case err := <-waitDone:
		t.Fatalf("T(ack) waiter completed before admitted DATA drained: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	select {
	case <-handleDone:
		t.Fatal("ASP Inactive Ack handler returned before admitted DATA drained")
	default:
	}
	if _, err := association.WriteSignal(data); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA admitted after ASP Inactive Ack committed ASP-INACTIVE: %v", err)
	}

	close(releaseData)
	if err := <-dataDone; err != nil {
		t.Fatalf("in-flight DATA: %v", err)
	}
	select {
	case <-handleDone:
	case <-time.After(time.Second):
		t.Fatal("ASP Inactive Ack handler did not finish after DATA drained")
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("T(ack) waiter: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("T(ack) waiter did not complete after DATA drained")
	}
}

func TestIPSPSingleExchangeInactiveAckExcludesOverriddenContextsFromState(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.cfg.RoutingContexts = params.NewRoutingContext(1, 2)
	association.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	association.noteRoutingContextsActive([]uint32{1, 2})
	association.handleSignals(context.Background(), messages.NewNotify(
		params.NewStatus(params.AlternateAspActive), nil,
		params.NewRoutingContext(1), nil,
	))
	if !association.routingContextOverridden(1) {
		t.Fatal("Alternate ASP Active Notify did not override Routing Context 1")
	}
	association.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	t.Cleanup(association.stopAllTAck)

	association.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(2), nil,
	))

	if got := association.State(); got != StateASPInactive {
		t.Fatalf("state after the last non-overridden context became inactive = %v, want ASP-INACTIVE", got)
	}
	for _, routingContext := range []uint32{1, 2} {
		if association.outboundRoutingContextActive(routingContext) {
			t.Errorf("Routing Context %d remained active", routingContext)
		}
	}
}

func TestIPSPSingleExchangeInitialProcedureInitiationIsExplicit(t *testing.T) {
	tests := []struct {
		name          string
		initialState  State
		enteredState  State
		initiateASPSM bool
		initiateASPTM bool
		wantMessage   string
	}{
		{
			name:          "initiate ASPSM",
			initialState:  StateASPDown,
			enteredState:  StateASPDown,
			initiateASPSM: true,
			wantMessage:   "ASP Up",
		},
		{
			name:         "await peer ASPSM",
			initialState: StateASPDown,
			enteredState: StateASPDown,
		},
		{
			name:          "initiate ASPTM",
			initialState:  StateASPDown,
			enteredState:  StateASPInactive,
			initiateASPTM: true,
			wantMessage:   "ASP Active",
		},
		{
			name:         "await peer ASPTM",
			initialState: StateASPDown,
			enteredState: StateASPInactive,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association, sent := newTestConn(t, test.initialState, RoleIPSP)
			association.cfg.IPSP = &IPSPConfig{
				ExchangeModel: IPSPExchangeSingle,
				InitiateASPSM: test.initiateASPSM,
				InitiateASPTM: test.initiateASPTM,
			}

			if err := association.handleStateUpdate(test.enteredState); err != nil {
				t.Fatalf("handleStateUpdate(%s): %v", test.enteredState, err)
			}

			if test.wantMessage == "" {
				if len(*sent) != 0 {
					t.Fatalf("sent %v while configured to await the peer", typeNames(*sent))
				}
				return
			}
			if got := typeNames(*sent); len(got) != 1 || got[0] != test.wantMessage {
				t.Fatalf("sent %v, want [%s]", got, test.wantMessage)
			}
		})
	}
}

func TestIPSPSingleExchangeAcceptsEitherPeerAsASPSMInitiator(t *testing.T) {
	association, sent := newTestConn(t, StateASPDown, RoleIPSP)
	association.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}

	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatalf("handleAspUp(): %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up Ack" {
		t.Fatalf("sent %v, want [ASP Up Ack]", got)
	}
}

func TestIPSPSingleExchangeRetainsPeerIdentifierFromASPUpAck(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPDown)

	if err := association.handleAspUpAck(messages.NewAspUpAck(params.NewAspIdentifier(73), nil)); err != nil {
		t.Fatalf("handleAspUpAck(): %v", err)
	}
	identifier, present := association.PeerASPIdentifier()
	if !present || identifier != 73 {
		t.Fatalf("PeerASPIdentifier() = (%d, %t), want (73, true)", identifier, present)
	}
}

func TestIPSPSingleExchangeRejectsMalformedASPIdentifierWithoutRetiringTAck(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPDown)
	association.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	t.Cleanup(association.stopAllTAck)

	tack := messages.NewAspUpAck(&params.Param{
		Tag:    params.AspIdentifier,
		Length: 7,
		Data:   []byte{0, 0, 73},
	}, nil)
	if err := association.handleAspUpAck(tack); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("handleAspUpAck() error = %v, want ErrInvalidParameterValue", err)
	}
	if got := association.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after malformed ASP Up Ack = %d, want 1", got)
	}
	if _, present := association.PeerASPIdentifier(); present {
		t.Fatal("malformed ASP Up Ack saved a peer ASP Identifier")
	}
}

func TestIPSPSingleExchangeASPTMControlsBothTrafficDirections(t *testing.T) {
	initiator, initiatorSent := newSingleExchangeIPSPForTest(t, StateASPInactive)
	receiver, receiverSent := newSingleExchangeIPSPForTest(t, StateASPInactive)

	if err := initiator.initiateASPActive(params.NewRoutingContext(1)); err != nil {
		t.Fatalf("initiateASPActive(): %v", err)
	}
	active, ok := (*initiatorSent)[0].(*messages.AspActive)
	if !ok {
		t.Fatalf("first initiator message = %T, want *messages.AspActive", (*initiatorSent)[0])
	}
	if err := receiver.handleAspActive(active); err != nil {
		t.Fatalf("receiver handleAspActive(): %v", err)
	}
	activeAck, ok := (*receiverSent)[0].(*messages.AspActiveAck)
	if !ok {
		t.Fatalf("first receiver message = %T, want *messages.AspActiveAck", (*receiverSent)[0])
	}
	if err := initiator.handleAspActiveAck(activeAck); err != nil {
		t.Fatalf("initiator handleAspActiveAck(): %v", err)
	}
	if err := receiver.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("receiver enter ASP-ACTIVE: %v", err)
	}
	if err := initiator.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("initiator enter ASP-ACTIVE: %v", err)
	}

	for name, association := range map[string]*Association{
		"initiator": initiator,
		"receiver":  receiver,
	} {
		if !association.outboundRoutingContextActive(1) {
			t.Errorf("%s cannot send Routing Context 1 after the Single Exchange activation", name)
		}
		if !association.activeForRoutingContext(1) {
			t.Errorf("%s does not accept Routing Context 1 after the Single Exchange activation", name)
		}
	}

	*receiverSent = nil
	if err := initiator.initiateASPInactive(params.NewRoutingContext(1)); err != nil {
		t.Fatalf("initiateASPInactive(): %v", err)
	}
	inactive, ok := (*initiatorSent)[1].(*messages.AspInactive)
	if !ok {
		t.Fatalf("second initiator message = %T, want *messages.AspInactive", (*initiatorSent)[1])
	}
	if err := receiver.handleAspInactive(inactive); err != nil {
		t.Fatalf("receiver handleAspInactive(): %v", err)
	}
	inactiveAck, ok := (*receiverSent)[0].(*messages.AspInactiveAck)
	if !ok {
		t.Fatalf("receiver message = %T, want *messages.AspInactiveAck", (*receiverSent)[0])
	}
	if err := initiator.handleAspInactiveAck(inactiveAck); err != nil {
		t.Fatalf("initiator handleAspInactiveAck(): %v", err)
	}
	if err := receiver.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("receiver enter ASP-INACTIVE: %v", err)
	}
	if err := initiator.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("initiator enter ASP-INACTIVE: %v", err)
	}

	for name, association := range map[string]*Association{
		"initiator": initiator,
		"receiver":  receiver,
	} {
		if association.State() != StateASPInactive {
			t.Errorf("%s state = %s, want ASP-INACTIVE", name, association.State())
		}
		data := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(
			1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte{0x01},
		), nil)
		if _, err := association.WriteSignal(data); !errors.Is(err, ErrNotEstablished) {
			t.Errorf("%s DATA after deactivation error = %v, want ErrNotEstablished", name, err)
		}
	}
}

func TestIPSPSingleExchangeReactivationClearsRoutingContextOverride(t *testing.T) {
	association, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	association.noteRoutingContextsActive([]uint32{1})
	association.noteRoutingContextsOverridden([]uint32{1})

	if association.outboundRoutingContextActive(1) {
		t.Fatal("Routing Context 1 remained sendable after Alternate ASP Active")
	}

	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1),
		nil,
	)); err != nil {
		t.Fatalf("handleAspActive(): %v", err)
	}
	if !association.outboundRoutingContextActive(1) {
		t.Fatal("Routing Context 1 remained overridden after a new ASP Active exchange")
	}
}

func newSingleExchangeIPSPForTest(t *testing.T, state State) (*Association, *[]messages.M3UA) {
	t.Helper()
	association, sent := newTestConn(t, state, RoleIPSP)
	association.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association.cfg.RoutingContexts = params.NewRoutingContext(1)
	return association, sent
}
