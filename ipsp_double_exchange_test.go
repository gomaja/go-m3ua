package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestIPSPDoubleExchangeConfigurationUsesRFC4666TrafficDirections(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()

	if err := validateAssociationConfigForRole(RoleIPSP, config); err != nil {
		t.Fatalf("validateAssociationConfigForRole() error = %v", err)
	}

	tests := []struct {
		name      string
		configure func(*AssociationConfig)
	}{
		{
			name: "ASPSM exchange agreement omitted",
			configure: func(config *AssociationConfig) {
				config.IPSP.ASPSMExchange = 0
			},
		},
		{
			name: "both traffic directions omitted",
			configure: func(config *AssociationConfig) {
				config.IPSP.TrafficToLocal = nil
				config.IPSP.TrafficToPeer = nil
			},
		},
		{
			name: "normal ASPSM initiation without TrafficToLocal",
			configure: func(config *AssociationConfig) {
				config.IPSP.TrafficToLocal = nil
				config.IPSP.TrafficToPeer = &IPSPTrafficConfig{}
				config.IPSP.ASPSMExchange = IPSPASPSMExchangeDouble
				config.IPSP.InitiateASPSM = true
				config.IPSP.InitiateASPTM = false
			},
		},
		{
			name: "ambiguous association routing contexts",
			configure: func(config *AssociationConfig) {
				config.RoutingContexts = params.NewRoutingContext(99)
			},
		},
		{
			name: "ambiguous association network appearance",
			configure: func(config *AssociationConfig) {
				config.NetworkAppearance = params.NewNetworkAppearance(99)
			},
		},
		{
			name: "ambiguous association traffic mode",
			configure: func(config *AssociationConfig) {
				config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			invalid := newDoubleExchangeAssociationConfigForTest()
			test.configure(invalid)
			if err := validateAssociationConfigForRole(RoleIPSP, invalid); !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("validateAssociationConfigForRole() error = %v, want ErrInvalidRoleConfiguration", err)
			}
		})
	}

	singleASPSMForPeerDirection := newDoubleExchangeAssociationConfigForTest()
	singleASPSMForPeerDirection.IPSP.TrafficToLocal = nil
	singleASPSMForPeerDirection.IPSP.TrafficToPeer = &IPSPTrafficConfig{}
	singleASPSMForPeerDirection.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
	singleASPSMForPeerDirection.IPSP.InitiateASPSM = true
	singleASPSMForPeerDirection.IPSP.InitiateASPTM = false
	if err := validateAssociationConfigForRole(RoleIPSP, singleASPSMForPeerDirection); err != nil {
		t.Fatalf("agreed single ASPSM initiation for TrafficToPeer-only config: %v", err)
	}
}

func TestIPSPDoubleExchangeRejectsInvalidDirectionalTrafficConfiguration(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*IPSPTrafficConfig)
	}{
		{
			name: "Traffic Mode parameter tag",
			configure: func(direction *IPSPTrafficConfig) {
				direction.TrafficModeType = params.NewNetworkAppearance(1)
			},
		},
		{
			name: "undefined Traffic Mode",
			configure: func(direction *IPSPTrafficConfig) {
				direction.TrafficModeType = params.NewTrafficModeType(99)
			},
		},
		{
			name: "Network Appearance parameter tag",
			configure: func(direction *IPSPTrafficConfig) {
				direction.NetworkAppearance = params.NewRoutingContext(10)
			},
		},
		{
			name: "empty Routing Context parameter",
			configure: func(direction *IPSPTrafficConfig) {
				direction.RoutingContexts = params.NewRoutingContext()
			},
		},
		{
			name: "Routing Context parameter tag",
			configure: func(direction *IPSPTrafficConfig) {
				direction.RoutingContexts = params.NewNetworkAppearance(11)
			},
		},
		{
			name: "duplicate Routing Context",
			configure: func(direction *IPSPTrafficConfig) {
				direction.RoutingContexts = params.NewRoutingContext(11, 11)
			},
		},
		{
			name: "undefined per-context Traffic Mode",
			configure: func(direction *IPSPTrafficConfig) {
				direction.TrafficModes[11] = 99
			},
		},
		{
			name: "Traffic Mode for unconfigured Routing Context",
			configure: func(direction *IPSPTrafficConfig) {
				direction.TrafficModes[12] = params.TrafficModeLoadshare
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := newDoubleExchangeAssociationConfigForTest()
			test.configure(config.IPSP.TrafficToLocal)
			if err := validateAssociationConfigForRole(RoleIPSP, config); !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("validateAssociationConfigForRole() error = %v, want ErrInvalidRoleConfiguration", err)
			}
		})
	}

	contextless := newDoubleExchangeAssociationConfigForTest()
	contextless.IPSP.TrafficToLocal = &IPSPTrafficConfig{}
	if err := validateAssociationConfigForRole(RoleIPSP, contextless); err != nil {
		t.Fatalf("contextless TrafficToLocal validation error = %v", err)
	}

	contextlessWithScopedMode := newDoubleExchangeAssociationConfigForTest()
	contextlessWithScopedMode.IPSP.TrafficToLocal = &IPSPTrafficConfig{
		TrafficModes: map[uint32]uint32{11: params.TrafficModeLoadshare},
	}
	if err := validateAssociationConfigForRole(RoleIPSP, contextlessWithScopedMode); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("contextless scoped mode validation error = %v, want ErrInvalidRoleConfiguration", err)
	}
}

func TestIPSPDoubleExchangeConfigurationIsDeepSnapshotted(t *testing.T) {
	original := newDoubleExchangeAssociationConfigForTest()
	snapshot := snapshotAssociationConfig(original)

	original.IPSP.TrafficToLocal.RoutingContexts.Data[3] = 99
	original.IPSP.TrafficToLocal.TrafficModes[11] = params.TrafficModeOverride
	original.IPSP.TrafficToPeer.NetworkAppearance.Data[3] = 99
	original.IPSP.TrafficToPeer.TrafficModeType.Data[3] = byte(params.TrafficModeBroadcast)

	if snapshot.IPSP == original.IPSP ||
		snapshot.IPSP.TrafficToLocal == original.IPSP.TrafficToLocal ||
		snapshot.IPSP.TrafficToPeer == original.IPSP.TrafficToPeer {
		t.Fatal("snapshot shares IPSP Double Exchange configuration with the caller")
	}
	if got := snapshot.IPSP.TrafficToLocal.RoutingContexts.RoutingContexts(); len(got) != 1 || got[0] != 11 {
		t.Fatalf("TrafficToLocal Routing Contexts = %v, want [11]", got)
	}
	if got := snapshot.IPSP.TrafficToLocal.TrafficModes[11]; got != params.TrafficModeLoadshare {
		t.Fatalf("TrafficToLocal Traffic Mode = %d, want Loadshare", got)
	}
	if got := snapshot.IPSP.TrafficToPeer.NetworkAppearance.NetworkAppearance(); got != 20 {
		t.Fatalf("TrafficToPeer Network Appearance = %d, want 20", got)
	}
	if got := snapshot.IPSP.TrafficToPeer.TrafficModeType.TrafficModeType(); got != params.TrafficModeLoadshare {
		t.Fatalf("TrafficToPeer Traffic Mode = %d, want Loadshare", got)
	}
}

func TestIPSPDoubleExchangeASPSMStateIsIndependentByTrafficDirection(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)

	if err := association.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("enter ASP-DOWN: %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up" {
		t.Fatalf("initial messages = %v, want [ASP Up]", got)
	}

	association.recvStream.Store(0)
	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatalf("handle peer ASP Up: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPDown,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after peer ASP Up = %+v", got)
	}

	if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("handle local ASP Up Ack: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after local ASP Up Ack = %+v", got)
	}
}

func TestIPSPDoubleExchangeASPUpWhilePeerDirectionActiveResetsOnlyThatDirection(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})
	association.recvStream.Store(0)

	aspUp := messages.NewAspUp(nil, nil)
	err := association.handleAspUp(aspUp)
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("ASP Up while TrafficToPeer was ASP-ACTIVE error = %v, want UnexpectedMessageError", err)
	}
	if got := countType(*sent, "ASP Up Ack"); got != 1 {
		t.Fatalf("ASP Up while TrafficToPeer was ASP-ACTIVE produced %d Acks, want 1", got)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after ASP Up = %+v, want only TrafficToPeer ASP-INACTIVE", got)
	}

	if err := association.handleAspUp(aspUp); err != nil {
		t.Fatalf("duplicate ASP Up while TrafficToPeer was ASP-INACTIVE: %v", err)
	}
	if got := countType(*sent, "ASP Up Ack"); got != 2 {
		t.Fatalf("duplicate ASP Up produced %d Acks, want 2", got)
	}
	if got := association.IPSPState().TrafficToLocal; got != StateASPActive {
		t.Fatalf("duplicate peer ASP Up changed TrafficToLocal to %v", got)
	}
}

func TestIPSPDoubleExchangeAbsorbsDelayedCompletedASPUpAck(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	association.cfg.TAck = time.Hour
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPDown,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})

	association.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("first ASP Up Ack: %v", err)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11),
		nil,
	)); err != nil {
		t.Fatalf("ASP Active Ack after first ASP Up Ack: %v", err)
	}
	if got := association.IPSPState().TrafficToLocal; got != StateASPActive {
		t.Fatalf("TrafficToLocal after activation = %v, want ASP-ACTIVE", got)
	}
	activeRequests := countType(*sent, "ASP Active")

	if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("delayed completed ASP Up Ack: %v", err)
	}
	if got := association.IPSPState().TrafficToLocal; got != StateASPActive {
		t.Fatalf("delayed completed ASP Up Ack changed TrafficToLocal to %v", got)
	}
	if !association.routingContextAcked(11) {
		t.Fatal("delayed completed ASP Up Ack cleared the acknowledged Routing Context")
	}
	if got := countType(*sent, "ASP Active"); got != activeRequests {
		t.Fatalf("delayed completed ASP Up Ack started %d additional ASP Active procedures", got-activeRequests)
	}
}

func TestIPSPDoubleExchangeASPSMSingleExchangeSimplificationAffectsBothDirections(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle

	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatalf("handle peer ASP Up: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after single ASPSM exchange = %+v", got)
	}
}

func TestIPSPDoubleExchangeASPTMAndDATAFollowIndependentTrafficDirections(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	})

	if err := association.ActivateRoutingContexts(11); err != nil {
		t.Fatalf("ActivateRoutingContexts(11): %v", err)
	}
	active := (*sent)[0].(*messages.AspActive)
	if got := active.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 11 {
		t.Fatalf("outgoing ASP Active Routing Contexts = %v, want [11]", got)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("handle ASP Active Ack for TrafficToLocal: %v", err)
	}
	if got := association.IPSPState(); got.TrafficToLocal != StateASPActive || got.TrafficToPeer != StateASPInactive {
		t.Fatalf("state after local activation = %+v", got)
	}

	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(22), nil,
	)); err != nil {
		t.Fatalf("handle ASP Active for TrafficToPeer: %v", err)
	}
	if err := association.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("enter TrafficToPeer ASP-ACTIVE: %v", err)
	}
	if got := association.IPSPState(); got.TrafficToLocal != StateASPActive || got.TrafficToPeer != StateASPActive {
		t.Fatalf("state after peer activation = %+v", got)
	}

	association.maxMessageStreamID = 4
	if err := association.SelectRoutingContext(22); err != nil {
		t.Fatalf("SelectRoutingContext(22): %v", err)
	}
	if _, err := association.Write([]byte("to-peer")); err != nil {
		t.Fatalf("Write TrafficToPeer DATA: %v", err)
	}
	data := (*sent)[len(*sent)-1].(*messages.Data)
	if got := data.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 22 {
		t.Fatalf("outgoing DATA Routing Contexts = %v, want [22]", got)
	}
	if got := data.NetworkAppearance.NetworkAppearance(); got != 20 {
		t.Fatalf("outgoing DATA Network Appearance = %d, want 20", got)
	}

	association.recvStream.Store(1)
	association.handleData(context.Background(), messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(11),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("to-local")), nil,
	))
	select {
	case received := <-association.dataChan:
		if string(received.ProtocolData.Data) != "to-local" {
			t.Fatalf("received DATA = %q, want to-local", received.ProtocolData.Data)
		}
	default:
		t.Fatal("TrafficToLocal DATA was not delivered")
	}
}

func TestIPSPDoubleExchangeASPTMRejectsTheOtherTrafficDirection(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	})

	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("peer ASP Active for TrafficToLocal error = %v, want ErrNoConfiguredAS", err)
	}

	if err := association.initiateASPActive(params.NewRoutingContext(11)); err != nil {
		t.Fatalf("initiate ASP Active: %v", err)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(22), nil,
	)); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("ASP Active Ack for TrafficToPeer error = %v, want ErrInvalidRoutingContext", err)
	}
}

func TestIPSPDoubleExchangeInactiveAndDownDoNotChangeTheOtherDirection(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})

	if err := association.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(22), nil)); err != nil {
		t.Fatalf("handle peer ASP Inactive: %v", err)
	}
	if err := association.handleStateUpdate(association.stateForActiveRoutingContexts()); err != nil {
		t.Fatalf("apply TrafficToPeer inactive state: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after peer ASP Inactive = %+v", got)
	}

	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})
	if err := association.handleAspDown(messages.NewAspDown(nil)); err != nil {
		t.Fatalf("handle peer ASP Down: %v", err)
	}
	if err := association.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("apply TrafficToPeer ASP-DOWN: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPDown,
	}) {
		t.Fatalf("state after peer ASP Down = %+v", got)
	}
}

func TestIPSPDoubleExchangeShutdownIgnoresDuplicateInactiveAck(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.terminating.Store(true)

	request, err := association.beginASPInactive(params.NewRoutingContext(11))
	if err != nil {
		t.Fatalf("begin ASP Inactive: %v", err)
	}
	ack := messages.NewAspInactiveAck(params.NewRoutingContext(11), nil)
	if err := association.handleAspInactiveAck(ack); err != nil {
		t.Fatalf("first ASP Inactive Ack: %v", err)
	}
	if err := association.waitTAck(context.Background(), request); err != nil {
		t.Fatalf("wait for ASP Inactive Ack: %v", err)
	}
	if _, err := association.initiateASPDown(); err != nil {
		t.Fatalf("initiate ASP Down: %v", err)
	}

	if err := association.handleAspInactiveAck(ack); err != nil {
		t.Fatalf("duplicate ASP Inactive Ack during ASP Down: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("duplicate ASP Inactive Ack changed state to %+v", got)
	}
}

func TestIPSPDoubleExchangeShutdownIgnoresDuplicateActiveAck(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	})

	active := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)
	activeRequest := association.startTAck(active, requestAspActive)
	activeAck := messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)
	if err := association.handleAspActiveAck(activeAck); err != nil {
		t.Fatalf("first ASP Active Ack: %v", err)
	}
	if err := association.waitTAck(context.Background(), activeRequest); err != nil {
		t.Fatalf("wait for ASP Active Ack: %v", err)
	}

	association.terminating.Store(true)
	inactiveRequest, err := association.beginASPInactive(params.NewRoutingContext(11))
	if err != nil {
		t.Fatalf("begin ASP Inactive: %v", err)
	}
	if err := association.handleAspInactiveAck(
		messages.NewAspInactiveAck(params.NewRoutingContext(11), nil),
	); err != nil {
		t.Fatalf("ASP Inactive Ack: %v", err)
	}
	if err := association.waitTAck(context.Background(), inactiveRequest); err != nil {
		t.Fatalf("wait for ASP Inactive Ack: %v", err)
	}
	if _, err := association.initiateASPDown(); err != nil {
		t.Fatalf("initiate ASP Down: %v", err)
	}

	if err := association.handleAspActiveAck(activeAck); err != nil {
		t.Fatalf("duplicate ASP Active Ack during ASP Down: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("duplicate ASP Active Ack changed state to %+v", got)
	}
}

func TestIPSPDoubleExchangeShutdownRejectsUnrequestedActiveAckScope(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	config.IPSP.TrafficToLocal.RoutingContexts = params.NewRoutingContext(11, 12)
	config.IPSP.TrafficToLocal.TrafficModes[12] = params.TrafficModeLoadshare
	association, _ := newDoubleExchangeIPSPWithConfigForTest(t, config)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	})

	activeRequest := association.startTAck(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	), requestAspActive)
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("ASP Active Ack for requested RC 11: %v", err)
	}
	if err := association.waitTAck(context.Background(), activeRequest); err != nil {
		t.Fatalf("wait for ASP Active Ack: %v", err)
	}

	association.terminating.Store(true)
	err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(12), nil,
	))
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("unrequested ASP Active Ack error = %v, want UnexpectedMessageError", err)
	}
	if association.routingContextAcked(12) {
		t.Fatal("unrequested ASP Active Ack activated RC 12")
	}
}

func TestIPSPDoubleExchangeInactiveAckWaitsForAdmittedOutboundDATA(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})
	association.maxMessageStreamID = 4

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	var startOnce sync.Once
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		startOnce.Do(func() { close(dataStarted) })
		<-releaseData
		return len(data), nil
	}
	ackWritten := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspInactiveAck); ok {
			ackWritten <- struct{}{}
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteWithRoutingContext([]byte("in-flight"), 22)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("outbound DATA did not enter its transport write")
	}

	inactiveDone := make(chan error, 1)
	go func() {
		inactiveDone <- association.handleAspInactive(
			messages.NewAspInactive(params.NewRoutingContext(22), nil),
		)
	}()
	select {
	case <-ackWritten:
		t.Fatal("ASP Inactive Ack overtook admitted outbound DATA")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("outbound DATA: %v", err)
	}
	if err := <-inactiveDone; err != nil {
		t.Fatalf("handle ASP Inactive: %v", err)
	}
	select {
	case <-ackWritten:
	case <-time.After(time.Second):
		t.Fatal("ASP Inactive Ack was not written after DATA drained")
	}
}

func TestIPSPDoubleExchangeLocalInactiveAckDoesNotWaitForTrafficToPeerDATA(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})
	association.maxMessageStreamID = 4
	if err := association.DeactivateRoutingContexts(11); err != nil {
		t.Fatalf("begin TrafficToLocal ASP Inactive: %v", err)
	}

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(dataStarted)
		<-releaseData
		return len(data), nil
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteWithRoutingContext([]byte("to-peer"), 22)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("TrafficToPeer DATA did not enter its transport write")
	}

	ackDone := make(chan error, 1)
	go func() {
		ackDone <- association.handleAspInactiveAck(
			messages.NewAspInactiveAck(params.NewRoutingContext(11), nil),
		)
	}()
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("TrafficToLocal ASP Inactive Ack: %v", err)
		}
	case <-time.After(2 * time.Second):
		close(releaseData)
		<-writeDone
		t.Fatal("TrafficToLocal ASP Inactive Ack waited behind TrafficToPeer DATA")
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("state after local ASP Inactive Ack = %+v", got)
	}
	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("TrafficToPeer DATA: %v", err)
	}
}

func TestIPSPDoubleExchangeLocalWithdrawalDrainsAdmittedSCON(t *testing.T) {
	for _, test := range []struct {
		name     string
		begin    func(*Association) error
		complete func(*Association) error
	}{
		{
			name: "ASP Inactive Ack",
			begin: func(association *Association) error {
				return association.DeactivateRoutingContexts(11)
			},
			complete: func(association *Association) error {
				return association.handleAspInactiveAck(
					messages.NewAspInactiveAck(params.NewRoutingContext(11), nil),
				)
			},
		},
		{
			name: "ASP Down Ack",
			begin: func(association *Association) error {
				_, err := association.initiateASPDown()
				return err
			},
			complete: func(association *Association) error {
				return association.handleAspDownAck(messages.NewAspDownAck(nil))
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			association, _ := newDoubleExchangeIPSPForTest(t)
			association.setIPSPState(IPSPState{
				TrafficToLocal: StateASPActive,
				TrafficToPeer:  StateASPActive,
			})
			association.noteRoutingContextsAcked(params.NewRoutingContext(11))
			association.noteRoutingContextsActive([]uint32{22})
			if err := test.begin(association); err != nil {
				t.Fatalf("begin local withdrawal: %v", err)
			}

			sconStarted := make(chan struct{})
			releaseSCON := make(chan struct{})
			association.signalWriter = func(message messages.M3UA) (int, error) {
				if _, ok := message.(*messages.SignallingCongestion); ok {
					close(sconStarted)
					<-releaseSCON
				}
				return message.MarshalLen(), nil
			}
			sconDone := make(chan error, 1)
			go func() {
				_, err := association.WriteSignal(messages.NewSignallingCongestion(
					params.NewNetworkAppearance(10),
					params.NewRoutingContext(11),
					params.NewAffectedPointCode(0x111111),
					nil, nil, nil,
				))
				sconDone <- err
			}()
			select {
			case <-sconStarted:
			case <-time.After(time.Second):
				t.Fatal("TrafficToLocal SCON did not enter its transport write")
			}

			withdrawalDone := make(chan error, 1)
			go func() { withdrawalDone <- test.complete(association) }()
			select {
			case err := <-withdrawalDone:
				close(releaseSCON)
				<-sconDone
				t.Fatalf("local withdrawal completed before admitted SCON drained: %v", err)
			case <-time.After(50 * time.Millisecond):
			}

			close(releaseSCON)
			if err := <-sconDone; err != nil {
				t.Fatalf("TrafficToLocal SCON: %v", err)
			}
			select {
			case err := <-withdrawalDone:
				if err != nil {
					t.Fatalf("complete local withdrawal: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("local withdrawal did not finish after admitted SCON drained")
			}
		})
	}
}

func TestIPSPDoubleExchangeDownAckQuiescesOnlyTheAgreedASPSMDirections(t *testing.T) {
	for _, test := range []struct {
		name          string
		aspsmExchange IPSPASPSMExchangeModel
		wantBlocked   bool
		wantPeerState State
	}{
		{"normal Double Exchange", IPSPASPSMExchangeDouble, false, StateASPActive},
		{"agreed Single ASPSM simplification", IPSPASPSMExchangeSingle, true, StateASPDown},
	} {
		t.Run(test.name, func(t *testing.T) {
			association, _ := newDoubleExchangeIPSPForTest(t)
			association.cfg.IPSP.ASPSMExchange = test.aspsmExchange
			association.setIPSPState(IPSPState{
				TrafficToLocal: StateASPInactive,
				TrafficToPeer:  StateASPActive,
			})
			association.noteRoutingContextsActive([]uint32{22})
			association.maxMessageStreamID = 4
			if _, err := association.initiateASPDown(); err != nil {
				t.Fatalf("begin local ASP Down: %v", err)
			}

			dataStarted := make(chan struct{})
			releaseData := make(chan struct{})
			association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
				close(dataStarted)
				<-releaseData
				return len(data), nil
			}
			writeDone := make(chan error, 1)
			go func() {
				_, err := association.WriteWithRoutingContext([]byte("to-peer"), 22)
				writeDone <- err
			}()
			select {
			case <-dataStarted:
			case <-time.After(time.Second):
				t.Fatal("TrafficToPeer DATA did not enter its transport write")
			}

			ackDone := make(chan error, 1)
			go func() { ackDone <- association.handleAspDownAck(messages.NewAspDownAck(nil)) }()
			if test.wantBlocked {
				select {
				case <-ackDone:
					close(releaseData)
					<-writeDone
					t.Fatal("Single ASPSM ASP Down Ack did not wait for TrafficToPeer DATA")
				case <-time.After(25 * time.Millisecond):
					close(releaseData)
					if err := <-writeDone; err != nil {
						t.Fatalf("TrafficToPeer DATA: %v", err)
					}
					if err := <-ackDone; err != nil {
						t.Fatalf("ASP Down Ack after quiescence: %v", err)
					}
				}
			} else {
				select {
				case err := <-ackDone:
					if err != nil {
						t.Fatalf("ASP Down Ack: %v", err)
					}
				case <-time.After(2 * time.Second):
					close(releaseData)
					<-writeDone
					t.Fatal("normal Double Exchange ASP Down Ack waited behind TrafficToPeer DATA")
				}
			}
			if !test.wantBlocked {
				close(releaseData)
				if err := <-writeDone; err != nil {
					t.Fatalf("TrafficToPeer DATA: %v", err)
				}
			}
			if got := association.IPSPState(); got != (IPSPState{
				TrafficToLocal: StateASPDown,
				TrafficToPeer:  test.wantPeerState,
			}) {
				t.Fatalf("state after ASP Down Ack = %+v", got)
			}
		})
	}
}

func TestIPSPSingleASPSMDownAckDefersASNotifyUntilTrafficDrains(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})
	association.maxMessageStreamID = 4

	registry := newApplicationServers(time.Hour)
	association.as = registry
	observer, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP, 22)
	observer.as = registry
	applicationServer := registry.get(22)
	applicationServer.mu.Lock()
	applicationServer.asps[association] = StateASPActive
	applicationServer.asps[observer] = StateASPInactive
	applicationServer.state = ASActive
	applicationServer.rebuildActiveLocked()
	applicationServer.mu.Unlock()

	notify := make(chan struct{}, 1)
	observer.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Notify); ok {
			notify <- struct{}{}
		}
		return message.MarshalLen(), nil
	}
	if _, err := association.initiateASPDown(); err != nil {
		t.Fatalf("begin local ASP Down: %v", err)
	}

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(dataStarted)
		<-releaseData
		return len(data), nil
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteWithRoutingContext([]byte("to-peer"), 22)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("TrafficToPeer DATA did not enter its transport write")
	}

	ackDone := make(chan error, 1)
	go func() { ackDone <- association.handleAspDownAck(messages.NewAspDownAck(nil)) }()
	select {
	case <-notify:
		close(releaseData)
		<-writeDone
		<-ackDone
		t.Fatal("AS-state Notify overtook admitted TrafficToPeer DATA")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("TrafficToPeer DATA: %v", err)
	}
	select {
	case err := <-ackDone:
		if err != nil {
			t.Fatalf("ASP Down Ack after quiescence: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ASP Down Ack handling did not finish after TrafficToPeer DATA drained")
	}
	select {
	case <-notify:
	case <-time.After(2 * time.Second):
		t.Fatal("AS-state Notify was not released after TrafficToPeer DATA drained")
	}
}

func TestIPSPDoubleExchangeSingleASPSMUpAckQuiescesTrafficToPeer(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})
	association.maxMessageStreamID = 4

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(dataStarted)
		<-releaseData
		return len(data), nil
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteWithRoutingContext([]byte("to-peer"), 22)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("TrafficToPeer DATA did not enter its transport write")
	}

	ackDone := make(chan error, 1)
	go func() { ackDone <- association.handleAspUpAck(messages.NewAspUpAck(nil, nil)) }()
	select {
	case err := <-ackDone:
		close(releaseData)
		<-writeDone
		t.Fatalf("single-ASPSM ASP Up Ack completed before TrafficToPeer DATA drained: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("TrafficToPeer DATA: %v", err)
	}
	if err := <-ackDone; err != nil {
		t.Fatalf("single-ASPSM ASP Up Ack: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("state after single-ASPSM ASP Up Ack = %+v", got)
	}
}

func TestIPSPDoubleExchangeSSNMUsesItsTrafficDirection(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})

	fromPeer := messages.NewSignallingCongestion(
		params.NewNetworkAppearance(20),
		params.NewRoutingContext(22),
		params.NewAffectedPointCode(0x222222),
		nil, nil, nil,
	)
	if err := association.handleSignallingCongestion(fromPeer); err != nil {
		t.Fatalf("incoming TrafficToPeer SCON: %v", err)
	}

	toPeer := messages.NewSignallingCongestion(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(11),
		params.NewAffectedPointCode(0x111111),
		nil, nil, nil,
	)
	if _, err := association.WriteSignal(toPeer); err != nil {
		t.Fatalf("outgoing TrafficToLocal SCON: %v", err)
	}
	if got := (*sent)[len(*sent)-1].(*messages.SignallingCongestion); got.NetworkAppearance.NetworkAppearance() != 10 ||
		got.RoutingContext.RoutingContext() != 11 {
		t.Fatalf("outgoing SCON scope = NA %d RC %d, want NA 10 RC 11",
			got.NetworkAppearance.NetworkAppearance(), got.RoutingContext.RoutingContext())
	}

	wrongIncoming := messages.NewSignallingCongestion(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(11),
		params.NewAffectedPointCode(0x111111),
		nil, nil, nil,
	)
	if err := association.handleSignallingCongestion(wrongIncoming); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("incoming TrafficToLocal SCON error = %v, want ErrInvalidNetworkAppearance", err)
	}

	wrongOutgoing := messages.NewSignallingCongestion(
		params.NewNetworkAppearance(20),
		params.NewRoutingContext(22),
		params.NewAffectedPointCode(0x222222),
		nil, nil, nil,
	)
	if _, err := association.WriteSignal(wrongOutgoing); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("outgoing TrafficToPeer SCON error = %v, want ErrInvalidNetworkAppearance", err)
	}
}

func TestIPSPDoubleExchangeSSNMOmissionUsesThePeerTrafficNetworkAppearance(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})

	const pointCode = uint32(0x222222)
	if err := association.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil,
		params.NewRoutingContext(22),
		params.NewAffectedPointCode(pointCode),
		nil, params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("incoming SCON with omitted Network Appearance: %v", err)
	}
	if got := association.DestinationStateForNetworkAndRoutingContext(20, 22, pointCode); got != DestinationCongested {
		t.Fatalf("destination state in TrafficToPeer Network Appearance = %v, want Congested", got)
	}
}

func TestIPSPDoubleExchangeErrorsRetainTheOffendingTrafficDirection(t *testing.T) {
	tests := []struct {
		name               string
		message            messages.M3UA
		reported           error
		wantRoutingContext uint32
		wantAppearance     uint32
	}{
		{
			name: "DATA to the local IPSP",
			message: messages.NewData(
				params.NewNetworkAppearance(10),
				params.NewRoutingContext(11),
				nil,
				nil,
			),
			reported:           ErrMissingProtocolData,
			wantRoutingContext: 11,
			wantAppearance:     10,
		},
		{
			name: "SCON about traffic to the peer IPSP",
			message: messages.NewSignallingCongestion(
				params.NewNetworkAppearance(20),
				params.NewRoutingContext(22),
				params.NewAffectedPointCode(0x222222),
				nil, nil, nil,
			),
			reported:           ErrInvalidParameterValue,
			wantRoutingContext: 22,
			wantAppearance:     20,
		},
		{
			name: "invalid Routing Context in DATA to the local IPSP",
			message: messages.NewData(
				params.NewNetworkAppearance(10),
				params.NewRoutingContext(99),
				params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, nil),
				nil,
			),
			reported:           NewInvalidRoutingContextError(99),
			wantRoutingContext: 99,
			wantAppearance:     10,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association, sent := newDoubleExchangeIPSPForTest(t)
			association.sendErrForMessage(test.message, test.reported)
			reported := <-association.errChan
			if err := association.handleErrors(reported); err != nil {
				t.Fatalf("handleErrors() error = %v", err)
			}

			response := lastError(t, *sent)
			if response.RoutingContext == nil ||
				response.RoutingContext.RoutingContext() != test.wantRoutingContext {
				t.Fatalf("Error Routing Context = %v, want %d",
					response.RoutingContext, test.wantRoutingContext)
			}
			if response.NetworkAppearance == nil ||
				response.NetworkAppearance.NetworkAppearance() != test.wantAppearance {
				t.Fatalf("Error Network Appearance = %v, want %d",
					response.NetworkAppearance, test.wantAppearance)
			}
		})
	}
}

func TestIPSPDoubleExchangeAlternateASPNotifyDoesNotOverrideTheOppositeDirection(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	config.IPSP.TrafficToLocal.RoutingContexts = params.NewRoutingContext(11, 12)
	config.IPSP.TrafficToLocal.TrafficModes = map[uint32]uint32{
		11: params.TrafficModeOverride,
		12: params.TrafficModeOverride,
	}
	config.IPSP.TrafficToPeer.RoutingContexts = params.NewRoutingContext(11)
	config.IPSP.TrafficToPeer.TrafficModes = map[uint32]uint32{
		11: params.TrafficModeLoadshare,
	}
	association, _ := newDoubleExchangeIPSPWithConfigForTest(t, config)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11, 12))
	association.noteRoutingContextsActive([]uint32{11})
	association.maxMessageStreamID = 4

	notify := messages.NewNotify(
		params.NewStatus(params.AlternateAspActive),
		params.NewAspIdentifier(0x10203040),
		params.NewRoutingContext(11),
		nil,
	)
	if err := association.handleNotify(notify); err != nil {
		t.Fatalf("handle Alternate ASP Active Notify: %v", err)
	}
	if wholeAssociation := association.overrideScope(notify); wholeAssociation {
		t.Fatal("one of two TrafficToLocal Routing Contexts overrode the whole local direction")
	}

	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("directional state after Notify = %+v, want both directions ASP-ACTIVE", got)
	}
	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("repeat peer ASP Active after local override: %v", err)
	}
	if _, err := association.WriteWithRoutingContext([]byte("still-to-peer"), 11); err != nil {
		t.Fatalf("TrafficToPeer DATA sharing the numeric Routing Context was overridden: %v", err)
	}

	association.recvStream.Store(1)
	association.handleData(context.Background(), messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(11),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("overridden-to-local")), nil,
	))
	select {
	case err := <-association.errChan:
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("TrafficToLocal DATA after override error = %v, want UnexpectedMessageError", err)
		}
	default:
		t.Fatal("TrafficToLocal DATA for the overridden Routing Context was not rejected")
	}

	if err := association.DeactivateRoutingContexts(12); err != nil {
		t.Fatalf("deactivate final usable TrafficToLocal Routing Context: %v", err)
	}
	if err := association.handleAspInactiveAck(
		messages.NewAspInactiveAck(params.NewRoutingContext(12), nil),
	); err != nil {
		t.Fatalf("ack final usable TrafficToLocal deactivation: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("directional state with only an overridden local Routing Context = %+v", got)
	}

	if err := association.ActivateRoutingContexts(11); err != nil {
		t.Fatalf("reactivate overridden TrafficToLocal Routing Context: %v", err)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("ack reactivated TrafficToLocal Routing Context: %v", err)
	}
	association.handleData(context.Background(), messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(11),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("reactivated-to-local")), nil,
	))
	select {
	case received := <-association.dataChan:
		if string(received.ProtocolData.Data) != "reactivated-to-local" {
			t.Fatalf("reactivated TrafficToLocal DATA = %q, want reactivated-to-local", received.ProtocolData.Data)
		}
	default:
		t.Fatal("reactivated TrafficToLocal DATA was not delivered")
	}
}

func TestIPSPDoubleExchangeSupportsOneContextlessTrafficDirection(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	config.IPSP.TrafficToLocal = &IPSPTrafficConfig{}
	config.IPSP.TrafficToPeer = nil
	association, sent := newDoubleExchangeIPSPWithConfigForTest(t, config)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPDown,
	})

	if err := association.ActivateRoutingContexts(); err != nil {
		t.Fatalf("activate contextless TrafficToLocal: %v", err)
	}
	active := (*sent)[0].(*messages.AspActive)
	if active.RoutingContext != nil {
		t.Fatalf("contextless ASP Active carried Routing Context %v", active.RoutingContext.RoutingContexts())
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); err != nil {
		t.Fatalf("contextless ASP Active Ack: %v", err)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPDown,
	}) {
		t.Fatalf("contextless directional state = %+v", got)
	}

	association.recvStream.Store(1)
	association.handleData(context.Background(), messages.NewData(
		nil,
		nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("contextless")),
		nil,
	))
	select {
	case received := <-association.dataChan:
		if received.RoutingContextSet || received.NetworkAppearanceSet {
			t.Fatalf("contextless DATA scope = %+v", received)
		}
	case err := <-association.errChan:
		t.Fatalf("contextless DATA error: %v", err)
	default:
		t.Fatal("contextless DATA was not delivered")
	}

	association.recvStream.Store(0)
	if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatalf("ASP Up for missing TrafficToPeer: %v", err)
	}
	if err := association.handleAspActive(messages.NewAspActive(nil, nil, nil)); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("ASP Active for missing TrafficToPeer error = %v, want ErrInvalidRoutingContext", err)
	}
	if _, err := association.Write([]byte("missing-direction")); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA toward missing TrafficToPeer error = %v, want ErrNotEstablished", err)
	}
}

func TestIPSPDoubleExchangeActivationIsPartialInEachDirection(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	config.IPSP.TrafficToLocal.RoutingContexts = params.NewRoutingContext(11, 12)
	config.IPSP.TrafficToLocal.TrafficModes = map[uint32]uint32{
		11: params.TrafficModeLoadshare,
		12: params.TrafficModeLoadshare,
	}
	config.IPSP.TrafficToPeer.RoutingContexts = params.NewRoutingContext(22, 23)
	config.IPSP.TrafficToPeer.TrafficModes = map[uint32]uint32{
		22: params.TrafficModeLoadshare,
		23: params.TrafficModeLoadshare,
	}
	association, _ := newDoubleExchangeIPSPWithConfigForTest(t, config)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	})
	association.maxMessageStreamID = 4

	if err := association.ActivateRoutingContexts(11); err != nil {
		t.Fatalf("activate local RC 11: %v", err)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("ack local RC 11: %v", err)
	}
	if association.routingContextAcked(12) {
		t.Fatal("local RC 12 became active before its ASP Active Ack")
	}

	association.recvStream.Store(1)
	association.handleData(context.Background(), messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(12),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("inactive-local")),
		nil,
	))
	select {
	case err := <-association.errChan:
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("DATA for inactive local RC error = %v, want UnexpectedMessageError", err)
		}
	default:
		t.Fatal("DATA for inactive local RC was not rejected")
	}

	association.recvStream.Store(0)
	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(22), nil,
	)); err != nil {
		t.Fatalf("activate peer RC 22: %v", err)
	}
	if err := association.handleStateUpdate(association.stateForActiveRoutingContexts()); err != nil {
		t.Fatalf("apply peer RC 22 activation: %v", err)
	}
	if _, err := association.WriteWithRoutingContext([]byte("inactive-peer"), 23); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("DATA for inactive peer RC error = %v, want ErrRoutingContextNotActive", err)
	}

	if err := association.ActivateRoutingContexts(12); err != nil {
		t.Fatalf("activate local RC 12: %v", err)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(12), nil,
	)); err != nil {
		t.Fatalf("ack local RC 12: %v", err)
	}
	if err := association.DeactivateRoutingContexts(11); err != nil {
		t.Fatalf("deactivate local RC 11: %v", err)
	}
	if err := association.handleAspInactiveAck(
		messages.NewAspInactiveAck(params.NewRoutingContext(11), nil),
	); err != nil {
		t.Fatalf("ack local RC 11 deactivation: %v", err)
	}
	if got := association.IPSPState().TrafficToLocal; got != StateASPActive {
		t.Fatalf("local state after partial deactivation = %v, want ASP-ACTIVE", got)
	}
	if err := association.DeactivateRoutingContexts(12); err != nil {
		t.Fatalf("deactivate local RC 12: %v", err)
	}
	if err := association.handleAspInactiveAck(
		messages.NewAspInactiveAck(params.NewRoutingContext(12), nil),
	); err != nil {
		t.Fatalf("ack local RC 12 deactivation: %v", err)
	}
	if got := association.IPSPState().TrafficToLocal; got != StateASPInactive {
		t.Fatalf("local state after final deactivation = %v, want ASP-INACTIVE", got)
	}
}

func TestIPSPDoubleExchangeDuplicateRequestsRemainDirectional(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})

	for attempt := 0; attempt < 2; attempt++ {
		if err := association.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(22), nil,
		)); err != nil {
			t.Fatalf("duplicate peer ASP Active attempt %d: %v", attempt+1, err)
		}
	}
	if got := countType(*sent, "ASP Active Ack"); got != 2 {
		t.Fatalf("duplicate peer ASP Active produced %d Acks, want 2", got)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	}) {
		t.Fatalf("duplicate peer ASP Active changed directional state to %+v", got)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if err := association.handleAspInactive(
			messages.NewAspInactive(params.NewRoutingContext(22), nil),
		); err != nil {
			t.Fatalf("duplicate peer ASP Inactive attempt %d: %v", attempt+1, err)
		}
		if err := association.handleStateUpdate(association.stateForActiveRoutingContexts()); err != nil {
			t.Fatalf("apply peer ASP Inactive attempt %d: %v", attempt+1, err)
		}
	}
	if got := countType(*sent, "ASP Inactive Ack"); got != 2 {
		t.Fatalf("duplicate peer ASP Inactive produced %d Acks, want 2", got)
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPInactive,
	}) {
		t.Fatalf("duplicate peer ASP Inactive changed directional state to %+v", got)
	}
}

func TestIPSPDoubleExchangeRetransmissionCompletesOnlyItsLocalDirection(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPDown,
	})
	association.cfg.TAck = 10 * time.Millisecond
	association.cfg.TAckRetries = 20
	writes := make(chan messages.M3UA, 32)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	if err := association.ActivateRoutingContexts(11); err != nil {
		t.Fatalf("activate local RC 11: %v", err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		select {
		case message := <-writes:
			if _, ok := message.(*messages.AspActive); !ok {
				t.Fatalf("T(ack) write %d = %T, want ASP Active", attempt+1, message)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for ASP Active attempt %d", attempt+1)
		}
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(11), nil,
	)); err != nil {
		t.Fatalf("ASP Active Ack after retransmission: %v", err)
	}
	select {
	case message := <-writes:
		t.Fatalf("T(ack) wrote %T after acknowledgement", message)
	case <-time.After(30 * time.Millisecond):
	}
	if got := association.IPSPState(); got != (IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPDown,
	}) {
		t.Fatalf("state after retransmission completion = %+v", got)
	}
}

func TestIPSPDoubleExchangeDataRejectsWrongDirectionalScope(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsAcked(params.NewRoutingContext(11))
	association.noteRoutingContextsActive([]uint32{22})
	association.recvStream.Store(1)

	tests := []struct {
		name              string
		networkAppearance uint32
		routingContext    uint32
		wantErr           error
	}{
		{"TrafficToPeer Routing Context", 10, 22, ErrInvalidRoutingContext},
		{"TrafficToPeer Network Appearance", 20, 11, ErrInvalidNetworkAppearance},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association.handleData(context.Background(), messages.NewData(
				params.NewNetworkAppearance(test.networkAppearance),
				params.NewRoutingContext(test.routingContext),
				params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("wrong-direction")), nil,
			))
			select {
			case err := <-association.errChan:
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("handleData() error = %v, want %v", err, test.wantErr)
				}
			default:
				t.Fatalf("handleData() produced no %v", test.wantErr)
			}
		})
	}
}

func TestIPSPDoubleExchangeDirectDATAUsesTrafficToPeerNetworkAppearance(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})

	protocolData := params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("to-peer"),
	)
	if _, err := association.WriteSignal(messages.NewData(
		params.NewNetworkAppearance(20),
		params.NewRoutingContext(22),
		protocolData,
		nil,
	)); err != nil {
		t.Fatalf("direct TrafficToPeer DATA: %v", err)
	}
	if _, err := association.WriteSignal(messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(22),
		protocolData,
		nil,
	)); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("direct DATA with TrafficToLocal Network Appearance error = %v, want ErrInvalidNetworkAppearance", err)
	}
}

func TestIPSPDoubleExchangeRestartAndCloseResetBothDirections(t *testing.T) {
	t.Run("SCTP restart", func(t *testing.T) {
		association, sent := newDoubleExchangeIPSPForTest(t)
		association.setIPSPState(IPSPState{
			TrafficToLocal: StateASPActive,
			TrafficToPeer:  StateASPActive,
		})
		association.noteRoutingContextsAcked(params.NewRoutingContext(11))
		association.noteRoutingContextsActive([]uint32{22})
		association.cfg.TAck = time.Hour
		oldEpochActive := messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(11), nil,
		)
		completedRequest := association.startTAck(oldEpochActive, requestAspActive)
		oldEpochActiveAck := messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(11), nil,
		)
		if err := association.handleAspActiveAck(oldEpochActiveAck); err != nil {
			t.Fatalf("complete old-epoch ASP Active: %v", err)
		}
		if err := association.waitTAck(context.Background(), completedRequest); err != nil {
			t.Fatalf("wait for completed old-epoch ASP Active: %v", err)
		}
		association.startTAck(oldEpochActive, requestAspActive)

		association.handleSCTPRestart()
		if got := association.IPSPState(); got != (IPSPState{
			TrafficToLocal: StateASPDown,
			TrafficToPeer:  StateASPDown,
		}) {
			t.Fatalf("state after SCTP restart = %+v", got)
		}
		if association.routingContextAcked(11) {
			t.Fatal("TrafficToLocal Routing Context remained active after SCTP restart")
		}
		select {
		case state := <-association.stateChan:
			if state != StateASPDown {
				t.Fatalf("restart state = %v, want ASP-DOWN", state)
			}
			if err := association.handleStateUpdate(state); err != nil {
				t.Fatalf("apply restart state: %v", err)
			}
		default:
			t.Fatal("restart published no ASP-DOWN state")
		}
		if got := countType(*sent, "ASP Up"); got != 1 {
			t.Fatalf("restart produced %d fresh ASP Up requests, want 1", got)
		}
		if got := association.pendingTAck(); got != 1 {
			t.Fatalf("restart retained %d T(ack) requests, want only the fresh ASP Up", got)
		}
		association.recvStream.Store(0)
		if err := association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
			t.Fatalf("peer ASP Up during independent restart recovery: %v", err)
		}
		if err := association.handleAspInactiveAck(
			messages.NewAspInactiveAck(params.NewRoutingContext(11), nil),
		); err == nil {
			t.Fatal("old-epoch ASP Inactive Ack was accepted after only the peer direction completed ASP Up")
		}
		if got := association.IPSPState().TrafficToLocal; got != StateASPDown {
			t.Fatalf("old-epoch ASP Inactive Ack changed TrafficToLocal to %v", got)
		}
		if err := association.handleAspActiveAck(oldEpochActiveAck); err == nil {
			t.Fatal("old-epoch ASP Active Ack was accepted before fresh ASP Up Ack")
		}
		if got := association.IPSPState(); got != (IPSPState{
			TrafficToLocal: StateASPDown,
			TrafficToPeer:  StateASPInactive,
		}) {
			t.Fatalf("old-epoch ASP Active Ack changed restart state to %+v", got)
		}
	})

	t.Run("Association close", func(t *testing.T) {
		association, _ := newDoubleExchangeIPSPForTest(t)
		association.setIPSPState(IPSPState{
			TrafficToLocal: StateASPActive,
			TrafficToPeer:  StateASPInactive,
		})
		if err := association.Close(); err != nil {
			t.Fatalf("Close(): %v", err)
		}
		if got := association.IPSPState(); got != (IPSPState{
			TrafficToLocal: StateASPDown,
			TrafficToPeer:  StateASPDown,
		}) {
			t.Fatalf("state after Close = %+v", got)
		}
	})
}

func TestIPSPDoubleExchangeRestartDrainsOutboundDATABeforeFreshASPSM(t *testing.T) {
	association, _ := newDoubleExchangeIPSPForTest(t)
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPActive,
		TrafficToPeer:  StateASPActive,
	})
	association.noteRoutingContextsActive([]uint32{22})
	association.maxMessageStreamID = 4

	dataStarted := make(chan struct{})
	releaseData := make(chan struct{})
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(dataStarted)
		<-releaseData
		return len(data), nil
	}
	freshASPUp := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspUp); ok {
			freshASPUp <- struct{}{}
		}
		return message.MarshalLen(), nil
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := association.WriteWithRoutingContext([]byte("old-epoch"), 22)
		writeDone <- err
	}()
	select {
	case <-dataStarted:
	case <-time.After(time.Second):
		t.Fatal("old-epoch DATA did not enter its transport write")
	}

	restartDone := make(chan struct{})
	go func() {
		association.handleSCTPRestart()
		close(restartDone)
	}()
	select {
	case state := <-association.stateChan:
		if err := association.handleStateUpdate(state); err != nil {
			t.Fatalf("apply restart state before DATA drained: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-freshASPUp:
		close(releaseData)
		<-writeDone
		<-restartDone
		t.Fatal("fresh ASP Up overtook old-epoch DATA")
	default:
	}

	close(releaseData)
	if err := <-writeDone; err != nil {
		t.Fatalf("old-epoch DATA: %v", err)
	}
	select {
	case state := <-association.stateChan:
		if state != StateASPDown {
			t.Fatalf("restart state = %v, want ASP-DOWN", state)
		}
		if err := association.handleStateUpdate(state); err != nil {
			t.Fatalf("apply restart state: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SCTP restart published no ASP-DOWN state after old-epoch DATA drained")
	}
	select {
	case <-restartDone:
	case <-time.After(2 * time.Second):
		t.Fatal("SCTP restart did not finish after ASP-DOWN was consumed")
	}
	select {
	case <-freshASPUp:
	case <-time.After(2 * time.Second):
		t.Fatal("fresh ASP Up was not sent after old-epoch DATA drained")
	}
}

func newDoubleExchangeAssociationConfigForTest() *AssociationConfig {
	config := NewAssociationConfig(1, 2, params.ServiceIndSCCP, 0, 0, 1)
	config.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeDouble,
		ASPSMExchange: IPSPASPSMExchangeDouble,
		InitiateASPSM: true,
		InitiateASPTM: true,
		TrafficToLocal: &IPSPTrafficConfig{
			TrafficModeType:   params.NewTrafficModeType(params.TrafficModeLoadshare),
			TrafficModes:      map[uint32]uint32{11: params.TrafficModeLoadshare},
			NetworkAppearance: params.NewNetworkAppearance(10),
			RoutingContexts:   params.NewRoutingContext(11),
		},
		TrafficToPeer: &IPSPTrafficConfig{
			TrafficModeType:   params.NewTrafficModeType(params.TrafficModeLoadshare),
			TrafficModes:      map[uint32]uint32{22: params.TrafficModeLoadshare},
			NetworkAppearance: params.NewNetworkAppearance(20),
			RoutingContexts:   params.NewRoutingContext(22),
		},
	}
	return config
}

func newDoubleExchangeIPSPForTest(t *testing.T) (*Association, *[]messages.M3UA) {
	t.Helper()
	return newDoubleExchangeIPSPWithConfigForTest(t, newDoubleExchangeAssociationConfigForTest())
}

func newDoubleExchangeIPSPWithConfigForTest(
	t *testing.T,
	config *AssociationConfig,
) (*Association, *[]messages.M3UA) {
	t.Helper()
	association, sent := newTestConn(t, StateASPDown, RoleIPSP)
	association.cfg = snapshotAssociationConfig(config)
	association.trafficModes = trafficModeSnapshot{}
	association.freezeTrafficModePolicies()
	association.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		message, err := messages.Parse(data)
		if err != nil {
			return 0, err
		}
		*sent = append(*sent, message)
		return len(data), nil
	}
	return association, sent
}

func (c *Association) setIPSPState(state IPSPState) {
	c.muState.Lock()
	c.localIPSPState = state.TrafficToLocal
	c.state = state.TrafficToPeer
	c.muState.Unlock()
}
