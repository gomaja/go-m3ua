package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPActivationSplitsRoutingContextsByTrafficMode(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1, 2, 3)
	asp.cfg.TrafficModeType = nil
	asp.cfg.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeLoadshare,
		2: params.TrafficModeBroadcast,
		3: params.TrafficModeLoadshare,
	}
	var sent []*messages.AspActive
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if active, ok := message.(*messages.AspActive); ok {
			sent = append(sent, active)
		}
		return message.MarshalLen(), nil
	}

	if err := asp.ActivateRoutingContexts(1, 2, 3); err != nil {
		t.Fatalf("ActivateRoutingContexts: %v", err)
	}
	if len(sent) != 2 {
		t.Fatalf("sent %d ASP Active messages, want one per distinct traffic mode", len(sent))
	}

	want := map[uint32][]uint32{
		params.TrafficModeLoadshare: {1, 3},
		params.TrafficModeBroadcast: {2},
	}
	for _, active := range sent {
		if active.TrafficModeType == nil {
			t.Fatal("ASP Active omitted its per-RC Traffic Mode Type")
		}
		mode := active.TrafficModeType.TrafficModeType()
		got, known := want[mode]
		if !known {
			t.Fatalf("ASP Active used unexpected Traffic Mode %d", mode)
		}
		if !equalTrafficModeContexts(active.RoutingContext.RoutingContexts(), got) {
			t.Fatalf("ASP Active mode %d Routing Contexts = %v, want %v",
				mode, active.RoutingContext.RoutingContexts(), got)
		}
		delete(want, mode)
	}
	if len(want) != 0 {
		t.Fatalf("missing ASP Active traffic-mode groups: %v", want)
	}
}

func TestASPRejectsActiveAckWithWrongRequestedTrafficMode(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1, 2)
	asp.cfg.TrafficModeType = nil
	asp.cfg.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeLoadshare,
		2: params.TrafficModeBroadcast,
	}
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	if err := asp.ActivateRoutingContexts(1, 2); err != nil {
		t.Fatal(err)
	}

	err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeBroadcast),
		params.NewRoutingContext(1),
		nil,
	))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("wrong-mode ASP Active Ack error = %v, want %v", err, ErrUnsupportedTrafficMode)
	}
	if hasRecordedRoutingContextAck(asp, 1) {
		t.Fatal("wrong-mode Ack made Routing Context 1 active")
	}
	if got := len(asp.pendingTAckRoutingContexts(requestAspActive)); got != 2 {
		t.Fatalf("wrong-mode Ack retired pending contexts; remaining = %d, want 2", got)
	}

	if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1),
		nil,
	)); err != nil {
		t.Fatalf("correct ASP Active Ack: %v", err)
	}
	if !hasRecordedRoutingContextAck(asp, 1) {
		t.Fatal("correct Ack did not activate Routing Context 1")
	}
}

func TestASPRefusesInvalidConfiguredPerRoutingContextTrafficModeBeforeWriting(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
	asp.cfg.TrafficModes = map[uint32]uint32{1: 99}
	writes := 0
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		return message.MarshalLen(), nil
	}

	if err := asp.ActivateRoutingContexts(1); !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("ActivateRoutingContexts invalid mode error = %v, want %v", err, ErrUnsupportedTrafficMode)
	}
	if writes != 0 {
		t.Fatalf("invalid configured mode wrote %d ASP Active messages, want 0", writes)
	}
}

func equalTrafficModeContexts(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func hasRecordedRoutingContextAck(connection *Association, routingContext uint32) bool {
	connection.muAckedRCs.RLock()
	defer connection.muAckedRCs.RUnlock()
	if !connection.ackedRCsScoped {
		return false
	}
	_, ok := connection.ackedRCs[routingContext]
	return ok
}
