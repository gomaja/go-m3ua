package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestExplicitASPProcedurePolicyControlsDialReadinessAndWireSequence(t *testing.T) {
	const port = 3178
	peer := newRawPeer(t, port, func(message messages.M3UA) messages.M3UA {
		switch message.(type) {
		case *messages.AspUp:
			return messages.NewAspUpAck(nil, nil)
		case *messages.AspActive:
			return messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(7),
				nil,
			)
		case *messages.AspInactive:
			return messages.NewAspInactiveAck(params.NewRoutingContext(7), nil)
		case *messages.AspDown:
			return messages.NewAspDownAck(nil)
		default:
			return nil
		}
	})

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	local, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatalf("ResolveSCTPAddr: %v", err)
	}
	config := newASPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x111111, 0x222222, 1, params.TrafficModeLoadshare, 10, 0,
		[]uint32{7}, params.ServiceIndSCCP, 0, 0, 1,
	)
	config.CorrelationID = nil
	config.EstablishTimeout = time.Second
	config.ASPProcedures = explicitASPProcedurePolicy()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	association, err := endpoint.Dial(ctx, "m3ua", local, peer.addr, config)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = association.Close() })
	if state := association.State(); state != StateASPDown {
		t.Fatalf("Dial state = %v, want ASP-DOWN", state)
	}
	time.Sleep(100 * time.Millisecond)
	if got := peer.count("ASP Up"); got != 0 {
		t.Fatalf("Dial emitted %d ASP Up messages under explicit policy", got)
	}

	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}
	if err := association.ASPUp(ctx); err != nil {
		t.Fatalf("ASPUp: %v", err)
	}
	if !waitFor(func() bool { return association.State() == StateASPInactive }, time.Second) {
		t.Fatalf("state after ASPUp = %v, want ASP-INACTIVE", association.State())
	}
	if got := peer.count("ASP Active"); got != 0 {
		t.Fatalf("ASPUp emitted %d automatic ASP Active messages under explicit policy", got)
	}

	if err := association.ASPActive(ctx, key); err != nil {
		t.Fatalf("ASPActive: %v", err)
	}
	if !waitFor(func() bool { return association.State() == StateASPActive }, time.Second) {
		t.Fatalf("state after ASPActive = %v, want ASP-ACTIVE", association.State())
	}
	if err := association.ASPInactive(ctx, key); err != nil {
		t.Fatalf("ASPInactive: %v", err)
	}
	if !waitFor(func() bool { return association.State() == StateASPInactive }, time.Second) {
		t.Fatalf("state after ASPInactive = %v, want ASP-INACTIVE", association.State())
	}
	if err := association.ASPDown(ctx); err != nil {
		t.Fatalf("ASPDown: %v", err)
	}
	if !waitFor(func() bool { return association.State() == StateASPDown }, time.Second) {
		t.Fatalf("state after ASPDown = %v, want ASP-DOWN", association.State())
	}
	if got := peer.count("ASP Up"); got != 1 {
		t.Fatalf("peer received %d ASP Up messages, want 1", got)
	}
	if got := peer.count("ASP Active"); got != 1 {
		t.Fatalf("peer received %d ASP Active messages, want 1", got)
	}
	if got := peer.count("ASP Inactive"); got != 1 {
		t.Fatalf("peer received %d ASP Inactive messages, want 1", got)
	}
	if got := peer.count("ASP Down"); got != 1 {
		t.Fatalf("peer received %d ASP Down messages, want 1", got)
	}
}
