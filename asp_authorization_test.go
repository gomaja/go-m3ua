package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPAuthorizationRestrictsEveryRoutingContextProcedure(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPDown, 1, 2)
	allowed := []uint32{1}
	authorizations := 0
	asp.cfg.AuthorizeASP = func(identity ASPIdentity) []uint32 {
		authorizations++
		if !identity.ASPIdentifierSet || identity.ASPIdentifier != 100 {
			t.Fatalf("authorizer identity = %#v, want ASP Identifier 100", identity)
		}
		return allowed
	}

	if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); err != nil {
		t.Fatalf("handleAspUp: %v", err)
	}
	if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); err != nil {
		t.Fatalf("retransmitted handleAspUp: %v", err)
	}
	if authorizations != 1 {
		t.Fatalf("immutable ASP authorization resolved %d times, want once", authorizations)
	}
	secondApplicationServer := registry.get(2)
	secondApplicationServer.mu.Lock()
	_, leakedMembership := secondApplicationServer.asps[asp]
	secondApplicationServer.mu.Unlock()
	if leakedMembership {
		t.Fatal("authorized ASP remained a member of unauthorized AS 2")
	}
	allowed[0] = 2
	asp.setState(StateASPInactive)
	*sent = nil

	if err := asp.validateRoutingContext(params.NewRoutingContext(1)); err != nil {
		t.Fatalf("authorized Routing Context 1 rejected: %v", err)
	}
	if err := asp.validateRoutingContext(params.NewRoutingContext(2)); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("foreign Routing Context validation error = %v, want %v", err, ErrInvalidRoutingContext)
	}
	if _, err := asp.routingContextFor(2); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("outbound foreign Routing Context error = %v, want %v", err, ErrInvalidRoutingContext)
	}
	if err := asp.validateDataRoutingContext(params.NewRoutingContext(2)); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("inbound DATA foreign Routing Context error = %v, want %v", err, ErrInvalidRoutingContext)
	}
	if err := asp.validateSSNMRoutingContext(params.NewRoutingContext(2)); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("inbound SSNM foreign Routing Context error = %v, want %v", err, ErrInvalidRoutingContext)
	}

	err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(2),
		nil,
	))
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("foreign ASP Active error = %v, want %v", err, ErrNoConfiguredAS)
	}
	if got := countType(*sent, "ASP Active Ack"); got != 0 {
		t.Fatalf("foreign ASP Active drew %d Acks, want 0", got)
	}
}

func TestOmittedASPActiveExpandsOnlyToAuthorizedApplicationServers(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPDown, 1, 2)
	asp.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }
	if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); err != nil {
		t.Fatal(err)
	}
	asp.setState(StateASPInactive)
	*sent = nil

	if err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil,
	)); err != nil {
		t.Fatalf("contextless ASP Active: %v", err)
	}
	var acknowledgement *messages.AspActiveAck
	for _, message := range *sent {
		if activeAck, ok := message.(*messages.AspActiveAck); ok {
			acknowledgement = activeAck
		}
	}
	if acknowledgement == nil {
		t.Fatal("contextless ASP Active drew no Ack")
	}
	if got := acknowledgement.RoutingContext.RoutingContexts(); !equalTrafficModeContexts(got, []uint32{1}) {
		t.Fatalf("contextless ASP Active Ack scope = %v, want authorized [1]", got)
	}
	if asp.activeForRoutingContext(2) {
		t.Fatal("contextless ASP Active activated the ASP in unauthorized AS 2")
	}
}

func TestEmptyASPAuthorizationCannotActivateAnyApplicationServer(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPDown, 1, 2)
	asp.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
	if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); err != nil {
		t.Fatalf("ASP Up with no AS membership: %v", err)
	}
	asp.setState(StateASPInactive)
	*sent = nil

	err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil,
	))
	if !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("contextless ASP Active with no membership error = %v, want %v", err, ErrInvalidRoutingContext)
	}
	if got := countType(*sent, "ASP Active Ack"); got != 0 {
		t.Fatalf("membership-less ASP Active drew %d Acks, want 0", got)
	}
}

func TestEmptyASPAuthorizationDoesNotJoinContextlessApplicationServer(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	empty, _ := asTestConn(t, registry, StateASPDown, 1)
	empty.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
	if err := empty.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); err != nil {
		t.Fatalf("empty-authorized ASP Up: %v", err)
	}
	if err := empty.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("empty-authorized ASP state update: %v", err)
	}

	authorized, _ := asTestConn(t, registry, StateASPDown, 1)
	authorized.cfg.RoutingContexts = nil
	if err := authorized.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(101), nil)); err != nil {
		t.Fatalf("contextless ASP Up: %v", err)
	}
	if err := authorized.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("contextless ASP state update: %v", err)
	}

	active := registry.get(ASKey{}).activeASPs()
	if len(active) != 0 {
		t.Fatalf("contextless AS active ASPs before activation = %v, want none", active)
	}
	applicationServer := registry.get(ASKey{})
	applicationServer.mu.Lock()
	_, leaked := applicationServer.asps[empty]
	_, member := applicationServer.asps[authorized]
	applicationServer.mu.Unlock()
	if leaked {
		t.Fatal("empty-authorized ASP joined the contextless AS")
	}
	if !member {
		t.Fatal("legitimate contextless ASP did not join the contextless AS")
	}
}

func TestDuplicateASPIdentifierUniquenessUsesAuthorizedApplicationServers(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown, 1, 2)
	second, _ := asTestConn(t, registry, StateASPDown, 1, 2)
	first.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }
	second.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{2} }

	for index, asp := range []*Association{first, second} {
		if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
			t.Fatalf("disjoint authorized ASP %d handleAspUp: %v", index, err)
		}
	}

	spoof, _ := asTestConn(t, registry, StateASPDown, 1, 2)
	spoof.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }
	if err := spoof.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); !errors.Is(err, ErrInvalidASPIdentifier) {
		t.Fatalf("duplicate identifier in authorized AS 1 error = %v, want %v", err, ErrInvalidASPIdentifier)
	}
}

func TestEmptyASPAuthorizationDoesNotClaimAnASPIdentifier(t *testing.T) {
	for _, tc := range []struct {
		name       string
		emptyFirst bool
	}{
		{name: "empty authorization connects first", emptyFirst: true},
		{name: "authorized ASP connects first", emptyFirst: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry := newApplicationServers(time.Hour)
			empty, _ := asTestConn(t, registry, StateASPDown, 1)
			empty.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
			authorized, _ := asTestConn(t, registry, StateASPDown, 1)
			authorized.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }

			connections := []*Association{empty, authorized}
			if !tc.emptyFirst {
				connections[0], connections[1] = connections[1], connections[0]
			}
			for index, connection := range connections {
				if err := connection.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
					t.Fatalf("connection %d handleAspUp: %v", index, err)
				}
			}
		})
	}
}

func TestASPAuthorizationCannotInventAListenerRoutingContext(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPDown, 1, 2)
	asp.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{99} }

	if err := asp.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(100), nil)); !errors.Is(err, ErrInvalidASPIdentifier) {
		t.Fatalf("foreign authorization result error = %v, want %v", err, ErrInvalidASPIdentifier)
	}
	if got := countType(*sent, "ASP Up Ack"); got != 0 {
		t.Fatalf("invalid authorization drew %d ASP Up Acks, want 0", got)
	}
}
