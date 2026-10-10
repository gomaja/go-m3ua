package m3ua

import (
	"context"
	"errors"
	"math/rand/v2"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func sameDataScopeError(got, want error) bool {
	var gotContext, wantContext *RoutingContextError
	gotHasContext := errors.As(got, &gotContext)
	wantHasContext := errors.As(want, &wantContext)
	if gotHasContext || wantHasContext {
		return gotHasContext && wantHasContext && gotContext.Code == wantContext.Code &&
			slices.Equal(gotContext.Contexts, wantContext.Contexts) &&
			(gotContext.Contexts == nil) == (wantContext.Contexts == nil)
	}
	return errors.Is(got, want) && errors.Is(want, got)
}

// Keep a list-based reference for the DATA rules in RFC 4666 Section 3.3.1
// and the offending-context errors in Section 3.8.1. The inventory may contain
// duplicates or overlapping static/dynamic scopes; DATA counts distinct flows.
func referenceValidateDataRoutingContext(conn *Association, peer *params.Param) error {
	configured := conn.configuredRoutingContexts()
	if conn.isIPSPDoubleExchange() {
		configured = conn.configuredLocalRoutingContexts()
	}
	if peer == nil {
		if len(configured) > 1 {
			return ErrMissingRoutingContext
		}
		return nil
	}
	if err := validateRoutingContextAgainst(peer, configured); err != nil {
		return err
	}
	if len(peer.Data) != 4 {
		return NewInvalidRoutingContextError(peer.RoutingContexts()...)
	}
	return nil
}

func TestDataRoutingContextValidationMatchesListReference(testContext *testing.T) {
	random := rand.New(rand.NewPCG(81, 43))
	peers := []*params.Param{nil, params.NewRoutingContext(0), params.NewRoutingContext(1),
		params.NewRoutingContext(4), params.NewRoutingContext(9), params.NewRoutingContext(0, 1),
		params.NewRoutingContext(1, 9), params.NewRoutingContext(9, 8), params.NewRoutingContext(1, 1),
		params.NewParam(int(params.RoutingContext), nil), params.NewParam(int(params.RoutingContext), []byte{1, 2, 3}),
		params.NewParam(int(params.RoutingContext), []byte{0, 0, 0, 1, 2}), params.NewNetworkAppearance(1)}
	for iteration := range 4000 {
		conn := randomScopeAssociation(random)
		if conn.isIPSPDoubleExchange() {
			conn.dynamicLocalASKeys = map[uint32]ASKey{0: routingContextASKey(0), 1: routingContextASKey(1)}
		}
		for _, peer := range peers {
			got, want := conn.validateDataRoutingContext(peer), referenceValidateDataRoutingContext(conn, peer)
			if !sameDataScopeError(got, want) {
				testContext.Fatalf("iteration %d role %v peer %+v: got %#v, want %#v", iteration, conn.role, peer, got, want)
			}
		}
		configured := conn.configuredRoutingContexts()
		if conn.isIPSPDoubleExchange() {
			configured = conn.configuredLocalRoutingContexts()
		}
		var want uint32
		wantSet := len(configured) == 1
		if wantSet {
			want = configured[0]
		}
		if got, gotSet := conn.receivedDataRoutingContext(nil); got != want || gotSet != wantSet {
			testContext.Fatalf("iteration %d: inferred (%d, %t), want (%d, %t)", iteration, got, gotSet, want, wantSet)
		}
	}
}

func TestDataRoutingContextAuthorizationAndDynamicChanges(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 2, false)
	// Resolve a fresh per-peer authorization concurrently with readers. The
	// existing lock publishes the owned snapshot once, at ASP Up (§4.3.4.1).
	conn.authorizationResolved = false
	conn.cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{1} }
	var wait sync.WaitGroup
	for range 4 {
		wait.Go(func() {
			for range 1000 {
				if err := conn.validateDataRoutingContext(params.NewRoutingContext(1)); err != nil {
					testContext.Errorf("authorized DATA: %v", err)
				}
				_ = conn.validateDataRoutingContext(nil)
			}
		})
	}
	wait.Go(func() {
		if err := conn.resolveASPAuthorization(nil); err != nil {
			testContext.Error(err)
		}
	})
	wait.Go(func() {
		for range 1000 {
			conn.addDynamicASKey(routingContextASKey(3), RoutingKey{}, false)
			conn.removeDynamicASKey(3, false)
		}
	})
	wait.Wait()
	if err := conn.validateDataRoutingContext(params.NewRoutingContext(2)); !errors.Is(err, ErrInvalidRoutingContext) {
		testContext.Fatalf("unauthorized context after publication: %v", err)
	}
	if err := conn.validateDataRoutingContext(nil); err != nil {
		testContext.Fatalf("dedicated authorized flow after deregistration: %v", err)
	}
	conn.addDynamicASKey(routingContextASKey(3), RoutingKey{}, false)
	if err := conn.validateDataRoutingContext(params.NewRoutingContext(3)); err != nil {
		testContext.Fatalf("registered context: %v", err)
	}
	if err := conn.validateDataRoutingContext(nil); !errors.Is(err, ErrMissingRoutingContext) {
		testContext.Fatalf("omitted context with registered sibling: %v", err)
	}
	conn.removeDynamicASKey(3, false)
	if err := conn.validateDataRoutingContext(params.NewRoutingContext(3)); !errors.Is(err, ErrInvalidRoutingContext) {
		testContext.Fatalf("deregistered context: %v", err)
	}
}

func TestDataRoutingContextOwnedListsRemainIndependent(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 2, false)
	owned := conn.staticallyConfiguredRoutingContexts()
	owned[0] = 99
	if got := conn.staticallyConfiguredRoutingContexts(); !slices.Equal(got, []uint32{1, 2}) {
		testContext.Fatalf("mutating authorized copy changed configuration: %v", got)
	}
	data := inboundData(1, "retained")
	conn.handleData(context.Background(), data, nil)
	first := <-conn.dataChan
	conn.handleData(context.Background(), data, nil)
	second := <-conn.dataChan
	first.Scope.RoutingContexts[0] = 99
	if second.Scope.RoutingContexts[0] != 1 || data.RoutingContext.RoutingContext() != 1 {
		testContext.Fatal("mutating retained WireScope changed another message or the input frame")
	}
}

// DATA retains the generic resolver's NA selection and error order, including
// invalid RC shapes, wildcard dynamic appearances, zero, and both IPSP scopes
// (RFC 4666 Sections 3.3.1, 3.8.1 and 5.6.2).
func TestDataNetworkAppearanceScopeMatchesListReference(testContext *testing.T) {
	random := rand.New(rand.NewPCG(31, 77))
	peers := []*params.Param{nil, params.NewRoutingContext(0), params.NewRoutingContext(1),
		params.NewRoutingContext(4), params.NewRoutingContext(9), params.NewRoutingContext(0, 1),
		params.NewRoutingContext(1, 9), params.NewParam(int(params.RoutingContext), []byte{1, 2, 3}),
		params.NewNetworkAppearance(1)}
	for iteration := range 4000 {
		conn := randomScopeAssociation(random)
		if conn.isIPSPDoubleExchange() {
			conn.dynamicLocalASKeys = map[uint32]ASKey{0: {RoutingContextSet: true},
				1: {RoutingContext: 1, RoutingContextSet: true, NetworkAppearance: 0, NetworkAppearanceSet: true}}
		}
		for _, peer := range peers {
			got, gotAll, gotErr := conn.resolveDataNetworkAppearanceScope(peer)
			want, wantAll, wantErr := conn.resolveNetworkAppearanceScope(peer, conn.isIPSPDoubleExchange())
			if !reflect.DeepEqual(got, want) || gotAll != wantAll || !sameDataScopeError(gotErr, wantErr) {
				testContext.Fatalf("iteration %d role %v peer %+v: got (%+v, %t, %v), want (%+v, %t, %v)",
					iteration, conn.role, peer, got, gotAll, gotErr, want, wantAll, wantErr)
			}
		}
	}
}

func FuzzDataRoutingContextValidationMatchesListReference(fuzz *testing.F) {
	for _, data := range [][]byte{nil, {0, 0, 0, 0}, {0, 0, 0, 1}, {0, 0, 0, 1, 0, 0, 0, 9}, {1, 2, 3}} {
		fuzz.Add(data, false)
		fuzz.Add(data, true)
	}
	fuzz.Fuzz(func(testContext *testing.T, data []byte, local bool) {
		if len(data) > 256 {
			testContext.Skip()
		}
		conn := newReceiveRoutingContextAssociation(testContext, 2, true)
		if local {
			conn.role = RoleIPSP
			conn.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeDouble,
				TrafficToLocal: &IPSPTrafficConfig{ApplicationServers: buildTestInventory(8, true, params.TrafficModeLoadshare, []uint32{0, 2})}}
			conn.addDynamicASKey(routingContextASKey(4), RoutingKey{}, true)
		}
		peer := params.NewParam(int(params.RoutingContext), data)
		if got, want := conn.validateDataRoutingContext(peer), referenceValidateDataRoutingContext(conn, peer); !sameDataScopeError(got, want) {
			testContext.Fatalf("got %#v, want %#v", got, want)
		}
	})
}
