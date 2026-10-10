package m3ua

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func sharedRoutingContextAssociation(testContext *testing.T, value uint32, explicit bool) *Association {
	testContext.Helper()
	cfg := newSGPAssociationConfigForTest(nil, 1, params.TrafficModeLoadshare, 7, []uint32{value})
	second := cfg.ApplicationServers[0]
	second.ASKey.NetworkAppearance = 9
	cfg.ApplicationServers = append(cfg.ApplicationServers, second)
	if explicit {
		cfg.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{value, value} }
	}
	if err := validateAssociationConfigForRole(RoleSGP, cfg); err != nil {
		testContext.Fatalf("distinct ASKeys sharing an RC must be legal: %v", err)
	}
	conn := newAssociation(RoleSGP, cfg)
	conn.state = StateASPActive
	conn.errChan = make(chan error, 1)
	conn.recvStream.Store(1)
	if err := conn.resolveASPAuthorization(nil); err != nil {
		testContext.Fatal(err)
	}
	return conn
}

// RFC 4666 Section 3.3.1 counts Routing Contexts, even when configuration
// distinguishes the same RC by Network Appearance. Publish each RC once.
func TestASPAuthorizationDeduplicatesRoutingContextAcrossNetworkAppearances(testContext *testing.T) {
	for _, explicit := range []bool{false, true} {
		conn := sharedRoutingContextAssociation(testContext, 17, explicit)
		owned := conn.staticallyConfiguredRoutingContexts()
		if !slices.Equal(owned, []uint32{17}) {
			testContext.Fatalf("explicit authorization %t: static contexts %v, want [17]", explicit, owned)
		}
		owned[0] = 99
		if got := conn.staticallyConfiguredRoutingContexts(); !slices.Equal(got, []uint32{17}) {
			testContext.Fatalf("mutating owned list changed published contexts: %v", got)
		}
		conn.addDynamicASKey(ASKey{RoutingContext: 17, RoutingContextSet: true,
			NetworkAppearance: 7, NetworkAppearanceSet: true}, RoutingKey{}, false)
		combined := conn.configuredRoutingContexts()
		combined[0] = 99
		if got := conn.configuredRoutingContexts(); !slices.Equal(got, []uint32{17}) {
			testContext.Fatalf("combined list lost deduplication or ownership: %v", got)
		}
	}
}

// Preserve the existing first-declaration NA choice, dedicated-flow inference
// and explicit-zero presence (RFC 4666 Sections 3.3.1 and 3.8.1).
func TestDataSharedRoutingContextAcrossNetworkAppearancesPreservesInboundDecision(testContext *testing.T) {
	for _, value := range []uint32{0, 17} {
		conn := sharedRoutingContextAssociation(testContext, value, false)
		for _, peer := range []*params.Param{nil, params.NewRoutingContext(value)} {
			if err := conn.validateDataRoutingContext(peer); err != nil {
				testContext.Fatalf("RC %d, peer %v: %v", value, peer, err)
			}
			if got, set := conn.receivedDataRoutingContext(peer); !set || got != value {
				testContext.Fatalf("inferred (%d, %t), want (%d, true)", got, set, value)
			}
			data := inboundData(value, "shared context")
			data.RoutingContext = peer
			conn.handleData(context.Background(), data, nil)
			select {
			case message := <-conn.dataChan:
				if message.AS.RoutingContext != value || !message.AS.RoutingContextSet ||
					message.AS.NetworkAppearance != 7 || !message.AS.NetworkAppearanceSet ||
					message.Scope.RoutingContextSet != (peer != nil) {
					testContext.Fatalf("wrong retained scope or AS: %+v", message)
				}
			default:
				testContext.Fatalf("DATA was not delivered: %v", <-conn.errChan)
			}
			if err := conn.validateDataNetworkAppearance(params.NewNetworkAppearance(9), peer); !errors.Is(err, ErrInvalidNetworkAppearance) {
				testContext.Fatalf("second declaration changed first-NA selection: %v", err)
			}
		}
	}
}

// Invalid NA is checked before RC and retains its offending value; malformed
// parameters retain the exact ERR cause (RFC 4666 Section 3.8.1).
func TestDataNetworkAppearanceScalarValidationErrors(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 1, false)
	rc := params.NewRoutingContext(1)
	for _, peer := range []*params.Param{nil, params.NewNetworkAppearance(7)} {
		if err := conn.validateDataNetworkAppearance(peer, rc); err != nil {
			testContext.Fatal(err)
		}
	}
	for _, value := range []uint32{0, 9} {
		var invalid *NetworkAppearanceError
		if err := conn.validateDataNetworkAppearance(params.NewNetworkAppearance(value), rc); !errors.As(err, &invalid) || invalid.Appearance != value {
			testContext.Fatalf("invalid NA %d lost offending value: %v", value, err)
		}
	}
	for _, fault := range []struct {
		peer  *params.Param
		code  uint32
		cause error
	}{
		{params.NewRoutingContext(7), params.ErrUnexpectedParameter, params.ErrInvalidType},
		{params.NewParam(int(params.NetworkAppearance), []byte{1, 2, 3}), params.ErrParameterFieldError, params.ErrInvalidLength},
	} {
		var got *ParameterFaultError
		if err := conn.validateDataNetworkAppearance(fault.peer, rc); !errors.As(err, &got) || got.Code != fault.code || !errors.Is(got, fault.cause) || got.Raw != nil {
			testContext.Fatalf("parameter fault changed: %+v", err)
		}
	}
	// An explicitly configured zero differs from an unset appearance.
	conn.cfg.ApplicationServers[0].ASKey.NetworkAppearance = 0
	if err := conn.validateDataNetworkAppearance(params.NewNetworkAppearance(0), rc); err != nil {
		testContext.Fatal(err)
	}
	conn.cfg.ApplicationServers[0].ASKey.NetworkAppearanceSet = false
	if err := conn.validateDataNetworkAppearance(params.NewNetworkAppearance(0), rc); !errors.Is(err, ErrInvalidNetworkAppearance) {
		testContext.Fatalf("unset appearance accepted explicit zero: %v", err)
	}
	conn.addDynamicASKey(routingContextASKey(1), RoutingKey{}, false)
	if err := conn.validateDataNetworkAppearance(params.NewNetworkAppearance(9), rc); err != nil {
		testContext.Fatalf("dynamic wildcard appearance rejected: %v", err)
	}
}

func TestNetworkAppearanceScopeReturnsIndependentOwnedParameters(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 1, false)
	for _, rc := range []*params.Param{nil, params.NewRoutingContext(1), params.NewRoutingContext(1, 1)} {
		first, _, err := conn.resolveNetworkAppearanceScope(rc, false)
		if err != nil || first == nil {
			testContext.Fatalf("owned scope: %v", err)
		}
		second, _, err := conn.resolveNetworkAppearanceScope(rc, false)
		if err != nil || second == nil {
			testContext.Fatalf("second owned scope: %v", err)
		}
		first.Data[3] = 99
		third, _, err := conn.resolveNetworkAppearanceScope(rc, false)
		if err != nil || third == nil || second.NetworkAppearance() != 7 || third.NetworkAppearance() != 7 {
			testContext.Fatalf("mutating NA changed configuration or another retained parameter: %v", err)
		}
	}
}

// Retain the pre-addendum owned resolver as an independent oracle for NA
// selection, dynamic wildcard scopes and error ordering.
func (c *Association) referenceNetworkAppearanceScope(
	routingContext *params.Param,
	local bool,
) (*params.Param, bool, error) {
	var contexts []uint32
	if routingContext != nil {
		contexts = routingContext.RoutingContexts()
	} else {
		contexts = c.configuredRoutingContexts()
		if local && c.isIPSPDoubleExchange() {
			contexts = c.configuredLocalRoutingContexts()
		}
	}
	if len(contexts) == 0 {
		contextless := c.contextlessASKey(local)
		if !contextless.NetworkAppearanceSet {
			return nil, false, nil
		}
		return params.NewNetworkAppearance(contextless.NetworkAppearance), false, nil
	}

	var resolvedValue uint32
	var resolvedSet bool
	var allNetworkAppearances bool
	resolved := false
	for _, routingContextValue := range contexts {
		configured := c.staticASKeyForRoutingContext(routingContextValue, local)
		value := configured.NetworkAppearance
		valueSet := configured.NetworkAppearanceSet
		all := false
		if key, ok := c.dynamicASKey(routingContextValue, local); ok {
			value = key.NetworkAppearance
			valueSet = key.NetworkAppearanceSet
			all = !key.NetworkAppearanceSet
		}
		if !resolved {
			resolvedValue = value
			resolvedSet = valueSet
			allNetworkAppearances = all
			resolved = true
			continue
		}
		if allNetworkAppearances != all || resolvedSet != valueSet || resolvedSet && resolvedValue != value {
			// RFC 4666 Section 3.4 gives one Network Appearance parameter to
			// an SSNM message. Every Routing Context named by that message must
			// therefore resolve to the same appearance scope.
			return nil, false, ErrInvalidNetworkAppearance
		}
	}
	if allNetworkAppearances {
		return nil, true, nil
	}
	if !resolvedSet {
		return nil, false, nil
	}
	return params.NewNetworkAppearance(resolvedValue), false, nil
}
