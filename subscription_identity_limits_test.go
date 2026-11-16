package m3ua

import (
	"errors"
	"strings"
	"testing"
)

func TestSubscriptionIdentityOctetLimits(t *testing.T) {
	for _, name := range []string{"gateway", "application-server", "asp-route", "ipsp-route"} {
		for _, size := range []int{255, 256} {
			t.Run(name+"/"+strings.Repeat("x", size-254), func(t *testing.T) {
				value := strings.Repeat("é", size/2) + strings.Repeat("x", size%2)
				config := EndpointConfig{Role: RoleASP, ASP: ssnmPeerInventoryConfig()}
				want := ErrInvalidASPConfig
				switch name {
				case "gateway":
					config.ASP.SignallingGateways[0].ID = SignallingGatewayID(value)
				case "application-server":
					config.ASP.SignallingGateways[0].SGPs[0].ApplicationServers[0].ID = RemoteASID(value)
				case "asp-route":
					config.ASP = validASPConfig()
					config.ASP.Routing.MTPRoutes[0].ID = MTPRouteID(value)
				case "ipsp-route":
					config = EndpointConfig{Role: RoleIPSP, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: MTPRouteID(value), PointCode: 100}, AS: ASKey{}}}}
					want = ErrInvalidRoleConfiguration
				}
				e, err := NewEndpoint(nationalCongestionEndpointConfig(config))
				if e != nil {
					_ = e.Close()
				}
				if size == 255 && err != nil {
					t.Fatalf("255-octet identity refused: %v", err)
				}
				if size == 256 && !errors.Is(err, want) {
					t.Fatalf("256-octet identity not refused with typed error: %v", err)
				}
			})
		}
	}
}

func TestExplicitSSNMRejectsUnencodedOrOversizedScope(t *testing.T) {
	large := make([]uint32, 16383)
	for i := range large {
		large[i] = uint32(i)
	}
	for _, scope := range []WireScope{{RoutingContexts: []uint32{1}}, {RoutingContextSet: true, RoutingContexts: large}} {
		_, _, err := buildWireScope(scope)
		if !errors.Is(err, ErrInvalidParameterValue) {
			t.Errorf("unbounded or unencoded scope accepted: %v", err)
		}
	}
	if _, err := buildAffectedPointCodes(make([]PointCodeRange, 16383)); !errors.Is(err, ErrInvalidParameterValue) {
		t.Errorf("oversized APC request accepted: %v", err)
	}
	if _, _, err := buildWireScope(WireScope{RoutingContextSet: true, RoutingContexts: large[:16382]}); err != nil {
		t.Fatal("maximum RC parameter refused", err)
	}
	if _, err := buildAffectedPointCodes(make([]PointCodeRange, 16382)); err != nil {
		t.Fatal("maximum APC parameter refused", err)
	}
}
