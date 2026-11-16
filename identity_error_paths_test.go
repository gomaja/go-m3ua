package m3ua

import (
	"errors"
	"github.com/gomaja/go-m3ua/messages/params"
	"reflect"
	"strings"
	"testing"
)

func requireIdentityLengthDetails(t *testing.T, err error, length, limit int) {
	t.Helper()
	var find func(error) bool
	find = func(e error) bool {
		if e == nil {
			return false
		}
		value := reflect.ValueOf(e)
		if value.Kind() == reflect.Pointer && !value.IsNil() && value.Elem().Type().Name() == "IdentityLengthError" {
			value = value.Elem()
			return value.FieldByName("Length").Int() == int64(length) && value.FieldByName("Limit").Int() == int64(limit) && value.FieldByName("Field").String() != ""
		}
		if multi, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range multi.Unwrap() {
				if find(child) {
					return true
				}
			}
			return false
		}
		return find(errors.Unwrap(e))
	}
	if !find(err) {
		t.Fatalf("missing typed identity length details (%d/%d): %v", length, limit, err)
	}
	if !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("length error lost parameter classification: %v", err)
	}
}

func TestIdentityLengthErrorsCoverAllEntryPaths(t *testing.T) {
	long := strings.Repeat("x", 256)
	for _, name := range []string{"gateway-config", "as-config", "asp-route-config", "ipsp-route-config", "peer-config", "path-gateway", "path-as", "sg-selection", "congestion-peer", "registration-request", "transfer-request", "result-install", "canonical-setter"} {
		t.Run(name, func(t *testing.T) {
			var err error
			switch name {
			case "gateway-config", "as-config", "asp-route-config", "path-gateway", "path-as", "sg-selection", "congestion-peer":
				cfg := validASPConfig()
				ec := nationalCongestionEndpointConfig(EndpointConfig{Role: RoleASP, ASP: cfg})
				switch name {
				case "gateway-config":
					cfg.SignallingGateways[0].ID = SignallingGatewayID(long)
				case "as-config":
					cfg.SignallingGateways[0].SGPs[0].ApplicationServers[0].ID = RemoteASID(long)
				case "asp-route-config":
					cfg.Routing.MTPRoutes[0].ID = MTPRouteID(long)
				case "path-gateway":
					cfg.Routing.Paths[0].SignallingGateway = SignallingGatewayID(long)
				case "path-as":
					cfg.Routing.Paths[0].ApplicationServers[0] = RemoteASID(long)
				case "sg-selection":
					cfg.Routing.SignallingGatewayProcessSelection[SignallingGatewayID(long)] = RouteSelectionLoadshare
				case "congestion-peer":
					ec.Congestion.Networks = []NetworkCongestionProfile{{PeerSGP: &SGPIdentity{SignallingGateway: SignallingGatewayID(long), SignallingGatewayProcess: "p"}, Profile: CongestionProfile{Method: CongestionInternational}}}
				}
				var e *Endpoint
				e, err = NewEndpoint(ec)
				if e != nil {
					_ = e.Close()
				}
			case "ipsp-route-config":
				_, err = newIPSPMTPState(RoleIPSP, []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: MTPRouteID(long), PointCode: 100}}})
			case "peer-config":
				cfg := NewAssociationConfig()
				cfg.PeerSGP = &SGPIdentity{SignallingGateway: SignallingGatewayID(long)}
				err = validateAssociationConfigForRole(RoleASP, cfg)
			case "registration-request":
				a := newAssociation(RoleASP, NewAssociationConfig())
				_, err = a.remoteASKeyFor(RemoteASID(long))
			case "transfer-request":
				r, e := newASPRoutes(validASPConfig())
				if e != nil {
					t.Fatal(e)
				}
				_, err = r.resolveTransferMTPRouteLocked(MTPRouteID(long), &params.ProtocolDataPayload{})
			case "result-install":
				a := newAssociation(RoleASP, NewAssociationConfig())
				err = a.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 10, RemoteAS: SGASKey{ApplicationServer: RemoteASID(long)}}}})
				if len(a.dynamicPeerASKeys) != 0 || len(a.canonicalRemoteAS) != 0 {
					t.Error("invalid result partially installed")
				}
			case "canonical-setter":
				a := newAssociation(RoleASP, NewAssociationConfig())
				returns := reflect.ValueOf(a.noteCanonicalRemoteAS).Call([]reflect.Value{reflect.ValueOf(uint32(10)), reflect.ValueOf(RemoteASID(long))})
				if len(returns) == 1 && !returns[0].IsNil() {
					err = returns[0].Interface().(error)
				}
				if len(a.canonicalRemoteAS) != 0 {
					t.Error("invalid canonical identity retained")
				}
			}
			requireIdentityLengthDetails(t, err, 256, 255)
		})
	}
	e, a, key := localCongestionFixture(t)
	err := e.ReportLocalCongestion(localRequest(a, key, strings.Repeat("s", 65), 2))
	requireIdentityLengthDetails(t, err, 65, 64)
	err = e.WithdrawLocalCongestion(a.ID(), a.Epoch(), key, 2, strings.Repeat("s", 65))
	requireIdentityLengthDetails(t, err, 65, 64)
}

func TestRegistrationResultIdentityValidationIsAtomic(t *testing.T) {
	for _, badFirst := range []bool{false, true} {
		a := newAssociation(RoleASP, NewAssociationConfig())
		good := registrationResultApplication{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 10, RemoteAS: SGASKey{ApplicationServer: "valid"}}}
		bad := registrationResultApplication{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(20, 200, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 20, RemoteAS: SGASKey{ApplicationServer: RemoteASID(strings.Repeat("b", 256))}}}
		batch := []registrationResultApplication{good, bad}
		if badFirst {
			batch = []registrationResultApplication{bad, good}
		}
		err := a.applyRegistrationResults(batch)
		requireIdentityLengthDetails(t, err, 256, 255)
		if len(a.dynamicPeerASKeys) != 0 || len(a.canonicalRemoteAS) != 0 {
			t.Fatal("invalid result batch partially installed")
		}
	}
}
