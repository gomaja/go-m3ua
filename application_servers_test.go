// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Association membership must have exactly one representation. The typed
// ApplicationServers inventory is it: an untyped Routing Context, Network
// Appearance or per-Routing-Context Traffic Mode selector left beside it would
// be a second way to name the same Application Server, and the two could
// disagree.
func TestAssociationConfigDeclaresMembershipOnlyOnce(t *testing.T) {
	removed := []string{"RoutingContexts", "NetworkAppearance", "TrafficModes", "TrafficModeType"}

	for _, subject := range []struct {
		name string
		typ  reflect.Type
	}{
		{"AssociationConfig", reflect.TypeOf(AssociationConfig{})},
		{"IPSPTrafficConfig", reflect.TypeOf(IPSPTrafficConfig{})},
	} {
		for _, field := range removed {
			if _, found := subject.typ.FieldByName(field); found {
				t.Errorf("%s still declares membership through %s", subject.name, field)
			}
		}
		inventory, found := subject.typ.FieldByName("ApplicationServers")
		if !found {
			t.Fatalf("%s has no typed ApplicationServers inventory", subject.name)
		}
		if got, want := inventory.Type.String(), "[]m3ua.ASConfig"; got != want {
			t.Errorf("%s.ApplicationServers is %s, want %s", subject.name, got, want)
		}
	}
}

// The setters are part of the same representation: one that writes an untyped
// selector keeps it reachable even once the field is gone.
func TestAssociationConfigHasNoUntypedMembershipSetters(t *testing.T) {
	configType := reflect.TypeOf(&AssociationConfig{})
	for _, setter := range []string{"SetRoutingContexts", "SetNetworkAppearance", "SetTrafficModeType"} {
		if _, found := configType.MethodByName(setter); found {
			t.Errorf("AssociationConfig still exposes %s", setter)
		}
	}
	if _, found := configType.MethodByName("SetApplicationServers"); !found {
		t.Error("AssociationConfig has no SetApplicationServers")
	}
}

// twoNetworkASPAssociation is an ASP carrying two Application Servers that sit
// in different SS7 network contexts. RFC 4666 Section 3.3.1 makes the Network
// Appearance "of local significance only, coordinated between the SGP and ASP"
// and Section 3.6.1 makes it part of Routing Key identity, so one Association
// may carry Application Servers in more than one of them.
func twoNetworkASPAssociation(t *testing.T) (*Association, *dataFrameCapture) {
	t.Helper()
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.ApplicationServers = []ASConfig{
		{ASKey: ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}},
		{ASKey: ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true}},
	}
	conn.maxMessageStreamID = 4
	conn.recvStream.Store(1)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	capture := &dataFrameCapture{}
	conn.dataWriter = capture.write
	return conn, capture
}

func applicationServerDataRequest(key ASKey) DataRequest {
	return DataRequest{
		AS: key,
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    1,
			DestinationPointCode:    2,
			ServiceIndicator:        params.ServiceIndSCCP,
			SignallingLinkSelection: 1,
			Data:                    []byte{0xde, 0xad},
		},
	}
}

// Each declaration owns its Network Appearance. A send is admitted for the
// exact pair the inventory declares and refused for any other, so one
// Application Server's appearance cannot carry another's traffic.
func TestDeclaredNetworkAppearanceIsPerApplicationServer(t *testing.T) {
	for _, test := range []struct {
		name string
		key  ASKey
		want error
	}{
		{
			name: "declared pair",
			key:  ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
		},
		{
			name: "second declared pair",
			key:  ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},
		},
		{
			name: "the other Application Server's appearance",
			key:  ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
			want: ErrInvalidNetworkAppearance,
		},
		{
			name: "the other Application Server's appearance, reversed",
			key:  ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},
			want: ErrInvalidNetworkAppearance,
		},
		{
			name: "an appearance neither declares",
			key:  ASKey{NetworkAppearance: 9, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
			want: ErrInvalidNetworkAppearance,
		},
		{
			name: "no appearance where one is declared",
			key:  ASKey{RoutingContext: 1, RoutingContextSet: true},
			want: ErrUnknownApplicationServerScope,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, capture := twoNetworkASPAssociation(t)
			_, err := conn.WriteData(applicationServerDataRequest(test.key))
			if test.want == nil {
				if err != nil {
					t.Fatalf("WriteData for declared %+v: %v", test.key, err)
				}
				if capture.submissions() != 1 {
					t.Fatalf("declared %+v submitted %d messages, want 1", test.key, capture.submissions())
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("WriteData for %+v error = %v, want %v", test.key, err, test.want)
			}
			if capture.submissions() != 0 {
				t.Fatalf("refused %+v still reached the transport", test.key)
			}
		})
	}
}

// A received DATA that omits the Network Appearance is resolved against the
// declaration naming its Routing Context, not against an appearance the whole
// Association is assumed to share.
func TestReceivedScopeResolvesTheDeclaringApplicationServersAppearance(t *testing.T) {
	conn, _ := twoNetworkASPAssociation(t)
	for _, test := range []struct {
		routingContext uint32
		want           uint32
	}{
		{routingContext: 1, want: 7},
		{routingContext: 2, want: 8},
	} {
		key := conn.staticASKeyForRoutingContext(test.routingContext, false)
		if !key.NetworkAppearanceSet || key.NetworkAppearance != test.want {
			t.Fatalf("Routing Context %d resolved to appearance %+v, want %d",
				test.routingContext, key, test.want)
		}
	}
}

// RFC 4666 Section 3.4 gives an SSNM message one Network Appearance parameter.
// With the declarations disagreeing there is no single appearance for the
// Association, and the optional parameter is omitted rather than guessed.
func TestAssociationWideAppearanceNeedsEveryDeclarationToAgree(t *testing.T) {
	conn, _ := twoNetworkASPAssociation(t)
	if _, set := conn.outboundNetworkAppearance(); set {
		t.Fatal("disagreeing declarations produced an Association-wide Network Appearance")
	}
	conn.cfg.ApplicationServers[1].ASKey.NetworkAppearance = 7
	appearance, set := conn.outboundNetworkAppearance()
	if !set || appearance != 7 {
		t.Fatalf("agreeing declarations produced appearance %d set %t, want 7 true", appearance, set)
	}
}

// The Traffic Mode belongs to the declaration, so RFC 4666 Section 4.3.4.3
// activation splits the Routing Contexts by the mode each Application Server
// was given.
func TestDeclaredTrafficModeGroupsASPActive(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	conn.cfg.ApplicationServers = []ASConfig{
		{ASKey: routingContextASKey(1), TrafficMode: params.TrafficModeLoadshare},
		{ASKey: routingContextASKey(2), TrafficMode: params.TrafficModeBroadcast},
		{ASKey: routingContextASKey(3), TrafficMode: params.TrafficModeLoadshare},
	}

	requests, err := conn.aspActiveRequests(params.NewRoutingContext(1, 2, 3))
	if err != nil {
		t.Fatalf("aspActiveRequests: %v", err)
	}
	grouped := map[uint32][]uint32{}
	for _, request := range requests {
		if request.trafficMode == nil {
			t.Fatalf("request for %v omitted the declared Traffic Mode",
				request.routingContext.RoutingContexts())
		}
		mode := request.trafficMode.TrafficModeType()
		grouped[mode] = append(grouped[mode], request.routingContext.RoutingContexts()...)
	}
	want := map[uint32][]uint32{
		params.TrafficModeLoadshare: {1, 3},
		params.TrafficModeBroadcast: {2},
	}
	if !reflect.DeepEqual(grouped, want) {
		t.Fatalf("ASP Active grouping = %v, want %v", grouped, want)
	}
}

// The contextless Application Server of RFC 4666 Section 3.6.1 is declared like
// any other, and carries its own Network Appearance and Traffic Mode.
func TestContextlessDeclarationCarriesItsAppearanceAndMode(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.ApplicationServers = []ASConfig{{
		ASKey:       ASKey{NetworkAppearance: 9, NetworkAppearanceSet: true},
		TrafficMode: params.TrafficModeBroadcast,
	}}
	conn.maxMessageStreamID = 4
	conn.recvStream.Store(1)
	conn.noteRoutingContextsAcked(nil)
	capture := &dataFrameCapture{}
	conn.dataWriter = capture.write

	admitted := ASKey{NetworkAppearance: 9, NetworkAppearanceSet: true}
	if _, err := conn.WriteData(applicationServerDataRequest(admitted)); err != nil {
		t.Fatalf("WriteData for the declared contextless Application Server: %v", err)
	}
	refused := ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true}
	if _, err := conn.WriteData(applicationServerDataRequest(refused)); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("WriteData for appearance 8 error = %v, want ErrInvalidNetworkAppearance", err)
	}
	if capture.submissions() != 1 {
		t.Fatalf("transport saw %d messages, want only the declared one", capture.submissions())
	}

	requests, err := conn.aspActiveRequests(nil)
	if err != nil {
		t.Fatalf("aspActiveRequests: %v", err)
	}
	if len(requests) != 1 || requests[0].trafficMode == nil ||
		requests[0].trafficMode.TrafficModeType() != params.TrafficModeBroadcast {
		t.Fatalf("contextless ASP Active = %+v, want one request carrying Broadcast", requests)
	}
}

// One inventory, one membership. Every shape that would let an Association say
// the same thing twice, or two things at once, is refused at configuration time
// rather than resolved by a rule the caller cannot see.
func TestApplicationServerInventoryRejectsAmbiguousDeclarations(t *testing.T) {
	for _, test := range []struct {
		name    string
		servers []ASConfig
		want    error
	}{
		{
			name:    "duplicate Application Server",
			servers: []ASConfig{{ASKey: routingContextASKey(1)}, {ASKey: routingContextASKey(1)}},
			want:    ErrInvalidApplicationServerConfig,
		},
		{
			name: "Network Appearance without its presence flag",
			servers: []ASConfig{{ASKey: ASKey{
				NetworkAppearance: 7, RoutingContext: 1, RoutingContextSet: true,
			}}},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			// The Traffic Mode keeps this from being the empty contextless
			// declaration as well, so only the presence rule can refuse it.
			name: "Routing Context without its presence flag",
			servers: []ASConfig{{
				ASKey:       ASKey{RoutingContext: 1},
				TrafficMode: params.TrafficModeLoadshare,
			}},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name:    "undefined Traffic Mode",
			servers: []ASConfig{{ASKey: routingContextASKey(1), TrafficMode: 4}},
			want:    ErrInvalidApplicationServerConfig,
		},
		{
			name: "two contextless Application Servers",
			servers: []ASConfig{
				{ASKey: ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}},
				{ASKey: ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true}},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name: "contextless beside a Routing-Context-scoped one",
			servers: []ASConfig{
				{ASKey: routingContextASKey(1)},
				{ASKey: ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name:    "contextless declaration that says nothing",
			servers: []ASConfig{{}},
			want:    ErrInvalidApplicationServerConfig,
		},
		{
			name:    "empty inventory",
			servers: nil,
		},
		{
			name:    "contextless declaration carrying a Network Appearance",
			servers: []ASConfig{{ASKey: ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}}},
		},
		{
			name:    "contextless declaration carrying only a Traffic Mode",
			servers: []ASConfig{{TrafficMode: params.TrafficModeLoadshare}},
		},
		{
			name: "explicit zero Routing Context and zero Network Appearance",
			servers: []ASConfig{{ASKey: ASKey{
				NetworkAppearanceSet: true, RoutingContextSet: true,
			}}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			config := NewAssociationConfig()
			config.ApplicationServers = test.servers
			err := validateAssociationConfigForRole(RoleSGP, config)
			if test.want == nil {
				if err != nil {
					t.Fatalf("validateAssociationConfigForRole: %v", err)
				}
				return
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("validateAssociationConfigForRole error = %v, want %v", err, test.want)
			}
		})
	}
}

// The inventory an Association runs on is its own. AssociationConfig is public
// and may be reused for the next Dial or shared by every Association a Listener
// accepts, so a snapshot that aliased it would let one Association's membership
// change under it.
func TestApplicationServerInventoryIsSnapshotted(t *testing.T) {
	original := NewAssociationConfig()
	original.ApplicationServers = []ASConfig{
		{ASKey: routingContextASKey(1), TrafficMode: params.TrafficModeLoadshare},
	}
	original.IPSP = &IPSPConfig{
		ExchangeModel:  IPSPExchangeDouble,
		ASPSMExchange:  IPSPASPSMExchangeDouble,
		TrafficToLocal: &IPSPTrafficConfig{ApplicationServers: []ASConfig{{ASKey: routingContextASKey(11)}}},
	}
	snapshot := snapshotAssociationConfig(original)

	original.ApplicationServers[0] = ASConfig{
		ASKey: routingContextASKey(99), TrafficMode: params.TrafficModeBroadcast,
	}
	original.IPSP.TrafficToLocal.ApplicationServers[0] = ASConfig{ASKey: routingContextASKey(99)}

	want := []ASConfig{{ASKey: routingContextASKey(1), TrafficMode: params.TrafficModeLoadshare}}
	if !reflect.DeepEqual(snapshot.ApplicationServers, want) {
		t.Fatalf("snapshot inventory = %+v, want %+v", snapshot.ApplicationServers, want)
	}
	wantLocal := []ASConfig{{ASKey: routingContextASKey(11)}}
	if !reflect.DeepEqual(snapshot.IPSP.TrafficToLocal.ApplicationServers, wantLocal) {
		t.Fatalf("snapshot TrafficToLocal inventory = %+v, want %+v",
			snapshot.IPSP.TrafficToLocal.ApplicationServers, wantLocal)
	}
}

// SetApplicationServers takes the inventory by value for the same reason.
func TestSetApplicationServersDoesNotAliasItsArgument(t *testing.T) {
	servers := []ASConfig{{ASKey: routingContextASKey(1)}}
	config := NewAssociationConfig().SetApplicationServers(servers...)
	servers[0] = ASConfig{ASKey: routingContextASKey(99)}
	if got := asConfigRoutingContexts(config.ApplicationServers); len(got) != 1 || got[0] != 1 {
		t.Fatalf("configured Routing Contexts = %v, want [1]", got)
	}
}

// RFC 4666 Section 5.6.2 keeps the two Double Exchange directions independent.
// Each has its own inventory, and neither reads the other's.
func TestIPSPDoubleExchangeDirectionsDeclareTheirOwnInventories(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	conn, _ := newDoubleExchangeIPSPWithConfigForTest(t, config)

	if got := conn.configuredRoutingContexts(); len(got) != 1 || got[0] != 22 {
		t.Fatalf("peer-directed Routing Contexts = %v, want [22]", got)
	}
	if got := conn.configuredLocalRoutingContexts(); len(got) != 1 || got[0] != 11 {
		t.Fatalf("local Routing Contexts = %v, want [11]", got)
	}
	if appearance, set := conn.outboundNetworkAppearance(); !set || appearance != 20 {
		t.Fatalf("peer-directed Network Appearance = %d set %t, want 20 true", appearance, set)
	}
	if appearance, set := conn.localNetworkAppearance(); !set || appearance != 10 {
		t.Fatalf("local Network Appearance = %d set %t, want 10 true", appearance, set)
	}
	if key := conn.staticASKeyForRoutingContext(22, false); key.NetworkAppearance != 20 {
		t.Fatalf("peer-directed RC 22 resolved to %+v, want appearance 20", key)
	}
	if key := conn.staticASKeyForRoutingContext(11, true); key.NetworkAppearance != 10 {
		t.Fatalf("local RC 11 resolved to %+v, want appearance 10", key)
	}
	if conn.staticRoutingContextConfigured(11) {
		t.Fatal("the local direction's Routing Context was found in the peer-directed inventory")
	}
}

// IPSP Single Exchange has one inventory shared by both directions, which is
// what RFC 4666 Section 5.6.1 means by one exchange changing the state of both
// IPSPs.
func TestIPSPSingleExchangeSharesOneInventory(t *testing.T) {
	config := NewAssociationConfig()
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	config.ASPProcedures = &ASPProcedurePolicy{
		ASPUp: ASPProcedureAutomatic, ASPDown: ASPProcedureAutomatic,
		ASPActive: ASPProcedureAutomatic, ASPInactive: ASPProcedureAutomatic,
	}
	config.ApplicationServers = []ASConfig{{
		ASKey:       ASKey{NetworkAppearance: 5, NetworkAppearanceSet: true, RoutingContext: 3, RoutingContextSet: true},
		TrafficMode: params.TrafficModeLoadshare,
	}}
	if err := validateAssociationConfigForRole(RoleIPSP, config); err != nil {
		t.Fatalf("validateAssociationConfigForRole: %v", err)
	}
	conn := newAssociation(RoleIPSP, config)
	for _, local := range []bool{false, true} {
		if got := asConfigRoutingContexts(conn.applicationServerInventory(local)); len(got) != 1 || got[0] != 3 {
			t.Fatalf("local=%t Routing Contexts = %v, want [3]", local, got)
		}
		if key := conn.staticASKeyForRoutingContext(3, local); !key.NetworkAppearanceSet || key.NetworkAppearance != 5 {
			t.Fatalf("local=%t RC 3 resolved to %+v, want appearance 5", local, key)
		}
	}
}

// An ASPTM request that omits the Routing Context applies to every Application
// Server the Association declares, so RFC 4666 Section 4.3.4.3's "one or more
// ASP Active Ack messages" for subsets of them are tracked against that whole
// inventory. Taking no inventory from it would let the first Ack, for whatever
// subset, complete the request.
func TestOmittedRoutingContextAwaitsEveryDeclaredApplicationServer(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	conn.cfg.ApplicationServers = buildTestInventory(0, false, params.TrafficModeLoadshare, []uint32{1, 2})

	conn.startTAck(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil,
	), requestAspActive)

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("Ack for the first declared Application Server: %v", err)
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after acknowledging one of two declarations = %d, want 1", got)
	}

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(2), nil,
	)); err != nil {
		t.Fatalf("Ack for the second declared Application Server: %v", err)
	}
	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pending T(ack) after acknowledging both declarations = %d, want 0", got)
	}
}

// RFC 4666 Section 3.4 gives an SSNM message one Network Appearance parameter.
// A Routing Context parameter that names no context leaves the appearance to
// the Association rather than to a named Application Server, so it is validated
// against the one every declaration shares.
func TestEmptyRoutingContextScopeUsesTheSharedNetworkAppearance(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	conn.cfg.ApplicationServers = []ASConfig{{ASKey: ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true,
	}}}
	empty := params.NewRoutingContext()

	if err := conn.validateSSNMNetworkAppearance(params.NewNetworkAppearance(7), empty); err != nil {
		t.Fatalf("shared Network Appearance rejected for an empty Routing Context: %v", err)
	}
	if err := conn.validateSSNMNetworkAppearance(params.NewNetworkAppearance(9), empty); !errors.Is(
		err, ErrInvalidNetworkAppearance) {
		t.Fatalf("foreign Network Appearance error = %v, want ErrInvalidNetworkAppearance", err)
	}
}
