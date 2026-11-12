package m3ua

import (
	"context"
	"encoding/binary"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func registrationWord(tag uint16, value uint32) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint16(b, tag)
	binary.BigEndian.PutUint16(b[2:], 8)
	binary.BigEndian.PutUint32(b[4:], value)
	return b
}

func registrationTLV(tag uint16, parts ...[]byte) []byte {
	b := make([]byte, 4)
	for _, part := range parts {
		b = append(b, part...)
	}
	binary.BigEndian.PutUint16(b, tag)
	binary.BigEndian.PutUint16(b[2:], uint16(len(b)))
	return append(b, make([]byte, (4-len(b)%4)%4)...)
}

func registrationWire(typ uint8, parts ...[]byte) []byte {
	b := []byte{1, 0, messages.MsgClassRKM, typ, 0, 0, 0, 0}
	for _, part := range parts {
		b = append(b, part...)
	}
	binary.BigEndian.PutUint32(b[4:], uint32(len(b)))
	return b
}

func registrationRegistry(t *testing.T, provisioned ...ProvisionedRoutingKey) *routingKeyRegistry {
	t.Helper()
	r, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration:   func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered },
		AllowDynamicRoutingKeys: true, ProvisionedRoutingKeys: provisioned,
	})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestRegistrationSemanticFaultPreservesSiblingResults(t *testing.T) {
	id := registrationWord(params.LocalRoutingKeyIdentifier, 1)
	dpc := registrationWord(params.DestinationPointCode, 100)
	cases := []struct {
		name   string
		parts  [][]byte
		status RegistrationStatus
	}{
		{"DPC mask", [][]byte{id, registrationWord(params.DestinationPointCode, 0x01000064)}, RegistrationInvalidDestinationPointCode},
		{"undefined traffic mode", [][]byte{id, dpc, registrationWord(params.TrafficModeType, 4)}, RegistrationUnsupportedTrafficHandlingMode},
		{"zero traffic mode", [][]byte{id, dpc, registrationWord(params.TrafficModeType, 0)}, RegistrationUnsupportedTrafficHandlingMode},
		{"missing DPC", [][]byte{id}, RegistrationInvalidRoutingKey},
		{"duplicate network appearance", [][]byte{id, dpc, registrationWord(params.NetworkAppearance, 10), registrationWord(params.NetworkAppearance, 20)}, RegistrationInvalidNetworkAppearance},
		{"duplicate mode", [][]byte{id, dpc, registrationWord(params.TrafficModeType, 1), registrationWord(params.TrafficModeType, 2)}, RegistrationInvalidRoutingKey},
		{"ungrouped SI", [][]byte{id, registrationTLV(params.ServiceIndicators, []byte{3}), dpc}, RegistrationInvalidRoutingKey},
		{"management SI", [][]byte{id, dpc, registrationTLV(params.ServiceIndicators, []byte{0})}, RegistrationInvalidRoutingKey},
		{"unexpected known field", [][]byte{id, dpc, registrationWord(params.ErrorCode, params.ErrProtocolError)}, RegistrationInvalidRoutingKey},
		{"identifier after fault", [][]byte{registrationWord(params.DestinationPointCode, 0x01000064), id}, RegistrationInvalidDestinationPointCode},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c, sent := newTestConn(t, StateASPInactive, RoleSGP)
			r := registrationRegistry(t)
			c.endpoint = &Endpoint{routingKeys: r}
			bad := registrationTLV(params.RoutingKey, tc.parts...)
			good := registrationTLV(params.RoutingKey, registrationWord(params.LocalRoutingKeyIdentifier, 2), registrationWord(params.DestinationPointCode, 200))
			c.dispatchRaw(context.Background(), inbound{data: registrationWire(messages.MsgTypeRegistrationRequest, bad, good), ppid: M3UAPPID})
			if err := firstErr(c); err != nil {
				t.Fatalf("dispatch: %v", err)
			}
			var responses []*messages.RegistrationResponse
			for _, m := range *sent {
				if response, ok := m.(*messages.RegistrationResponse); ok {
					responses = append(responses, response)
				}
				if _, ok := m.(*messages.Error); ok {
					t.Fatal("unexpected ERR")
				}
			}
			if len(responses) != 1 || len(responses[0].RegistrationResults) != 2 {
				t.Fatalf("responses = %v", responses)
			}
			for i, want := range []RegistrationStatus{tc.status, RegistrationSuccessfullyRegistered} {
				result, err := responses[0].RegistrationResults[i].RegistrationResult()
				if err != nil {
					t.Fatal(err)
				}
				if got := result.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier(); got != uint32(i+1) {
					t.Fatalf("identifier = %d", got)
				}
				if got := RegistrationStatus(result.RegistrationStatus.RegistrationStatus()); got != want {
					t.Fatalf("status = %d, want %d", got, want)
				}
				rc := result.RoutingContext.RoutingContext()
				if (i == 0 && rc != 0) || (i == 1 && rc == 0) {
					t.Fatalf("result %d context = %d", i, rc)
				}
			}
			if r.dynamicCount() != 1 {
				t.Fatalf("dynamic count = %d", r.dynamicCount())
			}
		})
	}
}

func TestRegistrationUncorrelatableFaultKeepsError(t *testing.T) {
	id := registrationWord(params.LocalRoutingKeyIdentifier, 1)
	dpc := registrationWord(params.DestinationPointCode, 100)
	for name, parts := range map[string][][]byte{
		"missing identifier": {dpc}, "duplicate identifier": {id, id, dpc},
		"short identifier":                                  {registrationTLV(params.LocalRoutingKeyIdentifier, []byte{1}), dpc},
		"broken nested framing":                             {id, []byte{2, 11, 0, 8, 1}},
		"bad scalar width":                                  {id, registrationTLV(params.NetworkAppearance, []byte{1}), dpc},
		"semantic fault before bad width":                   {registrationWord(params.DestinationPointCode, 0x01000064), id, registrationTLV(params.NetworkAppearance, []byte{1})},
		"semantic fault before multi-value Routing Context": {id, registrationWord(params.DestinationPointCode, 0x01000064), registrationTLV(params.RoutingContext, []byte{0, 0, 0, 1, 0, 0, 0, 2})},
		"semantic fault before duplicate identifier":        {registrationWord(params.DestinationPointCode, 0x01000064), id, id},
		"nested container":                                  {id, dpc, registrationTLV(params.RoutingKey, id, dpc)},
		"nested semantic fault":                             {id, dpc, registrationTLV(params.RoutingKey, id, registrationWord(params.DestinationPointCode, 0x01000064))},
		"nested identifier only":                            {registrationTLV(params.RoutingKey, id, registrationWord(params.DestinationPointCode, 0x01000064))},
	} {
		t.Run(name, func(t *testing.T) {
			c, sent := newTestConn(t, StateASPInactive, RoleSGP)
			c.endpoint = &Endpoint{routingKeys: registrationRegistry(t)}
			c.dispatchRaw(context.Background(), inbound{data: registrationWire(messages.MsgTypeRegistrationRequest, registrationTLV(params.RoutingKey, parts...),
				registrationTLV(params.RoutingKey, registrationWord(params.LocalRoutingKeyIdentifier, 2), registrationWord(params.DestinationPointCode, 200))), ppid: M3UAPPID})
			err := firstErr(c)
			if err == nil {
				t.Fatal("expected uncorrelatable request error")
			}
			if err := c.handleErrors(err); err != nil {
				t.Fatal(err)
			}
			if len(errorCodes(*sent)) != 1 {
				t.Fatalf("messages = %v", typeNames(*sent))
			}
			if c.endpoint.routingKeys.dynamicCount() != 0 {
				t.Fatal("invalid key registered")
			}
		})
	}
}

func TestAllNetworkAppearancesRegistrationCountsStaticMembership(t *testing.T) {
	for _, provisioned := range []bool{false, true} {
		t.Run(map[bool]string{false: "unprovisioned", true: "provisioned"}[provisioned], func(t *testing.T) {
			var keys []ProvisionedRoutingKey
			if provisioned {
				keys = []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: testRoutingKey(10, 100, 3)}, {RoutingContext: 8, RoutingKey: testRoutingKey(20, 200, 3)}}
			}
			r := registrationRegistry(t, keys...)
			cfg := NewAssociationConfig()
			cfg.ApplicationServers = []ASConfig{
				{ASKey: ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}},
				{ASKey: ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 8, RoutingContextSet: true}},
			}
			c := newAssociation(RoleSGP, cfg)
			c.as = newApplicationServers(time.Hour)
			defer c.as.close()
			c.as.register(c.staticallyConfiguredASKeys())
			c.as.aspStateChanged(c, StateASPInactive)
			key := testRoutingKey(0, 300, 3)
			key.NetworkAppearanceSet = false
			result := r.register(c, []RoutingKeyRegistrationRequest{{LocalRoutingKeyIdentifier: 3, RoutingKey: key}})[0]
			if result.Status != RegistrationCannotSupportUniqueRouting || result.RoutingContext != 0 {
				t.Fatalf("result = %+v", result)
			}
			if r.dynamicCount() != 0 {
				t.Fatal("all-NA key installed")
			}
		})
	}
}

func TestRegistrationResponseAcceptsActualZeroRoutingContext(t *testing.T) {
	for _, status := range []uint32{params.SuccessfullyRegistered, params.RoutingKeyAlreadyRegistered} {
		wire := registrationWire(messages.MsgTypeRegistrationResponse, registrationTLV(params.RegistrationResult,
			registrationWord(params.LocalRoutingKeyIdentifier, 1), registrationWord(params.RegistrationStatus, status), registrationWord(params.RoutingContext, 0)))
		decoded, err := messages.Parse(wire)
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		response := decoded.(*messages.RegistrationResponse)
		result, err := response.RegistrationResults[0].RegistrationResult()
		if err != nil || result.RoutingContext.RoutingContext() != 0 || result.RegistrationStatus.RegistrationStatus() != status {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
}

func TestRegistrationOmittedAppearanceUsesSingleStaticAppearance(t *testing.T) {
	c, sent := newTestConn(t, StateASPInactive, RoleSGP)
	c.cfg.ApplicationServers = []ASConfig{
		{ASKey: ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}},
		{ASKey: ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 8, RoutingContextSet: true}},
	}
	c.as = newApplicationServers(time.Hour)
	defer c.as.close()
	c.as.register(c.staticallyConfiguredASKeys())
	c.as.aspStateChanged(c, StateASPInactive)
	r := registrationRegistry(t)
	c.endpoint = &Endpoint{routingKeys: r}
	c.dispatchRaw(context.Background(), inbound{data: registrationWire(messages.MsgTypeRegistrationRequest, registrationTLV(params.RoutingKey,
		registrationWord(params.LocalRoutingKeyIdentifier, 1), registrationWord(params.DestinationPointCode, 300))), ppid: M3UAPPID})
	if err := firstErr(c); err != nil {
		t.Fatal(err)
	}
	for _, m := range *sent {
		if response, ok := m.(*messages.RegistrationResponse); ok {
			result, err := response.RegistrationResults[0].RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if result.RegistrationStatus.RegistrationStatus() != params.SuccessfullyRegistered {
				t.Fatalf("result=%v", result)
			}
			key, ok := r.asKey(result.RoutingContext.RoutingContext())
			if !ok || !key.NetworkAppearanceSet || key.NetworkAppearance != 10 {
				t.Fatalf("key=%+v exists=%v", key, ok)
			}
			return
		}
	}
	t.Fatal("no REG RSP")
}

func TestRegistrationRequesterBindsActualZeroRoutingContext(t *testing.T) {
	for _, status := range []uint32{params.SuccessfullyRegistered, params.RoutingKeyAlreadyRegistered} {
		t.Run(map[uint32]string{params.SuccessfullyRegistered: "success", params.RoutingKeyAlreadyRegistered: "already registered"}[status], func(t *testing.T) {
			c := newAssociation(RoleASP, NewAssociationConfig())
			c.muState.Lock()
			c.state = StateASPInactive
			c.muState.Unlock()
			defer func() { _ = c.Close() }()
			c.signalWriter = func(m messages.M3UA) (int, error) {
				request := m.(*messages.RegistrationRequest)
				key, err := request.RoutingKeys[0].RoutingKey()
				if err != nil {
					return 0, err
				}
				raw := registrationWire(messages.MsgTypeRegistrationResponse, registrationTLV(params.RegistrationResult,
					registrationWord(params.LocalRoutingKeyIdentifier, key.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()), registrationWord(params.RegistrationStatus, status), registrationWord(params.RoutingContext, 0)))
				response, err := messages.Parse(raw)
				if err != nil {
					return 0, err
				}
				return m.MarshalLen(), c.handleRegistrationResponse(response.(*messages.RegistrationResponse))
			}
			results, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 1 || results[0].Status != RegistrationStatus(status) || results[0].RoutingContext != 0 || !results[0].ASKey.RoutingContextSet {
				t.Fatalf("results=%+v", results)
			}
			key, ok := c.dynamicASKey(0, false)
			if !ok || key.RoutingContext != 0 || !key.RoutingContextSet {
				t.Fatalf("binding=%+v exists=%v", key, ok)
			}
		})
	}
}

func FuzzRegistrationRequestSemanticFault(f *testing.F) {
	id := registrationWord(params.LocalRoutingKeyIdentifier, 1)
	for _, parts := range [][][]byte{
		{id, registrationWord(params.DestinationPointCode, 0x01000064)},
		{id, registrationWord(params.DestinationPointCode, 100), registrationWord(params.TrafficModeType, 4)},
		{id, registrationTLV(params.RoutingKey, id, registrationWord(params.DestinationPointCode, 0x01000064))},
		{id, registrationWord(params.DestinationPointCode, 0x01000064), registrationTLV(params.RoutingContext, []byte{0, 0, 0, 1, 0, 0, 0, 2})},
		{id}, {id, id},
	} {
		f.Add(registrationTLV(params.RoutingKey, parts...))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		m, err := messages.Parse(registrationWire(messages.MsgTypeRegistrationRequest, body))
		if err != nil {
			return
		}
		request := m.(*messages.RegistrationRequest)
		for _, key := range request.RoutingKeys {
			if _, err := key.RoutingKey(); err != nil {
				// A deferred key must still fail strict origination validation.
				if _, err := key.MarshalBinary(); err == nil {
					t.Fatal("invalid received key can be originated")
				}
			}
		}
	})
}

func TestAllNetworkAppearancesRegistrationCountsExplicitContextlessMembership(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "implicit default", true: "explicit static AS"}[explicit], func(t *testing.T) {
			cfg := NewAssociationConfig()
			if explicit {
				cfg.ApplicationServers = []ASConfig{{TrafficMode: params.TrafficModeOverride}}
			}
			c := newAssociation(RoleSGP, cfg)
			c.as = newApplicationServers(time.Hour)
			defer c.as.close()
			c.as.register(c.configuredASKeys())
			c.as.aspStateChanged(c, StateASPInactive)
			r := registrationRegistry(t)
			result := r.register(c, []RoutingKeyRegistrationRequest{{LocalRoutingKeyIdentifier: 1, RoutingKey: testRoutingKey(10, 100, 3)}})[0]
			want := RegistrationSuccessfullyRegistered
			if explicit {
				want = RegistrationCannotSupportUniqueRouting
			}
			if result.Status != want {
				t.Fatalf("result=%+v, want status %d", result, want)
			}
		})
	}
}
