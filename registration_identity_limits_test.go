package m3ua

import (
	"context"
	"errors"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"strings"
	"testing"
)

func TestUnprovisionedRegistrationIdentityLimits(t *testing.T) {
	for _, model := range []string{"asp", "ipsp-single", "ipsp-double"} {
		role := RoleASP
		cfg := NewAssociationConfig()
		if model != "asp" {
			role = RoleIPSP
		}
		if model == "ipsp-single" {
			cfg = newDoubleExchangeAssociationConfigForTest()
			cfg.IPSP.ExchangeModel = IPSPExchangeSingle
			cfg.IPSP.ASPSMExchange = 0
			cfg.IPSP.TrafficToLocal = nil
			cfg.IPSP.TrafficToPeer = nil
		}
		if model == "ipsp-double" {
			cfg = newDoubleExchangeAssociationConfigForTest()
		}
		for _, size := range []int{255, 256} {
			t.Run(model+"/"+strings.Repeat("x", size-254), func(t *testing.T) {
				a := newAssociation(role, cfg)
				a.state = StateASPInactive
				if model == "ipsp-double" {
					a.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPDown})
				}
				t.Cleanup(func() { _ = a.Close() })
				writes := 0
				a.signalWriter = func(m messages.M3UA) (int, error) {
					writes++
					request := m.(*messages.RegistrationRequest)
					for _, p := range request.RoutingKeys {
						key, err := p.RoutingKey()
						if err != nil {
							return 0, err
						}
						response := messages.NewRegistrationResponse(params.NewRegistrationResult(params.NewRegistrationResultPayload(key.LocalRoutingKeyIdentifier.Copy(), params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(10))))
						if err := a.handleRegistrationResponse(response); err != nil {
							return 0, err
						}
					}
					return m.MarshalLen(), nil
				}
				name := RemoteASID(strings.Repeat("é", size/2) + strings.Repeat("x", size%2))
				results, err := a.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RemoteAS: name, RoutingKey: testRoutingKey(10, 100, 3)})
				if size == 255 {
					if err != nil || len(results) != 1 || results[0].RemoteAS.ApplicationServer != name || writes != 1 {
						t.Fatal("maximum dynamic identity not preserved", results, err, writes)
					}
				} else {
					requireIdentityLengthDetails(t, err, 256, 255)
					if !errors.Is(err, ErrInvalidParameterValue) {
						t.Error("overlong registration identity not refused with typed parameter error", err)
					}
					if writes != 0 || len(results) != 0 || len(a.canonicalRemoteAS) != 0 || len(a.rkmRegistrationRemoteAS) != 0 || len(a.dynamicPeerASKeys) != 0 || len(a.dynamicLocalASKeys) != 0 {
						t.Error("overlong registration reached transport or retention", writes, len(results))
					}
				}
			})
		}
	}
}

func TestUnprovisionedPeerGatewayIdentityLimit(t *testing.T) {
	for _, size := range []int{255, 256} {
		cfg := NewAssociationConfig()
		cfg.PeerSGP = &SGPIdentity{SignallingGateway: SignallingGatewayID(strings.Repeat("g", size)), SignallingGatewayProcess: "p"}
		err := validateAssociationConfigForRole(RoleASP, cfg)
		if size == 255 && err != nil {
			t.Fatal("maximum peer SG identity refused", err)
		}
		if size == 256 && !errors.Is(err, ErrInvalidRoleConfiguration) {
			t.Fatal("overlong unprovisioned peer SG accepted", err)
		}
	}
}

func TestRegistrationIdentityValidationIsAtomicAcrossBatch(t *testing.T) {
	for _, badFirst := range []bool{false, true} {
		a := newAssociation(RoleASP, NewAssociationConfig())
		a.state = StateASPInactive
		writes := 0
		a.signalWriter = func(messages.M3UA) (int, error) { writes++; return 0, errors.New("unexpected registration write") }
		good := RoutingKeyRegistration{RemoteAS: "valid", RoutingKey: testRoutingKey(10, 100, 3)}
		bad := RoutingKeyRegistration{RemoteAS: RemoteASID(strings.Repeat("b", 256)), RoutingKey: testRoutingKey(20, 200, 3)}
		batch := []RoutingKeyRegistration{good, bad}
		if badFirst {
			batch = []RoutingKeyRegistration{bad, good}
		}
		results, err := a.RegisterRoutingKeys(context.Background(), batch...)
		requireIdentityLengthDetails(t, err, 256, 255)
		if writes != 0 || len(results) != 0 || len(a.rkmRegistrationRemoteAS) != 0 || len(a.dynamicPeerASKeys) != 0 {
			t.Fatal("mixed invalid registration batch partially published")
		}
	}
}

func TestRegistrationResponseChecksSavedIdentityBeforeDelivery(t *testing.T) {
	a := newAssociation(RoleASP, NewAssociationConfig())
	a.state = StateASPInactive
	a.signalWriter = func(m messages.M3UA) (int, error) {
		request := m.(*messages.RegistrationRequest)
		key, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		id := key.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		// The wire response has no name. Check the response boundary's defensive
		// validation of the identity associated with its Local RK Identifier.
		a.rkmCorrelationMu.Lock()
		a.rkmRegistrationRemoteAS[id] = SGASKey{ApplicationServer: RemoteASID(strings.Repeat("x", 256))}
		a.rkmCorrelationMu.Unlock()
		reply := messages.NewRegistrationResponse(params.NewRegistrationResult(params.NewRegistrationResultPayload(key.LocalRoutingKeyIdentifier.Copy(), params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(10))))
		err = a.handleRegistrationResponse(reply)
		if err == nil {
			t.Error("invalid saved identity passed response correlation")
		}
		return 0, err
	}
	_, err := a.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RemoteAS: "valid", RoutingKey: testRoutingKey(10, 100, 3)})
	requireIdentityLengthDetails(t, err, 256, 255)
	if len(a.dynamicPeerASKeys) != 0 || len(a.canonicalRemoteAS) != 0 {
		t.Fatal("invalid response identity installed")
	}
}
