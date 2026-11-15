package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-sctp"
)

func TestListenerRejectsZeroMethodDefaultEvenWithSelector(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP, RoleSGP} {
		t.Run(role.String(), func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: role, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			invalid := NewAssociationConfig()
			invalid.CongestionProfiles = []NetworkCongestionProfile{{}}
			listenerConfig := NewListenerConfig(invalid)
			listenerConfig.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return NewAssociationConfig(), nil }
			// Even a regression cannot open a native socket with this unknown network.
			_, err = endpoint.Listen("invalid-network", &sctp.Addr{}, listenerConfig)
			if !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("zero method in listener default was accepted: %v", err)
			}
		})
	}
}
