package m3ua

import (
	"errors"
	"fmt"
	"net"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestCanonicalRoutingKeysIgnoreSetOrdering(t *testing.T) {
	first := RoutingKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		Groups: []RoutingKeyGroup{
			{
				DestinationPointCode: 2,
				ServiceIndicators:    []uint8{5, 3, 5},
				OriginatingPointCodes: []PointCodeRange{
					{PointCode: 0x1201, Mask: 4},
					{PointCode: 0x120f, Mask: 4},
				},
			},
			{DestinationPointCode: 1},
		},
	}
	second := RoutingKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		Groups: []RoutingKeyGroup{
			{DestinationPointCode: 1},
			{
				DestinationPointCode: 2,
				ServiceIndicators:    []uint8{3, 5},
				OriginatingPointCodes: []PointCodeRange{
					{PointCode: 0x1200, Mask: 4},
				},
			},
		},
	}

	firstCanonical, err := canonicalizeRoutingKey(first)
	if err != nil {
		t.Fatalf("canonicalize first Routing Key: %v", err)
	}
	secondCanonical, err := canonicalizeRoutingKey(second)
	if err != nil {
		t.Fatalf("canonicalize second Routing Key: %v", err)
	}
	if !firstCanonical.equal(secondCanonical) {
		t.Fatalf("equivalent Routing Keys differ:\n%+v\n%+v", firstCanonical, secondCanonical)
	}
}

func TestCanonicalRoutingKeyMaskAtPointCodeWidthMatchesAllPointCodes(t *testing.T) {
	key := RoutingKey{Groups: []RoutingKeyGroup{{
		DestinationPointCode:  100,
		OriginatingPointCodes: []PointCodeRange{{PointCode: 0x123456, Mask: 24}},
	}}}
	canonical, err := canonicalizeRoutingKey(key)
	if err != nil {
		t.Fatalf("canonicalizeRoutingKey: %v", err)
	}
	ranges := canonical.groups[0].originatingPointCodes
	if len(ranges) != 1 || ranges[0].mask != 24 {
		t.Fatalf("canonical ranges = %+v, want one mask-24 range", ranges)
	}
	lower, upper := ranges[0].bounds()
	if lower != 0 || upper != 0x00ffffff {
		t.Fatalf("range bounds = %#x-%#x, want all 24-bit point codes", lower, upper)
	}
}

func TestCanonicalRoutingKeyRejectsMaskWiderThanPointCode(t *testing.T) {
	for _, mask := range []uint8{25, 255} {
		t.Run(fmt.Sprintf("mask %d", mask), func(t *testing.T) {
			key := RoutingKey{Groups: []RoutingKeyGroup{{
				DestinationPointCode:  100,
				OriginatingPointCodes: []PointCodeRange{{PointCode: 0x123456, Mask: mask}},
			}}}
			if _, err := canonicalizeRoutingKey(key); err == nil {
				t.Fatalf("canonicalizeRoutingKey accepted mask %d", mask)
			}
		})
	}
}

func TestRoutingKeyManagementConfigRejectsWideOriginatingPointCodeMask(t *testing.T) {
	key := RoutingKey{Groups: []RoutingKeyGroup{{
		DestinationPointCode:  100,
		OriginatingPointCodes: []PointCodeRange{{PointCode: 0x123456, Mask: 25}},
	}}}
	_, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: key}},
	})
	if err == nil {
		t.Fatal("newRoutingKeyRegistry accepted a provisioned mask wider than 24 bits")
	}
}

func TestRoutingKeyOverlapUsesNetworkAppearanceSIAndOPCMasks(t *testing.T) {
	base := RoutingKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		Groups: []RoutingKeyGroup{{
			DestinationPointCode:  100,
			ServiceIndicators:     []uint8{3, 5},
			OriginatingPointCodes: []PointCodeRange{{PointCode: 0x1200, Mask: 8}},
		}},
	}
	tests := []struct {
		name    string
		other   RoutingKey
		overlap bool
	}{
		{
			name: "intersecting service and originating point code",
			other: RoutingKey{
				NetworkAppearance:    10,
				NetworkAppearanceSet: true,
				Groups: []RoutingKeyGroup{{
					DestinationPointCode:  100,
					ServiceIndicators:     []uint8{5},
					OriginatingPointCodes: []PointCodeRange{{PointCode: 0x1280, Mask: 4}},
				}},
			},
			overlap: true,
		},
		{
			name: "different Network Appearance",
			other: RoutingKey{
				NetworkAppearance:    20,
				NetworkAppearanceSet: true,
				Groups:               []RoutingKeyGroup{{DestinationPointCode: 100}},
			},
			overlap: false,
		},
		{
			name: "different Destination Point Code",
			other: RoutingKey{
				NetworkAppearance:    10,
				NetworkAppearanceSet: true,
				Groups:               []RoutingKeyGroup{{DestinationPointCode: 101}},
			},
			overlap: false,
		},
		{
			name: "disjoint Service Indicators",
			other: RoutingKey{
				NetworkAppearance:    10,
				NetworkAppearanceSet: true,
				Groups: []RoutingKeyGroup{{
					DestinationPointCode: 100,
					ServiceIndicators:    []uint8{4},
				}},
			},
			overlap: false,
		},
		{
			name: "disjoint Originating Point Code ranges",
			other: RoutingKey{
				NetworkAppearance:    10,
				NetworkAppearanceSet: true,
				Groups: []RoutingKeyGroup{{
					DestinationPointCode:  100,
					ServiceIndicators:     []uint8{3},
					OriginatingPointCodes: []PointCodeRange{{PointCode: 0x1300, Mask: 4}},
				}},
			},
			overlap: false,
		},
		{
			name: "omitted Network Appearance overlaps explicit appearance",
			other: RoutingKey{
				Groups: []RoutingKeyGroup{{DestinationPointCode: 100}},
			},
			overlap: true,
		},
	}

	canonicalBase, err := canonicalizeRoutingKey(base)
	if err != nil {
		t.Fatalf("canonicalize base Routing Key: %v", err)
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			other, err := canonicalizeRoutingKey(test.other)
			if err != nil {
				t.Fatalf("canonicalize other Routing Key: %v", err)
			}
			if got := canonicalBase.overlaps(other); got != test.overlap {
				t.Fatalf("overlaps = %v, want %v", got, test.overlap)
			}
		})
	}
}

func TestRoutingKeyManagementConfigIsDeepSnapshotted(t *testing.T) {
	authorize := func(RoutingKeyRegistrationRequest) RegistrationStatus {
		return RegistrationSuccessfullyRegistered
	}
	config := &RoutingKeyManagementConfig{
		AuthorizeRegistration: authorize,
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
			RoutingContext: 7,
			RoutingKey: RoutingKey{
				Groups: []RoutingKeyGroup{{
					DestinationPointCode:  1,
					ServiceIndicators:     []uint8{params.ServiceIndSCCP},
					OriginatingPointCodes: []PointCodeRange{{PointCode: 2}},
				}},
			},
		}},
	}

	snapshot := snapshotRoutingKeyManagementConfig(config)
	config.ProvisionedRoutingKeys[0].RoutingContext = 9
	config.ProvisionedRoutingKeys[0].RoutingKey.Groups[0].ServiceIndicators[0] = params.ServiceIndISUP
	config.ProvisionedRoutingKeys[0].RoutingKey.Groups[0].OriginatingPointCodes[0].PointCode = 3

	provisioned := snapshot.ProvisionedRoutingKeys[0]
	if provisioned.RoutingContext != 7 ||
		provisioned.RoutingKey.Groups[0].ServiceIndicators[0] != params.ServiceIndSCCP ||
		provisioned.RoutingKey.Groups[0].OriginatingPointCodes[0].PointCode != 2 {
		t.Fatalf("snapshot changed after caller mutation: %+v", provisioned)
	}
}

func TestRoutingKeyRegistrationRequestSnapshotOwnsPeerAddress(t *testing.T) {
	request := RoutingKeyRegistrationRequest{
		Peer: RoutingKeyPeer{RemoteAddr: &sctp.SCTPAddr{
			IPAddrs: []net.IPAddr{{IP: net.IPv4(192, 0, 2, 1), Zone: "zone"}},
			Port:    2905,
		}},
		RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
	}
	snapshot := snapshotRoutingKeyRegistrationRequest(request)
	request.Peer.RemoteAddr.IPAddrs[0].IP[len(request.Peer.RemoteAddr.IPAddrs[0].IP)-1] = 9
	request.Peer.RemoteAddr.IPAddrs[0].Zone = "changed"
	request.Peer.RemoteAddr.Port = 1

	if snapshot.Peer.RemoteAddr.Port != 2905 || snapshot.Peer.RemoteAddr.IPAddrs[0].Zone != "zone" || !snapshot.Peer.RemoteAddr.IPAddrs[0].IP.Equal(net.IPv4(192, 0, 2, 1)) {
		t.Fatalf("request snapshot address changed after caller mutation: %+v", snapshot.Peer.RemoteAddr)
	}
}

func TestEndpointValidatesRoutingKeyManagementRoleAndPolicy(t *testing.T) {
	valid := &RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
	}
	tests := []struct {
		name   string
		config EndpointConfig
		want   error
	}{
		{
			name:   "ASP cannot configure responder policy",
			config: EndpointConfig{Role: RoleASP, RoutingKeyManagement: valid},
			want:   ErrInvalidRoleConfiguration,
		},
		{
			name:   "SGP requires authorization policy",
			config: EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{}},
			want:   ErrInvalidRoleConfiguration,
		},
		{
			name: "duplicate provisioned Routing Context",
			config: EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{
				AuthorizeRegistration: valid.AuthorizeRegistration,
				ProvisionedRoutingKeys: []ProvisionedRoutingKey{
					{RoutingContext: 1, RoutingKey: RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 1}}}},
					{RoutingContext: 1, RoutingKey: RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 2}}}},
				},
			}},
			want: ErrInvalidRoleConfiguration,
		},
		{
			name: "overlapping provisioned Routing Keys",
			config: EndpointConfig{Role: RoleIPSP, RoutingKeyManagement: &RoutingKeyManagementConfig{
				AuthorizeRegistration: valid.AuthorizeRegistration,
				ProvisionedRoutingKeys: []ProvisionedRoutingKey{
					{RoutingContext: 1, RoutingKey: RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 1}}}},
					{RoutingContext: 2, RoutingKey: RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 1, ServiceIndicators: []uint8{3}}}}},
				},
			}},
			want: ErrInvalidRoleConfiguration,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(test.config)
			if endpoint != nil {
				_ = endpoint.Close()
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("NewEndpoint error = %v, want %v", err, test.want)
			}
		})
	}

	for _, role := range []Role{RoleSGP, RoleIPSP} {
		endpoint, err := NewEndpoint(EndpointConfig{Role: role, RoutingKeyManagement: valid})
		if err != nil {
			t.Fatalf("NewEndpoint(%s): %v", role, err)
		}
		_ = endpoint.Close()
	}
}
