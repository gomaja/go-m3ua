package m3ua

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSSNMOperationValidation(t *testing.T) {
	validAudit := DestinationStateAuditRequest{
		Scope: SSNMScope{
			NetworkAppearance:    7,
			NetworkAppearanceSet: true,
			RoutingContexts:      []uint32{1},
			RoutingContextSet:    true,
		},
		Destinations: []PointCodeRange{{PointCode: 0x123456}},
	}

	t.Run("DAUD role state and request", func(t *testing.T) {
		tests := []struct {
			name    string
			role    Role
			state   State
			request DestinationStateAuditRequest
			want    error
		}{
			{name: "valid", role: RoleASP, state: StateASPActive, request: validAudit},
			{name: "SGP role", role: RoleSGP, state: StateASPActive, request: validAudit, want: ErrUnsupportedRole},
			{name: "IPSP role", role: RoleIPSP, state: StateASPActive, request: validAudit, want: ErrUnsupportedRole},
			{name: "inactive", role: RoleASP, state: StateASPInactive, request: validAudit, want: ErrInvalidState},
			{name: "empty destinations", role: RoleASP, state: StateASPActive, request: withAuditDestinations(validAudit, nil), want: ErrMissingAffectedPointCode},
			{name: "point code too large", role: RoleASP, state: StateASPActive, request: withAuditDestinations(validAudit, []PointCodeRange{{PointCode: 0x1000000}}), want: ErrInvalidParameterValue},
			{name: "mask too large", role: RoleASP, state: StateASPActive, request: withAuditDestinations(validAudit, []PointCodeRange{{PointCode: 1, Mask: 25}}), want: ErrInvalidParameterValue},
			{name: "present empty RC", role: RoleASP, state: StateASPActive, request: withAuditScope(validAudit, SSNMScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true}), want: ErrMissingRoutingContext},
			{name: "duplicate RC", role: RoleASP, state: StateASPActive, request: withAuditScope(validAudit, SSNMScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1, 1}, RoutingContextSet: true}), want: ErrInvalidParameterValue},
			{name: "unknown RC", role: RoleASP, state: StateASPActive, request: withAuditScope(validAudit, SSNMScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{2}, RoutingContextSet: true}), want: ErrInvalidRoutingContext},
			{name: "wrong Network Appearance", role: RoleASP, state: StateASPActive, request: withAuditScope(validAudit, SSNMScope{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true}), want: ErrInvalidNetworkAppearance},
			{name: "invalid UTF-8 info", role: RoleASP, state: StateASPActive, request: withAuditInfo(validAudit, string([]byte{0xff})), want: ErrInvalidParameterValue},
			{name: "oversized info", role: RoleASP, state: StateASPActive, request: withAuditInfo(validAudit, string(make([]byte, 256))), want: ErrInvalidParameterValue},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				association, _ := newTestConnWithContexts(t, test.state, test.role, 1)
				association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
				association.noteRoutingContextsActive([]uint32{1})
				writes := 0
				association.signalWriter = func(message messages.M3UA) (int, error) {
					writes++
					return message.MarshalLen(), nil
				}
				err := association.DestinationStateAudit(test.request)
				if test.want == nil && err != nil {
					t.Fatalf("DestinationStateAudit: %v", err)
				}
				if test.want != nil && !errors.Is(err, test.want) {
					t.Fatalf("DestinationStateAudit error = %v, want %v", err, test.want)
				}
				wantWrites := 1
				if test.want != nil {
					wantWrites = 0
				}
				if writes != wantWrites {
					t.Fatalf("DestinationStateAudit writes = %d, want %d", writes, wantWrites)
				}
			})
		}
	})

	t.Run("SCON optional fields and direction", func(t *testing.T) {
		for level := uint8(0); level <= 3; level++ {
			association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
			association.noteRoutingContextsActive([]uint32{1})
			writes := 0
			association.signalWriter = func(message messages.M3UA) (int, error) {
				writes++
				return message.MarshalLen(), nil
			}
			err := association.SignallingCongestion(SignallingCongestionRequest{
				Scope:                   validAudit.Scope,
				Destinations:            validAudit.Destinations,
				CongestionLevel:         level,
				CongestionLevelSet:      true,
				ConcernedDestination:    0x654321,
				ConcernedDestinationSet: true,
			})
			if err != nil {
				t.Fatalf("level %d: %v", level, err)
			}
			if writes != 1 {
				t.Fatalf("level %d writes = %d, want 1", level, writes)
			}
		}

		association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
		association.noteRoutingContextsActive([]uint32{1})
		writes := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
		invalidLevel := SignallingCongestionRequest{
			Scope:              validAudit.Scope,
			Destinations:       validAudit.Destinations,
			CongestionLevel:    4,
			CongestionLevelSet: true,
		}
		if err := association.SignallingCongestion(invalidLevel); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("level 4 error = %v, want ErrInvalidParameterValue", err)
		}
		invalidConcerned := SignallingCongestionRequest{
			Scope:                   validAudit.Scope,
			Destinations:            validAudit.Destinations,
			ConcernedDestination:    0x1000000,
			ConcernedDestinationSet: true,
		}
		if err := association.SignallingCongestion(invalidConcerned); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("oversized Concerned Destination error = %v, want ErrInvalidParameterValue", err)
		}
		if writes != 0 {
			t.Fatalf("invalid SCON requests wrote %d messages, want 0", writes)
		}

		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope:                   validAudit.Scope,
			Destinations:            validAudit.Destinations,
			ConcernedDestination:    1,
			ConcernedDestinationSet: true,
		}); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("SGP Concerned Destination error = %v, want ErrInvalidParameterValue", err)
		}
	})

	t.Run("DUPU user cause and destination", func(t *testing.T) {
		valid := DestinationUserPartUnavailableRequest{
			Scope:       validAudit.Scope,
			Destination: PointCodeRange{PointCode: 0x123456},
			User:        params.SCCP,
			Cause:       params.Unequipped,
		}
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		if err := endpoint.DestinationUserPartUnavailable(valid); err != nil {
			t.Fatalf("valid DUPU: %v", err)
		}
		for _, test := range []struct {
			name    string
			request DestinationUserPartUnavailableRequest
		}{
			{name: "masked destination", request: withDUPUDestination(valid, PointCodeRange{PointCode: 1, Mask: 1})},
			{name: "oversized destination", request: withDUPUDestination(valid, PointCodeRange{PointCode: 0x1000000})},
			{name: "reserved user", request: withDUPUUser(valid, 6)},
			{name: "unknown user", request: withDUPUUser(valid, 16)},
			{name: "unknown cause", request: withDUPUCause(valid, 3)},
		} {
			t.Run(test.name, func(t *testing.T) {
				if err := endpoint.DestinationUserPartUnavailable(test.request); !errors.Is(err, ErrInvalidParameterValue) {
					t.Fatalf("DUPU error = %v, want ErrInvalidParameterValue", err)
				}
			})
		}
	})

	t.Run("Endpoint role rejects before association writes", func(t *testing.T) {
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope:        validAudit.Scope,
			Destinations: validAudit.Destinations,
		}); !errors.Is(err, ErrUnsupportedRole) {
			t.Fatalf("ASP Endpoint SCON error = %v, want ErrUnsupportedRole", err)
		}
		if err := endpoint.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{
			Scope:       validAudit.Scope,
			Destination: validAudit.Destinations[0],
			User:        params.SCCP,
			Cause:       params.UnknownCause,
		}); !errors.Is(err, ErrUnsupportedRole) {
			t.Fatalf("ASP Endpoint DUPU error = %v, want ErrUnsupportedRole", err)
		}
	})
}

func TestSSNMOperationAssociationWire(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(0)
	association.noteRoutingContextsActive([]uint32{1, 2})
	writes := make(chan messages.M3UA, 4)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	audit := DestinationStateAuditRequest{
		Scope: SSNMScope{
			NetworkAppearance:    0,
			NetworkAppearanceSet: true,
			RoutingContexts:      []uint32{2, 1},
			RoutingContextSet:    true,
		},
		Destinations: []PointCodeRange{
			{PointCode: 0x123456, Mask: 0},
			{PointCode: 0x654321, Mask: 8},
		},
		Info: "audit",
	}
	if err := association.DestinationStateAudit(audit); err != nil {
		t.Fatalf("DestinationStateAudit: %v", err)
	}
	message, ok := (<-writes).(*messages.DestinationStateAudit)
	if !ok {
		t.Fatalf("DAUD write = %T", message)
	}
	assertSSNMOperationScope(t,
		message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode,
		audit.Scope, audit.Destinations,
	)
	if message.InfoString == nil || message.InfoString.InfoString() != audit.Info {
		t.Fatalf("DAUD Info String = %v, want %q", message.InfoString, audit.Info)
	}

	if err := association.SignallingCongestion(SignallingCongestionRequest{
		Scope:                   audit.Scope,
		Destinations:            audit.Destinations,
		ConcernedDestination:    0x010203,
		ConcernedDestinationSet: true,
	}); err != nil {
		t.Fatalf("SignallingCongestion omitted level: %v", err)
	}
	omitted, ok := (<-writes).(*messages.SignallingCongestion)
	if !ok {
		t.Fatalf("SCON write = %T", omitted)
	}
	assertSSNMOperationScope(t,
		omitted.NetworkAppearance, omitted.RoutingContext, omitted.AffectedPointCode,
		audit.Scope, audit.Destinations,
	)
	if omitted.CongestionIndications != nil {
		t.Fatalf("omitted SCON congestion parameter = %v, want nil", omitted.CongestionIndications)
	}
	if omitted.ConcernedDestination == nil ||
		omitted.ConcernedDestination.ConcernedDestination() != 0x010203 {
		t.Fatalf("SCON Concerned Destination = %v", omitted.ConcernedDestination)
	}

	if err := association.SignallingCongestion(SignallingCongestionRequest{
		Scope:              audit.Scope,
		Destinations:       audit.Destinations[:1],
		CongestionLevel:    0,
		CongestionLevelSet: true,
	}); err != nil {
		t.Fatalf("SignallingCongestion level zero: %v", err)
	}
	abatement := (<-writes).(*messages.SignallingCongestion)
	if abatement.CongestionIndications == nil ||
		abatement.CongestionIndications.CongestionLevel() != 0 {
		t.Fatalf("SCON level-zero parameter = %v", abatement.CongestionIndications)
	}
	if abatement.ConcernedDestination != nil {
		t.Fatalf("SCON absent Concerned Destination = %v, want nil", abatement.ConcernedDestination)
	}
	if _, known := association.destinations.lookup(destinationKey{
		networkAppearance: 0, networkAppearanceSet: true,
		routingContext: 1, routingContextSet: true,
		pointCode: 0x123456,
	}); known {
		t.Fatal("locally originated ASP SSNM changed peer destination state")
	}
}

func TestSSNMOperationContextlessAssociationOmitsRoutingContext(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.noteRoutingContextsActive(nil)
	writes := make(chan messages.M3UA, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	if err := association.DestinationStateAudit(DestinationStateAuditRequest{
		Scope: SSNMScope{
			NetworkAppearance:    7,
			NetworkAppearanceSet: true,
		},
		Destinations: []PointCodeRange{{PointCode: 1}},
	}); err != nil {
		t.Fatalf("contextless DAUD: %v", err)
	}
	audit := (<-writes).(*messages.DestinationStateAudit)
	if audit.RoutingContext != nil {
		t.Fatalf("contextless DAUD Routing Context = %v, want nil", audit.RoutingContext)
	}
}

func TestSSNMOperationEndpointFanout(t *testing.T) {
	t.Run("SCON selects exact AS and records level", func(t *testing.T) {
		endpoint, _, firstSent, second, secondSent := multiAssociationDialedSGPFixture(t)
		if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope: SSNMScope{
				NetworkAppearance:    7,
				NetworkAppearanceSet: true,
				RoutingContexts:      []uint32{2},
				RoutingContextSet:    true,
			},
			Destinations:       []PointCodeRange{{PointCode: 0x123456, Mask: 4}},
			CongestionLevel:    2,
			CongestionLevelSet: true,
			Info:               "level two",
		}); err != nil {
			t.Fatalf("Endpoint.SignallingCongestion: %v", err)
		}
		if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
			t.Fatalf("unconcerned ASP received %d SSNM messages, want 0", got)
		}
		reports := ssnmMessages(secondSent.snapshot())
		if len(reports) != 1 {
			t.Fatalf("concerned ASP received %d SSNM messages, want 1", len(reports))
		}
		report, ok := reports[0].(*messages.SignallingCongestion)
		if !ok {
			t.Fatalf("concerned report = %T, want SCON", reports[0])
		}
		if report.CongestionIndications == nil || report.CongestionIndications.CongestionLevel() != 2 {
			t.Fatalf("SCON congestion = %v, want 2", report.CongestionIndications)
		}
		if report.ConcernedDestination != nil {
			t.Fatalf("SGP SCON Concerned Destination = %v, want nil", report.ConcernedDestination)
		}
		status, ok := endpoint.DestinationStatus(DestinationStatusKey{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContext: 2, RoutingContextSet: true,
			PointCode: 0x123456, Mask: 4,
		})
		if !ok || status.State != DestinationCongested ||
			!status.CongestionLevelSet || status.CongestionLevel != 2 {
			t.Fatalf("recorded SCON status = %+v, %v", status, ok)
		}
		if second.ID() == 0 {
			t.Fatal("fixture Association has no Endpoint identity")
		}
	})

	t.Run("DUPU selects exact AS", func(t *testing.T) {
		endpoint, first, firstSent, _, secondSent := multiAssociationDialedSGPFixture(t)
		if err := endpoint.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{
			Scope: SSNMScope{
				NetworkAppearance:    7,
				NetworkAppearanceSet: true,
				RoutingContexts:      []uint32{1},
				RoutingContextSet:    true,
			},
			Destination: PointCodeRange{PointCode: 0x123456},
			User:        params.ISUP,
			Cause:       params.Inaccessible,
			Info:        "unavailable",
		}); err != nil {
			t.Fatalf("Endpoint.DestinationUserPartUnavailable: %v", err)
		}
		reports := ssnmMessages(firstSent.snapshot())
		if len(reports) != 1 {
			t.Fatalf("concerned ASP received %d SSNM messages, want 1", len(reports))
		}
		report, ok := reports[0].(*messages.DestinationUserPartUnavailable)
		if !ok {
			t.Fatalf("concerned report = %T, want DUPU", reports[0])
		}
		if report.UserCause == nil || report.UserCause.UserIdentity() != params.ISUP ||
			report.UserCause.UnavailabilityCause() != params.Inaccessible {
			t.Fatalf("DUPU User/Cause = %v", report.UserCause)
		}
		if got := len(ssnmMessages(secondSent.snapshot())); got != 0 {
			t.Fatalf("unconcerned ASP received %d SSNM messages, want 0", got)
		}
		if first.ID() == 0 {
			t.Fatal("fixture Association has no Endpoint identity")
		}
	})

	t.Run("mixed contextless and RC scopes split messages", func(t *testing.T) {
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
		association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
		if !endpoint.trackAssociation(association) {
			t.Fatal("trackAssociation")
		}
		association.noteRoutingContextsActive(nil)
		for _, key := range []ASKey{
			{NetworkAppearance: 7, NetworkAppearanceSet: true},
			{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
		} {
			applicationServer := endpoint.as.get(key)
			applicationServer.setTrafficMode(params.TrafficModeLoadshare)
			applicationServer.setASPState(association, StateASPActive, time.Hour)
		}
		capture := new(distributionCapture)
		association.signalWriter = capture.write

		if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope:        SSNMScope{NetworkAppearance: 7, NetworkAppearanceSet: true},
			Destinations: []PointCodeRange{{PointCode: 1}},
		}); err != nil {
			t.Fatalf("broad SCON: %v", err)
		}
		reports := ssnmMessages(capture.snapshot())
		if len(reports) != 2 {
			t.Fatalf("mixed-scope ASP received %d SCON messages, want 2", len(reports))
		}
		var sawContextless, sawRoutingContext bool
		for _, message := range reports {
			scon := message.(*messages.SignallingCongestion)
			if scon.RoutingContext == nil {
				sawContextless = true
				continue
			}
			if reflect.DeepEqual(scon.RoutingContext.RoutingContexts(), []uint32{1}) {
				sawRoutingContext = true
			}
		}
		if !sawContextless || !sawRoutingContext {
			t.Fatalf("mixed-scope SCON split = contextless:%v RC:%v", sawContextless, sawRoutingContext)
		}
	})
}

func TestSSNMOperationEndpointCongestionPresenceAndAbatement(t *testing.T) {
	endpoint, _, _, second, secondSent := multiAssociationDialedSGPFixture(t)
	scope := SSNMScope{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContexts:      []uint32{2},
		RoutingContextSet:    true,
	}
	destination := PointCodeRange{PointCode: 0x123456}
	if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: scope, Destinations: []PointCodeRange{destination},
	}); err != nil {
		t.Fatalf("omitted congestion level: %v", err)
	}
	omitted := ssnmMessages(secondSent.snapshot())
	if len(omitted) != 1 {
		t.Fatalf("omitted congestion delivery count = %d, want 1", len(omitted))
	}
	if parameter := omitted[0].(*messages.SignallingCongestion).CongestionIndications; parameter != nil {
		t.Fatalf("omitted congestion parameter = %v, want nil", parameter)
	}
	status, ok := endpoint.DestinationStatus(DestinationStatusKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 2, RoutingContextSet: true,
		PointCode: destination.PointCode,
	})
	if !ok || status.State != DestinationCongested || status.CongestionLevelSet {
		t.Fatalf("omitted congestion status = %+v, %v", status, ok)
	}

	secondSent.reset()
	if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: scope, Destinations: []PointCodeRange{destination},
		CongestionLevel: 0, CongestionLevelSet: true,
	}); err != nil {
		t.Fatalf("congestion abatement: %v", err)
	}
	abatement := ssnmMessages(secondSent.snapshot())
	if len(abatement) != 1 {
		t.Fatalf("abatement delivery count = %d, want 1", len(abatement))
	}
	parameter := abatement[0].(*messages.SignallingCongestion).CongestionIndications
	if parameter == nil || parameter.CongestionLevel() != 0 {
		t.Fatalf("abatement congestion parameter = %v, want explicit zero", parameter)
	}
	status, ok = endpoint.DestinationStatus(DestinationStatusKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 2, RoutingContextSet: true,
		PointCode: destination.PointCode,
	})
	if !ok || status.State != DestinationAvailable ||
		!status.CongestionLevelSet || status.CongestionLevel != 0 {
		t.Fatalf("abatement status = %+v, %v", status, ok)
	}
	if second.ID() == 0 {
		t.Fatal("fixture Association has no Endpoint identity")
	}
}

func TestSSNMOperationEndpointOmittedNetworkAppearanceUsesUnambiguousAS(t *testing.T) {
	endpoint, _, firstSent, _, secondSent := multiAssociationDialedSGPFixture(t)
	if err := endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: SSNMScope{
			RoutingContexts:   []uint32{2},
			RoutingContextSet: true,
		},
		Destinations: []PointCodeRange{{PointCode: 1}},
	}); err != nil {
		t.Fatalf("omitted Network Appearance SCON: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Fatalf("unconcerned ASP received %d SSNM messages, want 0", got)
	}
	reports := ssnmMessages(secondSent.snapshot())
	if len(reports) != 1 {
		t.Fatalf("concerned ASP received %d SSNM messages, want 1", len(reports))
	}
	if networkAppearance := reports[0].(*messages.SignallingCongestion).NetworkAppearance; networkAppearance != nil {
		t.Fatalf("wire Network Appearance = %v, want omitted", networkAppearance)
	}
}

func TestSSNMOperationEndpointRejectsOmittedMixedNetworkAppearances(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	writes := 0
	for index, networkAppearance := range []uint32{10, 20} {
		routingContext := uint32(index + 1)
		association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, routingContext)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
		association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
		association.as.register(association.configuredASKeys())
		if !endpoint.trackAssociation(association) {
			t.Fatal("trackAssociation")
		}
		association.noteRoutingContextsActive([]uint32{routingContext})
		key := ASKey{
			NetworkAppearance: networkAppearance, NetworkAppearanceSet: true,
			RoutingContext: routingContext, RoutingContextSet: true,
		}
		applicationServer := endpoint.as.get(key)
		applicationServer.setTrafficMode(params.TrafficModeLoadshare)
		applicationServer.setASPState(association, StateASPActive, time.Hour)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
	}
	err = endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: SSNMScope{
			RoutingContexts: []uint32{1, 2}, RoutingContextSet: true,
		},
		Destinations: []PointCodeRange{{PointCode: 1}},
	})
	if !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("mixed Network Appearance error = %v, want ErrInvalidNetworkAppearance", err)
	}
	if writes != 0 {
		t.Fatalf("mixed Network Appearance request wrote %d messages, want 0", writes)
	}
}

func TestSSNMOperationPartialFanoutErrorPreservesEveryOutcome(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	attach := func() (*Association, *distributionCapture) {
		association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
		association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
		association.as.register(association.configuredASKeys())
		if !endpoint.trackAssociation(association) {
			t.Fatal("trackAssociation")
		}
		association.noteRoutingContextsActive([]uint32{1})
		key := ASKey{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
		}
		applicationServer := endpoint.as.get(key)
		applicationServer.setTrafficMode(params.TrafficModeLoadshare)
		applicationServer.setASPState(association, StateASPActive, time.Hour)
		capture := new(distributionCapture)
		association.signalWriter = capture.write
		return association, capture
	}
	failed, failedSent := attach()
	successful, successfulSent := attach()
	writeFailure := errors.New("injected SSNM failure")
	failed.signalWriter = func(message messages.M3UA) (int, error) {
		_, _ = failedSent.write(message)
		return 0, writeFailure
	}

	err = endpoint.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{
		Scope: SSNMScope{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContexts: []uint32{1}, RoutingContextSet: true,
		},
		Destination: PointCodeRange{PointCode: 1},
		User:        params.SCCP,
		Cause:       params.UnknownCause,
	})
	var deliveryError *SSNMDeliveryError
	if !errors.As(err, &deliveryError) {
		t.Fatalf("fan-out error = %v, want *SSNMDeliveryError", err)
	}
	if !errors.Is(err, writeFailure) {
		t.Fatalf("fan-out error does not unwrap injected failure: %v", err)
	}
	if !reflect.DeepEqual(deliveryError.Successful, []AssociationID{successful.ID()}) {
		t.Fatalf("successful Association IDs = %v, want [%d]",
			deliveryError.Successful, successful.ID())
	}
	if len(deliveryError.Failed) != 1 ||
		deliveryError.Failed[0].Association != failed.ID() ||
		!errors.Is(deliveryError.Failed[0].Cause, writeFailure) {
		t.Fatalf("failed deliveries = %+v, want Association %d", deliveryError.Failed, failed.ID())
	}
	if got := len(ssnmMessages(failedSent.snapshot())); got != 1 {
		t.Fatalf("failed ASP attempts = %d, want 1", got)
	}
	if got := len(ssnmMessages(successfulSent.snapshot())); got != 1 {
		t.Fatalf("successful ASP deliveries = %d, want 1", got)
	}
}

func TestSSNMOperationConcurrentAssociationCloseReturnsTypedFailure(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
	association.as.register(association.configuredASKeys())
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation")
	}
	association.noteRoutingContextsActive([]uint32{1})
	key := ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	applicationServer := endpoint.as.get(key)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer.setASPState(association, StateASPActive, time.Hour)

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	association.notificationQueue = make(chan mandatoryControl, 1)
	association.notificationWriter = func(message messages.M3UA) (int, error) {
		close(writeEntered)
		<-writeRelease
		return message.MarshalLen(), nil
	}
	result := make(chan error, 1)
	go func() {
		result <- endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope: SSNMScope{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				RoutingContexts: []uint32{1}, RoutingContextSet: true,
			},
			Destinations: []PointCodeRange{{PointCode: 1}},
		})
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("mandatory SSNM write did not start")
	}
	associationID := association.ID()
	if err := association.Close(); err != nil {
		close(writeRelease)
		t.Fatalf("Association.Close: %v", err)
	}
	select {
	case err := <-result:
		var deliveryError *SSNMDeliveryError
		if !errors.As(err, &deliveryError) {
			close(writeRelease)
			t.Fatalf("fan-out error = %v, want *SSNMDeliveryError", err)
		}
		if len(deliveryError.Failed) != 1 ||
			deliveryError.Failed[0].Association != associationID ||
			!errors.Is(deliveryError.Failed[0].Cause, ErrAssociationClosed) {
			close(writeRelease)
			t.Fatalf("close delivery failure = %+v", deliveryError.Failed)
		}
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("Endpoint SCON blocked after Association.Close")
	}
	close(writeRelease)
}

func TestSSNMOperationRevalidatesActiveScopeAfterFanoutSelection(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
	association.as.register(association.configuredASKeys())
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation")
	}
	association.noteRoutingContextsActive([]uint32{1})
	key := ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	applicationServer := endpoint.as.get(key)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer.setASPState(association, StateASPActive, time.Hour)

	firstWriteEntered := make(chan struct{})
	releaseFirstWrite := make(chan struct{})
	secondWrite := make(chan messages.M3UA, 1)
	writes := 0
	association.notificationQueue = make(chan mandatoryControl, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		if writes == 1 {
			close(firstWriteEntered)
			<-releaseFirstWrite
		} else {
			secondWrite <- message
		}
		return message.MarshalLen(), nil
	}
	if err := association.writeMandatoryControls(
		[]messages.M3UA{messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)},
		false,
		false,
	); err != nil {
		t.Fatalf("queue blocking control: %v", err)
	}
	select {
	case <-firstWriteEntered:
	case <-time.After(time.Second):
		t.Fatal("blocking control did not enter the writer")
	}

	result := make(chan error, 1)
	go func() {
		result <- endpoint.SignallingCongestion(SignallingCongestionRequest{
			Scope: SSNMScope{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				RoutingContexts: []uint32{1}, RoutingContextSet: true,
			},
			Destinations: []PointCodeRange{{PointCode: 1}},
		})
	}()
	deadline := time.Now().Add(time.Second)
	for len(association.notificationQueue) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(association.notificationQueue) == 0 {
		close(releaseFirstWrite)
		t.Fatal("SSNM control was not queued after target selection")
	}

	association.noteRoutingContextsInactive([]uint32{1})
	applicationServer.setASPState(association, StateASPInactive, time.Hour)
	close(releaseFirstWrite)

	select {
	case err := <-result:
		var deliveryError *SSNMDeliveryError
		if !errors.As(err, &deliveryError) {
			t.Fatalf("fan-out error = %v, want *SSNMDeliveryError", err)
		}
		if len(deliveryError.Failed) != 1 ||
			deliveryError.Failed[0].Association != association.ID() ||
			!errors.Is(deliveryError.Failed[0].Cause, ErrRoutingContextNotActive) {
			t.Fatalf("deactivated delivery failure = %+v", deliveryError.Failed)
		}
	case <-time.After(time.Second):
		t.Fatal("Endpoint SCON did not finish after the blocking write was released")
	}
	select {
	case message := <-secondWrite:
		t.Fatalf("deactivated Association received %T after fan-out selection", message)
	default:
	}
}

func TestSSNMOperationDAUDRetainsCongestionLevel(t *testing.T) {
	endpoint, first, firstSent, _, _ := multiAssociationDialedSGPFixture(t)
	request := SignallingCongestionRequest{
		Scope: SSNMScope{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContexts: []uint32{1}, RoutingContextSet: true,
		},
		Destinations:       []PointCodeRange{{PointCode: 0x123456, Mask: 4}},
		CongestionLevel:    3,
		CongestionLevelSet: true,
	}
	if err := endpoint.SignallingCongestion(request); err != nil {
		t.Fatalf("record congestion: %v", err)
	}
	firstSent.reset()
	if err := first.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCodeWithMask(4, 0x123456),
		nil,
	)); err != nil {
		t.Fatalf("handle DAUD: %v", err)
	}
	replies := ssnmMessages(firstSent.snapshot())
	if len(replies) != 2 {
		t.Fatalf("DAUD replies = %d, want SCON then DAVA", len(replies))
	}
	congestion, ok := replies[0].(*messages.SignallingCongestion)
	if !ok {
		t.Fatalf("first DAUD reply = %T, want SCON", replies[0])
	}
	if congestion.CongestionIndications == nil ||
		congestion.CongestionIndications.CongestionLevel() != 3 {
		t.Fatalf("DAUD SCON congestion = %v, want 3", congestion.CongestionIndications)
	}
	if _, ok := replies[1].(*messages.DestinationAvailable); !ok {
		t.Fatalf("second DAUD reply = %T, want DAVA", replies[1])
	}
}

func assertSSNMOperationScope(
	t *testing.T,
	networkAppearance, routingContext, affectedPointCode *params.Param,
	wantScope SSNMScope,
	wantDestinations []PointCodeRange,
) {
	t.Helper()
	if wantScope.NetworkAppearanceSet {
		if networkAppearance == nil ||
			networkAppearance.NetworkAppearance() != wantScope.NetworkAppearance {
			t.Fatalf("Network Appearance = %v, want %d", networkAppearance, wantScope.NetworkAppearance)
		}
	} else if networkAppearance != nil {
		t.Fatalf("Network Appearance = %v, want omitted", networkAppearance)
	}
	if wantScope.RoutingContextSet {
		want := append([]uint32(nil), wantScope.RoutingContexts...)
		sort.Slice(want, func(i, j int) bool { return want[i] < want[j] })
		if routingContext == nil ||
			!reflect.DeepEqual(routingContext.RoutingContexts(), want) {
			t.Fatalf("Routing Contexts = %v, want %v", routingContext, want)
		}
	} else if routingContext != nil {
		t.Fatalf("Routing Context = %v, want omitted", routingContext)
	}
	if affectedPointCode == nil {
		t.Fatal("Affected Point Code is nil")
	}
	gotPointCodes := affectedPointCode.AffectedPointCodes()
	gotMasks := affectedPointCode.AffectedPointCodeMasks()
	if len(gotPointCodes) != len(wantDestinations) || len(gotMasks) != len(wantDestinations) {
		t.Fatalf("Affected Point Codes = %v/%v, want %v", gotPointCodes, gotMasks, wantDestinations)
	}
	for index, destination := range wantDestinations {
		if gotPointCodes[index] != destination.PointCode || gotMasks[index] != destination.Mask {
			t.Fatalf("Affected Point Code %d = %#x/%d, want %#x/%d", index,
				gotPointCodes[index], gotMasks[index], destination.PointCode, destination.Mask)
		}
	}
}

func FuzzTypedSSNM(f *testing.F) {
	info := params.NewInfoString(strings.Repeat("x", 255))
	seeds := []messages.M3UA{
		messages.NewDestinationStateAudit(
			nil, nil, params.NewAffectedPointCodeWithMask(0, 1), nil,
		),
		messages.NewDestinationStateAudit(
			params.NewNetworkAppearance(0), params.NewRoutingContext(0),
			params.NewAffectedPointCodeWithMask(24, 0xffffff), info.Copy(),
		),
		messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCodeWithMask(0, 1), nil, nil, nil,
		),
		messages.NewDestinationUserPartUnavailable(
			params.NewNetworkAppearance(0), params.NewRoutingContext(0),
			params.NewAffectedPointCodeWithMask(0, 0xffffff),
			params.NewUserCause(params.GatewayControlProtocol, params.Inaccessible),
			info.Copy(),
		),
	}
	for level := uint8(0); level <= 3; level++ {
		seeds = append(seeds, messages.NewSignallingCongestion(
			params.NewNetworkAppearance(0), params.NewRoutingContext(0),
			params.NewAffectedPointCodeWithMask(24, 0xffffff),
			params.NewConcernedDestination(0xffffff),
			params.NewCongestionIndications(level), info.Copy(),
		))
	}
	for _, seed := range seeds {
		wire, err := seed.MarshalBinary()
		if err != nil {
			f.Fatalf("seed %T: %v", seed, err)
		}
		f.Add(wire)
	}

	f.Fuzz(func(t *testing.T, wire []byte) {
		message, err := messages.Parse(wire)
		if err != nil || message.MessageClass() != messages.MsgClassSSNM {
			return
		}
		encoded, err := message.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal parsed %T: %v", message, err)
		}
		if _, err := messages.Parse(encoded); err != nil {
			t.Fatalf("reparse %T: %v", message, err)
		}
	})
}

func withAuditDestinations(request DestinationStateAuditRequest, destinations []PointCodeRange) DestinationStateAuditRequest {
	request.Destinations = destinations
	return request
}

func withAuditScope(request DestinationStateAuditRequest, scope SSNMScope) DestinationStateAuditRequest {
	request.Scope = scope
	return request
}

func withAuditInfo(request DestinationStateAuditRequest, info string) DestinationStateAuditRequest {
	request.Info = info
	return request
}

func withDUPUDestination(request DestinationUserPartUnavailableRequest, destination PointCodeRange) DestinationUserPartUnavailableRequest {
	request.Destination = destination
	return request
}

func withDUPUUser(request DestinationUserPartUnavailableRequest, user uint16) DestinationUserPartUnavailableRequest {
	request.User = user
	return request
}

func withDUPUCause(request DestinationUserPartUnavailableRequest, cause uint16) DestinationUserPartUnavailableRequest {
	request.Cause = cause
	return request
}
