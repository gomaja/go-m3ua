package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestSSNMOperationLinuxRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	peer := integrationPeers()[0]

	sgpEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint SGP: %v", err)
	}
	t.Cleanup(func() { _ = sgpEndpoint.Close() })
	listener, err := sgpEndpoint.Listen(
		"m3ua",
		mcAddr(0, peer.ip),
		NewListenerConfig(integrationAssociationConfig(RoleSGP, peer)),
	)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatalf("Listen SGP: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan associationResult, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		accepted <- associationResult{association: association, err: acceptErr}
	}()

	aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: integrationASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint ASP: %v", err)
	}
	t.Cleanup(func() { _ = aspEndpoint.Close() })
	aspAssociation, err := aspEndpoint.Dial(
		ctx,
		"m3ua",
		mcAddr(0, "127.0.0.1"),
		listener.Addr().(*sctp.SCTPAddr),
		integrationAssociationConfig(RoleASP, peer),
	)
	if err != nil {
		t.Fatalf("Dial ASP: %v", err)
	}
	t.Cleanup(func() { _ = aspAssociation.Close() })
	var sgpAssociation *Association
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatalf("Accept SGP: %v", result.err)
		}
		sgpAssociation = result.association
	case <-ctx.Done():
		t.Fatalf("Accept SGP: %v", ctx.Err())
	}
	t.Cleanup(func() { _ = sgpAssociation.Close() })

	scope := SSNMScope{
		NetworkAppearance:    peer.networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContexts:      []uint32{peer.routingContext},
		RoutingContextSet:    true,
	}
	destination := PointCodeRange{PointCode: 0x123456, Mask: 4}
	if err := sgpEndpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope: scope, Destinations: []PointCodeRange{destination},
		CongestionLevel: 2, CongestionLevelSet: true,
	}); err != nil {
		t.Fatalf("SGP SCON: %v", err)
	}
	status := receiveDestinationStatus(t, ctx, aspAssociation)
	if status.State != DestinationCongested || status.CongestionLevel != 2 ||
		status.PointCode != destination.PointCode || status.Mask != destination.Mask {
		t.Fatalf("ASP SCON status = %+v", status)
	}

	if err := aspAssociation.DestinationStateAudit(DestinationStateAuditRequest{
		Scope: scope, Destinations: []PointCodeRange{destination},
	}); err != nil {
		t.Fatalf("ASP DAUD: %v", err)
	}
	congested := receiveDestinationStatus(t, ctx, aspAssociation)
	available := receiveDestinationStatus(t, ctx, aspAssociation)
	if congested.State != DestinationCongested || congested.CongestionLevel != 2 {
		t.Fatalf("DAUD SCON status = %+v", congested)
	}
	if available.State != DestinationAvailable {
		t.Fatalf("DAUD DAVA status = %+v", available)
	}

	if err := sgpEndpoint.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{
		Scope:       scope,
		Destination: PointCodeRange{PointCode: 0x654321},
		User:        params.SCCP,
		Cause:       params.Inaccessible,
	}); err != nil {
		t.Fatalf("SGP DUPU: %v", err)
	}
	dupu := receiveDestinationStatus(t, ctx, aspAssociation)
	if !dupu.UserPartUnavailable || dupu.PointCode != 0x654321 ||
		dupu.UserCause != params.NewUserCause(params.SCCP, params.Inaccessible).UserCause() {
		t.Fatalf("ASP DUPU status = %+v", dupu)
	}
}

func receiveDestinationStatus(
	t *testing.T,
	ctx context.Context,
	association *Association,
) *DestinationStatus {
	t.Helper()
	select {
	case status := <-association.SignallingStatus():
		if status == nil {
			t.Fatal("SignallingStatus closed")
		}
		return status
	case <-ctx.Done():
		t.Fatalf("SignallingStatus: %v", ctx.Err())
		return nil
	}
}
