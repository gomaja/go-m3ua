package m3ua

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 §4.3.4.4 and §4.7: the peer withdraws RC1 after reactivation;
// only the application's later activation request restores its traffic.
func TestASPApplicationReactivatesAfterSGPWithdrawalSCTP(t *testing.T) {
	testApplicationWithdrawalSCTP(t, 41865, false)
}

func TestASPApplicationReactivatesLastScopeWithAutomaticStartupSCTP(t *testing.T) {
	testApplicationWithdrawalSCTP(t, 41866, true)
}

func testApplicationWithdrawalSCTP(t *testing.T, port int, last bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var management <-chan *ManagementIndication
	asp, sgp, err := setupConnHB(t, ctx, port, &HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false}, func(c *Association) { management = c.ManagementIndications(); c.cfg.TAck = 100 * time.Millisecond })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asp.Close(); _ = sgp.Close() })
	// setupConnHB uses the default automatic startup policy: both ASes start active.
	if !asp.routingContextAcked(1) || !asp.routingContextAcked(2) {
		t.Fatal("automatic startup did not activate both scopes")
	}
	scopes := []uint32{1}
	if last {
		scopes = append(scopes, 2)
	}
	if err := asp.DeactivateRoutingContexts(scopes...); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return !asp.routingContextAcked(1) && asp.pendingTAck() == 0 }, 3*time.Second) {
		t.Fatal("initial deactivation did not complete")
	}
	if err := asp.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return asp.routingContextAcked(1) && asp.pendingTAck() == 0 }, 3*time.Second) {
		t.Fatal("reactivation did not complete")
	}
	if err := sgp.SetASAvailable(1, false); err != nil {
		t.Fatal(err)
	}
withdrawal:
	for {
		select {
		case ind := <-management:
			if ind == nil {
				t.Fatal("management subscription closed before withdrawal")
			}
			if ind.Kind == ManagementError && ind.ErrorCode == params.ErrRefusedManagementBlocking {
				t.Fatal("automatic ASP Active was sent before application activation")
			}
			if ind.Kind.String() == "ASP-WITHDRAWAL" {
				if !reflect.DeepEqual(ind.RoutingContexts, []uint32{1}) || !reflect.DeepEqual(ind.ASKeys, []ASKey{associationConfigASKey(asp.cfg, 1)}) {
					t.Fatalf("withdrawal scope=%+v", ind)
				}
				break withdrawal
			}
		case <-ctx.Done():
			t.Fatal("unsolicited withdrawal was not reported")
		}
	}
	if asp.routingContextAcked(1) || asp.routingContextAcked(2) == last || asp.pendingTAck() != 0 {
		t.Fatal("withdrawal did not stop exactly its scope without automatic recovery")
	}
	markApplicationActivationPhase(t, ctx, asp, "withdrawal-observed")
	quiet := time.NewTimer(350 * time.Millisecond)
	defer quiet.Stop()
waiting:
	for {
		select {
		case ind := <-management:
			if ind == nil {
				t.Fatal("association closed while awaiting application choice")
			}
			if ind.Kind == ManagementError && ind.ErrorCode == params.ErrRefusedManagementBlocking {
				t.Fatal("automatic ASP Active retried before application activation")
			}
		case <-quiet.C:
			break waiting
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if asp.pendingTAck() != 0 || asp.routingContextAcked(1) {
		t.Fatal("startup entry reactivated the withdrawn scope")
	}
	data := params.ProtocolDataPayload{OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2, Data: []byte{0x42, 0x17, 0, 1}}
	if _, err := asp.WriteData(DataRequest{AS: associationConfigASKey(asp.cfg, 1), ProtocolData: data}); err == nil {
		t.Fatal("withdrawn RC1 admitted DATA")
	}
	if !last {
		if _, err := asp.WriteData(DataRequest{AS: associationConfigASKey(asp.cfg, 2), ProtocolData: data}); err != nil {
			t.Fatal(err)
		}
		received, err := sgp.ReadData(ctx)
		if err != nil || received.AS.RoutingContext != 2 {
			t.Fatalf("unaffected RC2 DATA=%+v / %v", received, err)
		}
	}
	if err := sgp.SetASAvailable(1, true); err != nil {
		t.Fatal(err)
	}
	// The echoed marker identifies the end of the no-activation interval in the
	// packet capture. The following call is the application's explicit decision.
	markApplicationActivationPhase(t, ctx, asp, "application-activate")
	if err := asp.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return asp.routingContextAcked(1) && asp.pendingTAck() == 0 }, 3*time.Second) {
		t.Fatal("application activation did not complete")
	}
	if asp.routingContextAcked(2) == last {
		t.Fatal("application activation changed deliberately inactive sibling")
	}
	if _, err := asp.WriteData(DataRequest{AS: associationConfigASKey(asp.cfg, 1), ProtocolData: data}); err != nil {
		t.Fatal(err)
	}
	received, err := sgp.ReadData(ctx)
	if err != nil || received.AS.RoutingContext != 1 {
		t.Fatalf("restored RC1 DATA=%+v / %v", received, err)
	}
}

func markApplicationActivationPhase(t *testing.T, ctx context.Context, c *Association, phase string) {
	t.Helper()
	data := []byte(phase)
	c.setBeatData(data)
	if _, err := c.WriteSignal(messages.NewHeartbeat(params.NewHeartbeatData(data))); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return len(c.currentBeatData()) == 0 }, time.Second) {
		t.Fatalf("phase marker %q was not acknowledged: %v", phase, ctx.Err())
	}
}
