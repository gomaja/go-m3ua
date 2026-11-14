package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func recoveryASP(t *testing.T, state State, automatic bool) (*Association, *[]messages.M3UA) {
	t.Helper()
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	completeUpAndRCOne(t, c)
	if state != StateASPActive {
		c.setState(state)
		if state == StateASPInactive {
			c.noteNoRoutingContextsAcked()
		}
	}
	c.cfg.ASPProcedures = ipspInitiationPolicy(automatic, automatic)
	c.stateEntered = true
	c.appliedState = state
	for len(c.stateChan) > 0 {
		receiveASPTest(t, c.stateChan, "old state publication")
	}
	for len(c.mgmtChan) > 0 {
		receiveASPTest(t, c.mgmtChan, "old management indication")
	}
	*sent = nil
	return c, sent
}

func dispatchRecoveryAck(t *testing.T, c *Association, m messages.M3UA) {
	t.Helper()
	c.handleReceivedSignals(context.Background(), m, nil)
	for len(c.errChan) > 0 {
		if err := c.handleErrors(receiveASPTest(t, c.errChan, "local Error")); err != nil {
			t.Fatal(err)
		}
	}
	applyUpAckPublications(t, c)
}

func recoveryIndication(t *testing.T, c *Association, kind string, keys []ASKey, previous State) *ManagementIndication {
	t.Helper()
	var ind *ManagementIndication
	select {
	case ind = <-c.ManagementIndications():
	default:
		t.Fatalf("missing %s indication", kind)
	}
	if ind.Kind.String() != kind {
		t.Fatalf("indication kind %v, want %s", ind.Kind, kind)
	}
	expected := make(map[ASKey]bool, len(keys))
	labels := make(map[uint32]bool)
	for _, key := range keys {
		expected[key] = true
		if key.RoutingContextSet {
			labels[key.RoutingContext] = true
		}
	}
	if len(ind.ASKeys) != len(expected) {
		t.Errorf("indication ASKeys %+v, want %+v", ind.ASKeys, keys)
	}
	for _, key := range ind.ASKeys {
		if !expected[key] {
			t.Errorf("indication has unexpected exact ASKey %+v", key)
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		t.Errorf("indication omitted ASKeys %+v", expected)
	}
	if len(ind.RoutingContexts) != len(labels) {
		t.Errorf("indication RoutingContexts %v do not match exact keys %+v", ind.RoutingContexts, keys)
	}
	for _, rc := range ind.RoutingContexts {
		if !labels[rc] {
			t.Errorf("unexpected or duplicate Routing Context %d", rc)
		}
		delete(labels, rc)
	}
	if len(labels) != 0 {
		t.Errorf("indication omitted Routing Contexts %v", labels)
	}
	prior := reflect.ValueOf(ind).Elem().FieldByName("PreviousState")
	if !prior.IsValid() || prior.Interface() != previous {
		t.Errorf("indication does not report previous ASP state %v", previous)
	}
	if !strings.Contains(ind.Description, "application") {
		t.Errorf("report does not assign the decision to the application: %s", ind.Description)
	}
	return ind
}

func TestUnowedUpAckWhileActiveSendsErrorAndReportsExactScopes(t *testing.T) {
	c, sent := recoveryASP(t, StateASPActive, true)
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if got := errorCodes(*sent); !reflect.DeepEqual(got, []uint32{params.UnexpectedMessageError}) {
		t.Errorf("Error frames %v, want one Unexpected Message", got)
	}
	if c.State() != StateASPInactive || c.routingContextAcked(1) {
		t.Errorf("unowed Up Ack did not close active gates: state=%v RC1=%v", c.State(), c.routingContextAcked(1))
	}
	if countType(*sent, "ASP Active") != 0 {
		t.Error("unowed Up Ack started automatic ASP Active")
	}
	ind := recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
	if !strings.Contains(ind.Description, "ASP Up Ack") {
		t.Error("withdrawal report omitted its cause")
	}
}

func TestErrorPrecedesWithdrawalIndication(t *testing.T) {
	c, _ := recoveryASP(t, StateASPActive, true)
	entered := make(chan struct{})
	release, open := aspTestGate(t)
	c.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.Error); ok {
			close(entered)
			if err := awaitASPTestRelease(c, release); err != nil {
				return 0, err
			}
		}
		return m.MarshalLen(), nil
	}
	handled := startASPTestCall(func() error {
		c.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
		return nil
	})
	awaitASPTestSend(t, entered, handled, "Unexpected Message Error")
	select {
	case ind := <-c.ManagementIndications():
		t.Fatalf("indication preceded Error submission: %+v", ind)
	default:
	}
	open()
	if err := handled.wait(t, "Up Ack processing"); err != nil {
		t.Fatal(err)
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
}

func TestOwedDuplicateUpAckStillAbsorbed(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	c.cfg.ASPProcedures = explicitASPProcedurePolicy()
	req, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writePendingControl(context.Background(), req, req.msg); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	applyUpAckPublications(t, c)
	*sent = nil
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPActive || !c.routingContextAcked(1) || c.routingContextAcked(2) {
		t.Fatal("owed duplicate changed state or gates")
	}
	if len(*sent) != 0 || len(c.mgmtChan) != 0 {
		t.Fatal("owed duplicate wrote or reported anything")
	}
}

func TestUnowedUpAckWhileInactiveSendsNoError(t *testing.T) {
	c, sent := recoveryASP(t, StateASPInactive, true)
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPInactive || len(*sent) != 0 || len(c.mgmtChan) != 0 {
		t.Fatalf("inactive Up Ack changed state, wrote or reported: %v %v", c.State(), typeNames(*sent))
	}
}

func TestUnowedUpAckWhileDownReportsInactiveWithoutAutomaticActive(t *testing.T) {
	c, sent := recoveryASP(t, StateASPDown, true)
	dispatchRecoveryAck(t, c, messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPInactive {
		t.Errorf("unowed Down-state Up Ack state=%v", c.State())
	}
	if got := errorCodes(*sent); !reflect.DeepEqual(got, []uint32{params.UnexpectedMessageError}) {
		t.Errorf("Error frames %v, want Unexpected Message", got)
	}
	if countType(*sent, "ASP Active") != 0 {
		t.Error("unowed Up Ack while DOWN started ASP Active")
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", nil, StateASPDown)
}

func TestUnsolicitedDownAckCommitsDownWithoutErrorOrAutomaticUp(t *testing.T) {
	for _, state := range []State{StateASPActive, StateASPInactive, StateASPDown} {
		t.Run(state.String(), func(t *testing.T) {
			c, sent := recoveryASP(t, state, true)
			dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
			if c.State() != StateASPDown {
				t.Errorf("Down Ack state %v", c.State())
			}
			if len(errorCodes(*sent)) != 0 || countType(*sent, "ASP Up") != 0 || countType(*sent, "ASP Active") != 0 {
				t.Errorf("unsolicited Down Ack wrote %v", typeNames(*sent))
			}
		})
	}
}

func TestUnsolicitedDownAckReportsPriorActiveScopes(t *testing.T) {
	for _, state := range []State{StateASPActive, StateASPInactive} {
		t.Run(state.String(), func(t *testing.T) {
			c, _ := recoveryASP(t, state, true)
			dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
			var keys []ASKey
			if state == StateASPActive {
				keys = []ASKey{routingContextASKey(1)}
			}
			ind := recoveryIndication(t, c, "ASP-WITHDRAWAL", keys, state)
			if !strings.Contains(ind.Description, "ASP Down Ack") {
				t.Error("Down withdrawal omitted its cause")
			}
		})
	}
}

func TestExplicitASPUpAfterUnsolicitedDownAckSucceeds(t *testing.T) {
	c, _ := recoveryASP(t, StateASPActive, true)
	dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
	submitted := make(chan messages.M3UA, 8)
	c.signalWriter = func(m messages.M3UA) (int, error) { submitted <- m; return m.MarshalLen(), nil }
	ctx := aspTestContext(t)
	call := startASPTestCall(func() error { return c.ASPUp(ctx) })
	if _, ok := awaitASPTestSend(t, submitted, call, "explicit recovery ASPUp").(*messages.AspUp); !ok {
		t.Fatal("explicit recovery did not send Up")
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if err := call.wait(t, "explicit recovery Up Ack"); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateASPInactive {
		t.Errorf("explicit ASPUp state %v", c.State())
	}
}

func TestSolicitedDownAckUnchanged(t *testing.T) {
	c, sent := recoveryASP(t, StateASPActive, true)
	if _, err := c.initiateASPDown(); err != nil {
		t.Fatal(err)
	}
	*sent = nil
	dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
	if countType(*sent, "ASP Up") != 1 || len(errorCodes(*sent)) != 0 || len(c.mgmtChan) != 0 {
		t.Errorf("solicited nonterminating Down Ack changed: sent=%v", typeNames(*sent))
	}
}

func TestUnsolicitedActiveAckReportsExactScopes(t *testing.T) {
	c, _ := recoveryASP(t, StateASPInactive, true)
	dispatchRecoveryAck(t, c, messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	ind := recoveryIndication(t, c, "ASP-ACTIVATION", []ASKey{routingContextASKey(2)}, StateASPInactive)
	if !strings.Contains(ind.Description, "ASP Active Ack") {
		t.Error("activation report omitted its cause")
	}
}

func TestUnsolicitedActiveAckSendsNoErrorAndDeliversDATA(t *testing.T) {
	c, sent := recoveryASP(t, StateASPInactive, true)
	c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithoutPriorities), 3)
	dispatchRecoveryAck(t, c, messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	if len(errorCodes(*sent)) != 0 {
		t.Errorf("unsolicited Active Ack sent Error: %v", typeNames(*sent))
	}
	var delivered bool
	c.dataWriter = func(b []byte, _ *sctp.SndInfo) (int, error) { delivered = true; return len(b), nil }
	if _, err := c.WriteData(DataRequest{AS: routingContextASKey(2), Stream: 1, ProtocolData: simpleProtocolData("unsolicited-active-data")}); err != nil {
		t.Fatal(err)
	}
	if !delivered {
		t.Fatal("DATA did not reach the transport after unsolicited Active Ack")
	}
	if c.routingContextAcked(1) {
		t.Error("unsolicited Active Ack opened RC1")
	}
	recoveryIndication(t, c, "ASP-ACTIVATION", []ASKey{routingContextASKey(2)}, StateASPInactive)
}

func TestASPInactiveAfterUnsolicitedActiveAckCloses(t *testing.T) {
	c, _ := recoveryASP(t, StateASPInactive, false)
	c.dataWriter = func(b []byte, _ *sctp.SndInfo) (int, error) { return len(b), nil }
	dispatchRecoveryAck(t, c, messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	recoveryIndication(t, c, "ASP-ACTIVATION", []ASKey{routingContextASKey(2)}, StateASPInactive)
	sent := make(chan messages.M3UA, 8)
	c.signalWriter = func(m messages.M3UA) (int, error) { sent <- m; return m.MarshalLen(), nil }
	ctx := aspTestContext(t)
	call := startASPTestCall(func() error { return c.ASPInactive(ctx, routingContextASKey(2)) })
	if _, ok := awaitASPTestSend(t, sent, call, "application ASPInactive").(*messages.AspInactive); !ok {
		t.Fatal("application did not send Inactive")
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	if err := call.wait(t, "application Inactive Ack"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WriteData(DataRequest{AS: routingContextASKey(2), Stream: 1, ProtocolData: simpleProtocolData("closed")}); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("DATA after application Inactive: %v", err)
	}
}
