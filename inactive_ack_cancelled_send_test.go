package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// competingActivationSender keeps DATA blocked after the cancelled Active's
// transport call returns. It exercises both public send paths without a socket.
type competingActivationSender struct {
	first, cancelled, releaseFirst, data, releaseData chan struct{}
	firstOnce, dataOnce                               sync.Once
}

func (s *competingActivationSender) SendMsg(b []byte, opts sctp.SendOptions) (int, error) {
	return s.SendMsgContext(context.Background(), b, opts)
}
func (s *competingActivationSender) SendMsgContext(ctx context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	if isPayloadData(b) {
		s.dataOnce.Do(func() { close(s.data) })
		select {
		case <-s.releaseData:
			return len(b), nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	if message, err := messages.Parse(b); err == nil {
		if active, ok := message.(*messages.AspActive); ok && active.RoutingContext != nil && active.RoutingContext.RoutingContexts()[0] == 2 {
			return len(b), nil
		}
	}
	if opts.NoWait {
		return 0, syscall.EAGAIN
	}
	s.firstOnce.Do(func() { close(s.first) })
	<-ctx.Done()
	close(s.cancelled)
	<-s.releaseFirst
	return 0, ctx.Err()
}

// heldActivationMarshal parks the independent writer after it acquires
// retryMu, allowing DATA to acquire sendMu before that writer continues.
type heldActivationMarshal struct {
	messages.M3UA
	entered, proceed chan struct{}
}

func (p *heldActivationMarshal) MarshalTo(dst []byte) error {
	close(p.entered)
	<-p.proceed
	return p.M3UA.MarshalTo(dst)
}

func TestInactiveAckJoinDoesNotWaitForCompetingWriters(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	sender := &competingActivationSender{first: make(chan struct{}), cancelled: make(chan struct{}), releaseFirst: make(chan struct{}), data: make(chan struct{}), releaseData: make(chan struct{})}
	c.signalWriter = nil
	c.controlTransport = sender
	var releaseFirst, releaseData sync.Once
	unblockFirst := func() { releaseFirst.Do(func() { close(sender.releaseFirst) }) }
	unblockData := func() { releaseData.Do(func() { close(sender.releaseData) }) }
	defer unblockData()
	defer unblockFirst()
	first := messages.NewAspActive(nil, params.NewRoutingContext(1), nil)
	request := c.startTAck(first, requestAspActive)
	firstDone := make(chan error, 1)
	go func() { _, err := c.writePendingControl(request, first); firstDone <- err }()
	<-sender.first
	deadline := time.Now().Add(time.Hour)
	c.applicationWriteDeadline.Store(&deadline)
	dataCtx, cancelData := context.WithDeadline(context.Background(), deadline)
	defer cancelData()
	dataDone := make(chan error, 1)
	go func() {
		_, err := c.WriteDataContext(dataCtx, DataRequest{AS: associationConfigASKey(c.cfg, 2), ProtocolData: simpleProtocolData("independent")})
		dataDone <- err
	}()
	// Pause the cancelled request's completion snapshot. This lets the test
	// establish the competing locks before allowing the handler to join it.
	// It makes no assumption about which mutex waiter Go schedules first.
	request.writeMu.Lock()
	firstCall := request.write
	var releaseJoin sync.Once
	unblockJoin := func() { releaseJoin.Do(request.writeMu.Unlock) }
	defer unblockJoin()
	management := c.ManagementIndications()
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	}()
	select {
	case <-sender.cancelled:
	case <-time.After(time.Second):
		t.Fatal("withdrawal did not cancel RC1's call")
	}
	unblockFirst()
	select {
	case <-firstCall.done:
	case <-time.After(time.Second):
		t.Fatal("RC1's cancelled transport call did not return")
	}
	select {
	case <-sender.data:
	case <-time.After(time.Second):
		t.Fatal("DATA did not enter the transport")
	}
	// Only RC1 has used retryMu so far. Observe its release before starting
	// RC2, whose MarshalTo barrier then proves which writer owns the mutex.
	deadlineForRetry := time.Now().Add(time.Second)
	for !c.tack.retryMu.TryLock() {
		if time.Now().After(deadlineForRetry) {
			t.Fatal("RC1 retained retryMu after its transport call returned")
		}
		time.Sleep(time.Millisecond)
	}
	c.tack.retryMu.Unlock()
	second := &heldActivationMarshal{M3UA: messages.NewAspActive(nil, params.NewRoutingContext(2), nil), entered: make(chan struct{}), proceed: make(chan struct{})}
	secondRequest := c.startTAck(second, requestAspActive)
	var releaseSecond sync.Once
	unblockSecond := func() { releaseSecond.Do(func() { close(second.proceed) }) }
	defer unblockSecond()
	secondDone := make(chan error, 1)
	go func() { _, err := c.writePendingControl(secondRequest, second); secondDone <- err }()
	select {
	case <-second.entered:
	case <-time.After(time.Second):
		t.Fatal("independent RC2 activation did not reach its marshal barrier")
	}
	if c.tack.retryMu.TryLock() {
		c.tack.retryMu.Unlock()
		t.Fatal("fixture precondition: RC2 does not hold retryMu")
	}
	if c.sendMu.TryLock() {
		c.sendMu.Unlock()
		t.Fatal("fixture precondition: DATA does not hold sendMu")
	}
	unblockSecond()
	unblockJoin()
	reported := false
	select {
	case ind := <-management:
		reported = ind.Kind == ManagementASPWithdrawal
	case <-time.After(250 * time.Millisecond):
	}
	unblockData()
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	if err := <-firstDone; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("cancelled Active error=%v", err)
	}
	if err := <-dataDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	// Retire the independent request after its unblocked transport attempt.
	c.cancelTAckRequest(secondRequest)
	if !reported {
		t.Fatal("withdrawal report waited for unrelated DATA and activation after its cancelled call returned")
	}
}

func TestCancelledOmittedActiveAckRetainsMainDuplicateHandling(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	c.cfg.TAck = time.Hour
	c.addDynamicASKey(*staticASKey(10, 1), RoutingKey{}, false)
	c.addDynamicASKey(*staticASKey(20, 2), RoutingKey{}, false)
	original := messages.NewAspActive(nil, nil, nil)
	c.startTAck(original, requestAspActive)
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, nil, nil))
	transport := &acceptanceWinsSender{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	request := c.startTAck(original, requestAspActive)
	done := make(chan error, 1)
	go func() { _, err := c.writePendingControl(request, original); done <- err }()
	<-transport.waiting
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatal(err)
	}
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, nil, nil))
	if c.routingContextAcked(1) || c.routingContextAcked(2) {
		t.Fatal("delayed omitted Active Ack reopened scopes after cancellation")
	}
}

func TestCancelledActiveAckAfterPeerUpUsesDuplicateHandling(t *testing.T) {
	c, _ := cancelledAcceptedActivation(t, RoleIPSP)
	c.noteRoutingContextsActive([]uint32{1})
	c.recvStream.Store(0)
	c.handleSignals(context.Background(), messages.NewAspUp(nil, nil))
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	if c.activeForRoutingContext(2) {
		t.Fatal("old Active Ack after peer Up used pre-Up cancellation evidence")
	}
}

func TestRefusedCancelledActiveDoesNotPromoteDuplicateAck(t *testing.T) {
	c, _ := cancelledAcceptedActivation(t, RoleASP)
	if err := c.handleError(messages.NewError(params.NewErrorCode(params.ErrRefusedManagementBlocking), params.NewRoutingContext(2), nil, nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	if c.routingContextAcked(2) {
		t.Fatal("refused cancelled Active promoted an older duplicate Ack into activation")
	}
}

func TestCancelledAcceptedActiveIsReportedImmediately(t *testing.T) {
	for _, operation := range []string{"withdrawal", "confirmed Inactive", "ASP Down"} {
		for _, waiting := range []bool{false, true} {
			t.Run(operation+map[bool]string{false: "/nonblocking", true: "/waiting"}[waiting], func(t *testing.T) {
				c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
				c.cfg.TAck = time.Hour
				key := *staticASKey(10, 2)
				c.addDynamicASKey(key, RoutingKey{}, false)
				c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				transport := &acceptanceWinsSender{newFullSendBuffer()}
				c.signalWriter = nil
				c.controlTransport = transport
				c.transportCloser = transport.close
				management := c.ManagementIndications()
				done := make(chan error, 1)
				go func() {
					if waiting {
						done <- c.ASPActive(context.Background(), key)
					} else {
						done <- c.ActivateRoutingContexts(2)
					}
				}()
				<-transport.waiting
				if operation == "confirmed Inactive" {
					c.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
				}
				if operation == "ASP Down" {
					c.recvStream.Store(0)
					c.startTAck(messages.NewAspDown(nil), requestAspDown)
					if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
						t.Fatal(err)
					}
				} else if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
					t.Fatal(err)
				}
				if err := <-done; !errors.Is(err, ErrASPProcedureSuperseded) {
					t.Fatalf("cancelled application activation returned %v", err)
				}
				if operation == "withdrawal" {
					select {
					case ind := <-management:
						if ind.Kind != ManagementASPWithdrawal {
							t.Fatalf("withdrawal must precede acceptance report: %+v", ind)
						}
					default:
						t.Fatal("missing withdrawal")
					}
				}
				select {
				case ind := <-management:
					if ind.Kind != ManagementError || !errors.Is(ind.Cause, ErrASPProcedureSuperseded) || !strings.Contains(ind.Cause.Error(), "already accepted by SCTP") || !reflect.DeepEqual(ind.ASKeys, []ASKey{key}) {
						t.Fatalf("cancelled accepted send report=%+v", ind)
					}
				default:
					t.Fatal("accepted cancelled send was not reported before any Ack")
				}
				if waiting {
					select {
					case ind := <-management:
						assertApplicationActivationFailure(t, ind, key)
					default:
						t.Fatal("missing generic application failure after acceptance report")
					}
				}
				select {
				case ind := <-management:
					t.Fatalf("duplicate accepted-send report: %+v", ind)
				default:
				}
				if c.pendingTAck() != 0 {
					t.Fatal("cancelled accepted send left a retry pending")
				}
			})
		}
	}
}

func TestCancelledAcceptedActiveReportsOriginalWireScope(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "omitted"}[omitted], func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
			setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(0))
			c.cfg.TAck = time.Hour
			c.addDynamicASKey(*staticASKey(10, 1), RoutingKey{}, false)
			c.addDynamicASKey(*staticASKey(20, 2), RoutingKey{}, false)
			c.noteRoutingContextsAcked(nil)
			scope := params.NewRoutingContext(1, 2)
			if omitted {
				scope = nil
			}
			want := c.managementASKeys(nil, scope)
			sender := &acceptanceWinsSender{newFullSendBuffer()}
			c.signalWriter = nil
			c.controlTransport = sender
			c.transportCloser = sender.close
			message := messages.NewAspActive(nil, scope, nil)
			request := c.startTAck(message, requestAspActive)
			done := make(chan error, 1)
			go func() { _, err := c.writePendingControl(request, message); done <- err }()
			<-sender.waiting
			c.addDynamicASKey(*staticASKey(30, 3), RoutingKey{}, false)
			management := c.ManagementIndications()
			if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
				t.Fatal(err)
			}
			if err := <-done; !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatal(err)
			}
			if ind := <-management; ind.Kind != ManagementASPWithdrawal {
				t.Fatalf("first report=%+v", ind)
			}
			ind := <-management
			if ind.Kind != ManagementError || !reflect.DeepEqual(ind.ASKeys, want) {
				t.Fatalf("accepted message scope=%+v, want %+v", ind, want)
			}
			if !c.routingContextAcked(1) || c.routingContextAcked(2) || c.routingContextAcked(3) {
				t.Fatal("acceptance report changed traffic eligibility")
			}
			sender.mu.Lock()
			attempts := len(sender.attempted)
			sender.mu.Unlock()
			if attempts != 1 || len(sender.waitedFrames()) != 1 {
				t.Fatal("cancellation sent a replacement message")
			}
			select {
			case next := <-management:
				t.Fatalf("extra acceptance report=%+v", next)
			default:
			}
		})
	}
}

func TestCancelledUnacceptedActiveHasNoAcceptanceReport(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	sender := newFullSendBuffer()
	c.signalWriter = nil
	c.controlTransport = sender
	c.transportCloser = sender.close
	management := c.ManagementIndications()
	result := make(chan error, 1)
	go func() { result <- c.ASPActive(context.Background(), associationConfigASKey(c.cfg, 2)) }()
	<-sender.waiting
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatal(err)
	}
	if ind := <-management; ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("withdrawal report=%+v", ind)
	}
	select {
	case ind := <-management:
		assertApplicationActivationFailure(t, ind, associationConfigASKey(c.cfg, 2))
	default:
		t.Fatal("missing generic application failure after unaccepted cancellation")
	}
	select {
	case ind := <-management:
		t.Fatalf("unaccepted cancellation reported SCTP acceptance: %+v", ind)
	default:
	}
}

// Without an earlier Ack in history, main accepts an unsolicited Active Ack
// when no Active request is outstanding. Cancellation must add no correlation.
func TestCancelledActiveAckWithoutHistoryRetainsMainHandling(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	sender := &acceptanceWinsSender{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = sender
	c.transportCloser = sender.close
	message := messages.NewAspActive(nil, params.NewRoutingContext(2), nil)
	request := c.startTAck(message, requestAspActive)
	done := make(chan error, 1)
	go func() { _, err := c.writePendingControl(request, message); done <- err }()
	<-sender.waiting
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatal(err)
	}
	management := c.ManagementIndications()
	for len(management) > 0 {
		<-management
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(2), nil))
	if !c.routingContextAcked(2) {
		t.Fatal("unrequested Active Ack handling diverged from main")
	}
	select {
	case ind := <-management:
		t.Fatalf("later Ack emitted cancellation correlation report: %+v", ind)
	default:
	}
}
