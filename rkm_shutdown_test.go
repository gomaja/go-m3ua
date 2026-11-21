package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type rkmShutdownTransport struct{ frames chan []byte }

func (w *rkmShutdownTransport) SendMsg(b []byte, opts sctp.SendOptions) (int, error) {
	if opts.Info.Stream != 0 || opts.Info.PPID != M3UAPPID || opts.Info.Flags != 0 {
		return 0, errors.New("control must use ordered stream 0")
	}
	w.frames <- append([]byte(nil), b...)
	return len(b), nil
}
func (w *rkmShutdownTransport) SendMsgContext(_ context.Context, b []byte, opts sctp.SendOptions) (int, error) {
	return w.SendMsg(b, opts)
}

func newRKMShutdownAssociation(t *testing.T) (*Association, *rkmShutdownTransport) {
	t.Helper()
	c := newRoutingKeyTestAssociation(t, RoleASP, NewAssociationConfig())
	c.setState(StateASPActive)
	c.cfg.TAck = time.Hour
	c.cfg.ControlWriteTimeout = 50 * time.Millisecond
	c.sctpInfo = &sctp.SndInfo{PPID: M3UAPPID}
	w := &rkmShutdownTransport{frames: make(chan []byte, 16)}
	c.controlTransport = w
	return c, w
}
func receiveRKMShutdownFrame(t *testing.T, w *rkmShutdownTransport) messages.M3UA {
	t.Helper()
	raw := receiveASPTest(t, w.frames, "control frame")
	m, err := messages.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return m
}
func requireRKMShutdownRefusal(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrASPProcedureSuperseded) || !errors.Is(err, errASPProcedureShutdown) {
		t.Fatalf("registration error=%v; want shared orderly-shutdown refusal", err)
	}
}
func TestRegisterRoutingKeysRefusedAfterShutdownClaim(t *testing.T) {
	for _, state := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(state.String(), func(t *testing.T) {
			c, w := newRKMShutdownAssociation(t)
			c.setState(state)
			if !c.claimShutdown() {
				t.Fatal("shutdown claim lost")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			_, err := c.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
			requireRKMShutdownRefusal(t, err)
			if len(w.frames) != 0 {
				t.Fatal("registration reached transport")
			}
		})
	}
}

// Pause after entry admission and the request gate, before building the REG REQ.
type pausedRKMContext struct {
	context.Context
	calls   int
	entered chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (c *pausedRKMContext) Err() error {
	c.calls++
	if c.calls == 2 {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.Context.Err()
}
func TestRegisterRoutingKeysRechecksShutdownAtSend(t *testing.T) {
	c, w := newRKMShutdownAssociation(t)
	release, open := aspTestGate(t)
	ctx := &pausedRKMContext{Context: context.Background(), entered: make(chan struct{}), release: release}
	result := make(chan error, 1)
	go func() {
		_, err := c.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
		result <- err
	}()
	receiveASPTest(t, ctx.entered, "registration admitted")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- c.ShutdownContext(shutdownCtx) }()
	if _, ok := receiveRKMShutdownFrame(t, w).(*messages.AspInactive); !ok {
		t.Fatal("first withdrawal must be ASP Inactive")
	}
	open()
	// Closing would otherwise mask an admitted REG that waits forever for REG RSP.
	select {
	case err := <-result:
		requireRKMShutdownRefusal(t, err)
	case <-time.After(time.Second):
		_ = c.Close()
		endedErr := <-result
		t.Fatalf("registration passed final shutdown admission; close returned %v", endedErr)
	}
	if len(w.frames) != 0 {
		t.Fatal("REG REQ followed first withdrawal")
	}
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if _, ok := receiveRKMShutdownFrame(t, w).(*messages.AspDown); !ok {
		t.Fatal("second withdrawal must be ASP Down")
	}
	if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
		t.Fatal(err)
	}
	if err := receiveASPTest(t, shutdown, "shutdown completion"); err != nil {
		t.Fatal(err)
	}
}

func TestDeregisterApplicationServersAllowedDuringShutdown(t *testing.T) {
	c, w := newRKMShutdownAssociation(t)
	if !c.claimShutdown() {
		t.Fatal("shutdown claim lost")
	}
	result := make(chan error, 1)
	go func() {
		_, err := c.DeregisterApplicationServers(context.Background(), ASKey{RoutingContext: 1, RoutingContextSet: true})
		result <- err
	}()
	if _, ok := receiveRKMShutdownFrame(t, w).(*messages.DeregistrationRequest); !ok {
		t.Fatal("DEREG REQ refused")
	}
	err := c.handleDeregistrationResponse(messages.NewDeregistrationResponse(params.NewDeregistrationResult(params.NewDeregResultPayload(params.NewRoutingContext(1), params.NewDeregistrationStatus(params.SuccessfullyDeregistered)))))
	if err != nil {
		t.Fatal(err)
	}
	if err := receiveASPTest(t, result, "deregistration completion"); err != nil {
		t.Fatal(err)
	}
}

func TestPendingRegistrationWaitersEndOnShutdownClose(t *testing.T) {
	c, w := newRKMShutdownAssociation(t)
	first := make(chan error, 1)
	go func() {
		_, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
		first <- err
	}()
	if _, ok := receiveRKMShutdownFrame(t, w).(*messages.RegistrationRequest); !ok {
		t.Fatal("no pending registration")
	}
	// A second request has passed entry but is still behind the first RKM request.
	ctx := newRKMInitialCheckContext()
	second := make(chan error, 1)
	go func() {
		_, err := c.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 200, 3)})
		second <- err
	}()
	receiveASPTest(t, ctx.checked, "queued registration entry")
	if !c.claimShutdown() {
		t.Fatal("shutdown claim lost")
	}
	select {
	case err := <-first:
		t.Fatalf("pending REG unexpectedly ended on claim: %v", err)
	default:
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, result := range []chan error{first, second} {
		if err := receiveASPTest(t, result, "registration close"); !errors.Is(err, ErrAssociationClosed) && !errors.Is(err, errASPProcedureShutdown) && !errors.Is(err, ErrNotEstablished) {
			t.Fatalf("close error=%v", err)
		}
	}
}

func TestRKMRequestsRetainApplicationBackpressure(t *testing.T) {
	for _, registration := range []bool{true, false} {
		t.Run(map[bool]string{true: "REG", false: "DEREG"}[registration], func(t *testing.T) {
			c, _ := newRKMShutdownAssociation(t)
			full := newFullSendBuffer()
			c.controlTransport = full
			c.transportCloser = full.close
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			var err error
			if registration {
				_, err = c.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
			} else {
				_, err = c.DeregisterApplicationServers(ctx, ASKey{RoutingContext: 1, RoutingContextSet: true})
			}
			if !sendBufferFull(err) || c.Err() != nil || len(full.waitedFrames()) != 0 {
				t.Fatalf("application backpressure changed: err=%v association=%v", err, c.Err())
			}
		})
	}
}
