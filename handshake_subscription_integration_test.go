package m3ua

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type handshakeSubscription struct {
	association *Association
	states      <-chan State
	management  <-chan *ManagementIndication
}

func subscribeHandshake(t *testing.T, a *Association) handshakeSubscription {
	t.Helper()
	if a.ID() == 0 || a.LocalAddr() == nil || a.RemoteAddr() == nil || a.maxMessageStreamID == 0 {
		t.Error("Subscribe ran before identity, addresses and socket setup were available")
	}
	if _, ok := a.endpoint.AssociationStatus(a.ID()); !ok {
		t.Error("Subscribe ran before Endpoint tracking")
	}
	if a.stateEntered {
		t.Error("Subscribe ran after the handshake started")
	}
	return handshakeSubscription{a, a.StateChanges(), a.ManagementIndications()}
}

func assertHandshakeStates(t *testing.T, subscription handshakeSubscription) {
	t.Helper()
	for _, want := range []State{StateASPInactive, StateASPActive} {
		if got := receiveSubscription(t, subscription.states); got != want {
			t.Fatalf("state = %v, want %v", got, want)
		}
	}
	select {
	case got := <-subscription.states:
		t.Fatalf("unexpected state after handshake: %v", got)
	default:
	}
}

// Adapts the issue's two-AS reproducer. Each AS sends its own NTFY after
// the related Ack (RFC 4666 Section 4.3.4.5), so four indications are expected.
func TestDialSubscribeReceivesHandshakeIndications(t *testing.T) {
	ctx := subscriptionContext(t)
	cfg := mcSGPConfig()
	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(cfg))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	accepted := make(chan *Association, 1)
	acceptErr := make(chan error, 1)
	go func() {
		a, err := ln.Accept(ctx)
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- a
	}()
	subscribed := make(chan handshakeSubscription, 1)
	var calls atomic.Int32
	aspCfg := mcASPConfig(1)
	aspCfg.Subscribe = func(a *Association) {
		if calls.Add(1) != 1 {
			return
		}
		subscribed <- subscribeHandshake(t, a)
	}
	asp, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), aspCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close() }()
	var sgp *Association
	select {
	case sgp = <-accepted:
	case err = <-acceptErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer func() { _ = sgp.Close() }()
	sub := receiveSubscription(t, subscribed)
	if sub.association != asp || calls.Load() != 1 {
		t.Fatal("Subscribe did not run exactly once for Dial's association")
	}
	assertHandshakeStates(t, sub)
	// An ordered-stream barrier proves every earlier NTFY was dispatched.
	barrier := []byte("handshake notifications delivered")
	sgp.setBeatData(barrier)
	if _, err = sgp.WriteSignal(messages.NewHeartbeat(params.NewHeartbeatData(barrier))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-sgp.beatAckChan:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	got := map[uint32][]uint16{}
	for len(sub.management) > 0 {
		ind := receiveSubscription(t, sub.management)
		if ind.Kind != ManagementNotify || ind.Association != asp.ID() || len(ind.RoutingContexts) != 1 {
			t.Fatalf("unexpected handshake indication: %+v", ind)
		}
		got[ind.RoutingContexts[0]] = append(got[ind.RoutingContexts[0]], ind.StatusInfo)
	}
	want := map[uint32][]uint16{1: {2, 3}, 2: {2, 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("handshake NTFY by Routing Context = %v, want %v", got, want)
	}
	t.Logf("handshake indications: states INACTIVE, ACTIVE; NTFY %v", got)
}

func TestAcceptSubscribeReceivesEachPeersHandshake(t *testing.T) {
	for _, selected := range []bool{false, true} {
		name := "shared config"
		if selected {
			name = "selected config"
		}
		t.Run(name, func(t *testing.T) {
			ctx := subscriptionContext(t)
			subscribed := make(chan handshakeSubscription, 2)
			var calls atomic.Int32
			cfg := mcSGPConfig()
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			cfg.Subscribe = func(a *Association) {
				calls.Add(1)
				subscribed <- subscribeHandshake(t, a)
				<-release
			}
			lc := NewListenerConfig(cfg)
			if selected {
				lc = NewListenerConfig(mcSGPConfig())
				lc.DefaultAssociationConfig.Subscribe = func(*Association) { t.Error("default Subscribe used instead of selected config") }
				lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return cfg, nil }
			}
			ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.1"), lc)
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			results := make(chan *Association, 2)
			errs := make(chan error, 2)
			for range 2 {
				go func() {
					a, err := ln.Accept(ctx)
					if err != nil {
						errs <- err
						return
					}
					results <- a
				}()
			}
			dials := make(chan *Association, 2)
			for i, ip := range []string{"127.0.0.2", "127.0.0.3"} {
				go func() {
					asp, err := dialASP(ctx, "m3ua", mcAddr(0, ip), ln.Addr().(*sctp.Addr), mcASPConfig(uint32(i+1)))
					if err != nil {
						errs <- err
						return
					}
					dials <- asp
				}()
			}
			// Both callbacks must be able to run concurrently on the shared config.
			subscriptions := []handshakeSubscription{receiveSubscription(t, subscribed), receiveSubscription(t, subscribed)}
			close(release)
			peers := map[uint16]bool{}
			for range 2 {
				select {
				case asp := <-dials:
					defer func() { _ = asp.Close() }()
					peers[asp.LocalAddr().(*sctp.Addr).Port] = true
				case err := <-errs:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			accepted := map[*Association]bool{}
			for range 2 {
				select {
				case a := <-results:
					accepted[a] = true
				case err := <-errs:
					t.Fatal(err)
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
			}
			for _, sub := range subscriptions {
				if !accepted[sub.association] || !peers[sub.association.RemoteAddr().(*sctp.Addr).Port] {
					t.Fatal("subscription belongs to another peer")
				}
				delete(accepted, sub.association)
				assertHandshakeStates(t, sub)
			}
			if calls.Load() != 2 {
				t.Fatalf("Subscribe calls = %d, want 2", calls.Load())
			}
		})
	}
}

func assertSubscriptionEnded(t *testing.T, sub handshakeSubscription) {
	t.Helper()
	for {
		select {
		case _, ok := <-sub.states:
			if !ok {
				goto management
			}
		case <-time.After(3 * time.Second):
			t.Fatal("state channel did not close")
		}
	}
management:
	for {
		select {
		case _, ok := <-sub.management:
			if !ok {
				return
			}
		case <-time.After(3 * time.Second):
			t.Fatal("management channel did not close")
		}
	}
}

func assertNoSubscriptionWorkers(t *testing.T) {
	t.Helper()
	for _, fn := range []string{"go-m3ua.(*Association).monitor", "go-m3ua.(*Association).readLoop", "go-m3ua.(*Association).dispatchLoop", "go-m3ua.(*Association).heartbeat", "go-m3ua.(*Association).writeNotifications"} {
		if !waitFor(func() bool { return len(goroutinesBlockedIn(fn)) == 0 }, 3*time.Second) {
			t.Errorf("association workers leaked: %v", goroutinesBlockedIn(fn))
		}
	}
}

func TestDialSubscribeHandshakeFailureClosesStreams(t *testing.T) {
	for _, failure := range []string{"ERR", "timeout", "context", "Endpoint close"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(subscriptionContext(t))
			defer cancel()
			up := make(chan struct{}, 1)
			peer := newRawPeer(t, 27981, func(m messages.M3UA) messages.M3UA {
				if _, ok := m.(*messages.AspUp); ok {
					select {
					case up <- struct{}{}:
					default:
					}
					if failure == "ERR" {
						return messages.NewError(params.NewErrorCode(params.ErrRefusedManagementBlocking), nil, nil, nil, nil)
					}
				}
				return nil
			})
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			subscribed := make(chan handshakeSubscription, 1)
			cfg := mcASPConfig(1)
			cfg.EstablishTimeout = 200 * time.Millisecond
			cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a) }
			finished := make(chan error, 1)
			go func() { _, err := endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.addr, cfg); finished <- err }()
			sub := receiveSubscription(t, subscribed)
			receiveSubscription(t, up)
			want := ErrTimeout
			switch failure {
			case "context":
				cancel()
				want = context.Canceled
			case "Endpoint close":
				_ = endpoint.Close()
				want = ErrEndpointClosed
			}
			if err := receiveSubscription(t, finished); !errors.Is(err, want) {
				t.Fatalf("Dial = %v, want %v", err, want)
			}
			if failure == "ERR" {
				ind := receiveSubscription(t, sub.management)
				if ind.Kind != ManagementError || ind.ErrorCode != params.ErrRefusedManagementBlocking {
					t.Fatalf("M-ERROR = %+v", ind)
				}
			}
			assertSubscriptionEnded(t, sub)
			if len(endpoint.AssociationStatuses()) != 0 {
				t.Error("failed Dial remains tracked")
			}
			assertNoSubscriptionWorkers(t)
		})
	}
}

func TestSubscribePanicReleasesAssociation(t *testing.T) {
	for _, accept := range []bool{false, true} {
		name := "Dial"
		if accept {
			name = "Accept"
		}
		t.Run(name, func(t *testing.T) {
			ctx := subscriptionContext(t)
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			subscribed := make(chan handshakeSubscription, 1)
			token := new(int)
			cfg := mcSGPConfig()
			cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a); panic(token) }
			var recovered any
			if accept {
				ln, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(cfg))
				if err != nil {
					skipIfSCTPUnsupported(t, err)
					t.Fatal(err)
				}
				defer func() { _ = ln.Close() }()
				raw, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = raw.Close() }()
				func() { defer func() { recovered = recover() }(); _, _ = ln.Accept(ctx) }()
				ln.muConns.Lock()
				tracked := len(ln.conns)
				pending := len(ln.pendingSCTP)
				ln.muConns.Unlock()
				if tracked != 0 || pending != 0 {
					t.Errorf("Listener retains associations: tracked=%d pending=%d", tracked, pending)
				}
			} else {
				peer := newRawPeer(t, 27981, func(messages.M3UA) messages.M3UA { return nil })
				func() {
					defer func() { recovered = recover() }()
					_, _ = endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.addr, cfg)
				}()
			}
			if recovered != token {
				t.Fatalf("panic = %v, want original value %v", recovered, token)
			}
			sub := receiveSubscription(t, subscribed)
			select {
			case <-sub.association.Done():
			default:
				t.Error("panic left Association open")
			}
			if len(endpoint.AssociationStatuses()) != 0 {
				t.Error("panic left Association tracked")
			}
			if _, err := sub.association.sctpConn.Status(); err == nil {
				t.Error("panic left SCTP socket open")
			}
			for _, key := range sub.association.configuredASKeys() {
				if _, ok := sub.association.as.lookup(key); ok {
					t.Errorf("panic left AS reservation %v", key)
				}
			}
			assertSubscriptionEnded(t, sub)
			assertNoSubscriptionWorkers(t)
		})
	}
}

// With a controlled peer, compare delivery with every successful NTFY write,
// including notifications before ASP Active Ack allows Dial to return.
func TestDialSubscribeCountsEveryPeerHandshakeNotify(t *testing.T) {
	ctx, cancel := context.WithCancel(subscriptionContext(t))
	defer cancel()
	ln, err := sctp.Listen("sctp", mcAddr(0, "127.0.0.2"))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	type peerResult struct {
		sent int
		err  error
	}
	results := make(chan peerResult, 1)
	go func() {
		var count int
		run := func() error {
			conn, err := ln.AcceptSCTP()
			if err != nil {
				return err
			}
			defer func() { _ = conn.Close() }()
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			send := func(message messages.M3UA) error {
				b, err := message.MarshalBinary()
				if err != nil {
					return err
				}
				_, err = sendWithInfo(conn, b, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0})
				if _, ok := message.(*messages.Notify); ok && err == nil {
					count++
				}
				return err
			}
			buf := make([]byte, 65535)
			for _, active := range []bool{false, true} {
				n, _, err := recvWithInfo(conn, buf)
				if err != nil {
					return err
				}
				msg, err := messages.Parse(buf[:n])
				if err != nil {
					return err
				}
				if !active {
					if _, ok := msg.(*messages.AspUp); !ok {
						return errors.New("expected ASP Up")
					}
					if err := send(messages.NewAspUpAck(nil, nil)); err != nil {
						return err
					}
				} else if _, ok := msg.(*messages.AspActive); !ok {
					return errors.New("expected ASP Active")
				}
				status := params.AsStateInactive
				if active {
					status = params.AsStateActive
				}
				// Different counts make a lost or duplicated phase observable.
				nnotify := 3
				if active {
					nnotify = 5
				}
				for range nnotify {
					if err := send(messages.NewNotify(params.NewStatus(status), nil, params.NewRoutingContext(1), nil)); err != nil {
						return err
					}
				}
				if active {
					if err := send(messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1, 2), nil)); err != nil {
						return err
					}
				}
			}
			// Keep the transport live until the test ends.
			results <- peerResult{sent: count}
			<-ctx.Done()
			return nil
		}
		if err := run(); err != nil {
			results <- peerResult{err: err}
		}
	}()
	subscribed := make(chan handshakeSubscription, 1)
	cfg := mcASPConfig(1)
	cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a) }
	asp, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close() }()
	sub := receiveSubscription(t, subscribed)
	assertHandshakeStates(t, sub)
	peer := receiveSubscription(t, results)
	if peer.err != nil {
		t.Fatal(peer.err)
	}
	if got := len(sub.management); got != peer.sent {
		t.Fatalf("M-NOTIFY delivered=%d, peer sent=%d", got, peer.sent)
	}
	for i := range peer.sent {
		ind := receiveSubscription(t, sub.management)
		want := uint16(2)
		if i >= 3 {
			want = 3
		}
		if ind.Kind != ManagementNotify || ind.StatusType != 1 || ind.StatusInfo != want || !reflect.DeepEqual(ind.RoutingContexts, []uint32{1}) {
			t.Fatalf("notification %d: %+v", i, ind)
		}
	}
}

func TestAcceptSubscribeHandshakeFailureClosesStreams(t *testing.T) {
	for _, failure := range []string{"ERR", "timeout", "context", "Endpoint close"} {
		t.Run(failure, func(t *testing.T) {
			ctx, cancel := context.WithCancel(subscriptionContext(t))
			defer cancel()
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			subscribed := make(chan handshakeSubscription, 1)
			cfg := mcSGPConfig()
			cfg.EstablishTimeout = 200 * time.Millisecond
			cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a) }
			ln, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(cfg))
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			finished := make(chan error, 1)
			go func() { _, err := ln.Accept(ctx); finished <- err }()
			raw, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = raw.Close() }()
			sub := receiveSubscription(t, subscribed)
			want := ErrTimeout
			switch failure {
			case "ERR":
				b, err := messages.NewError(params.NewErrorCode(params.ErrRefusedManagementBlocking), nil, nil, nil, nil).MarshalBinary()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := sendWithInfo(raw, b, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
					t.Fatal(err)
				}
			case "context":
				cancel()
				want = context.Canceled
			case "Endpoint close":
				_ = endpoint.Close()
				want = ErrEndpointClosed
			}
			err = receiveSubscription(t, finished)
			// Accept preserves its existing per-peer wrapper even for Endpoint close.
			if failure == "Endpoint close" {
				want = ErrAssociationClosed
			}
			if !errors.Is(err, want) {
				t.Fatalf("Accept = %v, want %v", err, want)
			}
			if failure == "ERR" {
				ind := receiveSubscription(t, sub.management)
				if ind.Kind != ManagementError || ind.ErrorCode != params.ErrRefusedManagementBlocking {
					t.Fatalf("M-ERROR = %+v", ind)
				}
			}
			assertSubscriptionEnded(t, sub)
			if len(endpoint.AssociationStatuses()) != 0 {
				t.Error("failed Accept remains tracked")
			}
			assertNoSubscriptionWorkers(t)
		})
	}
}

func TestSubscribeIsNotCalledBeforeAnAssociationExists(t *testing.T) {
	ctx := subscriptionContext(t)
	var calls atomic.Int32
	callback := func(*Association) { calls.Add(1) }
	cfg := mcSGPConfig()
	cfg.Subscribe = callback
	lc := NewListenerConfig(cfg)
	refused := errors.New("selection refused")
	lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return nil, refused }
	ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.2"), lc)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	raw, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	if _, err := ln.Accept(ctx); !errors.Is(err, refused) {
		t.Fatalf("Accept = %v, want selection failure", err)
	}
	// Closing a listening socket gives Dial a real refused SCTP attempt.
	remote := ln.Addr().(*sctp.Addr)
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	aspCfg := mcASPConfig(1)
	aspCfg.Subscribe = callback
	aspCfg.InitTimeout = 100 * time.Millisecond
	if a, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), remote, aspCfg); err == nil {
		_ = a.Close()
		t.Fatal("Dial unexpectedly succeeded")
	}
	if calls.Load() != 0 {
		t.Fatalf("Subscribe calls = %d on failed SCTP/selection, want 0", calls.Load())
	}
}

func TestSubscribeReturnsBeforeHandshakeAndBudgetStart(t *testing.T) {
	for _, accept := range []bool{false, true} {
		name := "Dial"
		if accept {
			name = "Accept"
		}
		t.Run(name, func(t *testing.T) {
			ctx := subscriptionContext(t)
			entered := make(chan handshakeSubscription, 1)
			release := make(chan struct{})
			var released atomic.Bool
			defer func() {
				if released.CompareAndSwap(false, true) {
					close(release)
				}
			}()
			callback := func(a *Association) { entered <- subscribeHandshake(t, a); <-release }
			aspCfg, sgpCfg := mcASPConfig(1), mcSGPConfig()
			cfg := aspCfg
			if accept {
				cfg = sgpCfg
			}
			cfg.EstablishTimeout = time.Second
			cfg.Subscribe = callback
			ln, err := listenSGP("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(sgpCfg))
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			type result struct {
				association *Association
				err         error
			}
			accepts, dials := make(chan result, 1), make(chan result, 1)
			go func() { a, err := ln.Accept(ctx); accepts <- result{a, err} }()
			go func() {
				a, err := dialASP(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), aspCfg)
				dials <- result{a, err}
			}()
			sub := receiveSubscription(t, entered)
			// Intentionally keep this test callback parked beyond EstablishTimeout.
			time.Sleep(1200 * time.Millisecond)
			if sub.association.State() != StateASPDown || len(sub.states) != 0 || len(sub.management) != 0 {
				t.Error("handshake ran before Subscribe returned")
			}
			select {
			case r := <-accepts:
				t.Fatalf("Accept returned before callback: %v", r.err)
			default:
			}
			select {
			case r := <-dials:
				t.Fatalf("Dial returned before callback: %v", r.err)
			default:
			}
			if released.CompareAndSwap(false, true) {
				close(release)
			}
			for _, ch := range []<-chan result{accepts, dials} {
				r := receiveSubscription(t, ch)
				if r.association != nil {
					defer func() { _ = r.association.Close() }()
				}
				if r.err != nil {
					t.Fatalf("callback consumed the establishment budget: %v", r.err)
				}
			}
			assertHandshakeStates(t, sub)
		})
	}
}

func TestSubscribeOverlapsEndpointTeardown(t *testing.T) {
	for _, test := range []struct {
		name     string
		accept   bool
		explicit bool
	}{
		{"Dial", false, false}, {"Accept", true, false},
		{"Dial explicit ASP Up", false, true}, {"Accept explicit ASP Up", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			accept := test.accept
			ctx := subscriptionContext(t)
			role := RoleSGP
			cfg := mcSGPConfig()
			if test.explicit {
				role = RoleASP
				cfg = mcASPConfig(1)
				cfg.ASPProcedures = ipspInitiationPolicy(false, false)
			}
			endpoint, err := NewEndpoint(EndpointConfig{Role: role})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			subscribed := make(chan handshakeSubscription, 1)
			release := make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a); <-release }
			finished := make(chan error, 1)
			if accept {
				ln, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(cfg))
				if err != nil {
					skipIfSCTPUnsupported(t, err)
					t.Fatal(err)
				}
				defer func() { _ = ln.Close() }()
				go func() { _, err := ln.Accept(ctx); finished <- err }()
				raw, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = raw.Close() }()
			} else {
				peer := newRawPeer(t, 27981, func(messages.M3UA) messages.M3UA { return nil })
				go func() { _, err := endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.addr, cfg); finished <- err }()
			}
			sub := receiveSubscription(t, subscribed)
			closed := make(chan error, 1)
			go func() { closed <- endpoint.Close() }()
			// Endpoint teardown must close the subscribed channels even while the
			// caller is still in Subscribe and no monitor has started.
			assertSubscriptionEnded(t, sub)
			close(release)
			want := ErrEndpointClosed
			if accept {
				want = ErrAssociationClosed
			}
			if err := receiveSubscription(t, finished); !errors.Is(err, want) {
				t.Fatalf("establishment after callback teardown = %v, want %v", err, want)
			}
			if err := receiveSubscription(t, closed); err != nil {
				t.Fatal(err)
			}
			if len(endpoint.AssociationStatuses()) != 0 {
				t.Error("Endpoint teardown left the callback's association tracked")
			}
			assertNoSubscriptionWorkers(t)
			select {
			case <-sub.association.established:
				t.Fatal("closed association published establishment readiness")
			default:
			}
		})
	}
}

func TestSubscribeStreamsRemainIndependent(t *testing.T) {
	for index, statesOnly := range []bool{true, false} {
		name := "state only"
		if !statesOnly {
			name = "management only"
		}
		t.Run(name, func(t *testing.T) {
			ctx := subscriptionContext(t)
			var readers sync.WaitGroup
			var received atomic.Int32
			var calls atomic.Int32
			var subscribed *Association
			t.Cleanup(func() {
				if subscribed != nil {
					_ = subscribed.Close()
				}
				readers.Wait()
			})
			callback := func(a *Association) {
				calls.Add(1)
				subscribed = a
				if statesOnly {
					stream := a.StateChanges()
					readers.Go(func() {
						for range stream {
							received.Add(1)
						}
					})
				} else {
					stream := a.ManagementIndications()
					readers.Go(func() {
						for range stream {
							received.Add(1)
						}
					})
				}
			}
			asp, sgp, err := setupConnHB(t, ctx, 27984+index, &HeartbeatInfo{Enabled: false}, &HeartbeatInfo{Enabled: false}, callback)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = asp.Close(); _ = sgp.Close() })
			if calls.Load() != 1 || subscribed != asp {
				t.Fatal("Subscribe did not install the selected reader")
			}
			for range 140 {
				if _, err := sgp.WriteSignal(messages.NewNotify(params.NewStatus(params.AsStateActive), nil, params.NewRoutingContext(1), nil)); err != nil {
					t.Fatal(err)
				}
			}
			barrier := []byte("independent subscription")
			sgp.setBeatData(barrier)
			if _, err := sgp.WriteSignal(messages.NewHeartbeat(params.NewHeartbeatData(barrier))); err != nil {
				t.Fatal(err)
			}
			select {
			case <-sgp.beatAckChan:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			want := int32(144)
			if statesOnly {
				want = 2
			}
			if !waitFor(func() bool { return received.Load() == want }, time.Second) {
				t.Fatalf("selected stream delivered %d indications, want %d", received.Load(), want)
			}
			if statesOnly && len(asp.mgmtChan) != 0 {
				t.Error("unsubscribed management stream queued indications")
			}
			if !statesOnly && len(asp.stateEventChan) != 0 {
				t.Error("unsubscribed state stream queued transitions")
			}
			select {
			case <-asp.Done():
				t.Fatalf("independent subscription closed association: %v", asp.Err())
			default:
			}
		})
	}
}

func TestCanceledContextInSubscribeFailsEstablishment(t *testing.T) {
	for _, accept := range []bool{false, true} {
		name := "Dial"
		if accept {
			name = "Accept"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(subscriptionContext(t))
			defer cancel()
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			subscribed := make(chan handshakeSubscription, 1)
			cfg := mcASPConfig(1)
			cfg.ASPProcedures = ipspInitiationPolicy(false, false)
			cfg.Subscribe = func(a *Association) { subscribed <- subscribeHandshake(t, a); cancel() }
			var association *Association
			if accept {
				ln, listenErr := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.2"), NewListenerConfig(cfg))
				if listenErr != nil {
					skipIfSCTPUnsupported(t, listenErr)
					t.Fatal(listenErr)
				}
				defer func() { _ = ln.Close() }()
				raw, dialErr := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr))
				if dialErr != nil {
					t.Fatal(dialErr)
				}
				defer func() { _ = raw.Close() }()
				association, err = ln.Accept(ctx)
			} else {
				peer := newRawPeer(t, 27981, func(messages.M3UA) messages.M3UA { return nil })
				association, err = endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), peer.addr, cfg)
			}
			if association != nil {
				defer func() { _ = association.Close() }()
			}
			if err == nil {
				t.Fatal("canceled Subscribe returned success")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("establishment error = %v, want context.Canceled", err)
			}
			sub := receiveSubscription(t, subscribed)
			assertSubscriptionEnded(t, sub)
			assertNoSubscriptionWorkers(t)
		})
	}
}

func TestCanceledMonitorStartupDoesNotPublishReadiness(t *testing.T) {
	ctx := subscriptionContext(t)
	peer := newRawPeer(t, 27983, func(messages.M3UA) messages.M3UA { return nil })
	transport, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.1"), peer.addr)
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		_ = transport.Close()
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	cfg := mcASPConfig(1)
	cfg.ASPProcedures = ipspInitiationPolicy(false, false)
	a := newAssociation(RoleASP, cfg)
	a.sctpConn = transport
	if !endpoint.trackAssociation(a) {
		_ = a.Close()
		t.Fatal("could not track association")
	}
	if err := a.setUpSocket(); err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	sub := subscribeHandshake(t, a)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	// Drive startup synchronously so no Dial/Accept select can win the race
	// and close done before the monitor checks the explicit ASP-DOWN policy.
	a.monitor(canceled)
	select {
	case <-a.established:
		t.Fatal("canceled monitor published establishment readiness")
	default:
	}
	if !errors.Is(a.Err(), context.Canceled) {
		t.Fatalf("startup cause = %v", a.Err())
	}
	assertSubscriptionEnded(t, sub)
	assertNoSubscriptionWorkers(t)
}
