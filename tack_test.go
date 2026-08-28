// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

type tackMessageProbe struct {
	raw        []byte
	entered    chan struct{}
	release    chan struct{}
	concurrent chan struct{}
	enterOnce  sync.Once
	raceOnce   sync.Once
	active     atomic.Int32
}

func newTAckMessageProbe(t *testing.T) *tackMessageProbe {
	t.Helper()
	raw, err := messages.NewAspUp(nil, params.NewInfoString("retry clone probe")).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal probe ASP Up: %v", err)
	}
	return &tackMessageProbe{
		raw:        raw,
		entered:    make(chan struct{}),
		release:    make(chan struct{}),
		concurrent: make(chan struct{}),
	}
}

func (p *tackMessageProbe) MarshalBinary() ([]byte, error) {
	return bytes.Clone(p.raw), nil
}

func (p *tackMessageProbe) MarshalTo(dst []byte) error {
	if len(dst) < len(p.raw) {
		return messages.ErrTooShortToMarshalBinary
	}
	if p.active.Add(1) == 1 {
		p.enterOnce.Do(func() { close(p.entered) })
		<-p.release
	} else {
		p.raceOnce.Do(func() { close(p.concurrent) })
	}
	copy(dst, p.raw)
	p.active.Add(-1)
	return nil
}

func (p *tackMessageProbe) UnmarshalBinary([]byte) error { return nil }
func (p *tackMessageProbe) MarshalLen() int              { return len(p.raw) }
func (p *tackMessageProbe) Version() uint8               { return 1 }
func (p *tackMessageProbe) MessageClass() uint8          { return messages.MsgClassASPSM }
func (p *tackMessageProbe) MessageType() uint8           { return messages.MsgTypeAspUp }
func (p *tackMessageProbe) MessageClassName() string     { return messages.MsgClassNameASPSM }
func (p *tackMessageProbe) MessageTypeName() string      { return "ASP Up" }

// T(ack) is the other half of the deadlock this package already fixes on the
// receiving side. RFC 4666 Sections 4.3.4.1 to 4.3.4.4 each say the ASP "MAY
// restart T(ack) and resend [the request] until it receives [the] Ack": without
// it, a request lost in transit — or dropped by a peer that was briefly out of
// state — strands the association forever, because we wait for an Ack that will
// never come while the peer waits for a request it never saw.

// tackConn builds an ASP whose T(ack) fires quickly, with its outbound
// signals captured under a mutex so the retransmit goroutine and the test can
// both touch them.
func tackConn(t *testing.T, state State, interval time.Duration, retries int) (*Association, func() []messages.M3UA) {
	t.Helper()

	conn, _ := newTestConn(t, state, RoleASP)
	conn.cfg.TAck = interval
	conn.cfg.TAckRetries = retries

	var (
		mu   sync.Mutex
		sent []messages.M3UA
	)
	conn.signalWriter = func(m3 messages.M3UA) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		sent = append(sent, m3)
		return m3.MarshalLen(), nil
	}

	return conn, func() []messages.M3UA {
		mu.Lock()
		defer mu.Unlock()
		return append([]messages.M3UA(nil), sent...)
	}
}

// countType reports how many captured signals have the given type name.
func countType(msgs []messages.M3UA, name string) int {
	n := 0
	for _, m := range msgs {
		if m.MessageTypeName() == name {
			n++
		}
	}
	return n
}

// The headline behaviour: a peer that never answers an ASP Up must see it
// resent. Before T(ack) existed, a single lost ASP Up left the association
// stranded with no recovery short of tearing down the SCTP association.
func TestTAckResendsUnacknowledgedAspUp(t *testing.T) {
	conn, snapshot := tackConn(t, StateASPDown, 50*time.Millisecond, 5)

	if err := conn.initiateASPSM(); err != nil {
		t.Fatalf("initiateASPSM() error = %v", err)
	}

	// One initial send plus retransmissions.
	if !waitFor(func() bool { return countType(snapshot(), "ASP Up") >= 3 }, 3*time.Second) {
		t.Errorf("ASP Up sent %d times, want it resent while unacknowledged",
			countType(snapshot(), "ASP Up"))
	}
}

// The Ack must stop the retransmission. A retransmitter that keeps firing after
// the handshake completes floods the peer with duplicate requests, and RFC 4666
// has the SGP answer every one of them — turning a healthy link into a loop.
func TestTAckStopsOnAck(t *testing.T) {
	conn, snapshot := tackConn(t, StateASPDown, 50*time.Millisecond, 20)

	if err := conn.initiateASPSM(); err != nil {
		t.Fatal(err)
	}
	// Let at least one retransmission happen so the timer is demonstrably live.
	if !waitFor(func() bool { return countType(snapshot(), "ASP Up") >= 2 }, 3*time.Second) {
		t.Fatal("ASP Up was never retransmitted; the timer is not running")
	}

	// The Ack arrives.
	conn.muState.Lock()
	conn.state = StateASPInactive
	conn.muState.Unlock()
	if err := conn.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("handleAspUpAck() error = %v", err)
	}

	if conn.pendingTAck() != 0 {
		t.Errorf("pendingTAck() = %d after the Ack, want 0", conn.pendingTAck())
	}

	settled := countType(snapshot(), "ASP Up")
	time.Sleep(300 * time.Millisecond) // several more T(ack) periods
	if got := countType(snapshot(), "ASP Up"); got != settled {
		t.Errorf("ASP Up resent %d more times after the Ack; retransmission must stop", got-settled)
	}
}

// Retransmission is bounded. The RFC's "until it receives the Ack" is
// unbounded, but an ASP resending forever at a peer that will never answer just
// loads a network already in trouble; the budget converts a silent hang into a
// reportable failure an operator can see.
func TestTAckGivesUpAndReportsFailure(t *testing.T) {
	conn, snapshot := tackConn(t, StateASPDown, 30*time.Millisecond, 3)

	if err := conn.initiateASPSM(); err != nil {
		t.Fatal(err)
	}

	var reported bool
	if !waitFor(func() bool {
		for {
			select {
			case err := <-conn.errChan:
				if errors.Is(err, ErrTAckExpired) {
					reported = true
					return true
				}
				continue
			default:
				return false
			}
		}
	}, 3*time.Second) {
		t.Error("T(ack) exhausted its retries without reporting ErrTAckExpired")
	}
	if !reported {
		t.Error("no ErrTAckExpired on errChan")
	}

	// It must actually stop rather than retry forever.
	settled := countType(snapshot(), "ASP Up")
	time.Sleep(200 * time.Millisecond)
	if got := countType(snapshot(), "ASP Up"); got != settled {
		t.Errorf("still resending after the retry budget: %d -> %d", settled, got)
	}
	if conn.pendingTAck() != 0 {
		t.Errorf("pendingTAck() = %d after giving up, want 0", conn.pendingTAck())
	}
}

// ASP Active gets the same treatment (RFC 4666 Section 4.3.4.3), and its Ack
// must stop it. This is the request that carries traffic mode and routing
// context, so a lost one leaves an ASP that is up but never carries traffic.
func TestTAckResendsAspActiveUntilAcked(t *testing.T) {
	conn, snapshot := tackConn(t, StateASPInactive, 50*time.Millisecond, 20)

	if err := conn.initiateASPTM(); err != nil {
		t.Fatalf("initiateASPTM() error = %v", err)
	}
	if !waitFor(func() bool { return countType(snapshot(), "ASP Active") >= 2 }, 3*time.Second) {
		t.Fatalf("ASP Active sent %d times, want it resent while unacknowledged",
			countType(snapshot(), "ASP Active"))
	}

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		conn.cfg.TrafficModeType, conn.cfg.RoutingContexts, nil)); err != nil {
		t.Fatalf("handleAspActiveAck() error = %v", err)
	}

	settled := countType(snapshot(), "ASP Active")
	time.Sleep(250 * time.Millisecond)
	if got := countType(snapshot(), "ASP Active"); got != settled {
		t.Errorf("ASP Active resent %d more times after its Ack", got-settled)
	}
}

// Closing the association must stop every retransmitter. A goroutine that keeps
// writing to a closing socket leaks for the lifetime of the process, which is
// exactly the failure mode the heartbeat fix addressed elsewhere.
func TestTAckStopsOnClose(t *testing.T) {
	conn, _ := tackConn(t, StateASPDown, 50*time.Millisecond, 100)

	if err := conn.initiateASPSM(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return conn.pendingTAck() > 0 }, 2*time.Second) {
		t.Fatal("no pending request registered")
	}

	conn.stopAllTAck()

	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pendingTAck() = %d after stopping, want 0", got)
	}
	if !waitFor(func() bool {
		return len(goroutinesBlockedIn("go-m3ua.(*Association).runTAck")) == 0
	}, 2*time.Second) {
		t.Error("a T(ack) goroutine outlived the association")
	}
}

// A second request of the same kind supersedes the first: only the most recent
// one can still be legitimately answered. Without this, an ASP that retried a
// handshake would accumulate one retransmitter per attempt, each resending a
// stale request.
func TestTAckSupersedesEarlierRequestOfSameKind(t *testing.T) {
	conn, _ := tackConn(t, StateASPDown, 50*time.Millisecond, 50)

	for range 5 {
		if err := conn.initiateASPSM(); err != nil {
			t.Fatal(err)
		}
	}

	if got := conn.pendingTAck(); got != 1 {
		t.Errorf("pendingTAck() = %d after 5 ASP Ups, want 1 (each supersedes the last)", got)
	}
}

// A superseded timer can already be inside WriteSignal when its replacement is
// armed. When that stale attempt returns, it must neither delete the new timer
// nor report that the new request expired.
func TestStaleTAckCannotRetireSupersedingRequest(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleASP)
	conn.cfg.TAck = 5 * time.Millisecond
	conn.cfg.TAckRetries = 1

	oldRequest := messages.NewAspUp(nil, params.NewInfoString("old"))
	newRequest := messages.NewAspUp(nil, params.NewInfoString("new"))
	oldWriteStarted := make(chan struct{})
	releaseOldWrite := make(chan struct{})
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		aspUp, ok := message.(*messages.AspUp)
		if ok && aspUp.InfoString != nil && string(aspUp.InfoString.Data) == "old" {
			close(oldWriteStarted)
			<-releaseOldWrite
		}
		return message.MarshalLen(), nil
	}

	conn.startTAck(oldRequest, requestAspUp)
	select {
	case <-oldWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("old T(ack) did not enter its retransmission write")
	}

	// The old goroutine has read the short interval and retry count. Give the
	// replacement a long interval so only the stale goroutine can finish here.
	conn.cfg.TAck = time.Second
	conn.startTAck(newRequest, requestAspUp)
	close(releaseOldWrite)
	t.Cleanup(conn.stopAllTAck)

	time.Sleep(50 * time.Millisecond)
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after stale timer completed = %d, want the replacement", got)
	}
	select {
	case err := <-conn.errChan:
		if errors.Is(err, ErrTAckExpired) {
			t.Fatalf("stale timer reported expiry for its replacement: %v", err)
		}
	default:
	}
}

// The initiating write and its timer run on different goroutines. Reusing the
// exact message object lets both MarshalTo calls rewrite its Header.Payload at
// once when T(ack) is short, which is both a data race and potentially corrupts
// the control message put on the wire. The retry must own an immutable clone.
func TestTAckRetransmissionDoesNotReuseTheInitiatingMessage(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleASP)
	conn.cfg.TAck = time.Millisecond
	conn.cfg.TAckRetries = 100
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}

	probe := newTAckMessageProbe(t)
	conn.startTAck(probe, requestAspUp)
	t.Cleanup(conn.stopAllTAck)

	writeDone := make(chan error, 1)
	go func() {
		_, err := conn.WriteSignal(probe)
		writeDone <- err
	}()

	select {
	case <-probe.entered:
	case <-time.After(time.Second):
		t.Fatal("the initiating write never entered MarshalTo")
	}

	select {
	case <-probe.concurrent:
		t.Error("T(ack) re-entered MarshalTo on the initiating message")
	case <-time.After(25 * time.Millisecond):
	}
	close(probe.release)
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("initiating write: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("initiating write did not finish")
	}
}

func TestTAckRetrySnapshotDeepCopiesEveryParameter(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	request := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1, 2),
		params.NewInfoString("original"),
	)
	request.Others = []*params.Param{params.NewParam(0xfffe, []byte{1, 2, 3, 4})}
	request.SetLength()

	pending := conn.startTAck(request, requestAspActive)
	t.Cleanup(conn.stopAllTAck)
	retry, ok := pending.msg.(*messages.AspActive)
	if !ok {
		t.Fatalf("retry snapshot type = %T, want *messages.AspActive", pending.msg)
	}
	if retry == request {
		t.Fatal("retry snapshot aliases the initiating message")
	}

	request.TrafficModeType.Data[3] = byte(params.TrafficModeBroadcast)
	request.RoutingContext.Data[3] = 99
	request.InfoString.Data[0] = 'X'
	request.Others[0].Data[0] = 99

	if got := retry.TrafficModeType.TrafficModeType(); got != params.TrafficModeLoadshare {
		t.Errorf("retry Traffic Mode = %d after caller mutation, want Loadshare", got)
	}
	if got := retry.RoutingContext.RoutingContexts(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("retry Routing Contexts = %v after caller mutation, want [1 2]", got)
	}
	if got := string(retry.InfoString.Data); got != "original" {
		t.Errorf("retry INFO String = %q after caller mutation, want original", got)
	}
	if len(retry.Others) != 1 || !bytes.Equal(retry.Others[0].Data, []byte{1, 2, 3, 4}) {
		t.Errorf("retry extension parameters = %v after caller mutation, want independent copy", retry.Others)
	}
}

// Different request kinds are tracked independently: acknowledging one must not
// silently cancel another that is still outstanding.
func TestTAckTracksRequestKindsIndependently(t *testing.T) {
	conn, _ := tackConn(t, StateASPDown, 50*time.Millisecond, 50)

	aspUp := messages.NewAspUp(nil, nil)
	aspActive := messages.NewAspActive(nil, nil, nil)
	conn.startTAck(aspUp, requestAspUp)
	conn.startTAck(aspActive, requestAspActive)

	if got := conn.pendingTAck(); got != 2 {
		t.Fatalf("pendingTAck() = %d, want 2", got)
	}

	conn.stopTAck(requestAspUp)

	if got := conn.pendingTAck(); got != 1 {
		t.Errorf("pendingTAck() = %d after acknowledging only ASP Up, want 1", got)
	}
}

// T(ack) defaults to the RFC's 2 seconds when unset, and honours configuration.
func TestTAckIntervalDefaultsToRFCValue(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleASP)

	if got := conn.tackInterval(); got != DefaultTAck {
		t.Errorf("tackInterval() = %v with no configuration, want %v (RFC 4666 default)", got, DefaultTAck)
	}
	if DefaultTAck != 2*time.Second {
		t.Errorf("DefaultTAck = %v, want 2s per RFC 4666 Sections 4.3.4.1 to 4.3.4.4", DefaultTAck)
	}

	conn.cfg.TAck = 250 * time.Millisecond
	if got := conn.tackInterval(); got != 250*time.Millisecond {
		t.Errorf("tackInterval() = %v, want the configured value", got)
	}
}

// The retransmitter runs on its own goroutine while the dispatcher handles the
// Ack that stops it. Under -race this pins that the two do not collide on the
// pending map.
func TestTAckStartAndStopAreConcurrencySafe(t *testing.T) {
	conn, _ := tackConn(t, StateASPDown, time.Millisecond, 100)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				conn.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
				conn.stopTAck(requestAspUp)
			}
		}()
	}
	wg.Wait()

	conn.stopAllTAck()
	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pendingTAck() = %d after concurrent start/stop, want 0", got)
	}
}

// An SGP must never retransmit: it answers requests, it does not make them.
// An SGP that ran T(ack) would resend Acks as though they were requests.
func TestSGPDoesNotRetransmitOnTAck(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	conn.cfg.TAck = 20 * time.Millisecond

	// The SGP's own send paths never call startTAck; nothing must be pending
	// after answering a peer's request.
	_ = conn.handleAspUp(messages.NewAspUp(nil, nil))

	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pendingTAck() = %d at an SGP, want 0: only an ASP retransmits", got)
	}
}

// Routing Context validation: an Ack naming a context we never asked about must
// not take the data path active. RFC 4666 Section 3.8.1 defines "Invalid
// Routing Context" for exactly this.
func TestAspActiveAckWithForeignRoutingContextIsRejected(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleASP)

	// Configured contexts are 1 and 2; the peer answers about 99.
	err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		conn.cfg.TrafficModeType, params.NewRoutingContext(99), nil))
	if !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("handleAspActiveAck() error = %v, want ErrInvalidRoutingContext", err)
	}

	if e := conn.handleErrors(err); e != nil {
		t.Fatalf("handleErrors() error = %v", e)
	}
	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.ErrInvalidRoutingContext {
		t.Errorf("error codes = %v, want [%d] (Invalid Routing Context)",
			codes, params.ErrInvalidRoutingContext)
	}
}

// The Routing Context parameter is Optional (RFC 4666 Section 3.7.2), so an
// Ack that omits it is answering for the configured context and must be
// accepted — rejecting it would break every peer that leaves it out.
func TestAspActiveAckWithoutRoutingContextIsAccepted(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		conn.cfg.TrafficModeType, nil, nil)); err != nil {
		t.Errorf("handleAspActiveAck() error = %v, want nil when the peer omits Routing Context", err)
	}
}

// An Ack naming a traffic mode incompatible with ours is not agreement: the two
// ends would run different traffic handling for the same AS.
func TestAspActiveAckWithIncompatibleTrafficModeIsRejected(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)

	err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeBroadcast),
		conn.cfg.RoutingContexts, nil))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Errorf("handleAspActiveAck() error = %v, want ErrUnsupportedTrafficMode", err)
	}
}

// The Acks travel SGP to ASP, so an SGP that receives one must report an Error
// and hold its state: nothing authorises a stray Ack to move an SG.
func TestSGPRejectsAspBoundAcks(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func(*Association) error
	}{
		{"ASP Down Ack", func(c *Association) error {
			return c.handleAspDownAck(messages.NewAspDownAck(nil))
		}},
		{"ASP Active Ack", func(c *Association) error {
			return c.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil))
		}},
		{"ASP Inactive Ack", func(c *Association) error {
			return c.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
				conn, _ := newTestConn(t, st, RoleSGP)

				var unexpected *UnexpectedMessageError
				if err := tt.call(conn); !errors.As(err, &unexpected) {
					t.Errorf("in %v: error = %v, want *UnexpectedMessageError at an SGP", st, err)
				}
			}
		})
	}
}

// Ack validation runs on peer-controlled parameters, so it faces the fuzzer
// too: nothing a peer can put in a Routing Context or Traffic Mode Type may
// panic the handler or wrongly take the data path active.
func FuzzAckValidation(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{0x00, 0x00, 0x00, 0x01},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
		{0xff, 0xff, 0xff, 0xff},
		{0x00},
		{0x00, 0x00, 0x00},
	} {
		f.Add(seed, uint32(params.TrafficModeLoadshare))
	}

	conn := newFuzzConn(f, RoleASP)

	f.Fuzz(func(t *testing.T, rcData []byte, mode uint32) {
		rc := params.NewParam(int(params.RoutingContext), rcData)
		tm := params.NewTrafficModeType(mode)

		conn.muState.Lock()
		conn.state = StateASPInactive
		conn.muState.Unlock()

		// Any error is acceptable; a panic is not.
		err := conn.handleAspActiveAck(messages.NewAspActiveAck(tm, rc, nil))

		// If the Ack was accepted, every context it named must be one of ours:
		// accepting a foreign context would take the data path active on a
		// message that was never about this ASP.
		if err == nil {
			ours := map[uint32]struct{}{}
			for _, v := range conn.cfg.RoutingContexts.RoutingContexts() {
				ours[v] = struct{}{}
			}
			for _, v := range rc.RoutingContexts() {
				if _, ok := ours[v]; !ok {
					t.Fatalf("accepted an ASP Active Ack naming foreign routing context %#x", v)
				}
			}
			if tm.TrafficModeType() != conn.cfg.TrafficModeType.TrafficModeType() {
				t.Fatalf("accepted an ASP Active Ack with traffic mode %d, ours is %d",
					tm.TrafficModeType(), conn.cfg.TrafficModeType.TrafficModeType())
			}
		}

		conn.muState.Lock()
		conn.state = StateASPActive
		conn.muState.Unlock()
		_ = conn.handleAspInactiveAck(messages.NewAspInactiveAck(rc, nil))
	})
}

// The dispatcher must still publish exactly one state per Ack, with the T(ack)
// cancellation folded in.
func TestAckDispatchStillPublishesOneState(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state State
		msg   messages.M3UA
	}{
		{"ASP Up Ack", StateASPDown, messages.NewAspUpAck(nil, nil)},
		{"ASP Down Ack", StateASPDown, messages.NewAspDownAck(nil)},
		{"ASP Active Ack", StateASPInactive, messages.NewAspActiveAck(nil, nil, nil)},
		{"ASP Inactive Ack", StateASPActive, messages.NewAspInactiveAck(nil, nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, tt.state, RoleASP)

			conn.handleSignals(context.Background(), tt.msg)

			if got := len(conn.stateChan); got != 1 {
				t.Errorf("published %d states for a %s, want exactly 1", got, tt.name)
			}
		})
	}
}

// The timer exists before the request can reach the peer. On loopback or an
// otherwise fast association, an Ack can be dispatched synchronously with the
// write; registering afterwards turns that valid Ack into an unsolicited one
// and leaves a fresh retransmitter running after the handshake completed.
func TestTAckIsArmedBeforeAnImmediateAck(t *testing.T) {
	t.Run("ASP Up", func(t *testing.T) {
		conn, _ := newTestConn(t, StateASPDown, RoleASP)
		conn.signalWriter = func(message messages.M3UA) (int, error) {
			if _, ok := message.(*messages.AspUp); ok {
				if err := conn.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
					t.Fatalf("immediate ASP Up Ack: %v", err)
				}
			}
			return message.MarshalLen(), nil
		}

		if err := conn.initiateASPSM(); err != nil {
			t.Fatal(err)
		}
		if got := conn.pendingTAck(); got != 0 {
			t.Errorf("pending T(ack) after immediate ASP Up Ack = %d, want 0", got)
		}
	})

	t.Run("ASP Active", func(t *testing.T) {
		conn, _ := newTestConn(t, StateASPInactive, RoleASP)
		conn.signalWriter = func(message messages.M3UA) (int, error) {
			if _, ok := message.(*messages.AspActive); ok {
				if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
					conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
				)); err != nil {
					t.Fatalf("immediate ASP Active Ack: %v", err)
				}
			}
			return message.MarshalLen(), nil
		}

		if err := conn.initiateASPTM(); err != nil {
			t.Fatal(err)
		}
		if got := conn.pendingTAck(); got != 0 {
			t.Errorf("pending T(ack) after immediate ASP Active Ack = %d, want 0", got)
		}
	})
}

// Arming before the write must not turn a local write failure into a phantom
// outstanding request that retransmits a message which never left this node.
func TestFailedInitialRequestLeavesNoTAck(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleASP)
	conn.signalWriter = func(messages.M3UA) (int, error) { return 0, ErrFailedToWriteSignal }

	if err := conn.initiateASPSM(); !errors.Is(err, ErrFailedToWriteSignal) {
		t.Fatalf("initiateASPSM error = %v, want ErrFailedToWriteSignal", err)
	}
	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pending T(ack) after failed initial write = %d, want 0", got)
	}
}

// Only a valid answer retires T(ack). A foreign RC or incompatible traffic
// mode is an Error response from us, not completion of the request; cancelling
// first strands the handshake after a single forged or malformed Ack.
func TestInvalidAspActiveAckDoesNotStopTAck(t *testing.T) {
	for _, tt := range []struct {
		name string
		ack  *messages.AspActiveAck
	}{
		{
			name: "foreign Routing Context",
			ack: messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(99), nil),
		},
		{
			name: "incompatible Traffic Mode",
			ack: messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeBroadcast),
				params.NewRoutingContext(1, 2), nil),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPInactive, RoleASP)
			conn.startTAck(messages.NewAspActive(
				conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
			), requestAspActive)

			if err := conn.handleAspActiveAck(tt.ack); err == nil {
				t.Fatal("invalid ASP Active Ack was accepted")
			}
			if got := conn.pendingTAck(); got != 1 {
				t.Errorf("pending T(ack) after invalid Ack = %d, want 1", got)
			}
			conn.stopAllTAck()
		})
	}
}

// One ASP Active can be answered by several Acks for independent Routing
// Context subsets. The first partial Ack completes only that subset; the timer
// must remain armed for the rest and stop after the final subset arrives.
func TestTAckTracksPartialAspActiveAcknowledgements(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1, 2), nil,
	), requestAspActive)

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("first partial Ack: %v", err)
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after first partial Ack = %d, want 1", got)
	}

	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(2), nil,
	)); err != nil {
		t.Fatalf("final partial Ack: %v", err)
	}
	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pending T(ack) after final partial Ack = %d, want 0", got)
	}
}

func TestTAckTracksPartialAspInactiveAcknowledgements(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(1, 2), nil), requestAspInactive)

	if err := conn.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("first partial Ack: %v", err)
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after first partial Ack = %d, want 1", got)
	}
	if !conn.routingContextAcked(2) || conn.routingContextAcked(1) {
		t.Error("first partial Ack did not deactivate exactly Routing Context 1")
	}

	if err := conn.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatalf("final partial Ack: %v", err)
	}
	if got := conn.pendingTAck(); got != 0 {
		t.Errorf("pending T(ack) after final partial Ack = %d, want 0", got)
	}
}

func TestTAckTracksConcurrentScopedASPTMRequests(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   State
		request func(*Association, uint32) error
		ack     func(*Association, uint32) error
		active  func(*Association, uint32) bool
	}{
		{
			name:  "ASP Active",
			state: StateASPInactive,
			request: func(connection *Association, routingContext uint32) error {
				return connection.ActivateRoutingContexts(routingContext)
			},
			ack: func(connection *Association, routingContext uint32) error {
				return connection.handleAspActiveAck(messages.NewAspActiveAck(
					connection.cfg.TrafficModeType.Copy(), params.NewRoutingContext(routingContext), nil,
				))
			},
			active: func(connection *Association, routingContext uint32) bool {
				return connection.routingContextAcked(routingContext)
			},
		},
		{
			name:  "ASP Inactive",
			state: StateASPActive,
			request: func(connection *Association, routingContext uint32) error {
				return connection.DeactivateRoutingContexts(routingContext)
			},
			ack: func(connection *Association, routingContext uint32) error {
				return connection.handleAspInactiveAck(messages.NewAspInactiveAck(
					params.NewRoutingContext(routingContext), nil,
				))
			},
			active: func(connection *Association, routingContext uint32) bool {
				return connection.routingContextAcked(routingContext)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, _ := newTestConn(t, test.state, RoleASP)
			if test.state == StateASPActive {
				connection.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
			}
			if err := test.request(connection, 1); err != nil {
				t.Fatal(err)
			}
			if err := test.request(connection, 2); err != nil {
				t.Fatal(err)
			}
			if got := connection.pendingTAck(); got != 2 {
				t.Fatalf("pending T(ack) requests = %d, want 2 independent scopes", got)
			}

			for _, routingContext := range []uint32{2, 1} {
				if err := test.ack(connection, routingContext); err != nil {
					t.Fatalf("Ack RC %d: %v", routingContext, err)
				}
			}
			if got := connection.pendingTAck(); got != 0 {
				t.Fatalf("pending T(ack) after both Acks = %d, want 0", got)
			}
			wantActive := test.state == StateASPInactive
			for _, routingContext := range []uint32{1, 2} {
				if got := test.active(connection, routingContext); got != wantActive {
					t.Errorf("RC %d active = %t, want %t", routingContext, got, wantActive)
				}
			}
		})
	}
}

func TestInvalidAspInactiveAckDoesNotStopTAck(t *testing.T) {
	for _, tt := range []struct {
		name string
		ack  *params.Param
		want error
	}{
		{name: "different configured context", ack: params.NewRoutingContext(2), want: ErrInvalidRoutingContext},
		{name: "missing context", ack: nil, want: ErrMissingRoutingContext},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)

			err := conn.handleAspInactiveAck(messages.NewAspInactiveAck(tt.ack, nil))
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if got := conn.pendingTAck(); got != 1 {
				t.Errorf("pending T(ack) after invalid Ack = %d, want 1", got)
			}
			conn.stopAllTAck()
		})
	}
}

func TestUnsolicitedAspInactiveAckReturnsToThePreviousActiveState(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)
	conn.noteRoutingContextsAcked(conn.cfg.RoutingContexts)

	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(conn.cfg.RoutingContexts.Copy(), nil))
	if got := conn.State(); got != StateASPInactive {
		t.Fatalf("state after unsolicited ASP Inactive Ack = %v, want ASP-INACTIVE first", got)
	}
	if !conn.resumeAfterStrayAck() {
		t.Fatal("unsolicited ASP Inactive Ack did not arm return to ASP-ACTIVE")
	}

	if err := conn.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("ASP-INACTIVE entry action: %v", err)
	}
	if got := countType(*sent, "ASP Active"); got != 1 {
		t.Errorf("return procedure sent %d ASP Active messages, want 1", got)
	}
	if conn.pendingTAck() != 1 {
		t.Error("return ASP Active was not protected by T(ack)")
	}
}

func TestUnsolicitedScopedAspInactiveAckReactivatesOnlyThatScope(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))

	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	if got := conn.State(); got != StateASPActive {
		t.Fatalf("association state = %v, want ASP-ACTIVE while RC 2 remains active", got)
	}
	if !conn.routingContextAcked(2) || conn.routingContextAcked(1) {
		t.Error("unsolicited Ack did not deactivate exactly RC 1")
	}

	var request *messages.AspActive
	for _, signal := range *sent {
		if active, ok := signal.(*messages.AspActive); ok {
			request = active
		}
	}
	if request == nil {
		t.Fatal("no ASP Active was sent to restore the displaced scope")
	}
	if got := request.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Errorf("return ASP Active Routing Contexts = %v, want [1]", got)
	}
	if conn.pendingTAck() != 1 {
		t.Error("scoped return ASP Active was not protected by T(ack)")
	}
}

func TestUnsolicitedAspInactiveAckMovesDownOrInactiveASPToInactive(t *testing.T) {
	for _, state := range []State{StateASPDown, StateASPInactive} {
		t.Run(state.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, state, RoleASP)
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
			if got := conn.State(); got != StateASPInactive {
				t.Errorf("state = %v, want ASP-INACTIVE", got)
			}
			if conn.resumeAfterStrayAck() {
				t.Error("an ASP that was not active armed an ASP-ACTIVE return")
			}
			if got := countType(*sent, "ASP Active"); got != 0 {
				t.Errorf("sent %d ASP Active requests without a previous active state", got)
			}
		})
	}
}

func TestScopedActivationAndDeactivationAPIsArmTAckBeforeWriting(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   State
		request func(*Association) error
		ack     func(*Association) error
		kind    requestKind
		want    string
	}{
		{
			name:  "activate",
			state: StateASPInactive,
			request: func(conn *Association) error {
				return conn.ActivateRoutingContexts(2)
			},
			ack: func(conn *Association) error {
				return conn.handleAspActiveAck(messages.NewAspActiveAck(
					conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(2), nil,
				))
			},
			kind: requestAspActive,
			want: "ASP Active",
		},
		{
			name:  "deactivate",
			state: StateASPActive,
			request: func(conn *Association) error {
				return conn.DeactivateRoutingContexts(2)
			},
			ack: func(conn *Association) error {
				return conn.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
			},
			kind: requestAspInactive,
			want: "ASP Inactive",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, sent := newTestConn(t, test.state, RoleASP)
			conn.signalWriter = func(message messages.M3UA) (int, error) {
				if requestKindOf(message) == test.kind {
					if conn.pendingTAck() != 1 {
						t.Errorf("T(ack) was not armed before %s reached the writer", test.want)
					}
					if err := test.ack(conn); err != nil {
						t.Errorf("immediate Ack: %v", err)
					}
				}
				*sent = append(*sent, message)
				return message.MarshalLen(), nil
			}

			if err := test.request(conn); err != nil {
				t.Fatal(err)
			}
			if conn.pendingTAck() != 0 {
				t.Error("immediate valid Ack left T(ack) armed")
			}
			if len(*sent) != 1 || (*sent)[0].MessageTypeName() != test.want {
				t.Errorf("sent %v, want one %s", typeNames(*sent), test.want)
			}
			var routingContext *params.Param
			switch request := (*sent)[0].(type) {
			case *messages.AspActive:
				routingContext = request.RoutingContext
			case *messages.AspInactive:
				routingContext = request.RoutingContext
			}
			if got := routingContext.RoutingContexts(); len(got) != 1 || got[0] != 2 {
				t.Errorf("request Routing Contexts = %v, want [2]", got)
			}
		})
	}
}

func TestScopedActivationAndDeactivationAPIsRejectInvalidUse(t *testing.T) {
	for _, test := range []struct {
		name  string
		call  func(*Association) error
		mode  associationRole
		state State
		want  error
	}{
		{name: "SGP activate", call: func(c *Association) error { return c.ActivateRoutingContexts(1) }, mode: RoleSGP, state: StateASPInactive, want: ErrUnsupportedRole},
		{name: "SGP deactivate", call: func(c *Association) error { return c.DeactivateRoutingContexts(1) }, mode: RoleSGP, state: StateASPActive, want: ErrUnsupportedRole},
		{name: "activate while down", call: func(c *Association) error { return c.ActivateRoutingContexts(1) }, mode: RoleASP, state: StateASPDown, want: ErrInvalidState},
		{name: "deactivate while inactive", call: func(c *Association) error { return c.DeactivateRoutingContexts(1) }, mode: RoleASP, state: StateASPInactive, want: ErrInvalidState},
		{name: "foreign activate scope", call: func(c *Association) error { return c.ActivateRoutingContexts(99) }, mode: RoleASP, state: StateASPInactive, want: ErrInvalidRoutingContext},
		{name: "foreign deactivate scope", call: func(c *Association) error { return c.DeactivateRoutingContexts(99) }, mode: RoleASP, state: StateASPActive, want: ErrInvalidRoutingContext},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, sent := newTestConn(t, test.state, test.mode)
			if err := test.call(conn); !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
			if len(*sent) != 0 || conn.pendingTAck() != 0 {
				t.Errorf("invalid request sent %v with %d pending timers", typeNames(*sent), conn.pendingTAck())
			}
		})
	}
}

// Configuration membership is not enough: an Ack must answer the request that
// is actually outstanding. Accepting another configured RC lets an unsolicited
// Ack activate traffic the ASP did not ask to take, while also cancelling the
// timer for the RC it did request.
func TestAspActiveAckMustStayWithinTheOutstandingRequest(t *testing.T) {
	for _, tt := range []struct {
		name string
		ack  *params.Param
		want error
	}{
		{name: "different configured context", ack: params.NewRoutingContext(2), want: ErrInvalidRoutingContext},
		{name: "missing context", ack: nil, want: ErrMissingRoutingContext},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPInactive, RoleASP)
			conn.startTAck(messages.NewAspActive(
				conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
			), requestAspActive)

			err := conn.handleAspActiveAck(messages.NewAspActiveAck(
				conn.cfg.TrafficModeType.Copy(), tt.ack, nil,
			))
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if got := conn.pendingTAck(); got != 1 {
				t.Errorf("pending T(ack) after unrelated Ack = %d, want 1", got)
			}
			conn.muAckedRCs.RLock()
			_, activated := conn.ackedRCs[2]
			scopeChanged := conn.ackedRCsScoped
			conn.muAckedRCs.RUnlock()
			if activated || scopeChanged {
				t.Error("an invalid Ack changed the active Routing Context scope")
			}
			conn.stopAllTAck()
		})
	}
}
