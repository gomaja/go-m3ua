//go:build linux

package m3ua_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"sync"
	"testing"
	"time"

	m3ua "github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

const shutdownTestBudget = 15 * time.Second

func shutdownLoopback() *sctp.Addr {
	return &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
}

func shutdownNewEndpoint(t *testing.T, role m3ua.Role) *m3ua.Endpoint {
	t.Helper()
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: role,
		Congestion: &m3ua.CongestionConfig{
			Default: m3ua.CongestionProfile{Method: m3ua.CongestionInternational},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := endpoint.Close(); err != nil {
			t.Errorf("Endpoint.Close: %v", err)
		}
	})
	return endpoint
}

func shutdownAssociationConfig() *m3ua.AssociationConfig {
	cfg := m3ua.NewAssociationConfig()
	cfg.ApplicationServers = []m3ua.ASConfig{{
		ASKey:       m3ua.ASKey{RoutingContext: 379, RoutingContextSet: true},
		TrafficMode: params.TrafficModeLoadshare,
	}}
	// Longer than the test deadline: retransmissions must not drive the interleaving.
	cfg.TAck = 30 * time.Second
	cfg.EstablishTimeout = shutdownTestBudget
	return cfg
}

type shutdownWireMessage struct {
	Name       string
	Withdrawal bool
	Upward     bool
}

type shutdownHeldAck struct {
	conn *sctp.Conn
	ack  messages.M3UA
}

type shutdownRecordingPeer struct {
	mu       sync.Mutex
	conn     *sctp.Conn
	sequence []shutdownWireMessage
	first    chan shutdownHeldAck
	upward   chan struct{}
	done     chan struct{}
	err      error
}

func (p *shutdownRecordingPeer) snapshot() []shutdownWireMessage {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]shutdownWireMessage(nil), p.sequence...)
}

func shutdownWriteAck(ctx context.Context, conn *sctp.Conn, ack messages.M3UA) error {
	b, err := ack.MarshalBinary()
	if err != nil {
		return err
	}
	n, err := conn.SendMsgContext(ctx, b, sctp.SendOptions{
		Info: &sctp.SndInfo{Stream: 0, PPID: m3ua.M3UAPPID},
	})
	if err == nil && n != len(b) {
		return io.ErrShortWrite
	}
	return err
}

func shutdownAcknowledgement(message messages.M3UA) (messages.M3UA, bool, bool, error) {
	switch msg := message.(type) {
	case *messages.AspUp:
		return messages.NewAspUpAck(nil, nil), false, true, nil
	case *messages.AspActive:
		return messages.NewAspActiveAck(msg.TrafficModeType, msg.RoutingContext, nil), false, true, nil
	case *messages.AspInactive:
		return messages.NewAspInactiveAck(msg.RoutingContext, nil), true, false, nil
	case *messages.AspDown:
		return messages.NewAspDownAck(nil), true, false, nil
	default:
		return nil, false, false, fmt.Errorf("unexpected management message %T", message)
	}
}

func (p *shutdownRecordingPeer) serve(ctx context.Context, listener *sctp.Listener) error {
	conn, err := listener.AcceptSCTP()
	if err != nil {
		return err
	}
	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()
	defer func() { _ = conn.Close() }()
	deadline, _ := ctx.Deadline()
	if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	withdrawn := false
	for {
		b, info, err := conn.ReadMsg(64 * 1024)
		if errors.Is(err, io.EOF) {
			// Drain until orderly transport EOF, including messages queued before close.
			return nil
		}
		if err != nil {
			return err
		}
		message, err := messages.Parse(b)
		if err != nil {
			return fmt.Errorf("parse peer read: %w", err)
		}
		ack, withdrawal, upward, ackErr := shutdownAcknowledgement(message)
		p.mu.Lock()
		p.sequence = append(p.sequence, shutdownWireMessage{
			Name: message.MessageTypeName(), Withdrawal: withdrawal, Upward: upward,
		})
		p.mu.Unlock()
		if info.PPID != m3ua.M3UAPPID || info.Stream != 0 || info.Unordered {
			return fmt.Errorf("unexpected management transport metadata: %+v", info)
		}
		if ackErr != nil {
			return ackErr
		}
		if withdrawn && upward {
			select {
			case p.upward <- struct{}{}:
			default:
			}
		}
		if withdrawal && !withdrawn {
			withdrawn = true
			p.first <- shutdownHeldAck{conn: conn, ack: ack}
			// Keep reading while this one Ack is held by the test goroutine.
			continue
		}
		if err := shutdownWriteAck(ctx, conn, ack); err != nil {
			return fmt.Errorf("write %s: %w", ack.MessageTypeName(), err)
		}
	}
}

func shutdownStartPeer(t *testing.T, ctx context.Context) (*shutdownRecordingPeer, *sctp.Addr) {
	t.Helper()
	listener, err := sctp.Listen("sctp4", shutdownLoopback())
	if err != nil {
		t.Fatalf("SCTP listener (Linux SCTP support is required): %v", err)
	}
	deadline, _ := ctx.Deadline()
	if err := listener.SetDeadline(deadline); err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	p := &shutdownRecordingPeer{
		first: make(chan shutdownHeldAck, 1), upward: make(chan struct{}, 1), done: make(chan struct{}),
	}
	go func() {
		p.err = p.serve(ctx, listener)
		close(p.done)
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		p.mu.Lock()
		conn := p.conn
		p.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
		select {
		case <-p.done:
		case <-time.After(shutdownTestBudget):
			t.Error("recording peer did not stop")
		}
	})
	return p, listener.Addr().(*sctp.Addr)
}

// TestShutdownRefusesExplicitUpwardProceduresOnWire catches a new upward procedure after the
// first withdrawal has reached the peer. The expected contract is refusal and
// no later Up/Active on the wire, even if the admitted caller eventually errors.
func TestShutdownRefusesExplicitUpwardProceduresOnWire(t *testing.T) {
	for _, operation := range []string{"ASPUp", "ASPActive"} {
		t.Run(operation, func(t *testing.T) {
			for _, inactive := range []bool{true, false} {
				name := "inactive-first"
				if !inactive {
					name = "down-first"
				}
				t.Run(name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), shutdownTestBudget)
					defer cancel()
					endpoint := shutdownNewEndpoint(t, m3ua.RoleASP)
					peer, remote := shutdownStartPeer(t, ctx)
					cfg := shutdownAssociationConfig()
					if !inactive {
						cfg.ASPProcedures = &m3ua.ASPProcedurePolicy{
							ASPUp: m3ua.ASPProcedureAutomatic, ASPActive: m3ua.ASPProcedureAutomatic,
							ASPInactive: m3ua.ASPProcedureExplicit, ASPDown: m3ua.ASPProcedureAutomatic,
						}
					}
					association, err := endpoint.Dial(ctx, "m3ua4", shutdownLoopback(), remote, cfg)
					if err != nil {
						t.Fatalf("Dial: %v; sequence=%+v", err, peer.snapshot())
					}
					if association.State() != m3ua.StateASPActive {
						t.Fatalf("state=%v, want ASP active; sequence=%+v", association.State(), peer.snapshot())
					}
					shutdown := make(chan error, 1)
					go func() { shutdown <- association.ShutdownContext(ctx) }()
					var first shutdownHeldAck
					select {
					case first = <-peer.first:
					case <-peer.done:
						t.Fatalf("peer stopped before withdrawal: %v; sequence=%+v", peer.err, peer.snapshot())
					case <-ctx.Done():
						t.Fatalf("waiting for first withdrawal: %v; sequence=%+v", ctx.Err(), peer.snapshot())
					}
					_, gotInactive := first.ack.(*messages.AspInactiveAck)
					if gotInactive != inactive {
						t.Fatalf("wrong first withdrawal Ack %T; sequence=%+v", first.ack, peer.snapshot())
					}

					active := make(chan error, 1)
					go func() {
						if operation == "ASPUp" {
							active <- association.ASPUp(ctx)
						} else {
							active <- association.ASPActive(ctx)
						}
					}()
					var activeErr error
					returned := false
					// A completed call or an upward peer read lets the test proceed.
					// The final sequence check also catches sends followed by an error.
					select {
					case activeErr = <-active:
						returned = true
					case <-peer.upward:
					case <-peer.done:
						t.Fatalf("peer stopped during overlap: %v; sequence=%+v", peer.err, peer.snapshot())
					case <-ctx.Done():
						t.Fatalf("no refusal or upward wire message before deadline: %v; sequence=%+v", ctx.Err(), peer.snapshot())
					}
					if !returned {
						select {
						case activeErr = <-active:
						case <-ctx.Done():
							t.Fatalf("ASPActive did not return: %v; sequence=%+v", ctx.Err(), peer.snapshot())
						}
					}
					if err := shutdownWriteAck(ctx, first.conn, first.ack); err != nil {
						t.Fatalf("release first withdrawal Ack: %v; sequence=%+v; %s err=%v",
							err, peer.snapshot(), operation, activeErr)
					}
					select {
					case err := <-shutdown:
						if err != nil {
							t.Errorf("ShutdownContext: %v; sequence=%+v; %s err=%v", err, peer.snapshot(), operation, activeErr)
						}
					case <-ctx.Done():
						t.Fatalf("ShutdownContext did not return: %v; sequence=%+v; %s err=%v", ctx.Err(), peer.snapshot(), operation, activeErr)
					}
					select {
					case <-peer.done:
						if peer.err != nil {
							t.Errorf("peer did not reach orderly EOF: %v", peer.err)
						}
					case <-ctx.Done():
						t.Fatalf("peer did not drain to EOF: %v; sequence=%+v; %s err=%v", ctx.Err(), peer.snapshot(), operation, activeErr)
					}

					sequence := peer.snapshot()
					t.Logf("received sequence=%+v; %s err=%v", sequence, operation, activeErr)
					withdrawn := false
					lateUpward := false
					for _, message := range sequence {
						if withdrawn && message.Upward {
							lateUpward = true
						}
						withdrawn = withdrawn || message.Withdrawal
					}
					if !withdrawn || lateUpward || !errors.Is(activeErr, m3ua.ErrASPProcedureSuperseded) || ctx.Err() != nil {
						t.Fatalf("shutdown ordering violated: sequence=%+v; %s err=%v; context err=%v",
							sequence, operation, activeErr, ctx.Err())
					}
				})
			}
		})
	}
}
