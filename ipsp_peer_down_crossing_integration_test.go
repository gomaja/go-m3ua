package m3ua

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 §4.3 and §5.6: the raw peer withholds Up Ack until its Down
// has been acknowledged, reproducing the ordered-stream crossing.
func TestIPSPPeerDownCrossesPendingASPUpOverSCTP(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			listener, err := (&sctp.Config{InitMsg: sctp.InitMsg{OutStreams: 4, MaxInStreams: 4}}).Listen("sctp", mcAddr(0, "127.0.0.3"))
			if err != nil {
				if isSCTPUnsupported(err) {
					t.Skip(err)
				}
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			accepted := make(chan *sctp.Conn, 1)
			acceptErr := make(chan error, 1)
			go func() {
				conn, e := listener.AcceptSCTP()
				if e != nil {
					acceptErr <- e
				} else {
					accepted <- conn
				}
			}()
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })
			cfg := integrationIPSPConfig(0, 0, false, true)
			cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
			cfg.TAck = 80 * time.Millisecond
			cfg.IPSP.ExchangeModel = model.exchange
			cfg.IPSP.ASPSMExchange = model.aspsm
			setInventoryNetworkAppearance(&cfg.ApplicationServers, nil)
			setInventoryRoutingContexts(&cfg.ApplicationServers, params.NewRoutingContext(7))
			if model.exchange == IPSPExchangeDouble {
				cfg.IPSP.TrafficToLocal = &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers}
				cfg.IPSP.TrafficToPeer = &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers}
				cfg.ApplicationServers = nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			association, err := endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.2"), listener.Addr().(*sctp.Addr), cfg)
			if err != nil {
				t.Fatal(err)
			}
			var peer *sctp.Conn
			select {
			case peer = <-accepted:
			case err := <-acceptErr:
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			t.Cleanup(func() { _ = peer.Close() })
			t.Logf("local=%s peer=%s", association.LocalAddr(), peer.LocalAddr())
			send := func(m messages.M3UA) {
				t.Helper()
				raw, e := m.MarshalBinary()
				if e != nil {
					t.Fatal(e)
				}
				if _, e := sendWithInfo(peer, raw, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); e != nil {
					t.Fatal(e)
				}
				t.Logf("peer -> %s", m.MessageTypeName())
			}
			receive := func(timeout time.Duration) messages.M3UA {
				t.Helper()
				if e := peer.SetReadDeadline(time.Now().Add(timeout)); e != nil {
					t.Fatal(e)
				}
				buf := make([]byte, 65536)
				n, info, e := recvWithInfo(peer, buf)
				if e != nil {
					var ne net.Error
					if errors.As(e, &ne) && ne.Timeout() {
						return nil
					}
					t.Fatal(e)
				}
				if info.Stream != 0 || info.PPID != M3UAPPID {
					t.Fatalf("control metadata = %+v", info)
				}
				m, e := messages.Parse(buf[:n])
				if e != nil {
					t.Fatal(e)
				}
				t.Logf("local -> %s", m.MessageTypeName())
				return m
			}
			result := make(chan error, 1)
			go func() { result <- association.ASPUp(ctx) }()
			if m := receive(time.Second); m == nil || m.MessageTypeName() != "ASP Up" {
				t.Fatalf("opening request = %v", m)
			}
			send(messages.NewAspDown(nil))
			gotAck := false
			extraUp := 0
			deadline := time.Now().Add(3 * cfg.TAck)
			for time.Now().Before(deadline) {
				m := receive(time.Until(deadline))
				if m == nil {
					break
				}
				switch m.(type) {
				case *messages.AspDownAck:
					gotAck = true
				case *messages.AspUp:
					extraUp++
				}
			}
			if !gotAck {
				t.Fatal("peer Down received no Ack")
			}
			if model.sharedASPSM && extraUp != 0 {
				t.Errorf("superseded ASP Up retransmitted %d times after Down Ack", extraUp)
			}
			if model.sharedASPSM {
				select {
				case err := <-result:
					if !errors.Is(err, ErrASPProcedureSuperseded) {
						t.Errorf("superseded ASPUp error = %v", err)
					}
				default:
					t.Error("superseded ASPUp still awaits acknowledgement")
				}
			}
			send(messages.NewAspUpAck(nil, nil))
			deadline = time.Now().Add(3 * cfg.TAck)
			active := 0
			unexpected := 0
			for time.Now().Before(deadline) {
				m := receive(time.Until(deadline))
				if m == nil {
					break
				}
				if request, ok := m.(*messages.AspActive); ok {
					active++
					send(messages.NewAspActiveAck(request.TrafficModeType.Copy(), request.RoutingContext.Copy(), nil))
				}
				if response, ok := m.(*messages.Error); ok {
					if response.ErrorCode.ErrorCode() != params.UnexpectedMessageError {
						t.Fatalf("late Up Ack Error code = %d", response.ErrorCode.ErrorCode())
					}
					want, err := messages.NewAspUpAck(nil, nil).MarshalBinary()
					if err != nil {
						t.Fatal(err)
					}
					if response.DiagnosticInformation == nil || !bytes.Equal(response.DiagnosticInformation.Data, want) {
						t.Fatalf("late Up Ack Error diagnostic = %v", response.DiagnosticInformation)
					}
					unexpected++
				}
			}
			if model.sharedASPSM {
				if unexpected != 1 {
					t.Errorf("late Up Ack received %d Unexpected Message errors, want 1", unexpected)
				}
				if active != 0 {
					t.Errorf("late Up Ack triggered %d ASP Active requests", active)
				}
				if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}) {
					t.Errorf("late Up Ack reversed peer withdrawal: %+v", got)
				}
			} else {
				if active == 0 {
					t.Error("independent local direction did not activate")
				}
				if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPDown}) {
					t.Errorf("independent direction state = %+v", got)
				}
				if err := <-result; err != nil {
					t.Errorf("independent ASPUp = %v", err)
				}
			}
		})
	}
}
