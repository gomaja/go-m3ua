package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 §4.3, §4.3.4.2 and §5.6: a shared-ASPSM withdrawal
// supersedes local procedures, while normal Double Exchange is independent.
func TestIPSPPeerDownSupersedesPendingProcedures(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			for _, procedure := range []struct {
				name  string
				state State
				call  func(*Association, context.Context) error
			}{
				{"ASP Up", StateASPDown, func(a *Association, ctx context.Context) error { return a.ASPUp(ctx) }},
				{"ASP Active", StateASPInactive, func(a *Association, ctx context.Context) error { return a.ASPActive(ctx) }},
				{"ASP Inactive", StateASPActive, func(a *Association, ctx context.Context) error { return a.ASPInactive(ctx) }},
			} {
				t.Run(procedure.name, func(t *testing.T) {
					association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, procedure.state)
					writes := make(chan messages.M3UA, 32)
					association.signalWriter = func(m messages.M3UA) (int, error) { writes <- m; return m.MarshalLen(), nil }
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					result := make(chan error, 1)
					go func() { result <- procedure.call(association, ctx) }()
					select {
					case m := <-writes:
						if m.MessageTypeName() != procedure.name {
							t.Fatalf("sent %s, want %s", m.MessageTypeName(), procedure.name)
						}
					case <-ctx.Done():
						t.Fatal("procedure did not start")
					}
					association.handleReceivedSignals(ctx, messages.NewAspDown(nil), nil)
					applyIPSPDownPublicationForTest(t, association, StateASPDown)
					select {
					case m := <-writes:
						if _, ok := m.(*messages.AspDownAck); !ok {
							t.Fatalf("peer Down response = %T", m)
						}
					default:
						t.Fatal("missing ASP Down Ack")
					}
					if !model.sharedASPSM {
						if got := association.pendingTAck(); got != 1 {
							t.Fatalf("independent local procedure pending = %d, want 1", got)
						}
						select {
						case err := <-result:
							t.Fatalf("independent local procedure ended: %v", err)
						default:
						}
						var ack messages.M3UA
						wantLocal := StateASPInactive
						switch procedure.name {
						case "ASP Up":
							ack = messages.NewAspUpAck(nil, nil)
						case "ASP Active":
							ack = messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(11), nil)
							wantLocal = StateASPActive
						case "ASP Inactive":
							ack = messages.NewAspInactiveAck(params.NewRoutingContext(11), nil)
						}
						association.handleReceivedSignals(ctx, ack, nil)
						applyIPSPDownPublicationForTest(t, association, stateUnchanged)
						select {
						case err := <-result:
							if err != nil {
								t.Fatal(err)
							}
						case <-ctx.Done():
							t.Fatal("independent procedure did not finish")
						}
						if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: wantLocal, TrafficToPeer: StateASPDown}) {
							t.Fatalf("independent procedure state = %+v", got)
						}
						return
					}
					if got := association.pendingTAck(); got != 0 {
						t.Errorf("peer Down retained %d superseded T(ack) requests", got)
					}
					select {
					case err := <-result:
						if !errors.Is(err, ErrASPProcedureSuperseded) {
							t.Fatalf("superseded caller error = %v", err)
						}
					case <-time.After(100 * time.Millisecond):
						t.Fatal("superseded caller still waits for its context")
					}
				})
			}
		})
	}
}

func TestIPSPPeerDownStopsAutomaticPendingASPUpRetries(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			association.cfg.TAck = 20 * time.Millisecond
			writes := make(chan messages.M3UA, 32)
			association.signalWriter = func(m messages.M3UA) (int, error) { writes <- m; return m.MarshalLen(), nil }
			if err := association.initiateASPSM(); err != nil {
				t.Fatal(err)
			}
			<-writes
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			<-writes
			select {
			case m := <-writes:
				t.Fatalf("superseded procedure retransmitted %s", m.MessageTypeName())
			case <-time.After(3 * association.cfg.TAck):
			}
		})
	}
}

func TestIPSPPeerDownRejectsLateInactiveAck(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
			if _, err := association.beginASPInactive(association.configuredLocalRoutingContextParam()); err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			err := association.handleAspInactiveAck(messages.NewAspInactiveAck(association.configuredLocalRoutingContextParam(), nil))
			var unexpected *UnexpectedMessageError
			if !errors.As(err, &unexpected) {
				t.Fatalf("late Inactive Ack error = %v, want UnexpectedMessageError", err)
			}
			if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}) {
				t.Fatalf("late Inactive Ack resurrected %+v", got)
			}
		})
	}
}

func TestIPSPPeerDownRejectsSupersededASPUpAck(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			association.cfg.ASPProcedures.ASPActive = ASPProcedureAutomatic
			if err := association.initiateASPSM(); err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			for i := 0; i < 2; i++ {
				association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
				select {
				case err := <-association.errChan:
					var unexpected *UnexpectedMessageError
					if !errors.As(err, &unexpected) {
						t.Fatalf("superseded Up Ack error = %v", err)
					}
				default:
					t.Fatal("superseded Up Ack was accepted as solicited")
				}
				applyIPSPDownPublicationForTest(t, association, stateUnchanged)
				if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}) {
					t.Fatalf("superseded Up Ack resurrected %+v", got)
				}
				if got := association.pendingTAck(); got != 0 {
					t.Fatalf("superseded Up Ack initiated %d procedures", got)
				}
			}
		})
	}
}

func TestIPSPPeerDownRetainsPendingASPDown(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
			request, err := association.initiateASPDown()
			if err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			if got := association.pendingTAck(); got != 1 {
				t.Fatalf("crossed ASP Down pending = %d, want 1", got)
			}
			select {
			case err := <-request.result:
				t.Fatalf("crossed Down ended before its Ack: %v", err)
			default:
			}
			if err := association.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-request.result:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("Down Ack did not finish crossed local Down")
			}
		})
	}
}

func TestIPSPPeerDownDrainsPendingControlWrite(t *testing.T) {
	for _, retry := range []bool{false, true} {
		name := "initial write"
		if retry {
			name = "T(ack) retry"
		}
		t.Run(name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, IPSPExchangeSingle, 0, StateASPDown)
			association.cfg.TAck = 20 * time.Millisecond
			writing, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			ack := make(chan struct{}, 1)
			var writes atomic.Int32
			association.signalWriter = func(m messages.M3UA) (int, error) {
				switch m.(type) {
				case *messages.AspUp:
					n := writes.Add(1)
					if (!retry && n == 1) || (retry && n == 2) {
						close(writing)
						<-release
					}
				case *messages.AspDownAck:
					ack <- struct{}{}
				}
				return m.MarshalLen(), nil
			}
			started := make(chan error, 1)
			go func() { started <- association.initiateASPSM() }()
			select {
			case <-writing:
			case <-time.After(time.Second):
				t.Fatal("Up did not reach the blocked write")
			}
			downDone := make(chan struct{})
			go func() {
				association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
				close(downDone)
			}()
			select {
			case <-ack:
				t.Error("Down Ack overtook an in-flight superseded Up write")
			case <-time.After(40 * time.Millisecond):
			}
			releaseOnce.Do(func() { close(release) })
			select {
			case err := <-started:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("initial Up write did not drain")
			}
			select {
			case <-downDone:
			case <-time.After(time.Second):
				t.Fatal("peer Down did not finish")
			}
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			if got := association.pendingTAck(); got != 0 {
				t.Fatalf("drained peer Down retained %d procedures", got)
			}
		})
	}
}

func TestIPSPPeerDownBlocksProcedureDuringAcknowledgement(t *testing.T) {
	association, _ := newIPSPDownAssociationForTest(t, IPSPExchangeSingle, 0, StateASPActive)
	ackStarted, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var extra atomic.Int32
	association.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.AspDownAck); ok {
			close(ackStarted)
			<-release
		} else {
			extra.Add(1)
		}
		return m.MarshalLen(), nil
	}
	done := make(chan struct{})
	go func() {
		association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
		close(done)
	}()
	select {
	case <-ackStarted:
	case <-time.After(time.Second):
		t.Fatal("Down Ack did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- association.ASPUp(ctx) }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrASPProcedureSuperseded) {
			t.Errorf("Up during Down Ack error = %v", err)
		}
	case <-time.After(40 * time.Millisecond):
		t.Error("Up was accepted before peer Down Ack settled")
	}
	once.Do(func() { close(release) })
	<-done
	if got := extra.Load(); got != 0 {
		t.Fatalf("new procedure wrote %d messages before Down Ack settled", got)
	}
}

func TestIPSPPeerDownRejectsASPTMAcksUntilFreshUp(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			for _, kind := range []requestKind{requestAspActive, requestAspInactive} {
				for _, scoped := range []bool{false, true} {
					name := fmt.Sprintf("kind=%d/scoped=%t", kind, scoped)
					t.Run(name, func(t *testing.T) {
						association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPInactive)
						rc := association.configuredLocalRoutingContextParam()
						if !scoped {
							rc = nil
						}
						var ack messages.M3UA
						if kind == requestAspActive {
							if _, err := association.beginASPActive(rc); err != nil {
								t.Fatal(err)
							}
							ack = messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), rc, nil)
						} else {
							if _, err := association.beginASPInactive(rc); err != nil {
								t.Fatal(err)
							}
							ack = messages.NewAspInactiveAck(rc, nil)
						}
						// Leave both acknowledged history and an outstanding same-scope request.
						if !association.acknowledgeTAck(kind, rc) {
							t.Fatal("first request was not acknowledged")
						}
						if kind == requestAspActive {
							if _, err := association.beginASPActive(rc); err != nil {
								t.Fatal(err)
							}
						} else {
							if _, err := association.beginASPInactive(rc); err != nil {
								t.Fatal(err)
							}
						}
						association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
						applyIPSPDownPublicationForTest(t, association, StateASPDown)
						rejected := func() {
							t.Helper()
							association.handleReceivedSignals(context.Background(), ack, nil)
							select {
							case err := <-association.errChan:
								var unexpected *UnexpectedMessageError
								if !errors.As(err, &unexpected) {
									t.Fatalf("late ASPTM Ack = %v", err)
								}
							default:
								t.Fatal("late ASPTM Ack was accepted")
							}
							applyIPSPDownPublicationForTest(t, association, stateUnchanged)
						}
						rejected()
						if err := association.initiateASPSM(); err != nil {
							t.Fatal(err)
						}
						rejected() // Merely sending a fresh Up does not enable old ASPTM Acks.
						association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
						applyIPSPDownPublicationForTest(t, association, StateASPInactive)
						association.handleReceivedSignals(context.Background(), ack, nil)
						want := StateASPInactive
						if kind == requestAspActive {
							want = StateASPActive
						}
						publication := want
						if model.exchange == IPSPExchangeDouble {
							publication = stateUnchanged
						}
						applyIPSPDownPublicationForTest(t, association, publication)
						if got := association.IPSPState().TrafficToLocal; got != want {
							t.Fatalf("fresh-Up ASPTM state = %v, want %v", got, want)
						}
					})
				}
			}
		})
	}
}

func TestIPSPPeerDownInvalidStreamKeepsPendingUp(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			request, err := association.beginASPSM()
			if err != nil {
				t.Fatal(err)
			}
			association.recvStream.Store(1)
			if err := association.handleAspDown(messages.NewAspDown(nil)); err == nil {
				t.Fatal("nonzero-stream Down accepted")
			}
			if got := association.pendingTAck(); got != 1 {
				t.Fatalf("invalid Down retired %d procedures", got)
			}
			association.recvStream.Store(0)
			if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-request.result:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("valid Up Ack did not finish original Up")
			}
		})
	}
}

func TestIPSPUnsolicitedUpAckWithoutPeerDownStillInactivates(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			association.cfg.ASPProcedures.ASPActive = ASPProcedureExplicit
			association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
			publication := StateASPInactive
			if !model.sharedASPSM {
				publication = stateUnchanged
			}
			applyIPSPDownPublicationForTest(t, association, publication)
			if got := association.IPSPState().TrafficToLocal; got != StateASPInactive {
				t.Fatalf("ordinary unsolicited Up Ack local state = %v", got)
			}
		})
	}
}

func TestIPSPPeerDownNewPeerUpClearsSupersededAcknowledgements(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			if err := association.initiateASPSM(); err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			association.handleReceivedSignals(context.Background(), messages.NewAspUp(nil, nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPInactive)
			// Ordinary unsolicited-Up policy applies again after the peer establishes
			// a fresh shared ASPSM exchange (RFC 4666 §4.3.4.1.2).
			association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPInactive)
			rc := association.configuredLocalRoutingContextParam()
			pending, err := association.beginASPActive(rc)
			if err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), rc, nil), nil)
			publication := StateASPActive
			if model.exchange == IPSPExchangeDouble {
				publication = stateUnchanged
			}
			applyIPSPDownPublicationForTest(t, association, publication)
			for _, request := range pending {
				select {
				case err := <-request.result:
					if err != nil {
						t.Fatal(err)
					}
				default:
					t.Fatal("fresh peer Up left Active pending")
				}
			}
			if got := association.IPSPState().TrafficToLocal; got != StateASPActive {
				t.Fatalf("fresh peer Up did not permit activation: %v", got)
			}
		})
	}
}

func TestIPSPPeerDownStopsRetriesBeforeAcknowledgementCompletes(t *testing.T) {
	for _, kind := range []requestKind{requestAspUp, requestAspActive, requestAspInactive} {
		t.Run(fmt.Sprintf("kind=%d", kind), func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, IPSPExchangeSingle, 0, StateASPActive)
			association.cfg.TAck = 20 * time.Millisecond
			ackStarted, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var count atomic.Int32
			association.signalWriter = func(m messages.M3UA) (int, error) {
				if _, ok := m.(*messages.AspDownAck); ok {
					close(ackStarted)
					<-release
				} else {
					count.Add(1)
				}
				return m.MarshalLen(), nil
			}
			var request *pendingRequest
			var err error
			switch kind {
			case requestAspUp:
				request, err = association.beginASPSM()
			case requestAspActive:
				var requests []*pendingRequest
				requests, err = association.beginASPActive(params.NewRoutingContext(1))
				if len(requests) > 0 {
					request = requests[0]
				}
			case requestAspInactive:
				request, err = association.beginASPInactive(params.NewRoutingContext(1))
			}
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
				close(done)
			}()
			select {
			case <-ackStarted:
			case <-time.After(time.Second):
				t.Fatal("Down Ack did not start")
			}
			select {
			case err := <-request.result:
				t.Errorf("caller returned before Down Ack completed: %v", err)
			case <-time.After(3 * association.cfg.TAck):
			}
			if got := count.Load(); got != 1 {
				t.Errorf("superseded request sent %d times while Down Ack blocked, want 1", got)
			}
			once.Do(func() { close(release) })
			<-done
			select {
			case err := <-request.result:
				if !errors.Is(err, ErrASPProcedureSuperseded) {
					t.Errorf("superseded error=%v", err)
				}
			default:
				t.Fatal("caller not released after Down Ack")
			}
		})
	}
}

func TestIPSPPeerDownFreshLocalUpRestoresAcknowledgementPolicy(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			if err := association.initiateASPSM(); err != nil {
				t.Fatal(err)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			if err := association.initiateASPSM(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
				applyIPSPDownPublicationForTest(t, association, StateASPInactive)
			}
		})
	}
}

func TestIPSPPeerDownRestartRestoresAcknowledgementPolicy(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			association, _ := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
			association.cfg.ASPProcedures.ASPUp = ASPProcedureExplicit
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			association.handleSCTPRestart()
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			association.handleReceivedSignals(context.Background(), messages.NewAspUpAck(nil, nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPInactive)
		})
	}
}

func TestIPSPPeerDownSupersedesEveryRoutingContextRequest(t *testing.T) {
	association, _ := newIPSPDownAssociationForTest(t, IPSPExchangeSingle, 0, StateASPInactive)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1, 2, 3))
	first, err := association.beginASPActive(params.NewRoutingContext(1, 2))
	if err != nil {
		t.Fatal(err)
	}
	if !association.acknowledgeTAck(requestAspActive, params.NewRoutingContext(1)) {
		t.Fatal("partial Active Ack not matched")
	}
	second, err := association.beginASPActive(params.NewRoutingContext(3))
	if err != nil {
		t.Fatal(err)
	}
	third, err := association.beginASPInactive(nil)
	if err != nil {
		t.Fatal(err)
	}
	association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
	applyIPSPDownPublicationForTest(t, association, StateASPDown)
	for _, request := range append(append(first, second...), third) {
		select {
		case err := <-request.result:
			if !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("scoped supersession=%v", err)
			}
		default:
			t.Fatal("peer Down retained an outstanding RC procedure")
		}
	}
	if got := association.pendingTAck(); got != 0 {
		t.Fatalf("peer Down retained %d RC procedures", got)
	}
}
