package m3ua

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

var ipspDownModels = []struct {
	name        string
	exchange    IPSPExchangeModel
	aspsm       IPSPASPSMExchangeModel
	sharedASPSM bool
}{
	{"Single Exchange", IPSPExchangeSingle, 0, true},
	{"Double Exchange", IPSPExchangeDouble, IPSPASPSMExchangeDouble, false},
	{"Double Exchange with single ASPSM", IPSPExchangeDouble, IPSPASPSMExchangeSingle, true},
}

// RFC 4666 §4.3 and §5.6.1: a peer's Single Exchange ASP Down removes the
// shared communication from service. §5.6.2 keeps normal Double Exchange
// directions independent. Neither request asks us to start another ASP Up.
func TestIPSPPeerDownSuppressesAutomaticASPUp(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			for _, initial := range []struct {
				name    string
				state   State
				entered bool
			}{
				{"before initial entry", StateASPDown, false},
				{"already down", StateASPDown, true},
				{"inactive", StateASPInactive, true},
				{"active", StateASPActive, true},
			} {
				t.Run(initial.name, func(t *testing.T) {
					association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, initial.state)
					association.stateEntered = initial.entered
					for i := 1; i <= 2; i++ {
						association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
						applyIPSPDownPublicationForTest(t, association, StateASPDown)
						if got := countType(*sent, "ASP Down Ack"); got != i {
							t.Fatalf("ASP Down Ack count = %d, want %d", got, i)
						}
						if got := countType(*sent, "ASP Up"); got != 0 {
							t.Fatalf("peer ASP Down started %d ASP Up procedures, want 0", got)
						}
						wantLocal := initial.state
						if model.sharedASPSM {
							wantLocal = StateASPDown
						}
						if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: wantLocal, TrafficToPeer: StateASPDown}) {
							t.Fatalf("state after peer ASP Down = %+v", got)
						}
						if got := association.pendingTAck(); got != 0 {
							t.Fatalf("peer ASP Down left %d T(ack) requests, want 0", got)
						}
					}
				})
			}
		})
	}
}

// RFC 4666 §4.3.4.2 distinguishes an unrequested ASP Down Ack from a peer
// ASP Down request. The Ack still starts automatic recovery of shared ASPSM;
// explicit Layer Management and orderly termination retain their own policy.
func TestIPSPSharedASPSMDownAckRecovery(t *testing.T) {
	for _, model := range ipspDownModels {
		if !model.sharedASPSM {
			continue
		}
		t.Run(model.name, func(t *testing.T) {
			for _, test := range []struct {
				name        string
				requested   bool
				explicit    bool
				terminating bool
				wantUp      int
			}{
				{"unrequested Ack", false, false, false, 1},
				{"local Down Ack", true, false, false, 1},
				{"explicit unrequested Ack", false, true, false, 0},
				{"explicit local Down Ack", true, true, false, 0},
				{"terminating unrequested Ack", false, false, true, 0},
				{"terminating local Down Ack", true, false, true, 0},
			} {
				t.Run(test.name, func(t *testing.T) {
					association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
					if test.explicit {
						association.cfg.ASPProcedures.ASPUp = ASPProcedureExplicit
					}
					association.terminating.Store(test.terminating)
					if test.requested {
						if _, err := association.initiateASPDown(); err != nil {
							t.Fatalf("local ASP Down: %v", err)
						}
					}
					association.handleReceivedSignals(context.Background(), messages.NewAspDownAck(nil), nil)
					applyIPSPDownPublicationForTest(t, association, StateASPDown)
					if got := countType(*sent, "ASP Up"); got != test.wantUp {
						t.Fatalf("ASP Down Ack started %d ASP Up procedures, want %d", got, test.wantUp)
					}
					if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}) {
						t.Fatalf("state after ASP Down Ack = %+v", got)
					}
					if got := association.pendingTAck(); got != test.wantUp {
						t.Fatalf("T(ack) requests after ASP Down Ack = %d, want %d", got, test.wantUp)
					}
				})
			}
		})
	}
}

// RFC 4666 §4.3 and §5.6.2 keep normal Double Exchange ASPSM directions
// independent: an Ack to the local procedure does not take the peer down.
func TestIPSPDoubleASPSMDownAckKeepsIndependentPeerState(t *testing.T) {
	for _, requested := range []bool{false, true} {
		name := "unrequested Ack"
		if requested {
			name = "local Down Ack"
		}
		t.Run(name, func(t *testing.T) {
			association, sent := newIPSPDownAssociationForTest(t, IPSPExchangeDouble, IPSPASPSMExchangeDouble, StateASPActive)
			association.cfg.ASPProcedures.ASPUp = ASPProcedureExplicit
			if requested {
				if _, err := association.initiateASPDown(); err != nil {
					t.Fatalf("local ASP Down: %v", err)
				}
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDownAck(nil), nil)
			applyIPSPDownPublicationForTest(t, association, stateUnchanged)
			if got := association.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPActive}) {
				t.Fatalf("state after local-direction Down Ack = %+v", got)
			}
			if got := countType(*sent, "ASP Up"); got != 0 {
				t.Fatalf("explicit ASP Up policy sent %d automatic requests", got)
			}
			if got := association.pendingTAck(); got != 0 {
				t.Fatalf("local Down Ack left %d T(ack) requests", got)
			}
		})
	}
}

func TestIPSPPeerDownAllowsNewASPSM(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			t.Run("fresh association", func(t *testing.T) {
				association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPDown)
				association.stateEntered = false
				if err := association.handleStateUpdate(StateASPDown); err != nil {
					t.Fatalf("initial ASP-DOWN entry: %v", err)
				}
				if got := countType(*sent, "ASP Up"); got != 1 {
					t.Fatalf("initial entry sent %d ASP Up requests, want 1", got)
				}
			})
			t.Run("restart after peer Down", func(t *testing.T) {
				association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
				association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
				applyIPSPDownPublicationForTest(t, association, StateASPDown)
				*sent = nil
				association.handleSCTPRestart()
				applyIPSPDownPublicationForTest(t, association, StateASPDown)
				if got := countType(*sent, "ASP Up"); got != 1 {
					t.Fatalf("restart sent %d ASP Up requests, want 1", got)
				}
				if got := association.pendingTAck(); got != 1 {
					t.Fatalf("restart left %d T(ack) requests, want 1", got)
				}
			})
			t.Run("explicit ASPUp after peer Down", func(t *testing.T) {
				association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
				association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
				applyIPSPDownPublicationForTest(t, association, StateASPDown)
				// Normal Double Exchange keeps the local direction available.
				// Put that direction down too before asking to bring it up.
				if !model.sharedASPSM {
					association.commitLocalIPSPState(StateASPDown)
				}
				*sent = nil
				written := make(chan struct{}, 1)
				write := association.signalWriter
				association.signalWriter = func(message messages.M3UA) (int, error) {
					n, err := write(message)
					written <- struct{}{}
					return n, err
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				completed := make(chan error, 1)
				go func() { completed <- association.ASPUp(ctx) }()
				select {
				case <-written:
				case <-ctx.Done():
					t.Fatalf("explicit ASPUp wrote no request: %v", ctx.Err())
				}
				association.handleReceivedSignals(ctx, messages.NewAspUpAck(nil, nil), nil)
				wantPublication := StateASPInactive
				if !model.sharedASPSM {
					wantPublication = stateUnchanged
				}
				applyIPSPDownPublicationForTest(t, association, wantPublication)
				if err := <-completed; err != nil {
					t.Fatalf("explicit ASPUp: %v", err)
				}
				if got := countType(*sent, "ASP Up"); got != 1 {
					t.Fatalf("explicit ASPUp sent %d requests, want 1", got)
				}
				if got := association.IPSPState().TrafficToLocal; got != StateASPInactive {
					t.Fatalf("state after explicit ASPUp = %v, want ASP-INACTIVE", got)
				}
			})
		})
	}
}

func TestIPSPPeerUpSupersedesQueuedPeerDown(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			association, sent := newIPSPDownAssociationForTest(t, model.exchange, model.aspsm, StateASPActive)
			association.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			association.handleReceivedSignals(context.Background(), messages.NewAspUp(nil, nil), nil)
			applyIPSPDownPublicationForTest(t, association, StateASPDown)
			applyIPSPDownPublicationForTest(t, association, StateASPInactive)
			if got := association.State(); got != StateASPInactive {
				t.Fatalf("state after newer peer Up = %v, want ASP-INACTIVE", got)
			}
			if got := countType(*sent, "ASP Up"); got != 0 {
				t.Fatalf("superseded ASP-DOWN entry started %d ASP Up procedures", got)
			}
			association.handleReceivedSignals(context.Background(), messages.NewAspDownAck(nil), nil)
			wantPublication, wantUp := StateASPDown, 1
			if !model.sharedASPSM {
				wantPublication, wantUp = stateUnchanged, 0
			}
			applyIPSPDownPublicationForTest(t, association, wantPublication)
			if got := countType(*sent, "ASP Up"); got != wantUp {
				t.Fatalf("unrequested Ack after peer Up started %d ASP Up procedures, want %d", got, wantUp)
			}
		})
	}
}

func TestIPSPPeerDownSuppressesASPUpDuringConcurrentOverride(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, _ := newIPSPDownAssociationForTest(t, IPSPExchangeSingle, 0, StateASPActive)
	challenger, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	for _, association := range []*Association{incumbent, challenger} {
		setInventoryTrafficModeType(&association.cfg.ApplicationServers, params.NewTrafficModeType(params.TrafficModeOverride))
		association.as = registry
		association.noteRoutingContextsActive([]uint32{1})
	}
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeOverride)
	applicationServer.mu.Lock()
	applicationServer.asps[incumbent] = StateASPActive
	applicationServer.asps[challenger] = StateASPInactive
	applicationServer.mu.Unlock()

	var upCount, downAckCount atomic.Int32
	ackStarted, releaseAck := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseAck) })
	incumbent.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.AspDownAck:
			close(ackStarted)
			<-releaseAck
			downAckCount.Add(1)
		case *messages.AspUp:
			upCount.Add(1)
		}
		return message.MarshalLen(), nil
	}
	// Pause a real Override after it has selected the incumbent for
	// displacement, before it can commit that association's inactive state.
	incumbent.muAckedRCs.Lock()
	locked := true
	defer func() {
		if locked {
			incumbent.muAckedRCs.Unlock()
		}
	}()
	overrideDone := make(chan struct{})
	go func() {
		challenger.overrideOtherASPs([]uint32{1})
		close(overrideDone)
	}()
	if !waitFor(func() bool {
		applicationServer.mu.Lock()
		defer applicationServer.mu.Unlock()
		return applicationServer.asps[challenger] == StateASPActive &&
			applicationServer.asps[incumbent] == StateASPInactive
	}, time.Second) {
		t.Fatal("Override did not select the incumbent for displacement")
	}
	downDone := make(chan struct{})
	go func() {
		incumbent.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
		close(downDone)
	}()
	select {
	case <-ackStarted:
	case <-time.After(time.Second):
		t.Fatal("peer Down did not reach its Ack writer")
	}
	incumbent.muAckedRCs.Unlock()
	locked = false
	if !waitFor(func() bool { return incumbent.stateForActiveRoutingContexts() == StateASPInactive }, time.Second) {
		t.Fatal("Override did not remove the incumbent's active Routing Context")
	}
	// RFC 4666 §4.3 and §5.6.1: completing the peer's Down must keep its
	// withdrawal effective even if Override changed the state during the Ack.
	releaseOnce.Do(func() { close(releaseAck) })
	for _, completed := range []<-chan struct{}{overrideDone, downDone} {
		select {
		case <-completed:
		case <-time.After(time.Second):
			t.Fatal("concurrent peer Down or Override did not finish")
		}
	}
	for len(incumbent.stateChan) > 0 {
		published := <-incumbent.stateChan
		if err := incumbent.handlePublishedStateUpdate(published); err != nil {
			t.Fatalf("apply concurrent publication %v: %v", published, err)
		}
	}
	if got := upCount.Load(); got != 0 {
		t.Fatalf("peer ASP Down raced with Override and started %d ASP Up procedures, want 0", got)
	}
	if got := downAckCount.Load(); got != 1 {
		t.Fatalf("ASP Down Ack count = %d, want 1", got)
	}
	if got := incumbent.State(); got != StateASPDown {
		t.Fatalf("state after peer ASP Down = %v, want ASP-DOWN", got)
	}
}

func newIPSPDownAssociationForTest(t *testing.T, exchange IPSPExchangeModel, aspsm IPSPASPSMExchangeModel, state State) (*Association, *[]messages.M3UA) {
	t.Helper()
	var association *Association
	var sent *[]messages.M3UA
	if exchange == IPSPExchangeSingle {
		association, sent = newSingleExchangeIPSPForTest(t, state)
	} else {
		association, sent = newDoubleExchangeIPSPForTest(t)
		association.cfg.IPSP.ASPSMExchange = aspsm
		association.setIPSPState(IPSPState{TrafficToLocal: state, TrafficToPeer: state})
	}
	association.cfg.ASPProcedures = ipspInitiationPolicy(true, false)
	association.cfg.TAck = time.Hour
	association.stateEntered = true
	association.appliedState = state
	return association, sent
}

func applyIPSPDownPublicationForTest(t *testing.T, association *Association, want State) {
	t.Helper()
	select {
	case published := <-association.stateChan:
		if published != want {
			t.Fatalf("published state = %v, want %v", published, want)
		}
		if err := association.handlePublishedStateUpdate(published); err != nil {
			t.Fatalf("apply published state %v: %v", published, err)
		}
	default:
		t.Fatal("no state publication")
	}
	select {
	case err := <-association.errChan:
		t.Fatalf("handler error: %v", err)
	default:
	}
}
