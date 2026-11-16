package m3ua

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestScopedWithdrawalLocalRetirementAllowsReceivedCommit(t *testing.T) {
	testScopedRetirementLockOrder(t, false)
}

func TestPartialOverrideLocalRetirementAllowsReceivedCommit(t *testing.T) {
	testScopedRetirementLockOrder(t, true)
}

// A child process bounds a deliberately reproduced mutex cycle without leaving
// blocked goroutines or a fixture Close behind in the parent test process.
func testScopedRetirementLockOrder(t *testing.T, override bool) {
	t.Helper()
	for _, dynamic := range []bool{false, true} {
		name := "static"
		if dynamic {
			name = "dynamic"
		}
		t.Run(name, func(t *testing.T) {
			if os.Getenv("M3UA_SCOPED_RETIREMENT_CHILD") == t.Name() {
				testScopedRetirementLockChild(t, override, dynamic)
				return
			}
			binary, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, binary, "-test.run", "^"+regexp.QuoteMeta(t.Name())+"$", "-test.v", "-test.timeout=12s")
			command.Env = append(os.Environ(), "M3UA_SCOPED_RETIREMENT_CHILD="+t.Name())
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("scoped local retirement blocked a received SSNM commit: %v\n%s", err, output)
			}
		})
	}
}

func testScopedRetirementLockChild(t *testing.T, override, dynamic bool) {
	t.Helper()
	watchdog := make(chan struct{})
	t.Cleanup(func() { close(watchdog) })
	go func() {
		select {
		case <-watchdog:
		case <-time.After(8 * time.Second):
			_, _ = fmt.Fprintln(os.Stderr, "lock-order fixture failed to reach or finish its acquisition barriers")
			os.Exit(24)
		}
	}()
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	if dynamic {
		for i := range config.SignallingGateways[0].SGPs {
			config.SignallingGateways[0].SGPs[i].ApplicationServers = append(config.SignallingGateways[0].SGPs[i].ApplicationServers,
				RemoteASConfig{ID: "as-dynamic", RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: statusCommitPC}}}})
		}
		config.Routing.Paths[0].ApplicationServers = append(config.Routing.Paths[0].ApplicationServers, "as-dynamic")
	}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 1)
	if dynamic {
		a.addDynamicASKey(*staticASKey(7, 9), RoutingKey{}, false)
		if err := a.noteCanonicalRemoteAS(9, "as-dynamic"); err != nil {
			t.Fatal(err)
		}
		a.noteRoutingContextsAcked(params.NewRoutingContext(1, 9))
	}
	if len(e.CongestionPublications()) != 0 || len(e.aspRoutes.statusMarks.entries) != 0 {
		t.Fatal("lock-order fixture unexpectedly needs a pending publication or offered interval")
	}
	storeHeld, commit := make(chan struct{}), make(chan struct{})
	received := startASPTestCall(func() error {
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		close(storeHeld)
		<-commit
		return e.ssnm.applyPreparedLocked(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
			Association: b.ID(), Partition: canonicalSSNMPartition("sg-a", "as-core"),
			Scope:        WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
			Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevel: 2, CongestionLevelSet: true}, []ASKey{b.staticASKeyForRoutingContext(1, false)})
	})
	receiveASPTest(t, storeHeld, "received commit's store ownership")
	retired := startASPTestCall(func() error {
		if override {
			a.noteRoutingContextsOverridden([]uint32{1})
		} else {
			a.forgetAckedRoutingContexts()
		}
		return nil
	})
	// Both the old nesting and the corrected order reach this actual store
	// acquisition. Only the old nesting owns restart marks at that point.
	if !awaitASPTestStack(t, retired, "(*Association).retireLocalCongestionInputs(", "lockSlow(") {
		t.Fatal("scope change did not reach local retirement under the held store")
	}
	close(commit)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	receivedWait, retiredWait := received.done, retired.done
	for receivedWait != nil || retiredWait != nil {
		select {
		case <-receivedWait:
			receivedWait = nil
		case <-retiredWait:
			retiredWait = nil
		case <-tick.C:
			stack := make([]byte, 1<<20)
			stack = stack[:runtime.Stack(stack, true)]
			waitingStore, waitingMarks := false, false
			for _, goroutine := range strings.Split(string(stack), "\n\n") {
				waitingStore = waitingStore || strings.Contains(goroutine, "(*Association).retireLocalCongestionInputs(") && strings.Contains(goroutine, "lockSlow(")
				waitingMarks = waitingMarks || strings.Contains(goroutine, "(*ssnmState).beginReceivedStatusChangeLocked(") && strings.Contains(goroutine, "lockSlow(")
			}
			if waitingStore && waitingMarks {
				_, _ = fmt.Fprintln(os.Stderr, "SSNM commit and scope retirement formed a store/restart-mark lock cycle")
				os.Exit(23)
			}
		case <-deadline.C:
			_, _ = fmt.Fprintln(os.Stderr, "received commit and scope retirement did not both finish after releasing the commit barrier")
			os.Exit(25)
		}
	}
	if received.err != nil || retired.err != nil {
		t.Fatalf("received commit=%v, scope retirement=%v", received.err, retired.err)
	}
	if !e.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), statusCommitPC, *staticASKey(7, 1)).congested {
		t.Fatal("received commit did not retain the SCON after scope retirement")
	}
}

func TestOverrideRetirementKeepsLocalAdmissionFenced(t *testing.T) {
	e, a, _, _ := newStatusCommitFixture(t)
	e.congestionAutomationOnce.Do(func() {})
	key := *staticASKey(7, 1)
	request := localRequest(a, key, "service", 2)
	var attempts atomic.Int64
	a.signalWriter = func(messages.M3UA) (int, error) { attempts.Add(1); return 0, nil }
	if err := e.ReportLocalCongestion(request); err != nil {
		t.Fatal(err)
	}
	binding := a.queueCongestionBinding(key, localCongestionScope(key))
	old := a.captureQueueCongestionLifetime(binding)
	if old == nil || len(e.CongestionPublications()) != 1 {
		t.Fatal("fixture did not retain the original local lifetime and report")
	}
	unlockStore := holdASPTestLock(t, &e.ssnm.mu)
	retired := startASPTestCall(func() error { a.noteRoutingContextsOverridden([]uint32{1}); return nil })
	if !awaitASPTestStack(t, retired, "(*Association).retireLocalCongestionInputs(", "lockSlow(") {
		t.Fatal("override did not reach its retirement fence")
	}
	if old.live.Load() {
		t.Fatal("override waited for store before retiring the old admission token")
	}
	admission := startASPTestCall(func() error { return e.ReportLocalCongestion(request) })
	if err := admission.wait(t, "local admission while the ACK writer owns retirement"); !errors.Is(err, ErrCongestionDecisionStale) && !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("local admission escaped the held ACK retirement fence: %v", err)
	}
	unlockStore()
	if err := retired.wait(t, "override permission commit"); err != nil {
		t.Fatal(err)
	}
	if err := e.ReportLocalCongestion(request); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("local report admitted after override became visible: %v", err)
	}
	if old.live.Load() || len(e.CongestionPublications()) != 0 || len(e.ssnm.localCongestionSnapshot(key)) != 0 {
		t.Fatal("visible override retained old local evidence or publication authority")
	}
	e.runCongestionAutomation(time.Now().Add(time.Second))
	if attempts.Load() != 0 {
		t.Fatal("retired local report was submitted after override")
	}
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if err := e.ReportLocalCongestion(request); err != nil {
		t.Fatalf("reactivation did not permit fresh local evidence: %v", err)
	}
	if current := a.captureQueueCongestionLifetime(binding); current == nil || current == old || old.live.Load() {
		t.Fatal("reactivation reused the retired local admission lifetime")
	}
}
