package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// withholdingReceiver is an SGP receiver over loopback SCTP that validates the
// first allow messages it reads and then stops reading, the way a receiver
// behind an overloaded probe leaves submitted work undelivered when the drain
// deadline passes. With failResultsBeforeStop every /results request before
// the first /stop fails, as a receiver control endpoint that breaks during
// the drain does.
type withholdingReceiver struct {
	control *receiverControl
	server  *httptest.Server
	address string
	stopped atomic.Bool
}

func startWithholdingReceiver(testContext *testing.T, ctx context.Context, sharedClock bool, allow int, failResultsBeforeStop bool) *withholdingReceiver {
	testContext.Helper()
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = endpoint.Close() })
	address, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := endpoint.Listen("m3ua", address, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		testContext.Fatal(err)
	}
	receiver := &withholdingReceiver{control: newReceiverControl(1, maxOutstanding), address: listener.Addr().String()}
	receiver.control.enableSharedClock(sharedClock)
	if receiver.control.fatal != "" {
		testContext.Fatal(receiver.control.fatal)
	}
	handler := receiver.control.handler()
	receiver.server = httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/stop" {
			receiver.stopped.Store(true)
		}
		if failResultsBeforeStop && request.URL.Path == "/results" && !receiver.stopped.Load() {
			http.Error(writer, "receiver control failed", http.StatusServiceUnavailable)
			return
		}
		handler.ServeHTTP(writer, request)
	}))
	testContext.Cleanup(receiver.server.Close)
	go func() {
		association, err := listener.Accept(ctx)
		if err != nil {
			return
		}
		receiver.control.setAssociationReady(0, int(association.MaxMessageStreamID()))
		for read := 0; ; read++ {
			message, err := association.ReadData(ctx)
			if err != nil {
				return
			}
			if read == allow {
				<-ctx.Done()
				return
			}
			receiver.control.record(0, receivedMessage{
				ProtocolData:         protocolDataFromM3UA(message.ProtocolData),
				NetworkAppearance:    message.Scope.NetworkAppearance,
				NetworkAppearanceSet: message.Scope.NetworkAppearanceSet,
				RoutingContext:       firstRoutingContext(message.Scope),
				RoutingContextSet:    message.Scope.RoutingContextSet,
			})
		}
	}()
	return receiver
}

func withholdingSenderConfig(receiver *withholdingReceiver, sharedClock bool) commandConfig {
	return commandConfig{
		Role: "asp", Mode: modeThroughput, Direction: directionASPToSGP, Initiation: initiationASPDial,
		SCTPAddress: receiver.address, Associations: 1, PeerControl: receiver.server.URL, Cohort: "drain-live", Seed: 7,
		Rate: 1_000, Workload: workload128, Warmup: time.Second, Duration: time.Second, Drain: 500 * time.Millisecond,
		Outstanding: maxOutstanding, SameHostClock: sharedClock,
	}
}

// An overloaded warm-up whose receiver leaves 100 of 1,000 submitted messages
// undelivered at the drain deadline fails as a probe: the combined result
// carries exactly the error perfcapacity accepts as overload evidence, the
// sender record names the drain timeout with its undelivered count and has no
// fatal error.
func TestOverloadedWarmupDrainTimeoutIsAFailedProbeOverLoopbackSCTP(testContext *testing.T) {
	for _, sharedClock := range []bool{false, true} {
		name := "http-interval"
		if sharedClock {
			name = "same-host-clock"
		}
		testContext.Run(name, func(testContext *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			receiver := startWithholdingReceiver(testContext, ctx, sharedClock, 900, false)
			result, err := runSender(ctx, withholdingSenderConfig(receiver, sharedClock))
			if err == nil || result.Phase != "warmup" || result.Verdict != verdictInvalid ||
				result.Error != "warmup did not drain cleanly: cohort is invalid; inspect machine-readable reasons" {
				testContext.Fatalf("combined result phase %q verdict %q error %q (%v)", result.Phase, result.Verdict, result.Error, err)
			}
			sender, peer := result.Sender, result.Receiver
			if sender.FatalError != "" || peer.FatalError != "" {
				testContext.Fatalf("fatal errors: sender %q receiver %q", sender.FatalError, peer.FatalError)
			}
			timeout := sender.DrainTimeout
			if timeout == nil || timeout.Cause != drainTimeoutCause || timeout.Drain != 500*time.Millisecond ||
				timeout.Submitted != 1000 || timeout.Accounted != 900 || timeout.Undelivered != 100 {
				testContext.Fatalf("drain timeout = %+v", timeout)
			}
			if sender.Submitted != 1000 || sender.Delivery.Unique != 900 || sender.Delivery.Missing != 100 ||
				sender.FixtureVerdict != verdictInvalid || peer.Delivery != sender.Delivery ||
				!slices.Contains(sender.Reasons, "submitted traffic was still unaccounted at the receiver when the drain deadline passed") {
				testContext.Fatalf("sender record: submitted %d delivery %+v verdict %s reasons %q", sender.Submitted, sender.Delivery, sender.FixtureVerdict, sender.Reasons)
			}
		})
	}
}

// The same undelivered warm-up whose receiver control also fails during the
// drain is a fixture fault: the sender record keeps the fatal error and no
// drain timeout, and the combined error is not the overload text.
func TestDrainWaitControlFailureStaysFatalOverLoopbackSCTP(testContext *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	receiver := startWithholdingReceiver(testContext, ctx, true, 900, true)
	result, err := runSender(ctx, withholdingSenderConfig(receiver, true))
	if err == nil || result.Phase != "warmup" || result.Error == "warmup did not drain cleanly: cohort is invalid; inspect machine-readable reasons" {
		testContext.Fatalf("combined result phase %q error %q (%v)", result.Phase, result.Error, err)
	}
	if result.Sender.FatalError != "receiver results: 503 Service Unavailable" || result.Sender.DrainTimeout != nil {
		testContext.Fatalf("sender fatal %q drain timeout %+v", result.Sender.FatalError, result.Sender.DrainTimeout)
	}
}
