package main

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

// cloneRoutingDataReceipts copies receipts with owned payload bytes, so a test
// can mutate one set without touching another.
func cloneRoutingDataReceipts(receipts []routingDataReceiptDTO) []routingDataReceiptDTO {
	owned := append([]routingDataReceiptDTO(nil), receipts...)
	for index := range owned {
		owned[index].ProtocolData.Data = append([]byte(nil), owned[index].ProtocolData.Data...)
	}
	return owned
}

// routedLoopbackPortBase finds four consecutive free SCTP ports on loopback,
// one per SGP endpoint, and skips the test where the platform has no kernel
// SCTP.
func routedLoopbackPortBase(testContext *testing.T) int {
	testContext.Helper()
	for range 50 {
		base := 20000 + rand.IntN(40000)
		var listeners []*sctp.SCTPListener
		var err error
		for offset := range 4 {
			var listener *sctp.SCTPListener
			listener, err = sctp.ListenSCTP("sctp", &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.IPv4(127, 0, 0, 1)}}, Port: base + offset})
			if err != nil {
				break
			}
			listeners = append(listeners, listener)
		}
		for _, listener := range listeners {
			_ = listener.Close()
		}
		if errors.Is(err, sctp.ErrUnsupported) {
			testContext.Skipf("skipping socket-backed test: %v", err)
		}
		if err == nil {
			return base
		}
	}
	testContext.Fatal("no four consecutive free SCTP ports on loopback")
	return 0
}

func routedLoopbackControlAddress(testContext *testing.T) string {
	testContext.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		testContext.Fatal(err)
	}
	return address
}

// TestRoutedModesLiveOverLoopback runs the production routed entry points,
// runRoutedReceiver and runRoutedSender, over real SCTP on loopback for both
// routed modes: the four SGP endpoints, SSNM preparation, the MTPTransfer
// preflight with both ends freezing their path maps, the handoff to the timed
// readers, and a warm-up and a measurement cohort through runSenderCohortWith
// and the ordinary cohort control. Every scheduled message must be validated
// on its route's frozen path, and the receiver must never be stopped by the
// sender on the success path.
func TestRoutedModesLiveOverLoopback(testContext *testing.T) {
	for _, mode := range []string{modeRouted, modeRoutedDirect} {
		testContext.Run(mode, func(testContext *testing.T) {
			base := routedLoopbackPortBase(testContext)
			control := routedLoopbackControlAddress(testContext)
			sctpAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(base))
			receiverConfig, err := parseConfig([]string{
				"-role=sgp", "-transport=listen", "-mode=" + mode, "-sctp-address=" + sctpAddress,
				"-control-address=" + control, "-associations=8",
			})
			if err != nil {
				testContext.Fatal(err)
			}
			senderConfig, err := parseConfig([]string{
				"-role=asp", "-transport=dial", "-mode=" + mode, "-sctp-address=" + sctpAddress,
				"-local-address=127.0.0.1:0", "-peer-control=http://" + control, "-associations=8",
				"-payload=mix", "-rate=2000", "-warmup=1s", "-duration=2s", "-drain=2s",
				"-cohort=routed-live-" + mode, "-seed=5",
			})
			if err != nil {
				testContext.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			receiverContext, stopReceiver := context.WithCancel(ctx)
			defer stopReceiver()
			type receiverOutcome struct {
				record runRecord
				err    error
			}
			receiverDone := make(chan receiverOutcome, 1)
			go func() {
				record, err := runRoutedReceiver(receiverContext, receiverConfig)
				receiverDone <- receiverOutcome{record: record, err: err}
			}()
			// The receiver serves HTTP only once its SGP endpoints listen.
			for {
				response, err := http.Get("http://" + control + "/ready")
				if err == nil {
					_ = response.Body.Close()
					break
				}
				select {
				case outcome := <-receiverDone:
					testContext.Fatalf("receiver exited before serving: %v", outcome.err)
				case <-ctx.Done():
					testContext.Fatal(ctx.Err())
				case <-time.After(10 * time.Millisecond):
				}
			}
			result, senderErr := runRoutedSender(ctx, senderConfig)
			stopReceiver()
			outcome := <-receiverDone
			if senderErr != nil || result.Verdict == verdictInvalid || result.Warmup == nil || result.Warmup.Verdict == verdictInvalid || result.Measurement == nil {
				testContext.Fatalf("sender verdict=%s error=%q (%v) warm-up=%v", result.Verdict, result.Error, senderErr, result.Warmup != nil)
			}
			for _, record := range []runRecord{result.Sender, result.Receiver, outcome.record} {
				if record.FixtureVerdict != verdictPass || record.Spec.Mode != mode || record.Delivery.Unique != record.Expected || record.Expected == 0 ||
					record.Delivery != (deliveryResult{Unique: record.Expected, UniqueMeasurement: record.Delivery.UniqueMeasurement, UniqueDrain: record.Delivery.UniqueDrain}) {
					testContext.Fatalf("%s record fixture verdict=%s reasons=%v delivery=%+v expected=%d", record.Side, record.FixtureVerdict, record.Reasons, record.Delivery, record.Expected)
				}
			}
			if result.Sender.SendErrors != 0 || result.Sender.Capped != 0 || result.Sender.SendDuration.Count != result.Sender.Expected ||
				result.Sender.Manifest.FlowCount != routingRouteCount || len(result.Sender.NegotiatedOutboundStreams) != routedAssociations {
				testContext.Fatalf("sender record: errors=%d capped=%d timed=%d flows=%d streams=%v", result.Sender.SendErrors, result.Sender.Capped,
					result.Sender.SendDuration.Count, result.Sender.Manifest.FlowCount, result.Sender.NegotiatedOutboundStreams)
			}
			if outcome.err != nil || outcome.record.FatalError != "" || outcome.record.Manifest.FlowCount != routingRouteCount ||
				outcome.record.Spec.Cohort != senderConfig.Cohort {
				testContext.Fatalf("receiver exit=%v fatal=%q flows=%d cohort=%q", outcome.err, outcome.record.FatalError, outcome.record.Manifest.FlowCount, outcome.record.Spec.Cohort)
			}
			testContext.Logf("%s: %d messages validated on their frozen routes", mode, result.Sender.Delivery.Unique)
		})
	}
}
