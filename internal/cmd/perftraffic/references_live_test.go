package main

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// TestRouteReferencesLiveOverLoopback runs routed-direct with the
// application route table over real SCTP on loopback, churned and static:
// every message resolves through the table, the churn cycles 0-1-1000-0 at
// 1,000 operations/s, and the run must keep both ends' associations, ASP and
// AS state unchanged with no library indication, while delivering every
// message.
func TestRouteReferencesLiveOverLoopback(testContext *testing.T) {
	if testing.Short() {
		testContext.Skip("the route-reference workload needs multi-second cohorts")
	}
	for _, mode := range []string{routeReferencesChurn, routeReferencesStatic} {
		testContext.Run(mode, func(testContext *testing.T) {
			base := routedLoopbackPortBase(testContext)
			control := routedLoopbackControlAddress(testContext)
			sctpAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(base))
			receiverConfig, err := parseConfig([]string{
				"-role=sgp", "-transport=listen", "-mode=routed-direct", "-sctp-address=" + sctpAddress,
				"-control-address=" + control, "-associations=8", "-same-host-clock",
			})
			if err != nil {
				testContext.Fatal(err)
			}
			senderConfig, err := parseConfig([]string{
				"-role=asp", "-transport=dial", "-mode=routed-direct", "-sctp-address=" + sctpAddress,
				"-local-address=127.0.0.1:0", "-peer-control=http://" + control, "-associations=8",
				"-payload=mix", "-rate=2000", "-warmup=1s", "-duration=4s", "-drain=2s",
				"-cohort=route-references-" + mode, "-seed=11", "-same-host-clock", "-route-references=" + mode,
			})
			if err != nil {
				testContext.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			defer cancel()
			receiverContext, stopReceiver := context.WithCancel(ctx)
			defer stopReceiver()
			receiverDone := make(chan error, 1)
			go func() {
				_, err := runRoutedReceiver(receiverContext, receiverConfig)
				receiverDone <- err
			}()
			for {
				response, err := http.Get("http://" + control + "/ready")
				if err == nil {
					_ = response.Body.Close()
					break
				}
				select {
				case err := <-receiverDone:
					testContext.Fatalf("receiver exited before serving: %v", err)
				case <-time.After(10 * time.Millisecond):
				}
			}
			result, senderErr := runRoutedSender(ctx, senderConfig)
			stopReceiver()
			if err := <-receiverDone; err != nil {
				testContext.Fatalf("receiver: %v", err)
			}
			if senderErr != nil || result.Measurement == nil || result.Warmup == nil {
				testContext.Fatalf("sender error %v verdict %s error %q", senderErr, result.Verdict, result.Error)
			}
			for _, cohort := range []*cohortResult{result.Warmup, result.Measurement} {
				references := cohort.Sender.RouteReferences
				if cohort.Sender.FixtureVerdict != verdictPass || cohort.Receiver.FixtureVerdict != verdictPass || references == nil ||
					cohort.Sender.Spec.RouteReferences == nil || cohort.Receiver.Spec.RouteReferences == nil {
					testContext.Fatalf("%s cohort: fixture %s/%s reasons %v references %v", cohort.Phase, cohort.Sender.FixtureVerdict, cohort.Receiver.FixtureVerdict, cohort.Sender.Reasons, references != nil)
				}
				// A one-second warm-up is shorter than one 2 s churn cycle, so its
				// churn intensity may be unmeasurable; nothing else may miss.
				for _, criterion := range references.Criteria {
					testContext.Logf("%s %s: %s — %s", cohort.Phase, criterion.Name, criterion.Outcome, criterion.Detail)
					shortWarmup := cohort.Phase == "warmup" && criterion.Name == "churn_intensity" && criterion.Outcome == failoverNotMeasured
					if criterion.Outcome != failoverPass && !shortWarmup {
						testContext.Errorf("%s cohort criterion %s = %s", cohort.Phase, criterion.Name, criterion.Outcome)
					}
				}
				if cohort.Phase == "measurement" && references.Verdict != failoverPass {
					testContext.Fatalf("measurement cohort route-reference verdict %s", references.Verdict)
				}
				if (mode == routeReferencesChurn) != (references.Churn != nil) {
					testContext.Fatalf("%s cohort churn evidence present = %v", cohort.Phase, references.Churn != nil)
				}
			}
		})
	}
}
