package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func TestSenderCohortPreservesBoundaryCrossingProgress(testContext *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	address, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := endpoint.Listen("m3ua", address, m3ua.NewListenerConfig(associationConfig("sgp")))
	if err != nil {
		testContext.Fatal(err)
	}
	control := newReceiverControl(1, maxOutstanding)
	fatal := make(chan error, 1)
	go acceptAndRead(ctx, listener, 1, control, fatal)
	handler := control.handler()
	var requests atomic.Int32
	// The second progress request is the sampler's first observation, due one
	// sample interval into the window. Holding it for longer than the window
	// has left to run is what makes it straddle the boundary, and holding it
	// for well under progressRequestTimeout is what keeps it from being cut
	// off instead. Both margins are hundreds of milliseconds wide so that a
	// late goroutine on a loaded runner cannot decide the outcome.
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/progress" && requests.Add(1) == 2 {
			timer := time.NewTimer(700 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-request.Context().Done():
				return
			}
		}
		handler.ServeHTTP(writer, request)
	}))
	defer server.Close()
	config := commandConfig{
		SCTPAddress: listener.Addr().String(), Associations: 1,
		PeerControl: server.URL, Cohort: "crossing-progress", Seed: 7,
		Rate: 100, Workload: workload128, Duration: progressSampleInterval + 500*time.Millisecond,
		Drain: 2 * time.Second, Outstanding: maxOutstanding,
	}
	result, err := runSender(ctx, config)
	if err != nil {
		testContext.Fatal(err)
	}
	select {
	case err := <-fatal:
		testContext.Fatal(err)
	default:
	}
	observations := result.Sender.ProgressObservations
	if len(observations) < 3 {
		testContext.Fatalf("missing boundary observations: %+v", observations)
	}
	crossing := observations[1]
	if crossing.Before >= config.Duration || crossing.After <= config.Duration {
		testContext.Fatalf("request did not straddle measurement boundary: %+v", crossing)
	}
	if crossing.Error != "" || crossing.Snapshot == nil {
		testContext.Fatalf("measurement boundary canceled active progress: %+v", crossing)
	}
	if result.Sender.SenderWindow == nil || result.Sender.SenderWindow.Status != "bounded" {
		testContext.Fatalf("boundary crossing invalidated accounting: %+v", result.Sender.SenderWindow)
	}
	if result.Receiver.Delivery.Unique != 150 || result.Receiver.Delivery.Missing != 0 {
		testContext.Fatalf("delivery = %+v", result.Receiver.Delivery)
	}
}
