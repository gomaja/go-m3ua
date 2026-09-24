package main

import (
	"context"
	"errors"
	"testing"
)

// A requested stop closes the SGP endpoints too, so both are ready when the
// receiver looks: the stop must win every time, and endpoints closing on their
// own must still be a fault.
func TestRoutedReceiverEndTellsAStopFromLostEndpoints(testContext *testing.T) {
	closed := make(chan struct{})
	close(closed)
	stopped, stop := context.WithCancel(context.Background())
	stop()
	for attempt := 0; attempt < 200; attempt++ {
		if err := routedReceiverEnd(stopped, nil, nil, closed); err != nil {
			testContext.Fatalf("attempt %d: a requested stop that also closed the endpoints reported %v", attempt, err)
		}
	}
	if err := routedReceiverEnd(context.Background(), nil, nil, closed); err == nil || err.Error() != "routed SGP endpoints closed" {
		testContext.Fatalf("endpoints closing without a stop = %v, want the fault", err)
	}
	fatal := make(chan error, 1)
	fatal <- errors.New("association 3 lost")
	if err := routedReceiverEnd(context.Background(), fatal, nil, nil); err == nil || err.Error() != "association 3 lost" {
		testContext.Fatalf("a fixture fault = %v", err)
	}
	failure := make(chan error, 1)
	failure <- errors.New("listener closed")
	if err := routedReceiverEnd(context.Background(), nil, failure, nil); err == nil || err.Error() != "HTTP control server: listener closed" {
		testContext.Fatalf("an HTTP failure = %v", err)
	}
}
