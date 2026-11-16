package m3ua

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

func TestAuditCompletionPendingMatchesSurvivingRetry(t *testing.T) {
	for _, survivor := range []string{"none", "other-destination", "renewed-cause", "endpoint-cancelled"} {
		t.Run(survivor, func(t *testing.T) {
			r := newAuditTestRig(t)
			points := []uint32{100}
			if survivor == "other-destination" {
				points = append(points, 101)
			}
			sendDUNA(t, r.a, 7, 1, points...)
			_, sub, err := r.e.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			indications := r.a.ManagementIndications()
			attempts := 0
			r.a.signalWriter = func(messages.M3UA) (int, error) {
				attempts++
				if attempts == 1 {
					// The NoWait attempt is admitted, but has not completed.
					// Receive processing must be able to retire its last cause.
					if survivor == "endpoint-cancelled" {
						r.e.cancel(ErrEndpointClosed)
					} else {
						sendDAVA(t, r.a, 7, 1, 100)
					}
					if survivor == "renewed-cause" {
						sendDUNA(t, r.a, 7, 1, 100)
					}
				}
				return 0, syscall.EAGAIN
			}
			r.advance(30 * time.Second)
			pending := survivor == "other-destination" || survivor == "renewed-cause"
			observed := 0
		drain:
			for {
				select {
				case indication := <-indications:
					state := indication.CongestionPublication
					if state == nil || state.Kind != CongestionPublicationDAUD || state.Attempts == 0 {
						continue
					}
					observed++
					if !errors.Is(indication.Cause, syscall.EAGAIN) || state.Pending != pending {
						t.Errorf("completion pending=%v cause=%v, want pending=%v with actual EAGAIN", state.Pending, indication.Cause, pending)
					}
				default:
					break drain
				}
			}
			if observed != 1 {
				t.Fatalf("actual attempt reported %d times, want one", observed)
			}
			jobSurvives := pending || survivor == "endpoint-cancelled"
			if len(r.e.CongestionPublications()) != 0 || (len(r.e.ssnm.audits) != 0) != jobSurvives {
				t.Fatal("completion retained a transient publication or lost its surviving job")
			}
			events, _, lost := outcomeProbeEvents(sub)
			var deferred uint64
			for _, event := range events {
				if event.Kind == SSNMPublicationOutcomeEvent && event.Reason == "send-deferred" {
					deferred += event.PublicationCount
				}
			}
			wantDeferred := uint64(len(points))
			if survivor == "endpoint-cancelled" {
				// Closing the endpoint suppresses subsequent SSNM deltas; the
				// actual management completion above still reports once.
				wantDeferred = 0
			}
			if lost || deferred != wantDeferred {
				t.Fatalf("actual job outcomes: deferred=%d lost=%v, want %d once each", deferred, lost, wantDeferred)
			}
			r.advance(30 * time.Second)
			wantAttempts := 1
			if pending {
				wantAttempts++
			}
			if attempts != wantAttempts {
				t.Fatalf("later pass attempts=%d, want %d", attempts, wantAttempts)
			}
		})
	}
}
