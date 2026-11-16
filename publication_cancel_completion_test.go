package m3ua

import (
	"errors"
	"github.com/gomaja/go-m3ua/messages"
	"syscall"
	"testing"
	"time"
)

func TestLocalSCONCompletionRequiresLiveEndpoint(t *testing.T) {
	for _, scenario := range []string{"national-onset", "national-clear", "international", "newer-intent-after-success"} {
		t.Run(scenario, func(t *testing.T) {
			r := publicationRig(t, 4)
			state := nationalPublication(2)
			if scenario == "international" {
				r.e.congestion.defaultProfile = CongestionProfile{Method: CongestionInternational}
				state = CongestionState{Congested: true}
			}
			queuePublication(r, 100, 10, state, 1)
			if scenario == "national-clear" {
				r.e.runCongestionAutomation(r.now)
				r.now = r.now.Add(time.Second)
				queuePublication(r, 100, 10, nationalPublication(0), 2)
			}
			management := r.a.ManagementIndications()
			_, sub, err := r.e.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			attempts := 0
			r.a.signalWriter = func(m messages.M3UA) (int, error) {
				attempts++
				if scenario == "newer-intent-after-success" {
					queuePublication(r, 100, 10, nationalPublication(3), 2)
				}
				// Endpoint.Close cancels before association teardown. Hold that window
				// at the admitted NoWait callback without closing the association yet.
				r.e.cancel(ErrEndpointClosed)
				if scenario == "newer-intent-after-success" {
					return m.MarshalLen(), nil
				}
				return 0, syscall.EAGAIN
			}
			r.e.runCongestionAutomation(r.now)
			observed := 0
		drain:
			for {
				select {
				case i := <-management:
					p := i.CongestionPublication
					if p == nil || p.Attempts == 0 || p.LastAttempt.Before(r.now) {
						continue
					}
					observed++
					if p.Pending {
						t.Error("cancelled SCON completion advertised a pending retry")
					}
					if (scenario != "newer-intent-after-success") != errors.Is(i.Cause, syscall.EAGAIN) {
						t.Error("completion changed its actual outcome", i.Cause)
					}
				default:
					break drain
				}
			}
			if observed != 1 {
				t.Fatalf("actual local completion observations=%d, want one", observed)
			}
			for _, p := range r.e.CongestionPublications() {
				if p.Pending {
					t.Error("cancelled endpoint retained pending publication work")
				}
			}
			events, _, lost := outcomeProbeEvents(sub)
			outcomes := 0
			for _, e := range events {
				if e.Kind == SSNMPublicationOutcomeEvent && e.CongestionPublication != nil {
					outcomes++
					if e.CongestionPublication.Pending {
						t.Error("SSNM completion advertised a cancelled retry")
					}
				}
			}
			if lost || outcomes != 1 {
				t.Fatal("actual local result not reported once", outcomes, lost)
			}
			r.advance(time.Second)
			if attempts != 1 {
				t.Fatal("cancelled scheduler retried local SCON", attempts)
			}
		})
	}
}
