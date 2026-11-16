package m3ua

import (
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

func TestClearedUnsubmittedOnsetDoesNotReportPendingRetry(t *testing.T) {
	for _, international := range []bool{false, true} {
		name := map[bool]string{false: "national", true: "international"}[international]
		t.Run(name, func(t *testing.T) {
			r := publicationRig(t, 2)
			state := nationalPublication(3)
			if international {
				r.e.congestion.defaultProfile = CongestionProfile{Method: CongestionInternational}
				state = CongestionState{Congested: true}
			}
			_ = r.a.ManagementIndications()
			entered := make(chan struct{})
			release := make(chan struct{})
			var attempts atomic.Int64
			r.a.signalWriter = func(messages.M3UA) (int, error) {
				if attempts.Add(1) == 1 {
					close(entered)
				}
				<-release
				return 0, syscall.EAGAIN
			}
			queuePublication(r, 100, 10, state, 1)
			done := make(chan struct{})
			go func() { r.e.runCongestionAutomation(r.now); close(done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				close(release)
				<-done
				t.Fatal("onset never reached the controlled optional transport")
			}
			queuePublication(r, 100, 10, CongestionState{}, 2)
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("optional completion did not settle after transport release")
			}
			if len(r.e.CongestionPublications()) != 0 {
				t.Fatal("locally ended unsubmitted onset retained a reservation")
			}
			r.advance(time.Second)
			if attempts.Load() != 1 {
				t.Fatal("locally ended unsubmitted onset retried")
			}
			failures := 0
			indications := r.a.ManagementIndications()
		drain:
			for {
				select {
				case indication, ok := <-indications:
					if !ok {
						break drain
					}
					observation := indication.CongestionPublication
					if observation == nil || !errors.Is(indication.Cause, syscall.EAGAIN) {
						continue
					}
					failures++
					if observation.Pending {
						t.Error("discarded in-flight onset reported a pending retry after local clear")
					}
				default:
					break drain
				}
			}
			if failures != 1 {
				t.Fatalf("failed discarded attempt observations=%d, want one", failures)
			}
		})
	}
}

func TestRetiredInflightPublicationCompletionDoesNotReportPending(t *testing.T) {
	for _, failure := range []bool{false, true} {
		name := map[bool]string{false: "accepted", true: "eagain"}[failure]
		t.Run(name, func(t *testing.T) {
			r := publicationRig(t, 2)
			_ = r.a.ManagementIndications()
			entered, release := make(chan struct{}), make(chan struct{})
			var attempts atomic.Int64
			r.a.signalWriter = func(message messages.M3UA) (int, error) {
				if attempts.Add(1) == 1 {
					close(entered)
				}
				<-release
				if failure {
					return 0, syscall.EAGAIN
				}
				return message.MarshalLen(), nil
			}
			queuePublication(r, 100, 10, nationalPublication(3), 1)
			done := make(chan struct{})
			go func() { r.e.runCongestionAutomation(r.now); close(done) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				close(release)
				<-done
				t.Fatal("onset never reached its admitted optional attempt")
			}
			// Exercise the scope-retirement mark used by state/ledger teardown.
			r.a.retireLocalCongestionScopes(nil)
			close(release)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("retired attempt did not complete after transport release")
			}
			if len(r.e.CongestionPublications()) != 0 {
				t.Fatal("scope retirement retained the in-flight reservation")
			}
			r.advance(time.Second)
			if attempts.Load() != 1 {
				t.Fatal("retired scope retried its old optional attempt")
			}
			observed := 0
			indications := r.a.ManagementIndications()
		drain:
			for {
				select {
				case indication, ok := <-indications:
					if !ok {
						break drain
					}
					state := indication.CongestionPublication
					if state == nil || state.Attempts == 0 {
						continue
					}
					observed++
					if state.Pending {
						t.Error("retired in-flight completion reported pending optional work")
					}
					if failure != errors.Is(indication.Cause, syscall.EAGAIN) {
						t.Errorf("retired completion changed its actual outcome: %v", indication.Cause)
					}
				default:
					break drain
				}
			}
			if observed != 1 {
				t.Fatalf("retired attempt observations=%d, want one", observed)
			}
		})
	}
}
