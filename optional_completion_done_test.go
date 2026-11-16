package m3ua

import (
	"errors"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

// Pause real teardown after Done, before transport release and store cleanup.
func pauseOptionalAssociationRelease(t *testing.T, c *Association) func() {
	t.Helper()
	entered, release := make(chan struct{}), make(chan struct{})
	closed := make(chan error, 1)
	started := false
	var once sync.Once
	c.transportCloser = func() error { close(entered); <-release; return nil }
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		if started {
			select {
			case err := <-closed:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("association teardown did not finish after release")
			}
		}
	})
	return func() {
		t.Helper()
		started = true
		go func() { closed <- c.Close() }()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("association did not reach transport release")
		}
		select {
		case <-c.Done():
		default:
			t.Fatal("transport release began before Done")
		}
		if c.endpoint.ctx.Err() != nil {
			t.Fatal("association-only closure cancelled the endpoint")
		}
	}
}

func TestLocalSCONCompletionRequiresLiveAssociation(t *testing.T) {
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
			_, sub, err := r.e.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			management := r.a.ManagementIndications()
			closeAssociation := pauseOptionalAssociationRelease(t, r.a)
			before := r.e.CongestionPublicationCounters()
			attempts := 0
			r.a.signalWriter = func(m messages.M3UA) (int, error) {
				attempts++
				if scenario == "newer-intent-after-success" {
					queuePublication(r, 100, 10, nationalPublication(3), 3)
				}
				closeAssociation()
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
				case indication := <-management:
					p := indication.CongestionPublication
					if p == nil || p.Attempts == 0 || p.LastAttempt.Before(r.now) {
						continue
					}
					observed++
					if p.Pending {
						t.Error("ended association completion advertised a pending SCON retry")
					}
					if (scenario != "newer-intent-after-success") != errors.Is(indication.Cause, syscall.EAGAIN) {
						t.Error("association closure changed the actual SCON result", indication.Cause)
					}
				default:
					break drain
				}
			}
			if observed != 1 || attempts != 1 {
				t.Error("SCON completion not observed exactly once", observed, attempts)
			}
			if len(r.e.CongestionPublications()) != 0 {
				t.Error("ended association retained a local publication reservation")
			}
			events, _, lost := outcomeProbeEvents(sub)
			outcomes := 0
			for _, event := range events {
				if event.Kind == SSNMPublicationOutcomeEvent && event.CongestionPublication != nil {
					outcomes++
					if event.CongestionPublication.Pending {
						t.Error("SSNM completion advertised an ended association retry")
					}
				}
			}
			if lost || outcomes != 1 {
				t.Error("actual SCON outcome not published once", outcomes, lost)
			}
			after := r.e.CongestionPublicationCounters()
			if scenario == "newer-intent-after-success" {
				if after.Submitted != before.Submitted+1 || after.Failed != before.Failed {
					t.Error("accepted SCON result lost from counters")
				}
			} else if after.Failed != before.Failed+1 || after.Submitted != before.Submitted {
				t.Error("failed SCON result lost from counters")
			}
			r.advance(time.Second)
			if attempts != 1 {
				t.Error("ended association retried SCON", attempts)
			}
		})
	}
}

func TestAuditCompletionRequiresLiveAssociation(t *testing.T) {
	for _, scenario := range []string{"eagain", "accepted", "renewed-eagain", "cleared-eagain"} {
		t.Run(scenario, func(t *testing.T) {
			r := publicationRig(t, 4)
			sendDUNA(t, r.a, 7, 1, 100)
			_, sub, err := r.e.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			management := r.a.ManagementIndications()
			closeAssociation := pauseOptionalAssociationRelease(t, r.a)
			attempts := 0
			r.a.signalWriter = func(m messages.M3UA) (int, error) {
				attempts++
				switch scenario {
				case "renewed-eagain":
					sendDUNA(t, r.a, 7, 1, 100)
				case "cleared-eagain":
					sendDAVA(t, r.a, 7, 1, 100)
				}
				closeAssociation()
				if scenario == "accepted" {
					return m.MarshalLen(), nil
				}
				return 0, syscall.EAGAIN
			}
			r.advance(30 * time.Second)
			observed := 0
		drain:
			for {
				select {
				case indication := <-management:
					p := indication.CongestionPublication
					if p == nil || p.Kind != CongestionPublicationDAUD || p.Attempts == 0 {
						continue
					}
					observed++
					if p.Pending {
						t.Error("ended association completion advertised a pending DAUD retry")
					}
					if (scenario != "accepted") != errors.Is(indication.Cause, syscall.EAGAIN) {
						t.Error("association closure changed the actual DAUD result", indication.Cause)
					}
				default:
					break drain
				}
			}
			if observed != 1 || attempts != 1 {
				t.Error("DAUD completion not observed exactly once", observed, attempts)
			}
			r.e.ssnm.mu.Lock()
			remaining := len(r.e.ssnm.audits)
			r.e.ssnm.mu.Unlock()
			if remaining != 0 {
				t.Error("ended association retained live audit deadlines", remaining)
			}
			events, _, lost := outcomeProbeEvents(sub)
			var actual uint64
			for _, event := range events {
				if event.Kind == SSNMPublicationOutcomeEvent && (event.Reason == "send-deferred" || event.Reason == "submitted") {
					actual += event.PublicationCount
				}
			}
			if lost || actual != 1 {
				t.Error("retired DAUD actual outcome not reported once", actual, lost)
			}
			r.advance(30 * time.Second)
			if attempts != 1 {
				t.Error("ended association retried DAUD", attempts)
			}
		})
	}
}
