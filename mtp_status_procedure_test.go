package m3ua

import (
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

func offeredProcedureStatus(t *testing.T, p *MTPStatusProcedure, status MTPDestinationStatus) *MTPIndication {
	t.Helper()
	indication, err := p.Offered(status)
	if err != nil {
		t.Fatal(err)
	}
	return indication
}

func congestedProcedureStatus() MTPDestinationStatus {
	return MTPDestinationStatus{Destination: MTPDestination{MTPRoute: "local", PointCode: 123}, Availability: DestinationAvailable, Congested: true, CongestionLevel: 2, CongestionLevelSet: true}
}
func TestApplicationOwnedProcedureUsesSuppliedAggregateStatus(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionInternational, CongestionNationalWithoutPriorities, CongestionNationalWithPriorities} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			p, err := NewMTPStatusProcedure(CongestionProfile{Method: method})
			if err != nil {
				t.Fatal(err)
			}
			status := congestedProcedureStatus()
			var positions []int
			for offer := 1; offer <= 33; offer++ {
				if got := offeredProcedureStatus(t, p, status); got != nil {
					if got.Kind != MTPStatusIndication || got.Destination != status || got.Cause != MTPStatusSignallingNetworkCongested || got.Reason != MTPStatusOfferedTraffic {
						t.Fatalf("status=%+v", got)
					}
					positions = append(positions, offer)
				}
			}
			var want []int
			if method != CongestionNationalWithPriorities {
				want = []int{1, 9, 17, 25, 33}
			}
			if !reflect.DeepEqual(positions, want) {
				t.Fatalf("status offers=%v, want %v", positions, want)
			}
		})
	}
}
func TestOfferedStatusProcedureResetsDestinationClearExpiryAndExplicitReset(t *testing.T) {
	p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionNationalWithoutPriorities})
	if err != nil {
		t.Fatal(err)
	}
	status := congestedProcedureStatus()
	for _, reset := range []string{"clear", "expiry", "destination", "explicit"} {
		t.Run(reset, func(t *testing.T) {
			if offeredProcedureStatus(t, p, status) == nil {
				t.Fatal("initial offer missing")
			}
			if offeredProcedureStatus(t, p, status) != nil {
				t.Fatal("early repeat")
			}
			switch reset {
			case "clear":
				clear := status
				clear.Congested = false
				if offeredProcedureStatus(t, p, clear) != nil {
					t.Fatal("clear repeated")
				}
			case "expiry":
				expired := status
				expired.ReceivedCongestionUnknown = true
				for range 17 {
					if offeredProcedureStatus(t, p, expired) != nil {
						t.Fatal("expired knowledge repeated")
					}
				}
			case "destination":
				status.Destination.PointCode++
			case "explicit":
				p.Reset()
			}
			if offeredProcedureStatus(t, p, status) == nil {
				t.Fatal("reset did not restore initial indication")
			}
			p.Reset()
		})
	}
}
func TestOfferedStatusProcedureConcurrentCalls(t *testing.T) {
	p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionInternational})
	if err != nil {
		t.Fatal(err)
	}
	var due atomic.Int32
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() {
			for range 256 {
				if offeredProcedureStatus(t, p, congestedProcedureStatus()) != nil {
					due.Add(1)
				}
			}
		})
	}
	wg.Wait()
	if got := due.Load(); got != 512 {
		t.Fatalf("4096 concurrent offers produced %d indications, want 512", got)
	}
	// Reset contends with offers too; this phase asserts safety, not ordering.
	for range 8 {
		wg.Go(func() {
			for range 64 {
				p.Reset()
				offeredProcedureStatus(t, p, congestedProcedureStatus())
			}
		})
	}
	wg.Wait()
}

func TestOfferedStatusProcedureProfileValidationAndBoundedInterval(t *testing.T) {
	for _, profile := range []CongestionProfile{{}, {Method: 99}, {Method: CongestionInternational, MaxLevel: 1}, {Method: CongestionNationalWithoutPriorities, MaxLevel: 4}} {
		if _, err := NewMTPStatusProcedure(profile); err == nil {
			t.Fatalf("accepted profile %+v", profile)
		}
	}
	p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionNationalWithoutPriorities})
	if err != nil {
		t.Fatal(err)
	}
	status := congestedProcedureStatus()
	// Cross both eight-bit and sixteen-bit wrap boundaries without missing a repeat.
	for offer := 1; offer <= 65553; offer++ {
		got := offeredProcedureStatus(t, p, status)
		if (got != nil) != (offer%8 == 1) {
			t.Fatalf("offer %d: %+v", offer, got)
		}
		if p.interval.remaining > 7 {
			t.Fatalf("counter escaped bound: %d", p.interval.remaining)
		}
	}
	// A level change supplies current status, but does not postpone the next repeat.
	p.Reset()
	offeredProcedureStatus(t, p, status)
	status.CongestionLevel = 3
	for offer := 2; offer <= 9; offer++ {
		got := offeredProcedureStatus(t, p, status)
		if (got != nil) != (offer == 9) {
			t.Fatalf("level change offer %d=%+v", offer, got)
		}
		if got != nil && got.Destination != status {
			t.Fatalf("stale level=%+v", got)
		}
	}
}

func TestOfferedStatusProcedureTransportUnknownIsNotKnownCongestion(t *testing.T) {
	p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionNationalWithoutPriorities})
	if err != nil {
		t.Fatal(err)
	}
	status := congestedProcedureStatus()
	status.TransportCongestionUnknown = true
	if got := offeredProcedureStatus(t, p, status); got != nil {
		t.Fatalf("unknown transport repeated congestion: %+v", got)
	}
	status.TransportCongestionUnknown = false
	if got := offeredProcedureStatus(t, p, status); got == nil {
		t.Fatal("valid knowledge did not restore initial status")
	}
}

func TestOfferedStatusProcedureDestinationIdentityIncludesRouteAndMask(t *testing.T) {
	for _, field := range []string{"route", "mask"} {
		t.Run(field, func(t *testing.T) {
			p, err := NewMTPStatusProcedure(CongestionProfile{Method: CongestionInternational})
			if err != nil {
				t.Fatal(err)
			}
			status := congestedProcedureStatus()
			status.Destination.PointCode = 124
			if offeredProcedureStatus(t, p, status) == nil {
				t.Fatal("initial status missing")
			}
			switch field {
			case "route":
				status.Destination.MTPRoute = "other-boundary"
			case "mask":
				status.Destination.Mask = 2
			}
			if offeredProcedureStatus(t, p, status) == nil {
				t.Fatal("changed destination identity did not reset interval")
			}
		})
	}
}
