package m3ua

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestInterleavedScopedMaskedClearsCompleteEveryAuditOrigin(t *testing.T) {
	for _, congestion := range []bool{false, true} {
		for _, mask := range []uint8{1, 24} {
			t.Run(fmt.Sprintf("congestion=%v/mask=%d", congestion, mask), func(t *testing.T) {
				r := newScopedAutomationRig(t)
				r.e.ssnm.validity = 0
				base := uint32(100)
				if mask == 24 {
					base = 0
				}
				whole := uint32(mask)<<24 | base
				half := mask - 1
				upper := base + uint32(1)<<half
				clearPCs := []uint32{uint32(half)<<24 | base, uint32(half)<<24 | upper}
				positive := func() {
					t.Helper()
					var err error
					if congestion {
						err = r.a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(whole), nil, params.NewCongestionIndications(2), nil))
					} else {
						err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(whole), nil))
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				clear := func(rc uint32, halfIndex int) {
					t.Helper()
					if congestion {
						sendSCON(t, r.a, 7, rc, congestionLevel(0), clearPCs[halfIndex])
					} else {
						sendDAVA(t, r.a, 7, rc, clearPCs[halfIndex])
					}
				}
				positive()
				if len(r.e.ssnm.audits) != 1 {
					t.Fatalf("grouped masked setup retained %d jobs, want one", len(r.e.ssnm.audits))
				}
				original := r.job()
				if len(original.origins) != 2 || original.key.destination.mask != mask {
					t.Fatalf("grouped setup lost compact AS origins: %+v", original)
				}
				clear(1, 0)
				clear(2, 1)
				if len(r.e.ssnm.audits) != 1 {
					t.Fatal("partial clears discarded needed masked audit")
				}
				for i, binding := range r.job().bindings {
					origin := r.job().origins[i]
					needed := origin.availability
					if congestion {
						needed = origin.congestion
					}
					if !needed {
						t.Fatalf("RC%d stopped before both halves were answered", binding.key.RoutingContext)
					}
				}
				clear(1, 1)
				if len(r.e.ssnm.audits) != 1 {
					t.Fatal("RC1 completion stopped RC2's unanswered half")
				}
				for i, binding := range r.job().bindings {
					origin := r.job().origins[i]
					needed := origin.availability
					if congestion {
						needed = origin.congestion
					}
					if needed != (binding.key.RoutingContext == 2) {
						t.Fatalf("after RC1 completion RC%d needed=%v", binding.key.RoutingContext, needed)
					}
				}
				clear(2, 0)
				partition := r.a.ssnmPartitionFor(*staticASKey(7, 1))
				for _, rc := range []uint32{1, 2} {
					for _, pc := range []uint32{base, upper} {
						status := r.e.ssnm.destinationKnowledge(partition, pc, *staticASKey(7, rc))
						if congestion {
							if !status.congestionSet || status.congested || status.receivedUnknown {
								t.Fatalf("RC%d PC%d did not resolve measured clear: %+v", rc, pc, status)
							}
						} else if !status.availabilitySet || status.availability != DestinationAvailable {
							t.Fatalf("RC%d PC%d did not resolve available: %+v", rc, pc, status)
						}
					}
				}
				if len(r.e.ssnm.audits) != 0 || original.live.Load() {
					t.Fatalf("all ASes and prefix halves cleared but %d periodic jobs remain", len(r.e.ssnm.audits))
				}
				assertSSNMScopeAccounting(t, r.e)
				if r.e.ssnm.records > r.e.ssnm.limits.MaxRecords || r.e.ssnm.bytes > r.e.ssnm.limits.MaxBytes {
					t.Fatal("compact scoped statements exceeded configured retention limits")
				}
				before := len(r.sent)
				r.advance(time.Minute)
				r.advance(time.Minute)
				if len(r.sent) != before {
					t.Fatal("completed scoped mask continued periodic DAUD")
				}
			})
		}
	}
}

func TestAllClearOrdersPreserveScopedAuditAuthority(t *testing.T) {
	var permutations func([]int, []int) [][]int
	permutations = func(prefix, remaining []int) [][]int {
		if len(remaining) == 0 {
			return [][]int{append([]int(nil), prefix...)}
		}
		var result [][]int
		for i, value := range remaining {
			tail := append([]int(nil), remaining[:i]...)
			tail = append(tail, remaining[i+1:]...)
			result = append(result, permutations(append(append([]int(nil), prefix...), value), tail)...)
		}
		return result
	}
	for _, congestion := range []bool{false, true} {
		for _, order := range permutations(nil, []int{0, 1, 2, 3}) {
			t.Run(fmt.Sprintf("congestion=%v/order=%v", congestion, order), func(t *testing.T) {
				r := newScopedAutomationRig(t)
				r.e.ssnm.validity = 0
				if congestion {
					if err := r.a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(1<<24|100), nil, params.NewCongestionIndications(2), nil)); err != nil {
						t.Fatal(err)
					}
				} else if err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(1<<24|100), nil)); err != nil {
					t.Fatal(err)
				}
				var cleared [2][2]bool
				for step, operation := range order {
					as, half := operation/2, operation%2
					rc, pc := uint32(as+1), uint32(100+half)
					if congestion {
						sendSCON(t, r.a, 7, rc, congestionLevel(0), pc)
					} else {
						sendDAVA(t, r.a, 7, rc, pc)
					}
					cleared[as][half] = true
					for query := 0; query < 2; query++ {
						key := *staticASKey(7, uint32(query+1))
						partition := r.a.ssnmPartitionFor(key)
						for point := 0; point < 2; point++ {
							state := r.e.ssnm.destinationKnowledge(partition, uint32(100+point), key)
							if congestion {
								if !state.congestionSet || state.congested == cleared[query][point] || state.receivedUnknown {
									t.Fatalf("interleaved clear crossed AS authority: step%d AS%d point%d %+v", step, query+1, point, state)
								}
							} else {
								expected := DestinationUnavailable
								if cleared[query][point] {
									expected = DestinationAvailable
								}
								if !state.availabilitySet || state.availability != expected {
									t.Fatalf("interleaved availability crossed AS authority: step%d AS%d point%d %+v", step, query+1, point, state)
								}
							}
						}
					}
					if step < 3 {
						if len(r.e.ssnm.audits) != 1 {
							t.Fatal("partial AS answers retired grouped audit")
						}
						for i, binding := range r.job().bindings {
							expected := !cleared[binding.key.RoutingContext-1][0] || !cleared[binding.key.RoutingContext-1][1]
							actual := r.job().origins[i].availability
							if congestion {
								actual = r.job().origins[i].congestion
							}
							if actual != expected {
								t.Fatal("audit cause disagrees with scoped store authority", binding.key, actual, expected)
							}
						}
					} else if len(r.e.ssnm.audits) != 0 {
						t.Fatal("complete interleaved answers retained periodic audit")
					}
				}
				if r.e.ssnm.records != 9 {
					t.Fatal("scoped prefix state did not remain compact", r.e.ssnm.records)
				}
				assertSSNMScopeAccounting(t, r.e)
			})
		}
	}
}
