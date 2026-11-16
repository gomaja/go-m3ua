package m3ua

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestCollectiveRangeClearsRetireGroupedCongestionAudit(t *testing.T) {
	for _, width := range []uint8{1, 24} {
		for _, separate := range []bool{false, true} {
			t.Run(fmt.Sprintf("mask=%d/separate=%v", width, separate), func(t *testing.T) {
				r := newScopedAutomationRig(t)
				// Keep the positive statement fresh during the partial-clear send.
				r.e.ssnm.validity = 10 * time.Minute
				base := uint32(100)
				if width == 24 {
					base = 0
				}
				half := width - 1
				lower := uint32(half)<<24 | base
				upperPC := base + uint32(1)<<half
				upper := uint32(half)<<24 | upperPC
				masked := uint32(width)<<24 | base
				scon := func(level uint8, points ...uint32) {
					t.Helper()
					err := r.a.handleSignallingCongestion(messages.NewSignallingCongestion(
						params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2),
						params.NewAffectedPointCode(points...), nil,
						params.NewCongestionIndications(level), nil))
					if err != nil {
						t.Fatal(err)
					}
				}
				scon(2, masked)
				original := r.job()
				if len(r.e.ssnm.audits) != 1 || original == nil || len(original.origins) != 2 || original.key.destination.mask != width {
					t.Fatal("masked grouped report must retain one job and two AS origins")
				}
				if separate {
					scon(0, lower)
					if len(r.e.ssnm.audits) != 1 || r.job().key.destination != original.key.destination {
						t.Fatal("partial clear retired or expanded the compact range job")
					}
					for _, rc := range []uint32{1, 2} {
						key := *staticASKey(7, rc)
						partition := r.a.ssnmPartitionFor(key)
						clear := r.e.ssnm.destinationKnowledge(partition, base, key)
						positive := r.e.ssnm.destinationKnowledge(partition, upperPC, key)
						if clear.congested || clear.receivedUnknown || !positive.congested || positive.receivedUnknown {
							t.Fatalf("partial clear lost independent range knowledge: clear=%+v positive=%+v", clear, positive)
						}
					}
					r.advance(30 * time.Second)
					if len(r.sent) != 1 {
						t.Fatal("partial clear stopped the still-needed audit", len(r.sent))
					}
					audit, ok := r.sent[0].(*messages.DestinationStateAudit)
					if !ok {
						t.Fatalf("sent %T, want DAUD", r.sent[0])
					}
					if got := audit.AffectedPointCode.AffectedPointCodes(); !reflect.DeepEqual(got, []uint32{base}) || !reflect.DeepEqual(audit.AffectedPointCode.AffectedPointCodeMasks(), []uint8{width}) {
						t.Fatal("partial clear expanded outgoing APCs", got)
					}
					if got := audit.RoutingContext.RoutingContexts(); !reflect.DeepEqual(got, []uint32{1, 2}) {
						t.Fatal("partial clear lost a still-needed AS", got)
					}
					scon(0, upper)
				} else {
					scon(0, lower, upper)
				}
				for _, rc := range []uint32{1, 2} {
					key := *staticASKey(7, rc)
					for _, pc := range []uint32{base, upperPC} {
						status := r.e.ssnm.destinationKnowledge(r.a.ssnmPartitionFor(key), pc, key)
						if status.congested || status.receivedUnknown {
							t.Fatalf("complete clear did not resolve RC%d PC%d clear: %+v", rc, pc, status)
						}
					}
				}
				if len(r.e.ssnm.audits) != 0 {
					t.Fatalf("all destinations resolve clear but grouped range audit remains: %d", len(r.e.ssnm.audits))
				}
				sent := len(r.sent)
				r.advance(time.Minute)
				if len(r.sent) != sent {
					t.Fatal("fully cleared range continued periodic DAUD")
				}
				assertSSNMScopeAccounting(t, r.e)
			})
		}
	}
}

func TestOtherReporterCannotClearMaskedCongestionAudit(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.validity = 10 * time.Minute
	b := attachSSNMAssociation(t, r.e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 2)
	sendSCON(t, r.a, 7, 1, congestionLevel(2), uint32(1)<<24|100)
	original := r.job()
	sendSCON(t, b, 7, 2, congestionLevel(0), 100, 101)
	if len(r.e.ssnm.audits) != 1 || r.job() != original || !r.job().congestion {
		t.Fatal("another reporter's complete range clear retired the original audit")
	}
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("original reporter audit did not submit")
	}
	audit, ok := r.sent[0].(*messages.DestinationStateAudit)
	if !ok || !reflect.DeepEqual(audit.RoutingContext.RoutingContexts(), []uint32{1}) {
		t.Fatal("periodic audit changed original reporter scope")
	}
}

func auditOutcomeTotals(t *testing.T, sub *SSNMSubscription, association AssociationID, scope WireScope) map[string]uint64 {
	t.Helper()
	events, _, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("outcome subscription lost continuity")
	}
	totals := make(map[string]uint64)
	for _, event := range events {
		if event.Kind != SSNMPublicationOutcomeEvent {
			continue
		}
		if event.Report.Association != association || event.ReportSet || event.PublicationScopeOmitted || !reflect.DeepEqual(event.Report.Scope, scope) {
			t.Fatalf("misattributed audit outcome: %+v", event)
		}
		totals[event.Reason] += event.PublicationCount
	}
	return totals
}

func TestDynamicCongestionAuditRetirementOutcomeIsExactlyOnce(t *testing.T) {
	// Enumerate every ordering of the production registration, binding,
	// source-epoch and association retirement entry points. No report is
	// admitted between retirements, so all later passes must be idempotent.
	var permutations func([]string, []string) [][]string
	permutations = func(prefix, remaining []string) [][]string {
		if len(remaining) == 0 {
			return [][]string{append([]string(nil), prefix...)}
		}
		var result [][]string
		for i, name := range remaining {
			rest := append([]string(nil), remaining[:i]...)
			rest = append(rest, remaining[i+1:]...)
			result = append(result, permutations(append(append([]string(nil), prefix...), name), rest)...)
		}
		return result
	}
	orders := permutations(nil, []string{"registration", "binding", "epoch", "association"})
	for _, expired := range []bool{false, true} {
		for _, order := range orders {
			t.Run(fmt.Sprintf("expired=%v/%s", expired, strings.Join(order, "-")), func(t *testing.T) {
				e, a, key, now := newDynamicValidityAssociation(t, true)
				sendSCON(t, a, 7, 7, congestionLevel(2), 100)
				if expired {
					*now = now.Add(10 * time.Second)
					e.runCongestionAutomation(*now)
				}
				if len(e.ssnm.audits) != 1 {
					t.Fatal("setup missing one dynamic-only congestion audit")
				}
				var original *destinationAuditJob
				for _, job := range e.ssnm.audits {
					original = job
				}
				_, sub, err := e.SubscribeSSNM()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = sub.Close() }()
				for _, action := range order {
					switch action {
					case "registration":
						a.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{
							ASKey: key, RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered,
						}})()
					case "binding":
						a.noteRoutingContextsOverridden([]uint32{7})
					case "epoch":
						a.resetTransportCongestionEpoch()
					case "association":
						if err := a.Close(); err != nil {
							t.Fatal(err)
						}
					}
					if len(e.ssnm.audits) != 0 || original.live.Load() {
						t.Fatalf("%s retained a retired job", action)
					}
					totals := auditOutcomeTotals(t, sub, a.ID(), original.scope)
					if !reflect.DeepEqual(totals, map[string]uint64{"reporter-retired": 1}) {
						t.Fatalf("after %s outcomes=%v, want one reporter-retired job", action, totals)
					}
				}
			})
		}
	}
}

func TestRequesterDeregistrationReportsCongestionAuditRetirement(t *testing.T) {
	e, a, _, _ := newDynamicValidityAssociation(t, true)
	sendSCON(t, a, 7, 7, congestionLevel(2), 100)
	version := a.dynamicASKeyVersionFor(7, false)
	_, sub, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	a.removeRequesterRoutingKeyVersion(7, version)
	a.removeRequesterRoutingKeyVersion(7, version)
	if _, held := a.dynamicASKey(7, false); held || len(e.ssnm.audits) != 0 {
		t.Fatal("requester deregistration retained registration or audit")
	}
	if totals := auditOutcomeTotals(t, sub, a.ID(), testWireScope(7, true, 7)); !reflect.DeepEqual(totals, map[string]uint64{"reporter-retired": 1}) {
		t.Fatal("requester deregistration lost or repeated retirement outcome", totals)
	}
}

type refreshingOptionalAuditSender struct {
	attempts int
	noWait   bool
	callback func()
	result   error
	frames   [][]byte
}

func (s *refreshingOptionalAuditSender) SendMsg([]byte, sctp.SendOptions) (int, error) {
	return 0, fmt.Errorf("optional audit used contextless transport")
}

func (s *refreshingOptionalAuditSender) SendMsgContext(_ context.Context, frame []byte, options sctp.SendOptions) (int, error) {
	s.attempts++
	s.noWait = options.NoWait
	s.frames = append(s.frames, append([]byte(nil), frame...))
	if s.callback != nil {
		callback := s.callback
		s.callback = nil
		callback()
	}
	return 0, s.result
}

func TestOptionalAuditActualOutcomeSurvivesSameKeyRefresh(t *testing.T) {
	for _, result := range []error{syscall.EPIPE, nil, syscall.EAGAIN} {
		t.Run(fmt.Sprintf("error=%v", result), func(t *testing.T) {
			r := newAuditTestRig(t)
			sendDUNA(t, r.a, 7, 1, 100)
			original := r.job()
			_, sub, err := r.e.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			var refreshed *destinationAuditJob
			var refreshDue time.Time
			sender := &refreshingOptionalAuditSender{}
			sender.result = result
			sender.callback = func() {
				r.now = r.now.Add(5 * time.Second)
				sendDUNA(t, r.a, 7, 1, 100)
				refreshed = r.job()
				refreshDue = r.now.Add(r.e.ssnm.auditing.Period)
				// Distinguish a refresh deadline from a deadline blindly based
				// on transport completion; no real clock or wait is involved.
				r.now = r.now.Add(time.Second)
			}
			r.a.signalWriter = nil
			r.a.controlTransport = sender
			r.advance(30 * time.Second)
			if sender.attempts != 1 || !sender.noWait {
				t.Fatalf("transport attempts=%d NoWait=%v", sender.attempts, sender.noWait)
			}
			if len(sender.frames) != 1 {
				t.Fatal("transport frame missing")
			}
			audit, parseErr := messages.ParseDestinationStateAudit(sender.frames[0])
			if parseErr != nil || !reflect.DeepEqual(audit.AffectedPointCode.AffectedPointCodes(), []uint32{100}) || !reflect.DeepEqual(audit.RoutingContext.RoutingContexts(), []uint32{1}) {
				t.Fatalf("transport frame is not the original scoped DAUD: %v", parseErr)
			}
			next := r.job()
			if refreshed == nil || refreshed == original || next == nil || next.key != original.key || next.generation != refreshed.generation || original.live.Load() || !next.live.Load() || !next.availability || next.congestion || !next.due.Equal(refreshDue) {
				t.Fatalf("submission clobbered refreshed successor: old=%p refreshed=%p current=%+v due=%v", original, refreshed, next, refreshDue)
			}
			if !r.e.ssnm.auditNext.Equal(r.now.Add(auditRateInterval(r.e.ssnm.auditing.MaxMessagesPerSecond))) {
				t.Fatal("actual transport attempt lost its global rate slot")
			}
			reason := "submitted"
			if result != nil {
				reason = "send-failed"
			}
			if sendBufferFull(result) {
				reason = "send-deferred"
			}
			if totals := auditOutcomeTotals(t, sub, r.a.ID(), original.scope); !reflect.DeepEqual(totals, map[string]uint64{reason: 1}) {
				t.Fatalf("completed attempt outcome=%v, want exactly one %s", totals, reason)
			}
			events, _, _ := outcomeProbeEvents(sub)
			local := 0
			for _, event := range events {
				if event.Kind == SSNMReportObservedEvent && event.Report.Source == SSNMLocalReport && event.Report.Kind == SSNMDestinationStateAuditReport {
					local++
				}
			}
			wantLocal := 1
			if result != nil {
				wantLocal = 0
			}
			if local != wantLocal {
				t.Fatalf("successful-send observations=%d, want %d", local, wantLocal)
			}
			// A pass before the refresh deadline must neither retry the old job
			// nor repeat its outcome. A new completion belongs to the successor.
			r.e.runCongestionAutomation(r.now)
			if sender.attempts != 1 {
				t.Fatal("completed attempt immediately retried")
			}
			if totals := auditOutcomeTotals(t, sub, r.a.ID(), original.scope); !reflect.DeepEqual(totals, map[string]uint64{reason: 1}) {
				t.Fatal("idle pass replayed completed outcome", totals)
			}
			sender.result = nil
			r.now = refreshDue
			r.e.runCongestionAutomation(r.now)
			if sender.attempts != 2 || !r.job().due.Equal(r.now.Add(r.e.ssnm.auditing.Period)) {
				t.Fatal("successor failed to continue its own periodic schedule")
			}
			want := map[string]uint64{"submitted": 2}
			if result != nil {
				want = map[string]uint64{reason: 1, "submitted": 1}
			}
			if totals := auditOutcomeTotals(t, sub, r.a.ID(), original.scope); !reflect.DeepEqual(totals, want) {
				t.Fatal("successor lost or repeated a completed outcome", totals)
			}
		})
	}
}
