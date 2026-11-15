package m3ua

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func knownClearAuditFixture(t *testing.T, method CongestionMethod, contexts ...uint32) (*Association, *[]messages.M3UA) {
	t.Helper()
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, contexts...)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: method, MaxLevel: 3}}}
	if method == CongestionInternational {
		c.cfg.CongestionProfiles[0].Profile.MaxLevel = 0
	}
	return c, sent
}

func auditKnowledgeRequest(pcs ...uint32) *messages.DestinationStateAudit {
	return messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(pcs...), nil)
}

func TestNationalDAUDKnownClearSendsSCONZeroBeforeAvailability(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionNationalWithPriorities, CongestionNationalWithoutPriorities} {
		for _, availability := range []DestinationAvailability{DestinationAvailable, DestinationRestricted} {
			for _, levelSet := range []bool{false, true} {
				t.Run(fmt.Sprintf("%d/%s/level-set=%t", method, availability, levelSet), func(t *testing.T) {
					c, sent := knownClearAuditFixture(t, method, 1)
					seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(availability))
					seedDestinationCongestionInScope(c.destinations, 7, 1, 100, 0, DestinationNetworkState{Congestion: CongestionState{LevelSet: levelSet}})
					if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100)); err != nil {
						t.Fatal(err)
					}
					wantKind := uint8(messages.MsgTypeDestinationAvailable)
					if availability == DestinationRestricted {
						wantKind = messages.MsgTypeDestinationRestricted
					}
					if len(*sent) != 2 || (*sent)[0].MessageType() != messages.MsgTypeSignallingCongestion || (*sent)[1].MessageType() != wantKind {
						t.Fatalf("known-clear reply order=%v, want SCON(0) then availability", messageKinds(*sent))
					}
					scon := (*sent)[0].(*messages.SignallingCongestion)
					if scon.CongestionIndications == nil || scon.CongestionIndications.CongestionLevel() != 0 {
						t.Fatalf("known-clear SCON=%+v", scon)
					}
				})
			}
		}
	}
}

func messageKinds(ms []messages.M3UA) []uint8 {
	var kinds []uint8
	for _, m := range ms {
		kinds = append(kinds, m.MessageType())
	}
	return kinds
}

func TestNationalDAUDKnownClearClearsStaleASPOnlyThroughSCON(t *testing.T) {
	c, sent := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
	seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 100, 0, DestinationNetworkState{})
	receiver, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
	setInventoryNetworkAppearance(&receiver.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	receiver.noteRoutingContextsAcked(params.NewRoutingContext(1))
	seedDestinationCongestionInScope(receiver.destinations, 7, 1, 100, 0, DestinationNetworkState{Congestion: congestionStateFor(2, true)})
	if err := receiver.handleDestinationAvailable(messages.NewDestinationAvailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil)); err != nil {
		t.Fatal(err)
	}
	state, _ := receiver.destinations.lookupRange(destinationScopeKey(7, 1), 100, 0)
	if !state.Congestion.Congested || state.Congestion.Level != 2 {
		t.Fatalf("bare DAVA cleared stale congestion: %+v", state)
	}
	if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100)); err != nil {
		t.Fatal(err)
	}
	for _, reply := range *sent {
		var err error
		switch m := reply.(type) {
		case *messages.SignallingCongestion:
			err = receiver.handleSignallingCongestion(m)
		case *messages.DestinationAvailable:
			err = receiver.handleDestinationAvailable(m)
		default:
			t.Fatalf("reply=%T", reply)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	state, _ = receiver.destinations.lookupRange(destinationScopeKey(7, 1), 100, 0)
	if state.Congestion.Congested || !state.Congestion.LevelSet || state.Congestion.Level != 0 {
		t.Fatalf("audit retained stale level 2: %+v", state)
	}
}

func TestDAUDUnknownCongestionOmitsSCON(t *testing.T) {
	for _, availability := range []DestinationAvailability{DestinationAvailable, DestinationRestricted} {
		t.Run(availability.String(), func(t *testing.T) {
			c, sent := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
			seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(availability))
			if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100)); err != nil {
				t.Fatal(err)
			}
			if len(*sent) != 1 {
				t.Fatalf("absent congestion reply=%v", messageKinds(*sent))
			}
			want := uint8(messages.MsgTypeDestinationAvailable)
			if availability == DestinationRestricted {
				want = messages.MsgTypeDestinationRestricted
			}
			if (*sent)[0].MessageType() != want {
				t.Fatalf("reply=%T", (*sent)[0])
			}
		})
	}
}

func TestDAUDKeepsClearAndAbsentExceptionsAcrossRCGroups(t *testing.T) {
	for _, clearFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "clear-first", false: "absent-first"}[clearFirst], func(t *testing.T) {
			c, _ := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1, 2, 3)
			for rc := uint32(1); rc <= 3; rc++ {
				seedDestinationRangeInScope(c.destinations, 7, rc, 100, 1, availabilityState(DestinationAvailable))
			}
			clearPC := uint32(100)
			if !clearFirst {
				clearPC = 101
			}
			seedDestinationCongestionInScope(c.destinations, 7, 1, clearPC, 0, DestinationNetworkState{})
			seedDestinationCongestionInScope(c.destinations, 7, 3, clearPC, 0, DestinationNetworkState{})
			// The state bytes match exactly; only authoritative record presence differs.
			a, _ := c.destinations.lookupRange(destinationScopeKey(7, 1), clearPC, 0)
			b, _ := c.destinations.lookupRange(destinationScopeKey(7, 2), clearPC, 0)
			adjacent, _ := c.destinations.lookupRange(destinationScopeKey(7, 1), clearPC^1, 0)
			if a != b || a != adjacent {
				t.Fatal("fixture must differ only in knowledge presence")
			}
			d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2, 3), params.NewAffectedPointCodeWithMask(1, 100), nil)
			replies, err := c.destinationAuditResponse(d, []uint32{100}, []uint8{1})
			if err != nil {
				t.Fatal(err)
			}
			counts := [3][2]int{}
			for _, m := range replies {
				if _, ok := m.(*messages.SignallingCongestion); !ok {
					continue
				}
				if m.(*messages.SignallingCongestion).CongestionIndications == nil {
					t.Fatal("clear omitted level")
				}
				for _, rc := range routingContextOf(m).RoutingContexts() {
					apc := affectedPointCodeParamOf(m)
					for i, pc := range apc.AffectedPointCodes() {
						for p := uint32(100); p <= 101; p++ {
							if destinationRangeCovers(destinationRange{PointCode: pc, Mask: apc.AffectedPointCodeMasks()[i]}, p, 0) {
								counts[rc-1][p-100]++
							}
						}
					}
				}
			}
			want := [3][2]int{}
			want[0][clearPC-100], want[2][clearPC-100] = 1, 1
			if counts != want {
				t.Fatalf("SCON coverage=%v want=%v; absent/clear merged", counts, want)
			}
		})
	}
}

func TestInternationalDAUDDoesNotInventNumericClearOrRefusal(t *testing.T) {
	c, sent := knownClearAuditFixture(t, CongestionInternational, 1)
	for _, pc := range []uint32{100, 101} {
		seedDestinationRangeInScope(c.destinations, 7, 1, pc, 0, availabilityState(DestinationAvailable))
		seedDestinationCongestionInScope(c.destinations, 7, 1, pc, 0, DestinationNetworkState{Congestion: CongestionState{LevelSet: pc == 100}})
	}
	management := c.ManagementIndications()
	if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100, 101)); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(messageKinds(*sent), []uint8{messages.MsgTypeDestinationAvailable}) {
		t.Fatalf("international clear=%v", messageKinds(*sent))
	}
	select {
	case m := <-management:
		t.Fatalf("known clear was treated as refusal: %+v", m)
	default:
	}
}

func TestDAUDKnownClearBudgetIncludesSCONAndIsAtomic(t *testing.T) {
	c, _ := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
	seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 100, 0, DestinationNetworkState{})
	d := auditKnowledgeRequest(100)
	entries, err := c.destinationAuditReplies(d, []uint32{100}, []uint8{0})
	if err != nil {
		t.Fatal(err)
	}
	// Explicit wire sizes pin the extra clear independently of the implementation.
	scon := messages.NewSignallingCongestion(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, nil, params.NewCongestionIndications(0), nil)
	dava := messages.NewDestinationAvailable(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, nil)
	bytes := uint64(scon.MarshalLen() + dava.MarshalLen())
	for _, tc := range []struct {
		name   string
		budget auditReplyBudget
		ok     bool
	}{{"inclusive", auditReplyBudget{2, bytes}, true}, {"message", auditReplyBudget{1, bytes}, false}, {"byte", auditReplyBudget{2, bytes - 1}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			replies, err := buildDestinationAuditMessages(d, entries, 1, tc.budget, CongestionNationalWithoutPriorities)
			if tc.ok {
				if err != nil || len(replies) != 2 {
					t.Fatalf("clear budget=%v %v", messageKinds(replies), err)
				}
			} else if !errors.Is(err, ErrSSNMResourceLoss) || replies != nil {
				t.Fatalf("budget sent prefix: %v %v", messageKinds(replies), err)
			}
		})
	}
}

func TestDAUDUnavailableAndRestartIsolationReturnOnlyDUNA(t *testing.T) {
	e, c, sent := restartRetentionFixture(t, 16)
	if err := reportAvailability(e, testWireScope(7, true, 1), 100, 0, DestinationUnavailable); err != nil {
		t.Fatal(err)
	}
	if _, err := e.BeginMTP3Restart(ownerDestination(7, 1, 101, 0)); err != nil {
		t.Fatal(err)
	}
	seedDestinationRangeInScope(c.destinations, 7, 1, 101, 0, availabilityState(DestinationAvailable))
	for _, pc := range []uint32{100, 101} {
		seedDestinationCongestionInScope(c.destinations, 7, 1, pc, 0, DestinationNetworkState{})
	}
	sent.reset()
	if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100, 101, 102)); err != nil {
		t.Fatal(err)
	}
	replies := ssnmMessages(sent.snapshot())
	if !slices.Equal(messageKinds(replies), []uint8{messages.MsgTypeDestinationUnavailable}) || !slices.Equal(affectedPointCodeParamOf(replies[0]).AffectedPointCodes(), []uint32{100, 101, 102}) {
		t.Fatalf("unavailable/isolated/unknown=%v", replies)
	}
}

func TestDAUDDoesNotUsePeerOrTransportCongestionKnowledge(t *testing.T) {
	for _, level := range []uint8{0, 2} {
		c, sent := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
		seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(DestinationAvailable))
		if err := c.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(level), nil)); err != nil {
			t.Fatal(err)
		}
		if report := nextSSNMReport(t, c); !report.PeerReported {
			t.Fatalf("not a peer report: %+v", report)
		}
		c.transportSnapshot.Store(&TransportCongestionState{Known: true, Level: level, Epoch: c.Epoch()})
		*sent = nil
		if err := c.handleDestinationStateAudit(auditKnowledgeRequest(100)); err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(messageKinds(*sent), []uint8{messages.MsgTypeDestinationAvailable}) {
			t.Fatalf("peer/local state answered DAUD: %v", messageKinds(*sent))
		}
	}
}

func TestDAUDClearKnowledgeKeepsIndependentChronologyAndAppearance(t *testing.T) {
	c, _ := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0x123456, 0, DestinationNetworkState{Congestion: congestionStateFor(2, true)})
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0x123400, 8, DestinationNetworkState{})
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationRestricted))
	// A foreign appearance's newer positive level cannot replace known clear.
	seedDestinationCongestionInScope(c.destinations, 8, 1, 0x123400, 8, DestinationNetworkState{Congestion: congestionStateFor(3, true)})
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)
	replies, err := c.destinationAuditResponse(d, []uint32{0x123400}, []uint8{8})
	if err != nil {
		t.Fatal(err)
	}
	counts := [256]int{}
	availabilityCounts := [256]int{}
	for _, m := range replies {
		apc := affectedPointCodeParamOf(m)
		for i, pc := range apc.AffectedPointCodes() {
			for p := pc; p < pc+uint32(1)<<apc.AffectedPointCodeMasks()[i]; p++ {
				if p < 0x123400 || p >= 0x123500 {
					t.Fatal("reply escaped query")
				}
				if scon, ok := m.(*messages.SignallingCongestion); ok {
					if scon.CongestionIndications == nil || scon.CongestionIndications.CongestionLevel() != 0 {
						t.Fatalf("chronology lost known clear: %+v", scon)
					}
					counts[p-0x123400]++
				} else {
					want := uint8(messages.MsgTypeDestinationAvailable)
					if p == 0x123456 {
						want = messages.MsgTypeDestinationRestricted
					}
					if m.MessageType() != want {
						t.Fatalf("availability chronology at %#x: %T", p, m)
					}
					availabilityCounts[p-0x123400]++
				}
			}
		}
	}
	for i, n := range counts {
		if n != 1 || availabilityCounts[i] != 1 {
			t.Fatalf("point %d congestion/availability coverage=%d/%d", i, n, availabilityCounts[i])
		}
	}
}

func FuzzDestinationAuditCongestionKnowledge(f *testing.F) {
	f.Add(uint8(0), uint8(0), uint8(0), false)
	f.Add(uint8(15), uint8(2), uint8(3), true)
	f.Fuzz(func(t *testing.T, offset, level, availability uint8, lateClear bool) {
		c, _ := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1)
		base := uint32(0)
		if offset&16 != 0 {
			base = 0xfffff0
		}
		state := availabilityState(DestinationAvailability(availability % 3))
		seedDestinationRangeInScope(c.destinations, 7, 1, base, 4, state)
		var want [16]DestinationNetworkState
		var known [16]bool
		for i := range want {
			want[i] = state
		}
		seedDestinationCongestionInScope(c.destinations, 7, 1, base, 3, DestinationNetworkState{})
		for i := 0; i < 8; i++ {
			known[i] = true
		}
		pc := base + uint32(offset%16)
		congestion := congestionStateFor(level%4, true)
		seedDestinationCongestionInScope(c.destinations, 7, 1, pc, 0, DestinationNetworkState{Congestion: congestion})
		known[pc-base], want[pc-base].Congestion = true, congestion
		if lateClear {
			seedDestinationCongestionInScope(c.destinations, 7, 1, base, 2, DestinationNetworkState{})
			for i := 0; i < 4; i++ {
				known[i] = true
				want[i].Congestion = CongestionState{}
			}
		}
		d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(4, base), nil)
		replies, err := c.destinationAuditReplies(d, []uint32{base}, []uint8{4})
		if err != nil {
			t.Fatal(err)
		}
		var counts [16]int
		for _, r := range replies {
			if destinationRangePrefix(r.pointCode, r.mask) != r.pointCode {
				t.Fatal("unaligned reply")
			}
			for p := r.pointCode; p < r.pointCode+uint32(1)<<r.mask; p++ {
				if p < base || p >= base+16 {
					t.Fatal("reply escaped query")
				}
				i := p - base
				counts[i]++
				if r.state != want[i] || r.congestionKnown != known[i] {
					t.Fatalf("%#x reply=%+v want=%+v known=%v", p, r, want[i], known[i])
				}
			}
		}
		for _, n := range counts {
			if n != 1 {
				t.Fatalf("coverage=%d", n)
			}
		}
	})
}

func TestDAUDCompletePlanKeepsZeroStateKnowledgeAcrossContexts(t *testing.T) {
	c, _ := knownClearAuditFixture(t, CongestionNationalWithoutPriorities, 1, 2, 3)
	for rc := uint32(1); rc <= 3; rc++ {
		seedDestinationRangeInScope(c.destinations, 7, rc, 100, 0, availabilityState(DestinationAvailable))
		if rc != 2 {
			seedDestinationCongestionInScope(c.destinations, 7, rc, 100, 0, DestinationNetworkState{})
		}
	}
	// Every plan has the same range, availability and congestion value bytes.
	// Only the presence bit may distinguish the complete-plan grouping keys.
	for rc := uint32(1); rc <= 3; rc++ {
		a, _ := c.destinations.lookupRange(destinationScopeKey(7, rc), 100, 0)
		if a != availabilityState(DestinationAvailable) {
			t.Fatalf("state bytes differ: %+v", a)
		}
	}
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2, 3), params.NewAffectedPointCode(100), nil)
	replies, err := c.destinationAuditResponse(d, []uint32{100}, []uint8{0})
	if err != nil {
		t.Fatal(err)
	}
	var sconContexts []uint32
	for _, m := range replies {
		if _, ok := m.(*messages.SignallingCongestion); ok {
			sconContexts = append(sconContexts, routingContextOf(m).RoutingContexts()...)
		}
	}
	if !slices.Equal(sconContexts, []uint32{1, 3}) {
		t.Fatalf("complete plans merged known clear with absence: SCON RCs=%v", sconContexts)
	}
}

func TestDAUDKnownClearResolvesMethodWhenAppearanceIsOmitted(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionNationalWithoutPriorities, CongestionInternational} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			c, sent := knownClearAuditFixture(t, method, 1)
			seedDestinationRangeInScope(c.destinations, 7, 1, 100, 0, availabilityState(DestinationAvailable))
			seedDestinationCongestionInScope(c.destinations, 7, 1, 100, 0, DestinationNetworkState{})
			management := c.ManagementIndications()
			d := messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil)
			if err := c.handleDestinationStateAudit(d); err != nil {
				t.Fatal(err)
			}
			want := []uint8{messages.MsgTypeSignallingCongestion, messages.MsgTypeDestinationAvailable}
			if method == CongestionInternational {
				want = want[1:]
			}
			if !slices.Equal(messageKinds(*sent), want) {
				t.Fatalf("resolved method replies=%v want=%v", messageKinds(*sent), want)
			}
			select {
			case m := <-management:
				t.Fatalf("clear generated refusal: %+v", m)
			default:
			}
		})
	}
}

func TestDAUDKnownClearSurvivesAPCAndRoutingContextChunks(t *testing.T) {
	contexts := make([]uint32, auditRoutingContextChunk+1)
	for i := range contexts {
		contexts[i] = uint32(i + 1)
	}
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(contexts...), params.NewAffectedPointCode(1, 2, 3), nil)
	var entries []destinationAuditReply
	for pc := uint32(1); pc <= 3; pc++ {
		entries = append(entries, destinationAuditReply{destinationAuditGroup: destinationAuditGroup{state: availabilityState(DestinationAvailable), congestionKnown: true, routingContexts: contexts}, pointCode: pc})
	}
	replies, err := buildDestinationAuditMessages(d, entries, 2, auditReplyBudget{100, 1 << 24}, CongestionNationalWithoutPriorities)
	if err != nil {
		t.Fatal(err)
	}
	counts := make([][3][2]int, len(contexts))
	for _, m := range replies {
		if m.MarshalLen() > auditMaxMessage {
			t.Fatalf("oversized message %d", m.MarshalLen())
		}
		dimension := 1
		if scon, ok := m.(*messages.SignallingCongestion); ok {
			dimension = 0
			if scon.CongestionIndications == nil || scon.CongestionIndications.CongestionLevel() != 0 {
				t.Fatal("chunk lost known clear")
			}
		} else if m.MessageType() != messages.MsgTypeDestinationAvailable {
			t.Fatalf("reply=%T", m)
		}
		apc := affectedPointCodeParamOf(m)
		if len(apc.AffectedPointCodes()) > 2 {
			t.Fatal("APC limit ignored")
		}
		for _, rc := range routingContextOf(m).RoutingContexts() {
			for _, pc := range apc.AffectedPointCodes() {
				if rc == 0 || rc > uint32(len(contexts)) || pc < 1 || pc > 3 {
					t.Fatal("chunk escaped scope")
				}
				if dimension == 1 && counts[rc-1][pc-1][0] != 1 {
					t.Fatal("availability chunk preceded clear")
				}
				counts[rc-1][pc-1][dimension]++
			}
		}
	}
	for _, points := range counts {
		for _, dimensions := range points {
			if dimensions != [2]int{1, 1} {
				t.Fatalf("incomplete or duplicate chunk coverage=%v", dimensions)
			}
		}
	}
}
