package m3ua

import (
	"reflect"
	"sort"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDAUDSparseFullSpaceGetsCompleteGroupedAnswer(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	for i := uint32(0); i < 256; i++ {
		seedDestinationRangeInScope(c.destinations, 7, 1, i<<16, 0, availabilityState(DestinationAvailable))
	}
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(24, 0), nil)); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 5 {
		t.Fatalf("sparse reply messages=%d want5", len(*sent))
	}
	type piece struct {
		start, end uint32
		available  bool
	}
	var pieces []piece
	for _, m := range *sent {
		apc := affectedPointCodeParamOf(m)
		pcs, masks := apc.AffectedPointCodes(), apc.AffectedPointCodeMasks()
		if len(pcs) > DefaultMaxAffectedPointCodesPerSSNM {
			t.Fatal("oversized APC list")
		}
		_, available := m.(*messages.DestinationAvailable)
		if !available {
			if _, ok := m.(*messages.DestinationUnavailable); !ok {
				t.Fatalf("unexpected %T", m)
			}
		}
		for i, pc := range pcs {
			end := pc + uint32(1)<<masks[i]
			pieces = append(pieces, piece{pc, end, available})
			if available {
				if masks[i] != 0 || pc&65535 != 0 {
					t.Fatalf("false availability %#x/%d", pc, masks[i])
				}
			} else {
				for known := uint32(0); known < 256; known++ {
					if pc <= known<<16 && known<<16 < end {
						t.Fatalf("DUNA covers known %#x", known<<16)
					}
				}
			}
		}
	}
	if len(pieces) != 4352 {
		t.Fatalf("entries=%d want4352", len(pieces))
	}
	sort.Slice(pieces, func(i, j int) bool { return pieces[i].start < pieces[j].start })
	next := uint32(0)
	for _, p := range pieces {
		if p.start != next {
			t.Fatal("gap or overlap")
		}
		next = p.end
	}
	if next != 1<<24 {
		t.Fatal("incomplete full-space answer")
	}
}

func TestDAUDGroupsAllPermittedListsAndSplitsAtConfiguredLimit(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, SSNMState: &SSNMStateConfig{MaxAffectedPointCodes: 2}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	if !endpoint.trackAssociation(c) {
		t.Fatal("attach")
	}
	for pc := uint32(0); pc < 8; pc++ {
		state := DestinationAvailable
		if pc%2 == 1 {
			state = DestinationRestricted
		}
		seedDestinationRangeInScope(c.destinations, 7, 1, pc, 0, availabilityState(state))
		seedDestinationCongestionInScope(c.destinations, 7, 1, pc, 0, DestinationNetworkState{Congestion: congestionStateFor(2, true)})
	}
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(3, 0), nil)); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 8 {
		t.Fatalf("messages=%d want8", len(*sent))
	}
	counts := map[string]int{}
	sconAt := make(map[uint32]int)
	for i, m := range *sent {
		pcs := affectedPointCodeParamOf(m).AffectedPointCodes()
		if len(pcs) != 2 {
			t.Fatalf("list length=%d", len(pcs))
		}
		if !reflect.DeepEqual(routingContextOf(m).RoutingContexts(), []uint32{1}) {
			t.Fatal("lost RC")
		}
		if scon, ok := m.(*messages.SignallingCongestion); ok {
			if i >= 4 || scon.CongestionIndications.CongestionLevel() != 2 {
				t.Fatal("SCON order/level")
			}
			for _, pc := range pcs {
				sconAt[pc] = i
			}
		} else {
			counts[m.MessageTypeName()]++
			for _, pc := range pcs {
				index, found := sconAt[pc]
				if !found || index >= i {
					t.Fatal("availability preceded its SCON")
				}
			}
		}
	}
	if counts["Destination Available"] != 2 || counts["Destination Restricted"] != 2 {
		t.Fatalf("counts=%v", counts)
	}
}

func TestDAUDReplyBudgetRefusesWithScopedErrorAndManagement(t *testing.T) {
	endpoint, c, sent := restartRetentionFixture(t, 1)
	for i := uint32(0); i < 256; i++ {
		c.ForgetDestinations()
		if _, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, i<<16, 0)); err != nil {
			t.Fatal(err)
		}
	}
	c.ForgetDestinations()
	if err := reportAvailability(endpoint, testWireScope(7, true, 1), 0, 24, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	localIndications := c.ManagementIndications()
	audit := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(24, 0), nil)
	if err := c.handleDestinationStateAudit(audit); err != nil {
		t.Fatal(err)
	}
	replies := ssnmMessages(sent.snapshot())
	if len(replies) != 0 {
		t.Fatalf("partial status replies=%d", len(replies))
	}
	all := sent.snapshot()
	if len(all) != 1 {
		t.Fatalf("writes=%d want oneERR", len(all))
	}
	refusal, ok := all[0].(*messages.Error)
	if !ok || refusal.ErrorCode.ErrorCode() != params.ErrDestinationStatusUnknown {
		t.Fatalf("reply=%v", all)
	}
	if !reflect.DeepEqual(refusal.NetworkAppearance, audit.NetworkAppearance) || !reflect.DeepEqual(refusal.RoutingContext, audit.RoutingContext) || !reflect.DeepEqual(refusal.AffectedPointCode, audit.AffectedPointCode) {
		t.Fatal("ERR lost requested scope")
	}
	select {
	case indication := <-localIndications:
		if indication.ErrorCode != params.ErrDestinationStatusUnknown {
			t.Fatalf("indication=%+v", indication)
		}
	default:
		t.Fatal("missing local management refusal")
	}
	receiver, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	receiver.noteRoutingContextsAcked(params.NewRoutingContext(1))
	peerIndications := receiver.ManagementIndications()
	if err := receiver.handleError(refusal); err != nil {
		t.Fatal(err)
	}
	if receiver.State() != StateASPActive || !receiver.routingContextAcked(1) {
		t.Fatal("ERR changed authorization")
	}
	select {
	case indication := <-peerIndications:
		if indication.ErrorCode != params.ErrDestinationStatusUnknown || len(indication.AffectedDestinations) != 1 {
			t.Fatalf("ASP indication=%+v", indication)
		}
	default:
		t.Fatal("ASP lost management error")
	}
}

func TestDAUDMessageAndByteBudgetsAreInclusiveAndAtomic(t *testing.T) {
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(1, 2), nil)
	entries := []destinationAuditReply{
		{destinationAuditGroup: destinationAuditGroup{state: availabilityState(DestinationAvailable), routingContexts: []uint32{1}}, pointCode: 1},
		{destinationAuditGroup: destinationAuditGroup{state: availabilityState(DestinationUnavailable), routingContexts: []uint32{1}}, pointCode: 2},
	}
	replies, err := buildDestinationAuditMessages(d, entries, 2, auditReplyBudget{100, 10000})
	if err != nil {
		t.Fatal(err)
	}
	var bytes uint64
	for _, reply := range replies {
		bytes += uint64(reply.MarshalLen())
	}
	for _, tc := range []struct {
		name     string
		budget   auditReplyBudget
		accepted bool
	}{
		{"at both bounds", auditReplyBudget{uint64(len(replies)), bytes}, true},
		{"one message over", auditReplyBudget{uint64(len(replies) - 1), bytes}, false},
		{"one byte over", auditReplyBudget{uint64(len(replies)), bytes - 1}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := buildDestinationAuditMessages(d, entries, 2, tc.budget)
			if tc.accepted {
				if err != nil || len(got) != len(replies) {
					t.Fatalf("inclusive bound: %d %v", len(got), err)
				}
			} else if err != ErrSSNMResourceLoss || got != nil {
				t.Fatalf("partial/accepted over-budget plan: %d %v", len(got), err)
			}
		})
	}
}

func TestDAUDSweepKeepsNewestRecordPerDimension(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	// Older exact availability must lose to newer broad availability, while
	// congestion has its own chronology and must retain its newer exact level.
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationUnavailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0x123400, 8, DestinationNetworkState{Congestion: congestionStateFor(1, true)})
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0x123456, 0, DestinationNetworkState{Congestion: congestionStateFor(3, true)})
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)
	replies, err := c.destinationAuditReplies(d, []uint32{0x123400}, []uint8{8})
	if err != nil {
		t.Fatal(err)
	}
	for pc := uint32(0x123400); pc < 0x123500; pc++ {
		want, known := c.destinations.lookupRange(destinationScopeKey(7, 1), pc, 0)
		if !known {
			t.Fatal("missing oracle record")
		}
		count := 0
		for _, reply := range replies {
			if destinationRangeCovers(destinationRange{PointCode: reply.pointCode, Mask: reply.mask}, pc, 0) {
				count++
				if reply.state != want {
					t.Fatalf("%#x state=%+v want=%+v", pc, reply.state, want)
				}
			}
		}
		if count != 1 {
			t.Fatalf("%#x coverage=%d", pc, count)
		}
	}
}

func TestDAUDSweepPreservesExplicitRequestBoundaries(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(0, 1), nil)
	replies, err := c.destinationAuditReplies(d, []uint32{0, 1}, []uint8{0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if len(replies) != 2 || replies[0].pointCode != 0 || replies[1].pointCode != 1 || replies[0].mask != 0 || replies[1].mask != 0 {
		t.Fatalf("explicit request boundaries merged: %+v", replies)
	}
}

func TestDAUDRefusalIncludesInferredScope(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	d := messages.NewDestinationStateAudit(nil, nil, params.NewAffectedPointCodeWithMask(8, 0x123400), nil)
	if err := c.refuseDestinationAudit(d); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 {
		t.Fatalf("writes=%d", len(*sent))
	}
	refusal, ok := (*sent)[0].(*messages.Error)
	if !ok || refusal.NetworkAppearance == nil || refusal.NetworkAppearance.NetworkAppearance() != 7 || refusal.RoutingContext == nil || !reflect.DeepEqual(refusal.RoutingContext.RoutingContexts(), []uint32{1}) {
		t.Fatalf("refusal omitted associated scope: %+v", refusal)
	}
	if !reflect.DeepEqual(refusal.AffectedPointCode, d.AffectedPointCode) {
		t.Fatal("changed requested APC")
	}
}

func TestDAUDScopeSweepsKeepIndependentState(t *testing.T) {
	c, _ := denseAuditFixture(t, 0, 3)
	seedDestinationRangeInScope(c.destinations, 7, 1, 0, 24, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0, 24, DestinationNetworkState{Congestion: congestionStateFor(2, true)})
	seedDestinationRangeInScope(c.destinations, 7, 1, 0, 8, availabilityState(DestinationRestricted))
	seedDestinationRangeInScope(c.destinations, 7, 2, 0, 24, availabilityState(DestinationUnavailable))
	d := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2, 3), params.NewAffectedPointCodeWithMask(24, 0), nil)
	replies, err := c.destinationAuditReplies(d, []uint32{0}, []uint8{24})
	if err != nil {
		t.Fatal(err)
	}
	// Different record counts, dimensions and an empty scope must neither
	// inherit scratch state nor overwrite a preceding scope's retained plan.
	for rc := uint32(1); rc <= 3; rc++ {
		for _, pc := range []uint32{0, 0xff, 0x100, 0xffffff} {
			want, known := c.destinations.lookupRange(destinationScopeKey(7, rc), pc, 0)
			if !known {
				want = availabilityState(DestinationUnavailable)
			}
			count := 0
			for _, reply := range replies {
				matches := false
				for _, context := range reply.routingContexts {
					matches = matches || context == rc
				}
				if matches && destinationRangeCovers(destinationRange{PointCode: reply.pointCode, Mask: reply.mask}, pc, 0) {
					count++
					if reply.state != want {
						t.Fatalf("RC%d %#x state=%+v want=%+v", rc, pc, reply.state, want)
					}
				}
			}
			if count != 1 {
				t.Fatalf("RC%d %#x coverage=%d", rc, pc, count)
			}
		}
	}
}
