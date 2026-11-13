package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDAUDRangePreservesNarrowExceptions(t *testing.T) {
	for _, broad := range []DestinationAvailability{DestinationAvailable, DestinationUnavailable, DestinationRestricted} {
		t.Run(broad.String(), func(t *testing.T) {
			c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
			setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			narrow := DestinationUnavailable
			if broad == narrow {
				narrow = DestinationAvailable
			}
			seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(broad))
			seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(narrow))
			if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
				t.Fatal(err)
			}
			assertAuditRangeReplies(t, *sent, broad, narrow)
		})
	}
}

func assertAuditRangeReplies(t *testing.T, replies []messages.M3UA, broad, narrow DestinationAvailability) {
	t.Helper()
	// Each point must be covered exactly once. This also proves that a receiver
	// applying these availability messages in reverse order reaches the same view.
	for pc := uint32(0x123400); pc < 0x123500; pc++ {
		count := 0
		want := broad
		if pc == 0x123456 {
			want = narrow
		}
		for _, reply := range replies {
			var state DestinationAvailability
			switch reply.(type) {
			case *messages.DestinationAvailable:
				state = DestinationAvailable
			case *messages.DestinationUnavailable:
				state = DestinationUnavailable
			case *messages.DestinationRestricted:
				state = DestinationRestricted
			default:
				t.Fatalf("unexpected reply %T", reply)
			}
			apc := affectedPointCodeParamOf(reply)
			for i, p := range apc.AffectedPointCodes() {
				mask := apc.AffectedPointCodeMasks()[i]
				if destinationRangePrefix(p, mask) != p {
					t.Fatalf("unaligned %#x/%d", p, mask)
				}
				if destinationRangeCovers(destinationRange{PointCode: p, Mask: mask}, pc, 0) {
					count++
					if state != want {
						t.Fatalf("%T %#x/%d covers %#x with %v, want %v", reply, p, mask, pc, state, want)
					}
				}
			}
		}
		if count != 1 {
			t.Fatalf("%#x covered %d times, want once", pc, count)
		}
	}
	assertAuditReplyApplication(t, replies, broad, narrow)
}

func assertAuditReplyApplication(t *testing.T, replies []messages.M3UA, broad, narrow DestinationAvailability) {
	t.Helper()
	for _, reverse := range []bool{false, true} {
		receiver, _ := newObservedSSNMConn(t, StateASPActive, RoleASP, 1)
		receiver.noteRoutingContextsAcked(params.NewRoutingContext(1))
		for i := range replies {
			j := i
			if reverse {
				j = len(replies) - 1 - i
			}
			var err error
			switch m := replies[j].(type) {
			case *messages.DestinationAvailable:
				err = receiver.handleDestinationAvailable(m)
			case *messages.DestinationUnavailable:
				err = receiver.handleDestinationUnavailable(m)
			case *messages.DestinationRestricted:
				err = receiver.handleDestinationRestricted(m)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		for pc := uint32(0x123400); pc < 0x123500; pc++ {
			want := broad
			if pc == 0x123456 {
				want = narrow
			}
			got, known := receiver.destinations.lookupRange(destinationScopeKey(7, 1), pc, 0)
			if !known || got.Availability != want {
				t.Fatalf("reverse=%v %#x state=%+v known=%v want=%v", reverse, pc, got, known, want)
			}
		}
	}
}

func TestDAUDOverlappingRequestsAndCongestionExceptions(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	seedDestinationCongestionInScope(c.destinations, 7, 1, 0x123456, 0, DestinationNetworkState{Congestion: congestionStateFor(2, true)})
	apc := params.NewAffectedPointCodeWithMask(8, 0x123400)
	apc.Data = append(apc.Data, params.NewAffectedPointCode(0x123456).Data...)
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), apc, nil)); err != nil {
		t.Fatal(err)
	}
	var availability []messages.M3UA
	scon := 0
	for i, m := range *sent {
		if congestion, ok := m.(*messages.SignallingCongestion); ok {
			scon++
			if i+1 == len(*sent) {
				t.Fatal("SCON without availability")
			}
			if _, ok := (*sent)[i+1].(*messages.DestinationAvailable); !ok {
				t.Fatal("SCON must precede DAVA")
			}
			if congestion.CongestionIndications.CongestionLevel() != 2 {
				t.Fatal("lost level")
			}
			if got := congestion.AffectedPointCode.AffectedPointCodes(); len(got) != 1 || got[0] != 0x123456 {
				t.Fatalf("SCON destinations %v", got)
			}
		} else {
			availability = append(availability, m)
		}
	}
	if scon != 1 {
		t.Fatalf("SCON count=%d", scon)
	}
	assertAuditRangeReplies(t, availability, DestinationAvailable, DestinationAvailable)
}

func FuzzDestinationAuditPartition(f *testing.F) {
	f.Add(uint8(86), uint8(0), uint8(1))
	f.Add(uint8(0), uint8(1), uint8(2))
	f.Fuzz(func(t *testing.T, offset, broadValue, narrowValue uint8) {
		c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
		setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
		broad, narrow := DestinationAvailability(broadValue%3), DestinationAvailability(narrowValue%3)
		seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(broad))
		seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400+uint32(offset), 0, availabilityState(narrow))
		audit := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)
		replies, err := c.destinationAuditReplies(audit, []uint32{0x123400}, []uint8{8})
		if err != nil {
			t.Fatal(err)
		}
		counts := make([]int, 256)
		for _, reply := range replies {
			if destinationRangePrefix(reply.pointCode, reply.mask) != reply.pointCode {
				t.Fatal("unaligned reply")
			}
			for pc := reply.pointCode; pc < reply.pointCode+uint32(1)<<reply.mask; pc++ {
				if pc < 0x123400 || pc >= 0x123500 {
					t.Fatal("out of range")
				}
				counts[pc-0x123400]++
				want := broad
				if pc == 0x123400+uint32(offset) {
					want = narrow
				}
				if reply.state.Availability != want {
					t.Fatalf("%#x state=%v want=%v", pc, reply.state.Availability, want)
				}
			}
		}
		for _, count := range counts {
			if count != 1 {
				t.Fatalf("coverage=%d", count)
			}
		}
	})
}

func TestDAUDRangeReplyApplicationIsOrderIndependent(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationUnavailable))
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	assertAuditReplyApplication(t, *sent, DestinationAvailable, DestinationUnavailable)
}

func TestDAUDUnknownRangeKeepsKnownAvailableException(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationAvailable))
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	assertAuditRangeReplies(t, *sent, DestinationUnavailable, DestinationAvailable)
}

func TestDAUDRangePreservesRestartIsolationAfterForgettingRecords(t *testing.T) {
	listener, server, association, sent := restartFixture(t, 1)
	t.Cleanup(func() { _ = listener.endpoint.Close() })
	restartActivateASP(server, association, 1)
	if _, err := listener.endpoint.BeginMTP3Restart(restartDestination(1, 0x123456, 0)); err != nil {
		t.Fatal(err)
	}
	if released := association.ForgetDestinations(); released == 0 {
		t.Fatal("restart isolation record was not retained")
	}
	if err := reportAvailability(listener.endpoint, testWireScope(7, true, 1), 0x123400, 8, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	if err := association.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	assertAuditRangeReplies(t, ssnmMessages(sent.snapshot()), DestinationAvailable, DestinationUnavailable)
}

func TestDAUDRangePreservesCompletedRestartSubrangeAfterForgetting(t *testing.T) {
	listener, server, association, sent := restartFixture(t, 1)
	t.Cleanup(func() { _ = listener.endpoint.Close() })
	restartActivateASP(server, association, 1)
	restart, err := listener.endpoint.BeginMTP3Restart(restartDestination(1, 0x123400, 8))
	if err != nil {
		t.Fatal(err)
	}
	for _, pc := range []uint32{0x123456, 0x123457} {
		if err := restart.Update(restartDestination(1, pc, 0), availabilityState(DestinationAvailable)); err != nil {
			t.Fatal(err)
		}
	}
	association.signalWriter = func(m messages.M3UA) (int, error) {
		if _, available := m.(*messages.DestinationAvailable); available && affectedPointCodeParamOf(m).AffectedPointCodes()[0] == 0x123457 {
			return 0, errors.New("recovery refused")
		}
		return sent.write(m)
	}
	if err := restart.Complete(); err == nil {
		t.Fatal("partial completion reported success")
	}
	if completed := restart.Completed(); len(completed) != 1 || completed[0].PointCode != 0x123456 {
		t.Fatalf("completed=%v", completed)
	}
	association.signalWriter = sent.write
	association.ForgetDestinations()
	// This covering /9 remains outside the staged /8 restart scope. Only the
	// completed point has left isolation within the audited /8.
	if err := reportAvailability(listener.endpoint, testWireScope(7, true, 1), 0x123400, 9, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	if err := association.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	assertAuditRangeReplies(t, ssnmMessages(sent.snapshot()), DestinationUnavailable, DestinationAvailable)
}

func TestDAUDContextlessRangePreservesSharedRestartIsolation(t *testing.T) {
	endpoint, _, _ := restartRetentionFixture(t, 16)
	listener := addOwnedListener(t, endpoint, 7)
	association, sent := addActiveASP(t, listener, 7)
	listener.as.get(contextlessASKeyForConfig(association.cfg)).setASPState(association, StateASPActive, time.Hour)
	if _, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0)); err != nil {
		t.Fatal(err)
	}
	association.ForgetDestinations()
	if err := reportAvailability(endpoint, testWireScope(7, true), 0x123400, 8, DestinationAvailable); err != nil {
		t.Fatal(err)
	}
	sent.reset()
	if err := association.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), nil, params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	assertAuditRangeReplies(t, ssnmMessages(sent.snapshot()), DestinationAvailable, DestinationUnavailable)
}

func TestDAUDUniformRangeKeepsCompactReplyAfterBroaderUpdate(t *testing.T) {
	c, sent := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123456, 0, availabilityState(DestinationUnavailable))
	seedDestinationRangeInScope(c.destinations, 7, 1, 0x123400, 8, availabilityState(DestinationAvailable))
	if err := c.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(8, 0x123400), nil)); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 {
		t.Fatalf("uniform range fragmented into %d replies", len(*sent))
	}
	if _, ok := (*sent)[0].(*messages.DestinationAvailable); !ok {
		t.Fatalf("reply=%T", (*sent)[0])
	}
	apc := affectedPointCodeParamOf((*sent)[0])
	if apc.AffectedPointCodes()[0] != 0x123400 || apc.AffectedPointCodeMasks()[0] != 8 {
		t.Fatalf("range=%v masks=%v", apc.AffectedPointCodes(), apc.AffectedPointCodeMasks())
	}
}
