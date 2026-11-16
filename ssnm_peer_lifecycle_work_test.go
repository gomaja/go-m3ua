package m3ua

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func peerLifecycleGroupedSCON(t *testing.T, a *Association, rcs []uint32) {
	t.Helper()
	pcs := make([]uint32, 64)
	for i := range pcs {
		pcs[i] = 1000 + uint32(i)
	}
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(rcs...), params.NewAffectedPointCode(pcs...), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
}

func TestPeerMultiASInactivationBatchesStoreRetirement(t *testing.T) {
	// 32*64 fits the default 2048 records/partition limit. The sibling has
	// its own 64 records and survives the first association's withdrawal.
	rcs := make([]uint32, 32)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
	}
	listener, _, peer, _ := distributionFixtureForContexts(t, params.TrafficModeLoadshare, rcs, nil)
	sibling, _ := addDistributionASP(t, listener, StateASPInactive, 1)
	for _, a := range []*Association{peer, sibling} {
		active := rcs
		if a == sibling {
			active = []uint32{1}
		}
		if err := a.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(active...), nil)); err != nil {
			t.Fatal(err)
		}
	}
	peerLifecycleGroupedSCON(t, peer, rcs)
	peerLifecycleGroupedSCON(t, sibling, []uint32{1})
	e := listener.endpoint
	if got := len(e.SSNMKnowledge().PeerCongestion); got != 2112 {
		t.Fatalf("setup records=%d want2112", got)
	}
	transport := TransportCongestionState{Association: peer.ID(), Epoch: peer.Epoch(), Known: true, Level: 3}
	peer.transportSnapshot.Store(&transport)
	scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: rcs[:31]}
	oldAdmission := peer.capturePeerCongestionAdmission(scope)
	beforeSnapshot := e.SSNMKnowledge()
	wantSurvivors := make([]PeerCongestionState, 0, 128)
	for _, record := range beforeSnapshot.PeerCongestion {
		if record.Association == sibling.ID() || record.AS.RoutingContext == 32 {
			wantSurvivors = append(wantSurvivors, record)
		}
	}
	beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := peer.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(rcs[:31]...), nil)); err != nil {
		t.Fatal(err)
	}
	e.ssnm.mu.Lock()
	work := *w
	e.ssnm.work = nil
	for _, state := range e.ssnm.partitions {
		state.work = nil
	}
	e.ssnm.mu.Unlock()
	// 1984 retired rows, one surviving AS (64 rows), sibling (64 rows).
	snapshot := e.SSNMKnowledge()
	if len(snapshot.PeerCongestion) != 128 {
		t.Fatalf("surviving peer records=%d want128", len(snapshot.PeerCongestion))
	}
	if !reflect.DeepEqual(snapshot.PeerCongestion, wantSurvivors) {
		t.Error("withdrawal changed surviving AS/sibling records")
	}
	for _, record := range snapshot.PeerCongestion {
		if record.Association == peer.ID() && record.AS.RoutingContext != 32 {
			t.Errorf("retired AS survived: %+v", record)
		}
		if record.Association != peer.ID() && record.Association != sibling.ID() {
			t.Errorf("unexpected reporter: %+v", record)
		}
	}
	if peer.State() != StateASPActive {
		t.Error("remaining AS32 lost association activity")
	}
	if got := peer.TransportCongestion(); got != transport {
		t.Errorf("transport changed: %+v", got)
	}
	if e.ssnm.records != beforeRecords-1984 || e.ssnm.bytes != beforeBytes-1984*(256+8*32) {
		t.Error("peer retirement failed to release exact record/byte charges")
	}
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), oldAdmission); err != nil {
		t.Fatal(err)
	}
	if got := e.SSNMKnowledge(); got.Revision != snapshot.Revision || len(got.PeerCongestion) != 128 {
		t.Fatal("old admission revived retired AS")
	}
	// Four full retained-record passes plus four active-key passes are
	// deliberately generous. Per-AS repeated shrinking scans exceed this.
	t.Logf("store-locked peer rows=%d source.active rows=%d", work.peerRows, work.peerASRows)
	if work.peerRows > 4*2112 || work.peerASRows > 4*32 {
		t.Fatalf("AS event multiplied peer retirement work: %+v", work)
	}
}

func TestPeerMultiContextDeregistrationDoesNotRescanSiblingRows(t *testing.T) {
	// The requester remains INACTIVE so explicit DEREG is allowed. Its
	// dynamic scopes have no retained peer report; an unrelated active ASP
	// retains 64 real SCON records. Retirement must not scan those 63 times.
	e, peer, keys, sent := newPeerRegistrationWorkFixture(t)
	if err := peer.handleRegistrationRequest(messages.NewRegistrationRequest(keys...)); err != nil {
		t.Fatal(err)
	}
	rcs := make([]uint32, 0, 64)
	for _, message := range *sent {
		response, ok := message.(*messages.RegistrationResponse)
		if !ok {
			continue
		}
		for _, parameter := range response.RegistrationResults {
			result, err := parameter.RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if result.RegistrationStatus.RegistrationStatus() != uint32(RegistrationSuccessfullyRegistered) {
				t.Fatalf("registration refused: %+v", result)
			}
			rcs = append(rcs, result.RoutingContext.RoutingContext())
		}
	}
	if len(rcs) != 64 {
		t.Fatalf("setup registered=%d want64", len(rcs))
	}
	before := e.SSNMKnowledge()
	beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := peer.handleDeregistrationRequest(messages.NewDeregistrationRequest(params.NewRoutingContext(rcs[:63]...))); err != nil {
		t.Fatal(err)
	}
	e.ssnm.mu.Lock()
	work := *w
	e.ssnm.work = nil
	for _, state := range e.ssnm.partitions {
		state.work = nil
	}
	e.ssnm.mu.Unlock()
	for _, rc := range rcs[:63] {
		if _, held := peer.dynamicASKey(rc, false); held {
			t.Errorf("DEREG retained RC%d", rc)
		}
	}
	if _, held := peer.dynamicASKey(rcs[63], false); !held {
		t.Error("remaining dynamic AS removed")
	}
	if !reflect.DeepEqual(e.SSNMKnowledge().PeerCongestion, before.PeerCongestion) || e.ssnm.records != beforeRecords || e.ssnm.bytes != beforeBytes {
		t.Fatal("DEREG changed unrelated sibling peer records/charges")
	}
	// Allow four passes over the 64 unrelated records plus 64 retired scopes.
	// Baseline visits sibling records once for each removed RC (4032 visits),
	// despite no matching peer authority on the retiring association.
	t.Logf("store-locked peer rows=%d source.active rows=%d", work.peerRows, work.peerASRows)
	if work.peerRows > 4*(64+64) || work.peerASRows > 4*64 {
		t.Fatalf("multi-RC DEREG rescanned unrelated peer state: %+v", work)
	}
}
