package m3ua

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Every fixture below is a store or a mock Association: none opens a socket.
func newHistoryWorkStore(t *testing.T) (*ssnmState, SSNMPartition, *ssnmWorkCount) {
	t.Helper()
	s, err := newSSNMState(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	s.validity = time.Hour
	s.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
	now := time.Unix(100, 0)
	s.now = func() time.Time { return now }
	work := &ssnmWorkCount{}
	s.work = work
	p := canonicalSSNMPartition("work-sg", "work-as")
	for id := AssociationID(1); id <= 2; id++ {
		s.registerSource(id)
		if err := s.bind(p, id, false); err != nil {
			t.Fatal(err)
		}
	}
	return s, p, work
}
func historyWorkReport(t *testing.T, s *ssnmState, p SSNMPartition, id AssociationID, kind SSNMReportKind, keys []ASKey, destinations []PointCodeRange) {
	t.Helper()
	r := SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: id, Partition: p, Scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: destinations, CongestionLevelSet: true, CongestionLevel: 2}
	if err := s.apply(r, keys); err != nil {
		t.Fatal(err)
	}
}
func historyWorkDestinations(base uint32, n int) []PointCodeRange {
	d := make([]PointCodeRange, n)
	for i := range d {
		d[i] = PointCodeRange{PointCode: base + uint32(i)}
	}
	return d
}
func historyWorkSubscribe(t *testing.T, s *ssnmState) *SSNMSubscription {
	t.Helper()
	_, sub, err := (&Endpoint{ssnm: s}).SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}
func historyWorkSeed(t *testing.T, s *ssnmState, p SSNMPartition, n int) {
	t.Helper()
	keys := []ASKey{*staticASKey(7, 1)}
	for id := AssociationID(1); id <= 2; id++ {
		for _, kind := range []SSNMReportKind{SSNMSignallingCongestionReport, SSNMDestinationUnavailableReport} {
			historyWorkReport(t, s, p, id, kind, keys, historyWorkDestinations(1000, n))
		}
	}
}
func TestSSNMHistoryReportWorkFollowsTouchedDestinations(t *testing.T) {
	for _, kind := range []SSNMReportKind{SSNMSignallingCongestionReport, SSNMDestinationUnavailableReport} {
		for _, subscriber := range []bool{false, true} {
			t.Run(fmt.Sprintf("kind=%d/subscriber=%v", kind, subscriber), func(t *testing.T) {
				s, p, w := newHistoryWorkStore(t)
				historyWorkSeed(t, s, p, 256)
				if subscriber {
					historyWorkSubscribe(t, s)
				}
				*w = ssnmWorkCount{}
				historyWorkReport(t, s, p, 1, kind, []ASKey{*staticASKey(7, 1)}, historyWorkDestinations(1000, 32))
				t.Logf("source rows=%d knowledge rows=%d", w.sourceRows, w.knowledgeRows)
				if !subscriber && w.knowledgeRows != 0 {
					t.Fatalf("unsubscribed report constructed %d destination deltas", w.knowledgeRows)
				}
				if subscriber && (w.knowledgeRows != 32 || w.sourceRows > 128) {
					t.Fatalf("report rescanned unrelated source history: %+v", *w)
				}
			})
		}
	}
}
func TestSSNMHistorySnapshotWorkVisitsEachSourceOnce(t *testing.T) {
	s, p, w := newHistoryWorkStore(t)
	historyWorkSeed(t, s, p, 256)
	*w = ssnmWorkCount{}
	got := s.snapshot()
	if len(got.Partitions) != 1 || len(got.Partitions[0].Destinations) != 256 {
		t.Fatal("incomplete snapshot")
	}
	t.Logf("source rows=%d knowledge rows=%d", w.sourceRows, w.knowledgeRows)
	t.Logf("work counts: %+v", *w)
	if w.sourceRows > 4*1024 {
		t.Fatalf("snapshot multiplied destinations by source rows: %+v", *w)
	}
}
func TestSSNMHistoryProjectionWorkIgnoresUnrelatedPrefixes(t *testing.T) {
	s, p, w := newHistoryWorkStore(t)
	historyWorkSeed(t, s, p, 256)
	_ = s.destinationKnowledge(p, 9000) // Warm the immutable source view; reset counts for steady reads.
	*w = ssnmWorkCount{}
	for pc := uint32(9000); pc < 9032; pc++ {
		if got := s.destinationKnowledge(p, pc); got.congested || got.availabilitySet {
			t.Fatal("unrelated prefix applied")
		}
	}
	t.Logf("work counts: %+v", *w)
	if w.sourceRows != 0 {
		t.Fatalf("point projections scanned unrelated sources: %d", w.sourceRows)
	}
	for pc := uint32(1000); pc < 1032; pc++ {
		if got := s.destinationKnowledge(p, pc); !got.congested || !got.availabilitySet {
			t.Fatal("covering source omitted")
		}
	}
	t.Logf("work counts: %+v", *w)
	if w.sourceRows > 128 {
		t.Fatalf("point projections rescanned source history: %d", w.sourceRows)
	}
}
func TestSSNMHistoryGroupedASCopyWorkIsLinear(t *testing.T) {
	for _, kind := range []SSNMReportKind{SSNMSignallingCongestionReport, SSNMDestinationUnavailableReport} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			s, p, w := newHistoryWorkStore(t)
			keys := make([]ASKey, 32)
			for i := range keys {
				keys[i] = *staticASKey(7, uint32(i+1))
			}
			*w = ssnmWorkCount{}
			historyWorkReport(t, s, p, 1, kind, keys, historyWorkDestinations(1000, 4))
			t.Logf("work counts: %+v", *w)
			if w.copiedAS > 4*32 {
				t.Fatalf("grouped scalar copied AS list per source: %d", w.copiedAS)
			}
			entry := s.snapshot().Partitions[0].Destinations[0]
			if kind == SSNMSignallingCongestionReport && entry.Congestion.AS != keys[31] || kind == SSNMDestinationUnavailableReport && entry.Availability.AS != keys[31] {
				t.Fatal("scalar changed final sorted AS")
			}
		})
	}
}
func historyWorkAddJob(s *ssnmState, p SSNMPartition, id AssociationID, pc uint32, mask uint8, as []ASKey) *destinationAuditJob {
	job := &destinationAuditJob{key: destinationAuditKey{association: id, scope: peerCongestionScopeKey(WireScope{}), destination: ssnmDestinationKeyFor(PointCodeRange{PointCode: pc, Mask: mask})}, partitions: []SSNMPartition{p}, congestionRevision: 1, availabilityRevision: 1, isolationRevision: 1}
	for _, key := range as {
		job.bindings = append(job.bindings, queueCongestionBinding{key: key})
		job.origins = append(job.origins, destinationAuditOrigin{partition: p, congestion: true, availability: true, isolation: true})
	}
	job.summarizeOrigins()
	job.live.Store(true)
	s.retainAuditLocked(job)
	s.bytes += job.bytes()
	return job
}
func TestSSNMHistorySourceRetirementWorkDoesNotMultiplyJobs(t *testing.T) {
	for _, availability := range []bool{false, true} {
		t.Run(fmt.Sprint(availability), func(t *testing.T) {
			s, p, w := newHistoryWorkStore(t)
			kind := SSNMSignallingCongestionReport
			if availability {
				kind = SSNMDestinationUnavailableReport
			}
			historyWorkReport(t, s, p, 1, kind, []ASKey{*staticASKey(7, 1)}, historyWorkDestinations(1000, 128))
			s.mu.Lock()
			for i := 0; i < 128; i++ {
				historyWorkAddJob(s, p, 2, uint32(9000+i), 0, []ASKey{*staticASKey(7, 2)})
			}
			*w = ssnmWorkCount{}
			s.retireReceivedAssociationLocked(1, nil)
			s.mu.Unlock()
			t.Logf("work counts: %+v", *w)
			if w.jobRows > 256 {
				t.Fatalf("source retirement rescanned unrelated audit jobs: %+v", *w)
			}
			if len(s.audits) != 128 || len(s.partitions[p].receivedCongestion)+len(s.partitions[p].receivedAvailability) != 0 {
				t.Fatal("retirement changed independent jobs or retained source")
			}
		})
	}
}
func TestSSNMHistoryAssociationRetirementWorkSelectsPartitionsDirectly(t *testing.T) {
	s, p, w := newHistoryWorkStore(t)
	for i := 1; i < 16; i++ {
		other := p
		other.ApplicationServer = RemoteASID(fmt.Sprintf("as-%d", i))
		for id := AssociationID(1); id <= 2; id++ {
			if err := s.bind(other, id, false); err != nil {
				t.Fatal(err)
			}
		}
	}
	s.mu.Lock()
	for i := 0; i < 64; i++ {
		historyWorkAddJob(s, p, 2, uint32(9000+i), 0, []ASKey{*staticASKey(7, 2)})
	}
	*w = ssnmWorkCount{}
	s.mu.Unlock()
	s.retireAssociation(1)
	t.Logf("work counts: %+v", *w)
	if w.jobRows > 128 || w.partitionRows > 32 {
		t.Fatalf("association retirement repeated global scans: %+v", *w)
	}
	if len(s.partitions) != 16 || len(s.audits) != 64 {
		t.Fatal("sibling state was retired")
	}
}
func TestSSNMAuditCoverageWorkIsBoundedByRetainedPrefixes(t *testing.T) {
	statements := make([]auditRangeStatement, 128)
	for i := range statements {
		statements[i] = auditRangeStatement{destination: ssnmDestinationKey{pointCode: uint32(i)}, revision: uint64(i + 2)}
	}
	w := &ssnmWorkCount{}
	if auditRangeStillNeeded(ssnmDestinationKey{mask: 7}, statements, w) {
		t.Fatal("complete clear union still needs audit")
	}
	t.Logf("work counts: %+v", *w)
	if w.prefixRows > 32*128 {
		t.Fatalf("coverage recursively rescanned statements: %d", w.prefixRows)
	}
	statements[93].needed = true
	if !auditRangeStillNeeded(ssnmDestinationKey{mask: 7}, statements) {
		t.Fatal("positive hole stopped audit")
	}
}
func TestSSNMAuditOriginCoverageSharesOneHistoryPass(t *testing.T) {
	s, p, w := newHistoryWorkStore(t)
	clear := SSNMReport{Kind: SSNMDestinationAvailableReport, Source: SSNMPeerReport, Association: 1, Partition: p, Scope: WireScope{}, Destinations: historyWorkDestinations(0, 128)}
	if err := s.apply(clear, []ASKey{*staticASKey(7, 1)}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	for i := 0; i < 64; i++ {
		historyWorkAddJob(s, p, 1, uint32(2*i), 1, []ASKey{*staticASKey(7, 1)})
	}
	*w = ssnmWorkCount{}
	// A one-point report intersects only its job; all other clears still answer
	// the whole retained range when needed by a later intersecting report.
	clear.Destinations = nil
	for i := 0; i < 64; i++ {
		clear.Destinations = append(clear.Destinations, PointCodeRange{PointCode: uint32(2 * i)})
	}
	s.updateAuditOriginsLocked(1, 0, clear, []ASKey{*staticASKey(7, 1)}, peerCongestionScopeKey(clear.Scope), s.automationNow())
	s.mu.Unlock()
	t.Logf("work counts: %+v", *w)
	if w.sourceRows > 2*128 {
		t.Fatalf("coverage rescanned history per audit origin: %d", w.sourceRows)
	}
	if len(s.audits) != 64 {
		t.Fatal("coverage removed independent congestion jobs")
	}
	for _, job := range s.audits {
		for _, origin := range job.origins {
			if !origin.congestion || origin.availability || origin.isolation {
				t.Fatal("clear union changed the wrong causes", origin)
			}
		}
	}
}
func TestSSNMAuditAdmissionWorkDoesNotScanEveryJobPerDestination(t *testing.T) {
	r := newAuditTestRig(t)
	pcs := func(base uint32) []uint32 {
		v := make([]uint32, 64)
		for i := range v {
			v[i] = base + uint32(i)
		}
		return v
	}
	sendDUNA(t, r.a, 7, 1, pcs(1000)...)
	w := &ssnmWorkCount{}
	r.e.ssnm.work = w
	for _, p := range r.e.ssnm.partitions {
		p.work = w
	}
	sendDUNA(t, r.a, 7, 1, pcs(9000)...)
	t.Logf("work counts: %+v", *w)
	if w.jobRows > 4*128 {
		t.Fatalf("grouped admission rescanned jobs per destination: %d", w.jobRows)
	}
	if len(r.e.ssnm.audits) != 128 {
		t.Fatal("grouped admission lost jobs")
	}
}
func TestSSNMAuditClearWorkIndexesReportPrefixes(t *testing.T) {
	r := newAuditTestRig(t)
	pcs := make([]uint32, 128)
	for i := range pcs {
		pcs[i] = uint32(1000 + i)
	}
	sendDUNA(t, r.a, 7, 1, pcs...)
	w := &ssnmWorkCount{}
	r.e.ssnm.work = w
	for _, p := range r.e.ssnm.partitions {
		p.work = w
	}
	sendDAVA(t, r.a, 7, 1, pcs...)
	t.Logf("work counts: %+v", *w)
	if w.prefixRows > 32*256 {
		t.Fatalf("clear compared every report prefix against every job: %d", w.prefixRows)
	}
	if len(r.e.ssnm.audits) != 0 {
		t.Fatal("complete clear left audit jobs")
	}
}

func historyWorkAssociation(t *testing.T, n int) (*Endpoint, *Association, *ssnmWorkCount) {
	t.Helper()
	e := newSSNMStateEndpoint(t, nil, nil)
	now := time.Unix(100, 0)
	configureAutomationForTest(e, &now)
	rcs := make([]uint32, n)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, rcs...)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.noteRoutingContextsAcked(params.NewRoutingContext(rcs...))
	if !e.trackAssociation(a) {
		t.Fatal("standalone attach")
	}
	t.Cleanup(func() { _ = a.Close() })
	w := &ssnmWorkCount{}
	e.ssnm.work = w
	for _, p := range e.ssnm.partitions {
		p.work = w
	}
	return e, a, w
}
func TestSSNMAuditGroupedBindingWorkIsLinear(t *testing.T) {
	e, a, w := historyWorkAssociation(t, 32)
	rcs := make([]uint32, 32)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
	}
	report := SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Association: a.ID(), Scope: testWireScope(7, true, rcs...), Destinations: []PointCodeRange{{PointCode: 100}}}
	a.scheduleDestinationAudit(report, false)
	if len(e.ssnm.audits) != 1 {
		t.Fatal("grouped job missing")
	}
	*w = ssnmWorkCount{}
	a.scheduleDestinationAudit(report, false)
	t.Logf("work counts: %+v", *w)
	if w.asRows > 8*32 {
		t.Fatalf("grouped replacement compared ASes/bindings quadratically: %d", w.asRows)
	}
	if len(e.ssnm.audits) != 1 {
		t.Fatal("grouped refresh duplicated job")
	}
}
func TestSSNMHistoryRegistrationRetirementIndexesStaticAS(t *testing.T) {
	e, a, w := historyWorkAssociation(t, 64)
	p := a.ssnmPartitionFor(*staticASKey(7, 1))
	historyWorkReport(t, e.ssnm, p, a.ID(), SSNMSignallingCongestionReport, []ASKey{*staticASKey(7, 999)}, historyWorkDestinations(1000, 64))
	*w = ssnmWorkCount{}
	a.retireReceivedRegistration(999, false)
	t.Logf("work counts: %+v", *w)
	if w.asRows > 4*64 {
		t.Fatalf("retirement rescanned static ASes per source: %d", w.asRows)
	}
	if len(e.ssnm.partitions[p].receivedCongestion) != 0 {
		t.Fatal("retired dynamic source survived")
	}
}

func TestSSNMHistoryAdmissionIndexesStaticAS(t *testing.T) {
	_, a, w := historyWorkAssociation(t, 64)
	rcs := make([]uint32, 64)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
	}
	*w = ssnmWorkCount{}
	got := a.captureReceivedSSNMAdmission(testWireScope(7, true, rcs...))
	if !got.valid || len(got.bindings) != 64 {
		t.Fatal("static admission missing bindings")
	}
	t.Logf("work counts: %+v", *w)
	if w.asRows > 3*64 {
		t.Fatalf("receive admission rescanned static ASes per binding: %d", w.asRows)
	}
}

func TestSSNMHistoryBroadProjectionSharesSourceAggregation(t *testing.T) {
	s, p, w := newHistoryWorkStore(t)
	for id := AssociationID(1); id <= 64; id++ {
		s.registerSource(id)
		if err := s.bind(p, id, false); err != nil {
			t.Fatal(err)
		}
		historyWorkReport(t, s, p, id, SSNMSignallingCongestionReport, []ASKey{*staticASKey(7, 1)}, []PointCodeRange{{Mask: 16}})
	}
	*w = ssnmWorkCount{}
	for pc := uint32(0); pc < 128; pc++ {
		if got := s.destinationKnowledge(p, pc); !got.congested || got.congestionLevel != 2 {
			t.Fatal("broad source aggregate lost pressure", got)
		}
	}
	t.Logf("work counts: %+v", *w)
	if w.sourceRows > 4*64 {
		t.Fatalf("broad point projections multiplied queries by sources: %d", w.sourceRows)
	}
	s.expireCongestion(time.Unix(100, 0).Add(time.Hour))
	if got := s.destinationKnowledge(p, 100); got.congested || !got.receivedUnknown {
		t.Fatal("cached broad aggregate survived expiry", got)
	}
}

func TestSSNMScopedInvalidationWorkIndexesAffectedAS(t *testing.T) {
	s, _, w := newHistoryWorkStore(t)
	p := SSNMPartition{Kind: SSNMStandalonePartition, Association: 1}
	if err := s.bind(p, 1, false); err != nil {
		t.Fatal(err)
	}
	held, refused := make([]ASKey, 64), make([]ASKey, 64)
	for i := range held {
		held[i] = *staticASKey(7, uint32(i+1))
		refused[i] = *staticASKey(7, uint32(1000+i))
	}
	historyWorkReport(t, s, p, 1, SSNMSignallingCongestionReport, held, []PointCodeRange{{PointCode: 100}})
	*w = ssnmWorkCount{}
	s.mu.Lock()
	s.invalidateScopedDestinationKnowledgeLocked(s.partitions[p], "disjoint refusal", ssnmDestinationRefusal{resolved: true, keys: refused, work: w})
	s.mu.Unlock()
	t.Logf("work counts: %+v", *w)
	if w.asRows > 8*128 {
		t.Fatalf("scoped invalidation multiplied affected ASes by source history: %d", w.asRows)
	}
	if len(s.partitions[p].receivedCongestion) != 64 {
		t.Fatal("disjoint invalidation removed sources")
	}
}
func TestSSNMScopedInvalidationWorkBatchesGroupedJobOrigins(t *testing.T) {
	s, _, w := newHistoryWorkStore(t)
	var partitions []SSNMPartition
	rcs := make([]uint32, 32)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
	}
	scope := testWireScope(7, true, rcs...)
	job := &destinationAuditJob{key: destinationAuditKey{association: 1, scope: peerCongestionScopeKey(scope), destination: ssnmDestinationKey{pointCode: 100}}, scope: scope}
	refusal := ssnmDestinationRefusal{resolved: true, byPartition: make(map[SSNMPartition][]ASKey), work: w}
	for i := 0; i < 32; i++ {
		p := canonicalSSNMPartition("grouped-sg", RemoteASID(fmt.Sprintf("as-%d", i)))
		if err := s.bind(p, 1, false); err != nil {
			t.Fatal(err)
		}
		as := *staticASKey(7, uint32(i+1))
		partitions = append(partitions, p)
		refusal.byPartition[p] = []ASKey{as}
		report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: 1, Partition: p, Scope: scope, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 2, CongestionLevelSet: true}
		if err := s.apply(report, []ASKey{as}); err != nil {
			t.Fatal(err)
		}
		job.bindings = append(job.bindings, queueCongestionBinding{key: as})
		job.partitions = append(job.partitions, p)
		job.origins = append(job.origins, destinationAuditOrigin{partition: p, congestion: true, isolation: true})
	}
	s.mu.Lock()
	job.summarizeOrigins()
	job.live.Store(true)
	s.retainAuditLocked(job)
	s.bytes += job.bytes()
	*w = ssnmWorkCount{}
	s.mu.Unlock()
	_ = s.oversized(partitions, 1025, 1024, 1, refusal)
	t.Logf("work counts: %+v", *w)
	if w.copiedOrigins > 4*32 {
		t.Fatalf("partition invalidation repeatedly cloned grouped origins: %d", w.copiedOrigins)
	}
	if len(s.audits) != 1 {
		t.Fatal("invalidation removed independent isolation")
	}
	for _, job := range s.audits {
		for _, o := range job.origins {
			if o.congestion || o.availability || !o.isolation {
				t.Fatal("invalidation changed wrong origins", o)
			}
		}
	}
}

func TestSourceRetirementPreservesCapturedIsolationJobFence(t *testing.T) {
	s, p, _ := newHistoryWorkStore(t)
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: 1, Partition: p, Scope: WireScope{}, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevelSet: true}
	if err := s.apply(report, []ASKey{*staticASKey(7, 1)}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	job := historyWorkAddJob(s, p, 1, 100, 0, []ASKey{*staticASKey(7, 1)})
	job.origins[0].congestion, job.origins[0].availability = false, false
	job.summarizeOrigins()
	var outcomes auditOutcomeBatch
	s.retireReceivedSourcesLocked(s.partitions[p], func(receivedCongestionKey, SSNMCongestion) bool { return true }, false, &outcomes)
	if job.live.Load() {
		s.mu.Unlock()
		t.Fatal("source retirement delayed captured isolation fence until outcome publication")
	}
	outcomes.publishLocked(s)
	replacement := s.audits[job.key]
	s.mu.Unlock()
	if job.live.Load() {
		t.Fatal("retired source left captured isolation job live")
	}
	if replacement == nil || replacement == job || !replacement.live.Load() || !replacement.isolation || replacement.congestion || replacement.availability {
		t.Fatal("retirement changed independent isolation instead of refreshing its fence")
	}
}

func TestSourceRetirementKeepsUnmatchedIsolationJobLive(t *testing.T) {
	s, p, _ := newHistoryWorkStore(t)
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: 1, Partition: p, Scope: WireScope{}, Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevelSet: true}
	if err := s.apply(report, []ASKey{*staticASKey(7, 1)}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	job := historyWorkAddJob(s, p, 1, 100, 0, []ASKey{*staticASKey(7, 2)})
	job.origins[0].congestion, job.origins[0].availability = false, false
	job.summarizeOrigins()
	var outcomes auditOutcomeBatch
	s.retireReceivedSourcesLocked(s.partitions[p], func(receivedCongestionKey, SSNMCongestion) bool { return true }, false, &outcomes)
	outcomes.publishLocked(s)
	current := s.audits[job.key]
	s.mu.Unlock()
	if current != job || !job.live.Load() || !job.isolation || job.congestion || job.availability {
		t.Fatal("unrelated AS retirement changed isolation fence")
	}
}
