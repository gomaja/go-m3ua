package m3ua

import (
	"errors"
	"testing"
	"time"
)

func TestForgetGenerationFencesReceivedReportsWithAutomationDisabled(t *testing.T) {
	for _, congestion := range []bool{false, true} {
		t.Run(map[bool]string{false: "availability", true: "congestion"}[congestion], func(t *testing.T) {
			r := newAuditTestRig(t)
			r.e.ssnm.validity = 0
			r.e.ssnm.auditing = nil
			// A prior row makes the unchanged legacy return count observable.
			sendSCON(t, r.a, 7, 1, congestionLevel(1), 90)
			reached := false
			r.e.aspRoutes.applyResolved = func() {
				reached = true
				if released := r.a.ForgetDestinations(); released != 1 {
					t.Fatal("forget changed the legacy row count", released)
				}
			}
			if congestion {
				sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
			} else {
				sendDUNA(t, r.a, 7, 1, 100)
			}
			r.e.aspRoutes.applyResolved = nil
			if !reached {
				t.Fatal("receive did not pause before route resolution")
			}
			if len(r.a.destinations.state) != 0 {
				t.Fatal("pre-forget disabled-policy report recreated legacy knowledge")
			}
			if r.e.ssnm.records != 0 {
				t.Fatal("pre-forget disabled-policy report recreated canonical knowledge", r.e.ssnm.records)
			}
			for key := range r.e.aspRoutes.availability {
				if key.pointCode == 100 {
					t.Fatal("pre-forget report recreated ASP availability cache")
				}
			}
			for key := range r.e.aspRoutes.congestion {
				if key.pointCode == 100 {
					t.Fatal("pre-forget report recreated ASP congestion cache")
				}
			}
			// A genuinely later report must remain admissible with both policies nil.
			sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
			if len(r.a.destinations.state) == 0 || r.e.ssnm.records == 0 {
				t.Fatal("forget rejected a new disabled-policy report")
			}
		})
	}
}

func TestDisabledPolicyForgetCaptureDoesNotJoinTracker(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.validity = 0
	r.e.ssnm.auditing = nil
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1)}
	r.a.sconSubmissions.mu.Lock()
	done := make(chan receivedSSNMAdmission, 1)
	go func() { done <- r.a.receivedReportAdmission(report) }()
	select {
	case token := <-done:
		if !token.forgetOnly || !token.forget.current() {
			t.Error("disabled receive did not capture a current atomic-only token")
		}
	case <-time.After(time.Second):
		r.a.sconSubmissions.mu.Unlock()
		t.Fatal("disabled-policy receive joined submission tracker")
	}
	r.a.sconSubmissions.mu.Unlock()
	if allocations := testing.AllocsPerRun(1000, func() { _ = r.a.receivedReportAdmission(report) }); allocations != 0 {
		t.Fatal("disabled-policy generation capture allocated", allocations)
	}
}

func TestForgetFenceRejectsDelayedCanonicalCommitWithoutAutomation(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.validity = 0
	r.e.ssnm.auditing = nil
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevelSet: true, CongestionLevel: 2}
	token := r.a.receivedReportAdmission(report)
	r.a.ForgetDestinations()
	if err := r.a.publishSSNMReport(report, token); err != nil {
		t.Fatal(err)
	}
	if r.e.ssnm.records != 0 {
		t.Fatal("old disabled-policy canonical report recreated knowledge")
	}
	release := r.a.beginOptionalSSNMChange(true)
	during := r.a.receivedReportAdmission(report)
	release()
	if err := r.a.publishSSNMReport(report, during); err != nil {
		t.Fatal(err)
	}
	if r.e.ssnm.records != 0 {
		t.Fatal("report captured during forget recreated knowledge")
	}
}

func TestOverlappingForgetGenerationsRejectInProgressReports(t *testing.T) {
	r := newAuditTestRig(t)
	r.e.ssnm.validity = 0
	r.e.ssnm.auditing = nil
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevelSet: true, CongestionLevel: 2}
	first := r.a.beginOptionalSSNMChange(true)
	second := r.a.beginOptionalSSNMChange(true)
	first()
	during := r.a.receivedReportAdmission(report)
	if during.forget.current() {
		t.Fatal("first forget reopened another forget's generation")
	}
	second()
	if during.forget.current() {
		t.Fatal("odd-generation report became current after release")
	}
	if err := r.a.publishSSNMReport(report, during); err != nil {
		t.Fatal(err)
	}
	if r.e.ssnm.records != 0 {
		t.Fatal("in-progress report recreated canonical knowledge")
	}
	fresh := r.a.receivedReportAdmission(report)
	if !fresh.forget.current() {
		t.Fatal("last forget did not reopen generation")
	}
	if err := r.a.publishSSNMReport(report, fresh); err != nil {
		t.Fatal(err)
	}
	if r.e.ssnm.records != 1 {
		t.Fatal("new report was not retained after overlapping forget")
	}
}

func TestLegacyDestinationCommitRejectsOldForgetToken(t *testing.T) {
	for _, kind := range []string{"availability", "scoped availability", "congestion", "scoped congestion"} {
		t.Run(kind, func(t *testing.T) {
			r := newAuditTestRig(t)
			r.e.ssnm.validity = 0
			r.e.ssnm.auditing = nil
			report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Scope: testWireScope(7, true, 1)}
			token := r.a.receivedReportAdmission(report)
			r.a.ForgetDestinations()
			ranges := []destinationRange{{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}}
			var err error
			switch kind {
			case "availability":
				err = r.a.destinations.setRangesWithinBudget(ranges, token.forget)
			case "scoped availability":
				err = r.a.destinations.setScopedRangesWithinBudget([]uint32{1}, ranges, token.forget)
			case "congestion":
				err = r.a.destinations.setCongestionRangesWithinBudget(ranges, token.forget)
			case "scoped congestion":
				err = r.a.destinations.setScopedCongestionRangesWithinBudget([]uint32{1}, ranges, token.forget)
			}
			if !errors.Is(err, errReceivedBeforeForget) {
				t.Fatal("legacy destination commit accepted stale atomic token", err)
			}
			if len(r.a.destinations.state) != 0 {
				t.Fatal("legacy destination commit recreated stale knowledge")
			}
		})
	}
}

func TestForgetGenerationGuardsASPRouteCacheWithPoliciesNil(t *testing.T) {
	e := newSSNMStateEndpoint(t, twoApplicationServerASPConfig(), nil)
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1, 2)
	const pc = uint32(0x123456)
	before, found := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: pc})
	if !found || before.Congested {
		t.Fatal("route fixture missing available uncongested status", before)
	}
	reached := false
	e.aspRoutes.applyResolved = func() { reached = true; a.ForgetDestinations() }
	sendSCON(t, a, 7, 1, congestionLevel(2), pc)
	e.aspRoutes.applyResolved = nil
	if !reached {
		t.Fatal("route fixture did not reach receive resolution seam")
	}
	if len(a.destinations.state) != 0 || e.ssnm.records != 0 {
		t.Fatal("stale route report recreated legacy or canonical knowledge")
	}
	for key := range e.aspRoutes.congestion {
		if key.pointCode == pc {
			t.Fatal("stale route report recreated routing cache after forget")
		}
	}
	after, found := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: pc})
	if !found || after != before {
		t.Fatal("stale route report changed exposed destination status", before, after)
	}
}
