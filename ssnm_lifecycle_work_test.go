package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func newGroupedLifecycleFixture(t *testing.T, count int) (*Endpoint, *Association, []uint32) {
	t.Helper()
	rcs := make([]uint32, count)
	servers := make([]RemoteASConfig, count)
	for i := range rcs {
		rcs[i] = uint32(i + 1)
		servers[i] = RemoteASConfig{ID: RemoteASID(fmt.Sprintf("as-%02d", i)), ASKey: staticASKey(7, rcs[i])}
	}
	config := &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "group-sg",
		SGPs: []SignallingGatewayProcessConfig{{ID: "group-sgp", ApplicationServers: servers}}}}}
	e := newSSNMStateEndpoint(t, config, nil)
	e.ssnm.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
	e.ssnm.now = func() time.Time { return time.Unix(100, 0) }
	a := attachSSNMAssociation(t, e, SGPIdentity{"group-sg", "group-sgp"}, 7, rcs...)
	pcs := make([]uint32, count)
	for i := range pcs {
		pcs[i] = 1000 + uint32(i)
	}
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7),
		params.NewRoutingContext(rcs...), params.NewAffectedPointCode(pcs...), nil)); err != nil {
		t.Fatal(err)
	}
	if len(e.ssnm.audits) != count || len(e.ssnm.partitions) != count {
		t.Fatalf("fixture lost grouped knowledge: jobs=%d partitions=%d", len(e.ssnm.audits), len(e.ssnm.partitions))
	}
	return e, a, rcs
}

func attachLifecycleWorkProbe(s *ssnmState) *ssnmWorkCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := &ssnmWorkCount{}
	s.work = w
	for _, p := range s.partitions {
		p.work = w
	}
	for _, job := range s.audits {
		job.work = w
	}
	return w
}

func TestPartialNotifyWithdrawalBatchesAuditOrigins(t *testing.T) {
	e, a, rcs := newGroupedLifecycleFixture(t, 64)
	w := attachLifecycleWorkProbe(e.ssnm)
	a.handleSignals(context.Background(), messages.NewNotify(params.NewStatus(params.AlternateAspActive),
		params.NewAspIdentifier(7), params.NewRoutingContext(rcs[:63]...), nil))
	if err := firstErr(a); err != nil {
		t.Fatal(err)
	}
	e.ssnm.mu.Lock()
	work := *w
	jobs, partitions := len(e.ssnm.audits), len(e.ssnm.partitions)
	for _, job := range e.ssnm.audits {
		needed := 0
		for i, origin := range job.origins {
			if origin.needed() {
				needed++
				if job.bindings[i].key.RoutingContext != 64 {
					t.Error("withdrawn AS still needs an audit")
				}
			}
		}
		if needed != 1 || !job.live.Load() {
			t.Error("remaining AS lost its live audit", needed)
		}
	}
	e.ssnm.mu.Unlock()
	t.Logf("store retirement work: %+v", work)
	if jobs != 64 || partitions != 1 {
		t.Fatalf("partial withdrawal retired surviving knowledge: jobs=%d partitions=%d", jobs, partitions)
	}
	if work.copiedOrigins > 3*4096 || work.jobRows > 4*64 {
		t.Fatalf("partial withdrawal multiplied jobs by partition origins: %+v", work)
	}
}
