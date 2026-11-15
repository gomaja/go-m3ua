package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

// A fake monotonic clock drives the production handler and scheduler paths.
func TestSCONValidityExpiryIsUnknownNotClear(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Now()
	advance := configureAutomationForTest(e, &now)
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	sendSCON(t, a, 7, 1, congestionLevel(2), 100)
	now = now.Add(30 * time.Second)
	advance()
	status := e.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), 100)
	if status.congested || !status.receivedUnknown {
		t.Fatal("received congestion remained current after validity deadline; expiry must remove its current contribution")
	}
	record := ssnmDestination(t, ssnmPartitionKnowledge(t, e.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")), 100, 0).Congestion
	if !record.Congested || record.Level != 2 {
		t.Fatalf("expiry fabricated clear: %+v", record)
	}
}

func TestCongestionAuditTargetsOriginalSGP(t *testing.T) {
	config := ssnmPeerInventoryConfig()
	config.SignallingGateways[0].SGPs[1].ApplicationServers[0].ASKey = staticASKey(7, 1)
	e := newSSNMStateEndpoint(t, config, nil)
	now := time.Now()
	advance := configureAutomationForTest(e, &now)
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	b := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a2"}, 7, 1)
	counts := [2]int{}
	a.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.DestinationStateAudit); ok {
			counts[0]++
		}
		return 0, nil
	}
	b.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.DestinationStateAudit); ok {
			counts[1]++
		}
		return 0, nil
	}
	sendSCON(t, a, 7, 1, congestionLevel(2), 100)
	now = now.Add(30 * time.Second)
	advance()
	if counts != [2]int{1, 0} {
		t.Fatalf("DAUD submissions = %v, want original reporter only [1 0]", counts)
	}
}
