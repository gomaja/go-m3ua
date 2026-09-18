package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// attachActivatingSSNMAssociation attaches an Association inside the RFC 4666
// Section 4.5.1 window: the ASP Active has gone out and the Ack has not come
// back, so the Association is still ASP-INACTIVE.
func attachActivatingSSNMAssociation(
	t *testing.T,
	endpoint *Endpoint,
	identity SGPIdentity,
	networkAppearance uint32,
	routingContexts ...uint32,
) *Association {
	t.Helper()
	association, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, routingContexts...)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	peer := identity
	association.cfg.PeerSGP = &peer
	if !endpoint.trackAssociation(association) {
		t.Fatalf("failed to attach Association to SGP %+v", identity)
	}
	association.startTAck(messages.NewAspActive(
		association.cfg.TrafficModeType.Copy(),
		params.NewRoutingContext(routingContexts...),
		nil,
	), requestAspActive)
	association.syncSSNMBindings()
	t.Cleanup(func() { _ = association.Close() })
	return association
}

// Bullet: same Signalling Gateway across multiple SGPs versus alternative
// Signalling Gateways.
//
// RFC 4666 Section 1.4.2 reaches one Application Server through the SGPs of
// its Signalling Gateway, so those SGPs contribute to one body of knowledge.
// Section 3.6.1 makes the Routing Context a per-peer label, so the same value
// on another Signalling Gateway names something else entirely.
func TestSSNMStateSharesOnePartitionAcrossSGPsOfOneSignallingGateway(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)

	partition := canonicalSSNMPartition("sg-a", "as-core")
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Bindings) != 2 {
		t.Fatalf("bindings = %+v, want both SGPs of one Signalling Gateway", knowledge.Bindings)
	}

	// The two SGPs label the Application Server differently; the knowledge
	// they contribute lands in one place regardless.
	sendDUNA(t, first, 7, 1, 0x123456)
	sendDUNA(t, second, 7, 2, 0x123457)
	knowledge = ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Destinations) != 2 {
		t.Fatalf("retained %d destinations, want both SGPs' reports in one partition: %+v",
			len(knowledge.Destinations), knowledge.Destinations)
	}
	if scope := ssnmDestination(t, knowledge, 0x123457, 0).Availability.Scope; scope.RoutingContexts[0] != 2 {
		t.Errorf("exact wire scope was lost: %+v", scope)
	}
}

func TestSSNMStateSeparatesAlternativeSignallingGateways(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-b",
		SignallingGatewayProcess: "sgp-b1",
	}, 7, 1)

	// Both Signalling Gateways label their Application Server RC 1 in Network
	// Appearance 7; they are still different Application Servers.
	sendDUNA(t, first, 7, 1, 0x123456)
	snapshot := endpoint.SSNMKnowledge()
	if got := len(ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core")).Destinations); got != 1 {
		t.Fatalf("reporting Signalling Gateway holds %d destinations, want 1", got)
	}
	if got := len(ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-b", "as-core")).Destinations); got != 0 {
		t.Fatalf("alternative Signalling Gateway holds %d destinations, want none", got)
	}
}

// A sibling of the same canonical Signalling Gateway and Application Server
// preserves the knowledge when one binding goes.
func TestSSNMStateSiblingBindingPreservesKnowledge(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	sendDUNA(t, first, 7, 1, 0x123456)
	epoch := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition).Epoch

	if err := first.Close(); err != nil {
		t.Fatalf("close reporting Association: %v", err)
	}

	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Bindings) != 1 || knowledge.Bindings[0].Association != second.ID() {
		t.Fatalf("bindings = %+v, want the sibling alone", knowledge.Bindings)
	}
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("sibling did not preserve the knowledge: %+v", knowledge.Destinations)
	}
	if knowledge.Epoch != epoch {
		t.Fatalf("epoch moved from %d to %d while a binding remained", epoch, knowledge.Epoch)
	}
}

// An unrelated Application Server cannot preserve another one's knowledge,
// even on the same Signalling Gateway.
func TestSSNMStateUnrelatedApplicationServerCannotPreserveKnowledge(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	core := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	edge := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 3)
	sendDUNA(t, core, 7, 1, 0x123456)
	sendDUNA(t, edge, 7, 3, 0x123457)

	if err := core.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	snapshot := endpoint.SSNMKnowledge()
	if ssnmPartitionPresent(snapshot, canonicalSSNMPartition("sg-a", "as-core")) {
		t.Fatal("an unrelated Application Server kept the retired one's partition alive")
	}
	if got := len(ssnmPartitionKnowledge(t, snapshot,
		canonicalSSNMPartition("sg-a", "as-edge")).Destinations); got != 1 {
		t.Fatalf("unrelated Application Server holds %d destinations, want its own 1", got)
	}
}

// Losing the last binding retires the partition and invalidates its knowledge
// in one step: knowledge with no owner is knowledge nobody can refresh.
func TestSSNMStateRetiresPartitionOnLastBindingLoss(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	sendDUNA(t, association, 7, 1, 0x123456)

	if err := association.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	snapshot := endpoint.SSNMKnowledge()
	if ssnmPartitionPresent(snapshot, canonicalSSNMPartition("sg-a", "as-core")) {
		t.Fatalf("partition outlived its last binding: %+v", snapshot.Partitions)
	}
	if snapshot.PartitionsInvalidated == 0 {
		t.Error("last-binding loss was not reported as an invalidation")
	}
}

// A source reset withdraws the acknowledged scope, so the knowledge that scope
// carried goes with it and a fresh activation starts a new epoch.
func TestSSNMStateSourceResetAndFreshReactivationStartANewEpoch(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	sendDUNA(t, association, 7, 1, 0x123456)
	first := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition).Epoch

	// The source resets: the ASP leaves ASP-ACTIVE and the acknowledged scope
	// it held goes with it.
	association.forgetAckedRoutingContexts()
	association.sendState(StateASPInactive)
	if ssnmPartitionPresent(endpoint.SSNMKnowledge(), partition) {
		t.Fatal("a source reset left the partition in place")
	}

	association.sendState(StateASPActive)
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if knowledge.Epoch <= first {
		t.Fatalf("epoch after reactivation = %d, want more than %d", knowledge.Epoch, first)
	}
	if len(knowledge.Destinations) != 0 {
		t.Fatalf("reactivation resurrected knowledge from before the reset: %+v", knowledge.Destinations)
	}
}

// An Application Server withdrawn by an ASP Inactive Ack takes its knowledge
// with it, while a sibling Application Server on the same Association keeps
// its own. RFC 4666 Section 4.3.4.4 scopes the deactivation to the Routing
// Contexts the acknowledgment named.
func TestSSNMStateApplicationServerWithdrawalKeepsSiblingKnowledge(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1, 3)
	sendDUNA(t, association, 7, 1, 0x123456)
	sendDUNA(t, association, 7, 3, 0x123457)

	association.noteRoutingContextsUnacked(params.NewRoutingContext(3))

	snapshot := endpoint.SSNMKnowledge()
	if ssnmPartitionPresent(snapshot, canonicalSSNMPartition("sg-a", "as-edge")) {
		t.Fatalf("the withdrawn Application Server kept its partition: %+v", snapshot.Partitions)
	}
	kept := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(kept.Destinations) != 1 {
		t.Fatalf("the retained Application Server lost knowledge: %+v", kept.Destinations)
	}
}

// Bullet: failed pending activation. Knowledge admitted under the Section
// 4.5.1 window belongs to that activation; if the activation never completes,
// the knowledge goes with it.
func TestSSNMStateFailedPendingActivationDiscardsAdmittedKnowledge(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachActivatingSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	sendDUNA(t, association, 7, 1, 0x123456)
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Bindings) != 1 || !knowledge.Bindings[0].Pending {
		t.Fatalf("bindings = %+v, want one pending binding", knowledge.Bindings)
	}
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("Section 4.5.1 report was not admitted: %+v", knowledge.Destinations)
	}

	// The activation is abandoned without an Ack.
	if !association.stopTAck(requestAspActive) {
		t.Fatal("no outstanding ASP Active to abandon")
	}
	association.syncSSNMBindings()
	if ssnmPartitionPresent(endpoint.SSNMKnowledge(), partition) {
		t.Fatalf("knowledge admitted for an activation that failed survived it: %+v",
			endpoint.SSNMKnowledge().Partitions)
	}
}

// Bullet: pending-activation reports do not authorize DATA before activation
// acknowledgment.
//
// RFC 4666 Section 4.5.1 sends DUNA, DRST and SCON before the ASP Active Ack
// precisely so the ASP does not send traffic to destinations it would
// otherwise not know about. Admitting that knowledge must not itself become
// permission to send.
func TestSSNMPendingActivationKnowledgeDoesNotAuthorizeTraffic(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachActivatingSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	sendDAVA(t, association, 7, 1, 0x123456)
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if knowledge.TrafficAuthorized {
		t.Fatal("a pending binding authorized traffic before the ASP Active Ack")
	}
	if destination := ssnmDestination(t, knowledge, 0x123456, 0); destination.Availability.State != DestinationAvailable {
		t.Fatalf("admitted availability = %v, want Available", destination.Availability.State)
	}
	if _, err := writePayload(association, 1, []byte{0x01}); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA before the ASP Active Ack: error = %v, want ErrNotEstablished", err)
	}

	// The acknowledgment completes the binding and keeps what it admitted.
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	association.sendState(StateASPActive)
	knowledge = ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if !knowledge.TrafficAuthorized {
		t.Fatal("the ASP Active Ack did not authorize traffic")
	}
	if len(knowledge.Bindings) != 1 || knowledge.Bindings[0].Pending {
		t.Fatalf("bindings = %+v, want one completed binding", knowledge.Bindings)
	}
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("activation discarded knowledge admitted during the window: %+v", knowledge.Destinations)
	}
}

// A partition with both a pending and a completed binding authorizes traffic:
// the pending window belongs to one Association, not to the Application Server.
func TestSSNMPartitionIsAuthorizedWhileAnyBindingIsComplete(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	attachActivatingSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)

	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(),
		canonicalSSNMPartition("sg-a", "as-core"))
	if !knowledge.TrafficAuthorized {
		t.Fatal("a completed sibling binding did not authorize the partition")
	}
	pending := 0
	for _, binding := range knowledge.Bindings {
		if binding.Pending {
			pending++
		}
	}
	if len(knowledge.Bindings) != 2 || pending != 1 {
		t.Fatalf("bindings = %+v, want one completed and one pending", knowledge.Bindings)
	}
}

// An Association with no provisioned Application Server owns its knowledge
// alone, and that knowledge cannot be preserved by anyone else.
func TestSSNMStateStandalonePartitionIsOwnedByOneAssociation(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID:                 "sgp-a1",
				ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}},
			}},
		}},
	}})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	// An Association that names no provisioned peer resolves to no canonical
	// Application Server.
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	association.endpoint = endpoint
	association.managementID.Store(99)
	t.Cleanup(func() { _ = association.Close() })
	association.syncSSNMBindings()

	sendDUNA(t, association, 7, 1, 0x123456)
	partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: association.ID()}
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("standalone partition retained %+v, want one destination", knowledge.Destinations)
	}
	if ssnmPartitionPresent(endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")) {
		t.Fatal("an unprovisioned Association was attributed to a canonical Application Server")
	}
}

// An Association the Endpoint forgets leaves no bindings behind, whatever
// state the Association still believes it is in.
func TestSSNMStateForgettingAnAssociationRetiresItsBindings(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	sendDUNA(t, association, 7, 1, 0x123456)

	// Forgotten while it is still ASP-ACTIVE, so nothing derived from its own
	// state can retire the binding on its behalf.
	endpoint.forgetAssociation(association)
	if association.State() != StateASPActive {
		t.Fatalf("fixture state = %v, want the Association still ASP-ACTIVE", association.State())
	}
	if ssnmPartitionPresent(endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")) {
		t.Fatalf("a forgotten Association kept its binding: %+v", endpoint.SSNMKnowledge().Partitions)
	}
}

// Within one canonical Application Server and one dimension, the last report
// this node validated wins, whichever SGP carried it.
//
// That is an ordering over what this node accepted, not a reconstruction of
// the order the peers produced: RFC 4666 gives SSNM no sequence number, so no
// remote causal order exists to recover. The record therefore keeps the source
// and revision that installed it, so a reader can see which one it was.
func TestSSNMStateLastValidatedReportWinsWithinOneDimension(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	sendDUNA(t, first, 7, 1, 0x123456)
	earlier := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0).Availability

	sendDAVA(t, second, 7, 2, 0x123456)
	later := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0).Availability
	if later.State != DestinationAvailable {
		t.Fatalf("availability = %v, want the later report's Available", later.State)
	}
	if later.Association != second.ID() {
		t.Fatalf("source association = %d, want the later reporter %d", later.Association, second.ID())
	}
	if later.Revision <= earlier.Revision {
		t.Fatalf("revision %d did not advance past %d", later.Revision, earlier.Revision)
	}
	if later.Scope.RoutingContexts[0] != 2 {
		t.Fatalf("retained wire scope = %+v, want the later reporter's label", later.Scope)
	}

	// The order is the order of local validation, so the first SGP reporting
	// again takes the record back.
	sendDUNA(t, first, 7, 1, 0x123456)
	latest := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0).Availability
	if latest.State != DestinationUnavailable || latest.Association != first.ID() {
		t.Fatalf("availability = %v from %d, want Unavailable from %d",
			latest.State, latest.Association, first.ID())
	}
	if latest.Revision <= later.Revision {
		t.Fatalf("revision %d did not advance past %d", latest.Revision, later.Revision)
	}
}
