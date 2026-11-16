package m3ua

import (
	"github.com/gomaja/go-m3ua/messages/params"
	"testing"
	"time"
)

func TestIdleOptionalPassDoesNotWaitForAssociationRegistry(t *testing.T) {
	r := publicationRig(t, 2)
	r.e.mu.Lock()
	done := make(chan struct{})
	go func() { r.e.runCongestionAutomation(r.now); close(done) }()
	select {
	case <-done:
		r.e.mu.Unlock()
	case <-time.After(time.Second):
		r.e.mu.Unlock()
		<-done
		t.Fatal("idle optional pass scanned the owned association registry")
	}
}

func TestOverflowWatcherRepairsDeferredSourceWithoutSubmitting(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithPriorities), 2)
	attachOverflowEndpointForTest(t, c)
	c.dataChan = make(chan *DataMessage, 1)
	c.dataChan <- &DataMessage{}
	c.muState.Lock()
	onset := c.recordDataOverflow(&DataMessage{AS: routingContextASKey(1), Scope: testWireScope(7, true, 1), ProtocolData: transferProtocolData(100, 1, nil)})
	c.muState.Unlock()
	if onset == nil {
		t.Fatal("fixture did not record the discarded scope")
	}
	if len(c.endpoint.ssnm.localCongestionSnapshot(routingContextASKey(1))) != 0 {
		t.Fatal("contended initial source admission unexpectedly succeeded")
	}
	if !waitFor(func() bool { return len(c.endpoint.ssnm.localCongestionSnapshot(routingContextASKey(1))) == 1 }, time.Second) {
		t.Fatal("bounded episode watcher did not repair deferred source admission")
	}
	if len(*sent) != 0 {
		t.Fatal("source watcher submitted optional transport outside the common scheduler")
	}
	runOverflowPublicationsForTest(c)
	if len(*sent) != 1 {
		t.Fatal("repaired source did not reach the common publication scheduler")
	}
}

func TestEndpointCloseRetiresPublicationReservations(t *testing.T) {
	r := publicationRig(t, 2)
	queuePublication(r, 100, 10, nationalPublication(3), 1)
	r.e.runCongestionAutomation(r.now)
	if len(r.e.CongestionPublications()) != 1 {
		t.Fatal("fixture did not retain the submitted national reservation")
	}
	if err := r.e.Close(); err != nil {
		t.Fatal(err)
	}
	if len(r.e.CongestionPublications()) != 0 {
		t.Fatal("Endpoint.Close retained optional publication reservations")
	}
}
