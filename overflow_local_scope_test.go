package m3ua

import (
	"context"
	"github.com/gomaja/go-m3ua/messages"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestQueueContributionPreservesAllAppearanceBinding(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	e := methodEndpointForTest(t, RoleASP, uint8(CongestionInternational), 0)
	e.congestionAutomationOnce.Do(func() {})
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	c.dataChan = make(chan *DataMessage, 1)
	for range 2 {
		deliver(t, c, 1, inboundData(1, "queue"))
	}
	inputs, err := e.CongestionInputs(routingContextASKey(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(inputs.Local) != 1 || inputs.Local[0].Request.AS != routingContextASKey(1) || !inputs.Local[0].Scope.NetworkAppearanceSet || inputs.Local[0].Scope.NetworkAppearance != 7 {
		t.Fatalf("queue contribution lost all-appearance binding or exact wire NA: %+v", inputs.Local)
	}
}

func TestForeignAppearanceQueueKeepsConfiguredMethod(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(0))
	c.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 0, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionInternational}}}
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	e := methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithPriorities), 3)
	e.congestionAutomationOnce.Do(func() {})
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	c.dataChan = make(chan *DataMessage, 1)
	for range 2 {
		deliver(t, c, 1, inboundData(1, "foreign accepted appearance"))
	}
	local, err := e.CongestionInputs(*staticASKey(0, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(local.Local) != 1 || !local.Local[0].Scope.NetworkAppearanceSet || local.Local[0].Scope.NetworkAppearance != 7 {
		t.Fatalf("accepted queue scope not retained under its binding: %+v", local.Local)
	}
	runOverflowPublicationsForTest(c)
	if len(*sent) != 1 || (*sent)[0].(*messages.SignallingCongestion).CongestionIndications != nil {
		t.Fatal("queue onset changed configured method")
	}
	if _, err := c.ReadData(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.muDataOverflow.Lock()
	c.dataOverflowLowSince = time.Now().Add(-2 * dataOverflowDrainDwell)
	c.muDataOverflow.Unlock()
	c.observeDataQueue()
	runOverflowPublicationsForTest(c)
	if len(*sent) != 1 || len(e.CongestionPublications()) != 0 {
		t.Fatal("foreign wire appearance manufactured a national clear")
	}
}
