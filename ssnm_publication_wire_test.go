// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// ssnmPublicationWireTranscript is the octets this scenario puts on the wire,
// recorded before SSNM subscription events became deltas. What a subscription
// delivers is local to this node, so changing it must not change one octet
// sent to a peer. Regenerate it deliberately, and read the diff, with
//
//	M3UA_UPDATE_SSNM_WIRE_TRANSCRIPT=1 go test -run TestSSNMPublicationLeavesTheWireUnchanged .
const ssnmPublicationWireTranscript = "testdata/ssnm-publication-wire.txt"

// TestSSNMPublicationLeavesTheWireUnchanged runs inbound SSNM through the
// dispatcher at an ASP -- valid reports in both dimensions, masked ranges, an
// unknown Routing Context, malformed and oversized messages, a report a
// retention bound refuses, and association loss -- and SGP fan-out and DAUD
// answers, with one subscriber draining and one stalled past its queue. Every
// message written, including the Errors the protocol requires, is compared
// octet for octet with the recorded transcript.
func TestSSNMPublicationLeavesTheWireUnchanged(t *testing.T) {
	var transcript []string
	record := func(label string, capture *distributionCapture) {
		t.Helper()
		for _, message := range capture.snapshot() {
			encoded, err := message.MarshalBinary()
			if err != nil {
				t.Fatalf("%s: marshal %s: %v", label, message.MessageTypeName(), err)
			}
			transcript = append(transcript, fmt.Sprintf("%s %s %s", label, message.MessageTypeName(), hex.EncodeToString(encoded)))
		}
		capture.reset()
	}
	drainSubscription := func(subscription *SSNMSubscription) {
		ready, cancel := context.WithCancel(context.Background())
		cancel()
		for {
			if _, err := subscription.Next(ready); err != nil {
				return
			}
		}
	}

	// ASP role.
	asp := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{
		SubscriptionQueueSize: 4, MaxAffectedPointCodes: 8, MaxRecordsPerPartition: 6,
	})
	aspDraining := mustSubscribeSSNM(t, asp)
	_ = mustSubscribeSSNM(t, asp) // stalled
	type peer struct {
		label       string
		association *Association
		capture     *distributionCapture
	}
	attach := func(label string, identity SGPIdentity, contexts ...uint32) peer {
		association := attachSSNMAssociation(t, asp, identity, 7, contexts...)
		capture := new(distributionCapture)
		association.signalWriter = capture.write
		return peer{label, association, capture}
	}
	a1 := attach("asp/sgp-a1", SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 1, 3)
	a2 := attach("asp/sgp-a2", SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 2)
	b1 := attach("asp/sgp-b1", SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}, 1)
	dispatch := func(target peer, message messages.M3UA) {
		t.Helper()
		// A message missing a mandatory parameter does not marshal; it is
		// dispatched without its octets, as the Error path permits.
		raw, err := message.MarshalBinary()
		if err != nil {
			raw = nil
		}
		target.association.handleReceivedSignals(context.Background(), message, raw)
		for drained := false; !drained; {
			select {
			case err := <-target.association.errChan:
				if closing := target.association.handleErrors(err); closing != nil {
					transcript = append(transcript, fmt.Sprintf("%s close %v", target.label, closing))
				}
			case <-target.association.stateChan:
			default:
				drained = true
			}
		}
		drainSubscription(aspDraining)
		record(target.label, target.capture)
	}
	pointCodes := func(codes ...uint32) *params.Param { return params.NewAffectedPointCode(codes...) }
	na := params.NewNetworkAppearance(7)
	rc := params.NewRoutingContext
	dispatch(a1, messages.NewDestinationUnavailable(na, rc(1), pointCodes(0x123456, 8<<24|0x123400), nil))
	dispatch(a1, messages.NewSignallingCongestion(na, rc(1), pointCodes(0x123456), nil, params.NewCongestionIndications(2), nil))
	dispatch(a2, messages.NewDestinationAvailable(na, rc(2), pointCodes(0x123456), nil))
	dispatch(b1, messages.NewDestinationRestricted(na, rc(1), pointCodes(0x223344), nil))
	dispatch(b1, messages.NewSignallingCongestion(na, rc(1), pointCodes(3<<24|0x223340), nil, nil, nil))
	dispatch(a1, messages.NewDestinationUserPartUnavailable(na, rc(3), pointCodes(0x123456), params.NewUserCause(1, 5), nil))
	dispatch(a1, messages.NewDestinationUnavailable(na, rc(99), pointCodes(0x123456), nil))
	dispatch(a1, messages.NewDestinationStateAudit(na, rc(1), pointCodes(0x123456), nil))
	dispatch(a1, messages.NewDestinationUnavailable(na, rc(1),
		pointCodes(0x100001, 0x100002, 0x100003, 0x100004, 0x100005, 0x100006, 0x100007, 0x100008, 0x100009), nil))
	dispatch(a2, messages.NewDestinationUnavailable(na, rc(2),
		pointCodes(0x200001, 0x200002, 0x200003, 0x200004, 0x200005, 0x200006, 0x200007), nil))
	dispatch(a1, messages.NewDestinationUnavailable(na, rc(1), nil, nil))
	dispatch(a1, messages.NewSignallingCongestion(na, rc(1), pointCodes(0x123456), nil, params.NewCongestionIndications(4), nil))
	asp.forgetAssociation(a1.association)
	dispatch(a2, messages.NewDestinationAvailable(na, rc(2), pointCodes(8<<24|0x123400), nil))
	dispatch(b1, messages.NewDestinationUnavailable(na, rc(1), pointCodes(0x223344), params.NewInfoString("audit")))

	// SGP role.
	sgp, first, firstSent, second, secondSent := multiAssociationDialedSGPFixture(t)
	sgpDraining := mustSubscribeSSNM(t, sgp)
	_ = mustSubscribeSSNM(t, sgp) // stalled
	flush := func() {
		drainSubscription(sgpDraining)
		record("sgp/first", firstSent)
		record("sgp/second", secondSent)
	}
	for _, availability := range []DestinationAvailability{DestinationUnavailable, DestinationRestricted, DestinationAvailable} {
		if err := sgp.ReportDestinationAvailability(DestinationAvailabilityRequest{
			Scope:        testWireScope(7, true, 1),
			Destinations: []PointCodeRange{{PointCode: 0x123456}, {PointCode: 0x123400, Mask: 8}},
			Availability: availability,
		}); err != nil {
			t.Fatalf("report %v: %v", availability, err)
		}
		flush()
	}
	if err := sgp.SignallingCongestion(SignallingCongestionRequest{
		Scope:           testWireScope(7, true, 2),
		Destinations:    []PointCodeRange{{PointCode: 0x123456}},
		CongestionLevel: 1, CongestionLevelSet: true,
	}); err != nil {
		t.Fatalf("congestion: %v", err)
	}
	flush()
	if err := sgp.DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest{
		Scope: testWireScope(7, true, 1), Destination: PointCodeRange{PointCode: 0x123456}, User: 3, Cause: 1,
	}); err != nil {
		t.Fatalf("DUPU: %v", err)
	}
	flush()
	if err := reportAvailability(sgp, testWireScope(7, true, 1), 0x123456, 0, DestinationUnavailable); err != nil {
		t.Fatalf("report before audit: %v", err)
	}
	flush()
	for _, audit := range []struct {
		association *Association
		context     uint32
		codes       *params.Param
	}{
		{first, 1, pointCodes(0x123456)},
		{first, 1, pointCodes(8<<24 | 0x123400)},
		{second, 2, pointCodes(0x123456, 0x654321)},
	} {
		if err := audit.association.handleDestinationStateAudit(
			messages.NewDestinationStateAudit(na, rc(audit.context), audit.codes, nil)); err != nil {
			t.Fatalf("DAUD: %v", err)
		}
		flush()
	}
	if err := first.handleSignallingCongestion(messages.NewSignallingCongestion(
		na, rc(1), pointCodes(0x123456), params.NewConcernedDestination(0x111111), params.NewCongestionIndications(1), nil)); err != nil {
		t.Fatalf("ASP SCON: %v", err)
	}
	flush()

	got := strings.Join(transcript, "\n") + "\n"
	if os.Getenv("M3UA_UPDATE_SSNM_WIRE_TRANSCRIPT") != "" {
		if err := os.WriteFile(ssnmPublicationWireTranscript, []byte(got), 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %d wire lines", len(transcript))
		return
	}
	want, err := os.ReadFile(ssnmPublicationWireTranscript)
	if err != nil {
		t.Fatal(err)
	}
	wantLines := strings.Split(strings.TrimSpace(strings.ReplaceAll(string(want), "\r\n", "\n")), "\n")
	if len(wantLines) < 10 {
		t.Fatalf("the recorded transcript holds only %d messages", len(wantLines))
	}
	for index := range max(len(wantLines), len(transcript)) {
		var gotLine, wantLine string
		if index < len(transcript) {
			gotLine = transcript[index]
		}
		if index < len(wantLines) {
			wantLine = wantLines[index]
		}
		if gotLine != wantLine {
			t.Fatalf("wire message %d differs:\n got %s\nwant %s", index, gotLine, wantLine)
		}
	}
}
