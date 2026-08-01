// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages_test

import (
	"bytes"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// unknownTag is outside every parameter this version defines. RFC 4666 Section
// 3.2 reserves 0x0200 to 0x02ff for M3UA, and nothing in that block is assigned
// this high, so it stands in for a parameter a later version might add.
const unknownTag = 0x02fe

// TestUnknownParameterDoesNotDiscardTheMessage covers the forward-compatibility
// rule stated in the introduction to RFC 4666 Section 3:
//
//	The general M3UA message format includes a Common Message Header
//	followed by zero or more parameters as defined by the Message Type.
//	For forward compatibility, all Message Types may have attached
//	parameters even if none are specified in this version.
//
// Rejecting the whole message on an unrecognised tag makes this implementation
// unable to talk to any peer that adopts a later extension, and the parameters
// it did understand are thrown away with it.
func TestUnknownParameterDoesNotDiscardTheMessage(t *testing.T) {
	aspUp := messages.NewAspUp(params.NewAspIdentifier(0x11223344), nil)
	b, err := aspUp.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	// Append a parameter from the future and fix up the header length.
	extra, err := params.NewParam(unknownTag, []byte{0xde, 0xad, 0xbe, 0xef}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal extra: %v", err)
	}
	b = append(b, extra...)
	b[4], b[5], b[6], b[7] = byte(len(b)>>24), byte(len(b)>>16), byte(len(b)>>8), byte(len(b))

	msg, err := messages.Parse(b)
	if err != nil {
		t.Fatalf("an ASP Up carrying an unrecognised parameter was rejected: %v", err)
	}
	up, ok := msg.(*messages.AspUp)
	if !ok {
		t.Fatalf("parsed as %T, want *messages.AspUp", msg)
	}
	if up.AspIdentifier == nil {
		t.Fatal("the ASP Identifier was discarded along with the unknown parameter")
	}
	if got := up.AspIdentifier.AspIdentifier(); got != 0x11223344 {
		t.Errorf("ASP Identifier = %#x, want 0x11223344", got)
	}
	if len(up.Others) != 1 || up.Others[0].Tag != unknownTag {
		t.Errorf("Others = %v, want the one unrecognised parameter", up.Others)
	}
}

// TestBeatWithAnUnknownParameterIsStillAcked covers RFC 4666 Section 3.5.5:
//
//	The receiver MUST respond with a BEAT Ack message.
//
// and Section 3.5.6, which fixes what the Ack must contain:
//
//	The BEAT Ack message is sent in response to a received BEAT message.
//	It includes all the parameters of the received BEAT message, without
//	any change.
//
// The requirement is unqualified, so a parameter the receiver does not
// recognise cannot be grounds for withholding the Ack — and since the Ack
// echoes everything, the parameter has to survive decoding to be echoed.
func TestBeatWithAnUnknownParameterIsStillAcked(t *testing.T) {
	beat := messages.NewHeartbeat(params.NewHeartbeatData([]byte("keepalive")))
	b, err := beat.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	extra, err := params.NewParam(unknownTag, []byte{0x01, 0x02, 0x03, 0x04}).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal extra: %v", err)
	}
	b = append(b, extra...)
	b[4], b[5], b[6], b[7] = byte(len(b)>>24), byte(len(b)>>16), byte(len(b)>>8), byte(len(b))

	msg, err := messages.Parse(b)
	if err != nil {
		t.Fatalf("a BEAT carrying an unrecognised parameter was rejected: %v", err)
	}
	got, ok := msg.(*messages.Heartbeat)
	if !ok {
		t.Fatalf("parsed as %T, want *messages.Heartbeat", msg)
	}
	if got.HeartbeatData == nil {
		t.Fatal("Heartbeat Data was discarded")
	}

	// The echo must carry everything back, so re-marshalling has to reproduce
	// the unknown parameter as well as the known one.
	out, err := got.MarshalBinary()
	if err != nil {
		t.Fatalf("re-marshal: %v", err)
	}
	if !bytes.Contains(out, extra) {
		t.Errorf("the re-marshalled BEAT dropped the unrecognised parameter;\n"+
			"Section 3.5.6 requires the Ack to include all parameters "+
			"\"without any change\"\n got % x\n want it to contain % x", out, extra)
	}
}

// TestDuplicateParameterIsRejected covers RFC 4666 Section 3.2:
//
//	Unless explicitly stated or shown in a message format diagram, only
//	one parameter of the same type is allowed in a message.
//
// Taking the last occurrence silently is what lets a peer append a second
// Routing Context after one that would pass validation, and have the second be
// the one that is acted on.
func TestDuplicateParameterIsRejected(t *testing.T) {
	act := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1),
		nil,
	)
	b, err := act.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	second, err := params.NewRoutingContext(999).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal second Routing Context: %v", err)
	}
	b = append(b, second...)
	b[4], b[5], b[6], b[7] = byte(len(b)>>24), byte(len(b)>>16), byte(len(b)>>8), byte(len(b))

	msg, err := messages.Parse(b)
	if err == nil {
		act, ok := msg.(*messages.AspActive)
		if ok && act.RoutingContext != nil {
			t.Errorf("a second Routing Context was accepted; the message decoded "+
				"with contexts %v", act.RoutingContext.RoutingContexts())
		} else {
			t.Error("a message with two Routing Context parameters was accepted")
		}
	}
}
