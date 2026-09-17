// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The dispatch goroutine owns a decoded message and goes on using it; the
// monitor goroutine renders the ERR. Rendering must not write into the message,
// which is what marshalling it would do.
func TestUnexpectedMessageErrorRenderingIsRaceFree(t *testing.T) {
	const messageCount = 64

	conn, _ := newTestConn(t, StateASPActive, RoleSGP)

	var rendered sync.WaitGroup
	rendered.Add(1)
	go func() {
		defer rendered.Done()
		for i := 0; i < messageCount; i++ {
			if err := conn.handleErrors(<-conn.errChan); err != nil {
				return
			}
		}
	}()

	for i := 0; i < messageCount; i++ {
		msg := messages.NewAspUp(params.NewAspIdentifier(uint32(i)), nil)
		received, err := msg.MarshalBinary()
		if err != nil {
			t.Errorf("marshal: %v", err)
			return
		}
		conn.sendErrForMessage(msg, received, NewUnexpectedMessageError(msg))
		// The dispatcher keeps working with both the message and the buffer it
		// reported from while the responder is still rendering.
		for j := 0; j < 4; j++ {
			if _, err := msg.MarshalBinary(); err != nil {
				t.Errorf("dispatcher marshal: %v", err)
				return
			}
		}
		for j := range received {
			received[j] ^= 0xff
		}
	}
	rendered.Wait()
}

// The octets quoted are the ones received, and the error owns them: the
// dispatcher reuses its buffer immediately afterwards.
func TestUnexpectedMessageErrorQuotesTheReceivedOctets(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	msg := messages.NewAspUp(params.NewAspIdentifier(0x11223344), nil)
	received, err := msg.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	want := bytes.Clone(received)
	if len(want) > 40 {
		want = want[:40]
	}

	unexpected := NewUnexpectedMessageError(msg)
	conn.sendErrForMessage(msg, received, unexpected)
	<-conn.errChan

	for i := range received {
		received[i] ^= 0xff
	}

	got := first40(unexpected.Raw)
	if len(got) == 0 {
		t.Fatalf("Diagnostic Information octets are empty, want the received message")
	}
	if !bytes.Equal(got, want) {
		t.Errorf("quoted octets = % x, want % x", got, want)
	}
}

// RFC 4666 Section 4.3.4.6 makes Heartbeat Data optional, so a peer may send a
// BEAT Ack without it. Such a message reaches the error path with no Header,
// and reporting it must not crash the process.
func TestUnexpectedMessageErrorForPartlyPopulatedMessageDoesNotPanic(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panic reporting a partly populated peer message: %v", r)
		}
	}()
	if err := conn.handleErrors(NewUnexpectedMessageError(&messages.HeartbeatAck{})); err != nil {
		t.Logf("handleErrors: %v", err)
	}
}

// The snapshot is taken from the dispatcher's buffer, which the dispatcher goes
// on using, so it must be a copy and not a view of it.
func TestReportedErrorsDoNotAliasTheDispatchersBuffer(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	msg := messages.NewAspUp(params.NewAspIdentifier(1), nil)
	received := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}

	unexpected := NewUnexpectedMessageError(msg)
	conn.sendErrForMessage(msg, received, unexpected)
	<-conn.errChan

	snapshots := map[string][]byte{
		"unexpected message":  unexpected.Raw,
		"unsupported class":   newUnsupportedClassErrorForMessage(msg, received).Raw,
		"unsupported message": newUnsupportedMessageErrorForMessage(msg, received).Raw,
	}
	for name, snapshot := range snapshots {
		t.Run(name, func(t *testing.T) {
			before := string(snapshot)
			for i := range received {
				received[i] ^= 0xff
			}
			if string(snapshot) != before {
				t.Errorf("snapshot followed the dispatcher's buffer: % x, want % x", snapshot, before)
			}
			for i := range received {
				received[i] ^= 0xff
			}
		})
	}
}

// Octets already attached are the ones closest to the fault. A later report of
// the same error must not overwrite them with a different message's octets.
func TestAttachedOctetsAreNotOverwritten(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	msg := messages.NewAspUp(params.NewAspIdentifier(1), nil)

	first := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}
	second := []byte{0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef}

	unexpected := NewUnexpectedMessageError(msg)
	conn.sendErrForMessage(msg, first, unexpected)
	<-conn.errChan
	conn.sendErrForMessage(msg, second, unexpected)
	<-conn.errChan

	if !bytes.Equal(unexpected.Raw, first) {
		t.Errorf("attached octets = % x, want the first report's % x", unexpected.Raw, first)
	}
}
