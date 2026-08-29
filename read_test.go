// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"io"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Association.Read copied as much of the DATA payload as fitted but returned the
// payload's full length, so a caller whose buffer was smaller than the message
// was told it had received more bytes than its buffer holds. The idiomatic
//
//	n, _ := conn.Read(buf)
//	handle(buf[:n])
//
// then panicked with a slice-bounds error — on a length the remote peer chooses.
// The payload beyond the buffer was gone either way, since the message had
// already been taken off the queue.
func TestReadIntoShortBufferReportsWhatItWrote(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	payload := []byte("0123456789")
	conn.dataChan <- &DataMessage{ProtocolData: &params.ProtocolDataPayload{Data: payload}}

	buf := make([]byte, 4)
	n, err := conn.Read(buf)

	if n > len(buf) {
		t.Fatalf("Read returned n = %d for a %d-byte buffer: buf[:n] panics", n, len(buf))
	}
	if n != len(buf) {
		t.Errorf("Read returned n = %d, want %d (the buffer was filled)", n, len(buf))
	}
	if !errors.Is(err, io.ErrShortBuffer) {
		t.Errorf("Read error = %v, want io.ErrShortBuffer: truncation must be reported, not silent", err)
	}
	if got := string(buf[:n]); got != "0123" {
		t.Errorf("Read wrote %q, want %q", got, "0123")
	}
}

// A buffer large enough for the payload must behave exactly as before: the full
// payload, no error.
func TestReadIntoAdequateBufferIsUnaffected(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bufSize int
	}{
		{"exact fit", 10},
		{"room to spare", 64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)

			payload := []byte("0123456789")
			conn.dataChan <- &DataMessage{ProtocolData: &params.ProtocolDataPayload{Data: payload}}

			buf := make([]byte, tc.bufSize)
			n, err := conn.Read(buf)
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			if n != len(payload) {
				t.Errorf("Read returned n = %d, want %d", n, len(payload))
			}
			if got := string(buf[:n]); got != string(payload) {
				t.Errorf("Read wrote %q, want %q", got, payload)
			}
		})
	}
}

// An empty DATA payload must read as zero bytes and no error, not as a short
// buffer: nothing was truncated.
func TestReadOfEmptyPayloadIsNotShortBuffer(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	conn.dataChan <- &DataMessage{ProtocolData: &params.ProtocolDataPayload{Data: nil}}

	n, err := conn.Read(make([]byte, 0))
	if err != nil {
		t.Errorf("Read of an empty payload returned %v, want nil", err)
	}
	if n != 0 {
		t.Errorf("Read returned n = %d, want 0", n)
	}
}

// ReadPD is the way to take a payload whole, and must be unaffected by any of
// the above: it never sizes a buffer.
func TestReadPDReturnsTheWholePayload(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	payload := []byte("0123456789")
	conn.dataChan <- &DataMessage{ProtocolData: &params.ProtocolDataPayload{Data: payload}}

	pd, err := conn.ReadPD()
	if err != nil {
		t.Fatalf("ReadPD: %v", err)
	}
	if got := string(pd.Data); got != string(payload) {
		t.Errorf("ReadPD returned %q, want %q", got, payload)
	}
}
