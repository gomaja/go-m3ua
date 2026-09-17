// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// M3UA is message-oriented, so a read yields one DATA payload whole.
//
// The byte-stream read this replaced copied as much of the payload as fitted
// the caller's buffer but reported the payload's full length, so the idiomatic
// buf[:n] panicked on a length the remote peer chose. Truncation is now
// structurally impossible: the payload is handed over as it arrived, and there
// is no buffer for it to be too large for.
func TestReadDataDeliversThePayloadWhole(t *testing.T) {
	for _, test := range []struct {
		name    string
		payload []byte
	}{
		{name: "ten octets", payload: []byte("0123456789")},
		{name: "larger than any buffer a caller would guess", payload: make([]byte, 9000)},
		{name: "empty", payload: nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			conn.dataChan <- &DataMessage{ProtocolData: &params.ProtocolDataPayload{Data: test.payload}}

			message, err := conn.ReadData(context.Background())
			if err != nil {
				t.Fatalf("ReadData: %v", err)
			}
			if got := len(message.ProtocolData.Data); got != len(test.payload) {
				t.Errorf("ReadData returned %d octets, want %d", got, len(test.payload))
			}
			if string(message.ProtocolData.Data) != string(test.payload) {
				t.Errorf("ReadData returned %q, want %q", message.ProtocolData.Data, test.payload)
			}
		})
	}
}
