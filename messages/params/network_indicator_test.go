// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-pointcode"
)

// The codec preserves all octets; RFC 4666 §3.3.1 constrains sends, without
// defining a receive-side error for NI bits outside the SS7 NI field.
func TestNetworkIndicatorCodecAllOctets(t *testing.T) {
	for ni := 0; ni < 256; ni++ {
		t.Run(fmt.Sprint(ni), func(t *testing.T) {
			payload := params.NewProtocolDataPayload(1, 2, 3, pointcode.NetworkIndicator(ni), 1, 5, []byte{1, 2, 3})
			raw, err := payload.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if raw[9] != byte(ni) {
				t.Fatalf("encoded NI=%d, want %d", raw[9], ni)
			}
			parsed, err := params.ParseProtocolDataPayload(raw)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.NetworkIndicator != pointcode.NetworkIndicator(ni) {
				t.Fatalf("decoded NI=%d, want %d", parsed.NetworkIndicator, ni)
			}
			again, err := parsed.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, again) {
				t.Fatal("round trip changed octets")
			}
			if !strings.Contains(parsed.String(), fmt.Sprintf("NetworkIndicator: %d,", ni)) {
				t.Fatalf("String lost numeric NI: %s", parsed)
			}
		})
	}
}
