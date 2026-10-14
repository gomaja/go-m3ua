// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"io"
	"testing"
)

func TestContextSendsWithNilDoneKeepOrdinaryOutcomes(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	for _, variant := range []struct {
		name string
		ctx  context.Context
	}{
		{"background", context.Background()},
		{"todo", context.TODO()},
		{"without cancel", context.WithoutCancel(parent)},
	} {
		t.Run(variant.name, func(t *testing.T) {
			t.Run("WriteDataContext", func(t *testing.T) {
				c, _ := newDataWriteAssociation(t, 1)
				c.dataWriter = nil
				transport := newContextSendBuffer(c)
				transport.failure = io.ErrUnexpectedEOF
				c.controlTransport = transport
				n, err := c.WriteDataContext(variant.ctx, allocationRequest(128))
				if n != 0 {
					t.Fatalf("octets = %d, want 0", n)
				}
				// The ordinary SendMsg contract cannot exclude acceptance on
				// an unknown error; using SendMsgContext would report NotSent.
				requireDataWriteError(t, err, DataSendIndeterminate, io.ErrUnexpectedEOF)
			})
			t.Run("MTPTransferContext", func(t *testing.T) {
				config := validASPConfig()
				config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
				e, associations, _ := newASPTransferFixture(t, config)
				c := associations["sg-a/sgp-a1"]
				c.dataWriter = nil
				transport := newContextSendBuffer(c)
				transport.failure = io.ErrUnexpectedEOF
				c.controlTransport = transport
				result, err := e.MTPTransferContext(variant.ctx, MTPTransferRequest{
					ProtocolData: transferProtocolData(0x123456, 1, []byte("ordinary")),
				})
				if result.UserDataOctets != 0 || len(result.SuccessfulPaths) != 0 {
					t.Fatalf("unknown acceptance reported success: %#v", result)
				}
				requireDataWriteError(t, err, DataSendIndeterminate, io.ErrUnexpectedEOF)
			})
		})
	}
}
