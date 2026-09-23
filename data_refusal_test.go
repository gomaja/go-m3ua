// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// transportRefusals are the transport errors that mean the SCTP stack refused
// the whole message: sctp_sendmsg queues a message whole or not at all, so
// nothing of a refused one is queued or on the wire.
var transportRefusals = []struct {
	name  string
	err   error
	cause error
}{
	{"a full send buffer", syscall.EAGAIN, syscall.EAGAIN},
	{"a full send buffer reported as EWOULDBLOCK", syscall.EWOULDBLOCK, syscall.EWOULDBLOCK},
	// A write deadline the send waited out: the last attempt was refused
	// before the wait began. SCTPWrite returns the poller's error unwrapped;
	// both shapes are covered so a wrapped one is classified the same way.
	{"an expired write deadline", os.ErrDeadlineExceeded, os.ErrDeadlineExceeded},
	{"an expired write deadline, wrapped", &net.OpError{Op: "write", Net: "sctp", Err: os.ErrDeadlineExceeded}, os.ErrDeadlineExceeded},
}

func TestWriteDataReportsATransportRefusalAsNotSent(t *testing.T) {
	for _, refusal := range transportRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			conn, capture := newDataWriteAssociation(t, 1)
			capture.err = refusal.err
			_, err := conn.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("refused")})
			requireDataWriteError(t, err, DataNotSent, refusal.cause)
			if capture.submissions() != 1 {
				t.Errorf("the message was submitted %d times, want exactly 1 with no retry", capture.submissions())
			}
		})
	}
}

func TestWriteSignalOfDataReportsATransportRefusalAsNotSent(t *testing.T) {
	for _, refusal := range transportRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			conn, capture := newDataWriteAssociation(t, 1)
			conn.signalWriter = nil
			capture.err = refusal.err
			_, err := conn.WriteSignal(messages.NewData(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("refused")),
				nil,
			))
			requireDataWriteError(t, err, DataNotSent, refusal.cause)
		})
	}
}

// The signal seam stands in for the transport in the tests that observe
// decoded messages; a refusal it reports is classified like the transport's.
func TestWriteSignalSeamReportsATransportRefusalAsNotSent(t *testing.T) {
	for _, refusal := range transportRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			conn, _ := newDataWriteAssociation(t, 1)
			conn.signalWriter = func(messages.M3UA) (int, error) { return 0, refusal.err }
			_, err := conn.WriteSignal(messages.NewData(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("refused")),
				nil,
			))
			requireDataWriteError(t, err, DataNotSent, refusal.cause)
		})
	}
}

func TestMTPTransferReportsATransportRefusalAsNotSent(t *testing.T) {
	const pointCode = uint32(0x123456)
	for _, refusal := range transportRefusals {
		t.Run(refusal.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })
			identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
			association, capture := attachMultiScopeASPAssociation(t, endpoint, identity, 7, 1, 2)
			applyASPDAVA(t, association, 7, 1, pointCode, 0)
			applyASPDAVA(t, association, 7, 2, pointCode, 0)
			capture.writeErr = refusal.err

			_, err = endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, []byte("x"))})
			var transferErr *MTPTransferError
			if !errors.As(err, &transferErr) || len(transferErr.Failures) != 1 {
				t.Fatalf("transfer error = %v, want one failure", err)
			}
			requireDataWriteError(t, transferErr.Failures[0].Err, DataNotSent, refusal.cause)
		})
	}
}

// Everything else the transport reports stays indeterminate: the message was
// submitted and the library cannot say how much of it left.
func TestOtherTransportFailuresStayIndeterminate(t *testing.T) {
	for _, failure := range []error{syscall.EPIPE, syscall.ECONNRESET, errors.New("unclassified transport failure")} {
		conn, capture := newDataWriteAssociation(t, 1)
		capture.err = failure
		_, err := conn.WriteData(DataRequest{AS: writeScope(1), ProtocolData: simpleProtocolData("submitted")})
		requireDataWriteError(t, err, DataSendIndeterminate, failure)
	}
}
