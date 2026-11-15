// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

// DataSendOutcome is what a failed DATA send leaves behind, and therefore what
// the application may safely do next.
//
// M3UA has no acknowledgement for DATA: RFC 4666 Section 3.3.1 defines the
// message and no response to it, so the library can never report delivery. What
// it can report is whether the message reached the transport at all, which is
// the difference between a resend that duplicates SS7 traffic and one that
// recovers it.
type DataSendOutcome uint8

const (
	// DataNotSent means the send was refused before any octet was submitted to
	// the transport, or the transport refused the whole message: SCTP queues a
	// message whole or not at all, so a send refused for want of send-buffer
	// space or message-allocation memory leaves nothing behind. Nothing of
	// the message can be on the wire, so the application may resend it — to
	// this association or another — without risking a duplicate.
	DataNotSent DataSendOutcome = iota + 1

	// DataSendIndeterminate means submission had begun when the failure was
	// detected. The transport may have put some or all of the message on the
	// wire, so the library refuses to claim otherwise: a resend may duplicate
	// the message, and the application owns that decision.
	DataSendIndeterminate
)

// String names the outcome.
func (o DataSendOutcome) String() string {
	switch o {
	case DataNotSent:
		return "not sent"
	case DataSendIndeterminate:
		return "indeterminate"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(o))
	}
}

// DataWriteError is the error every failed DATA send reports, whether it came
// from WriteData, WriteDataContext, an Endpoint-selected MTP-TRANSFER path,
// or WriteSignal carrying a DATA message.
//
// It classifies the failure without hiding it: Unwrap returns the cause, so
// errors.Is and errors.As reach the sentinel, the *RoutingContextError, the
// *InvalidSCTPStreamIDError or the transport's own error exactly as before.
type DataWriteError struct {
	// Outcome is what is known about the message's fate.
	Outcome DataSendOutcome
	// AS is the exact Application Server scope the message named.
	AS ASKey
	// Stream is the SCTP stream the message was to travel on, or zero when the
	// failure was decided before a stream was chosen.
	Stream uint16
	// Err is the cause.
	Err error
}

func (e *DataWriteError) Error() string {
	return fmt.Sprintf("m3ua: DATA write failed (%s): %v", e.Outcome, e.Err)
}

// Unwrap keeps the cause matchable.
func (e *DataWriteError) Unwrap() error {
	return e.Err
}

// newDataNotSent reports a refusal decided before submission began.
func newDataNotSent(key ASKey, stream uint16, cause error) *DataWriteError {
	return &DataWriteError{Outcome: DataNotSent, AS: key, Stream: stream, Err: cause}
}

// newDataSendIndeterminate reports a failure detected after submission began.
func newDataSendIndeterminate(key ASKey, stream uint16, cause error) *DataWriteError {
	return &DataWriteError{Outcome: DataSendIndeterminate, AS: key, Stream: stream, Err: cause}
}

// newDataSubmissionError classifies a failure of the transport write itself.
//
// Such a failure is indeterminate unless the transport refused the message
// whole. SendMsg queues a message whole or not at all, and the SCTP
// dependency documents the same, so allocation refusal or a send refused for
// want of send-buffer space -- EAGAIN from NoWait without a write deadline,
// or a write deadline that expired while the send waited for space -- leaves
// nothing of the message queued, let alone on the wire. A whole refusal permits
// retry without duplicating this message. Shutdown and closed-transport
// refusals also queue nothing.
func newDataSubmissionError(key ASKey, stream uint16, cause error) *DataWriteError {
	if transportRefusedWhole(cause) {
		return newDataNotSent(key, stream, cause)
	}
	return newDataSendIndeterminate(key, stream, cause)
}

// transportRefusedWhole reports a transport error that means the SCTP stack
// refused the whole message rather than failing partway.
func transportRefusedWhole(err error) bool {
	// RFC 9260 Section 9.2 refuses new data during shutdown. Linux's
	// sctp_sendmsg_to_asoc checks buffer space before allocating the message;
	// PRIMITIVE_SEND in the shutdown states calls sctp_sf_error_shutdown
	// (net/sctp/socket.c, sm_statetable.c and sm_statefuns.c in Linux v5.14).
	// go-sctp SendMsg also maps an EAGAIN in those states, or a locally woken
	// buffer wait, to ESHUTDOWN. EPIPE after association removal and
	// net.ErrClosed from descriptor release likewise queue nothing. SCTP's
	// atomic send contract is RFC 6458 Section 3.1.4 with Errata 6111;
	// callers classify a positive short write separately before reaching
	// this function.
	//
	// ENOMEM is also prequeue on the ordinary single-association send path:
	// Linux v5.0 net/sctp/socket.c:1890-1973, v5.14:1801-1887 and
	// v7.4-rc2:1827-1937 allocate the stream/datamsg before primitive SEND.
	// Stream allocation is stream.c:255-265 /166-183 /173-190; datamsg
	// construction and private-fragment cleanup is chunk.c:163-314 /146-298
	// /146-298, respectively. SEND emits only SEND_MSG (sm_statefuns.c:
	// 4908-4919 /5044-5055 /5135-5146). Its corked queue/void-flush path
	// has no later error return (sm_sideeffect.c:1779-1828 /1768-1817
	// /1782-1831; outqueue.c:767-773 /753-759 /760-766).
	// Later transmit errors may set sk_err but this call returns msg_len.
	//
	// go-sctp 2d7d4d9 send_linux.go:273-294,376-479 and
	// syscall_linux_other.go:147-156 normalize syscall errors to (0, err),
	// and preserve a positive short count with io.ErrShortWrite. That zero
	// alone is not the proof: the kernel control flow above is. SENDALL can
	// queue to earlier associations before a later error; it and explicit
	// EOR are excluded by msginfo.go:145-149,233-239. MSG_MORE only bundles.
	// Unlisted causes are not classified here; positive error counts are
	// handled separately and remain indeterminate.
	return sendBufferFull(err) || errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, syscall.EMSGSIZE) ||
		errors.Is(err, syscall.ENOMEM) || errors.Is(err, syscall.ESHUTDOWN) || errors.Is(err, syscall.EPIPE) || errors.Is(err, net.ErrClosed)
}
