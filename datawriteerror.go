// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"os"
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
	// space leaves nothing behind. Nothing of the message can be on the wire,
	// so the application may resend it — to this association or another —
	// without risking a duplicate.
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
// from WriteData or from WriteSignal carrying a DATA message.
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
// whole. sctp_sendmsg queues a message whole or not at all, and the SCTP
// dependency documents the same, so a send refused for want of send-buffer
// space -- EAGAIN without a write deadline, or a write deadline that expired
// while the send waited for space -- leaves nothing of the message queued, let
// alone on the wire. Reporting that as indeterminate told the application a
// resend might duplicate exactly when backpressure made resending the right
// thing to do.
func newDataSubmissionError(key ASKey, stream uint16, cause error) *DataWriteError {
	if transportRefusedWhole(cause) {
		return newDataNotSent(key, stream, cause)
	}
	return newDataSendIndeterminate(key, stream, cause)
}

// transportRefusedWhole reports a transport error that means the SCTP stack
// refused the whole message rather than failing partway.
func transportRefusedWhole(err error) bool {
	return sendBufferFull(err) || errors.Is(err, os.ErrDeadlineExceeded)
}
