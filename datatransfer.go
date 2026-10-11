// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// DataRequest is one outbound DATA message.
//
// Everything the message carries is in the request, because RFC 4666 Section
// 3.3.1 makes all of it per-message: the Protocol Data holds "the MTP3 routing
// label" and the user part payload, and the Routing Context "contains the
// Routing Context value associated with the DATA message". Nothing is completed
// from association-wide state, so concurrent senders on one association cannot
// take each other's scope or routing label.
type DataRequest struct {
	// AS is the exact Application Server scope this message belongs to: the
	// Network Appearance and Routing Context, each with its own presence flag
	// because zero is a legitimate value for either. It must name a scope this
	// association is configured and authorized to carry.
	AS ASKey

	// ProtocolData is the complete MTP3 routing label and user octets. The
	// Data slice is read during the call and never retained.
	ProtocolData params.ProtocolDataPayload

	// Stream selects the SCTP stream. Zero asks for the stream this message's
	// own Signalling Link Selection maps to, which is what keeps one SLS in
	// sequence; an explicit value is validated against the negotiated stream
	// count. RFC 4666 Section 1.4.7 rule 1 forbids stream 0 for DATA either way.
	Stream uint16

	// CorrelationID carries the Section 3.3.1 Correlation Id parameter for this
	// one message. CorrelationIDSet distinguishes an explicit zero from an
	// omitted parameter.
	CorrelationID    uint32
	CorrelationIDSet bool
}

const (
	// m3uaVersionRelease1 is the Version field of RFC 4666 Section 3.1.1:
	// "1 Release 1.0".
	m3uaVersionRelease1 = 1
	// commonHeaderOctets is the Section 3.1 common header.
	commonHeaderOctets = 8
	// parameterHeaderOctets is the Tag and Length of Section 3.2.
	parameterHeaderOctets = 4
	// uint32ParameterOctets is a whole 32-bit parameter, header included.
	uint32ParameterOctets = parameterHeaderOctets + 4
	// routingLabelOctets is the fixed part of Protocol Data: OPC, DPC, SI, NI,
	// MP and SLS (Section 3.3.1).
	routingLabelOctets = 12
	// maxProtocolDataOctets is the largest user payload one DATA can carry. The
	// Section 3.2 Length field is 16 bits and covers the whole parameter.
	maxProtocolDataOctets = int(^uint16(0)) - parameterHeaderOctets - routingLabelOctets
)

// WriteData sends one DATA message and reports the SS7 user octets it carried.
//
// Success means the local transport accepted the message, not that the peer
// received it: RFC 4666 defines no acknowledgement for DATA. Every failure is a
// *DataWriteError whose Outcome says whether the message can safely be sent
// again; the cause remains matchable with errors.Is and errors.As.
//
// The checks are, in order: the association state, which Section 4.3.1 defines
// as ASP-ACTIVE — "the remote M3UA peer at the ASP/IPSP is available and
// application traffic is active (for a particular Routing Context or set of
// Routing Contexts)" — then the structure of the message, the stream
// constraints of Section 1.4.7, and the Application Server binding with its
// activation. Destination availability is deliberately not among them: an
// application that owns outbound selection has already made that decision, and
// SSNM state is published to it separately.
func (c *Association) WriteData(request DataRequest) (int, error) {
	return c.WriteDataContext(context.Background(), request)
}

// WriteDataContext sends one DATA message with cancellation for this call.
// It observes ctx before admission, while waiting for Application Server and
// association delivery barriers, while waiting behind any sender on the socket,
// and while waiting for send-buffer space under an association write deadline.
// Without a write deadline, a full buffer still returns EAGAIN immediately.
// It neither changes socket deadlines nor cancels other sends.
//
// A cancellation before any octet is accepted returns zero and a *DataWriteError
// with Outcome DataNotSent and a cause matching ctx.Err() through errors.Is.
// The call retains no pending submission after returning DataNotSent. A failure
// after acceptance may have begun has Outcome DataSendIndeterminate: resending
// could duplicate DATA. A completed acceptance returns success even if ctx is
// cancelled concurrently; success says nothing about delivery to the peer
// (RFC 4666 §3.3.1). The transport's single nonblocking attempt must finish
// before the call can determine its outcome.
//
// ctx must be non-nil. WriteData uses context.Background().
func (c *Association) WriteDataContext(ctx context.Context, request DataRequest) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, newDataNotSent(request.AS, 0, err)
	}
	if !c.outboundDataActive() {
		return 0, newDataNotSent(request.AS, 0, ErrNotEstablished)
	}
	if len(request.ProtocolData.Data) > maxProtocolDataOctets {
		return 0, newDataNotSent(request.AS, 0, ErrProtocolDataTooLarge)
	}

	stream := request.Stream
	if stream == 0 {
		// Section 1.4.7: "MTP3-User traffic may be assigned to individual
		// streams based on, for example, the SLS value in the MTP3 Routing
		// Label, subject of course to the maximum number of streams supported
		// by the underlying SCTP association."
		stream = c.streamFor(request.ProtocolData.SignallingLinkSelection)
	}
	if err := c.checkDataStream(stream); err != nil {
		return 0, newDataNotSent(request.AS, request.Stream, err)
	}

	// Encoded before admission so the barrier the send holds at an SGP is held
	// only across the transport write, not across marshalling.
	frame := c.encodeDataFrame(&request)

	release, err := c.admitDataWriteContext(ctx, request.AS)
	if err != nil {
		return 0, newDataNotSent(request.AS, stream, err)
	}
	defer release()

	if err := c.submitDataContext(ctx, frame, request.AS, stream); err != nil {
		return 0, err
	}
	return len(request.ProtocolData.Data), nil
}

// submitDataContext owns the socket gate across submission. Every other
// SendMsg takes the same gate, so NoWait cannot queue on go-sctp's send mutex.
func (c *Association) submitDataContext(ctx context.Context, frame []byte, key ASKey, stream uint16) error {
	if err := c.sendMu.LockContext(ctx); err != nil {
		return newDataNotSent(key, stream, err)
	}
	defer c.sendMu.Unlock()
	// Copied by value: the template is shared by every send on this
	// association, and only the stream varies per message.
	info := *c.sctpInfo
	info.Stream = stream
	if ctx.Done() == nil {
		written, err := c.writeSCTPData(frame, &info)
		if result := dataSubmissionResult(key, stream, len(frame), written, err); result != nil {
			return result
		}
		return nil
	}

	waitForSpace := c.applicationWriteDeadline.Load() != nil
	var retry *time.Timer
	defer func() {
		if retry != nil {
			retry.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return newDataNotSent(key, stream, err)
		}
		if deadline := c.applicationWriteDeadline.Load(); deadline != nil && !time.Now().Before(*deadline) {
			return newDataNotSent(key, stream, os.ErrDeadlineExceeded)
		}
		var written int
		var err error
		if c.dataWriter != nil {
			written, err = c.dataWriter(frame, &info)
		} else {
			written, err = c.controlSender().SendMsg(frame, sctp.SendOptions{Info: &info, NoWait: true})
		}
		if written != 0 || !sendBufferFull(err) || !waitForSpace {
			// Once an attempt succeeds, cancellation cannot retract it. On an
			// error retain both causes without turning uncertainty into a
			// promise that nothing was submitted.
			result := dataSubmissionResult(key, stream, len(frame), written, err)
			if result != nil && ctx.Err() != nil {
				result.Err = errors.Join(result.Err, ctx.Err())
			}
			if result != nil {
				return result
			}
			return nil
		}
		// NoWait refused the whole message (RFC 6458 §3.1.4). go-sctp has
		// no per-call poller wake-up, so the uncommon deadline-bound context
		// path retries at 1 ms intervals. The timer is reused, and cancellation
		// wakes this wait directly. There is no detached send to finish later.
		if retry == nil {
			retry = time.NewTimer(time.Millisecond)
		} else {
			retry.Reset(time.Millisecond)
		}
		select {
		case <-ctx.Done():
			return newDataNotSent(key, stream, ctx.Err())
		case <-c.done:
			return newDataNotSent(key, stream, errors.Join(ErrAssociationClosed, ctx.Err()))
		case <-retry.C:
		}
	}
}

func dataSubmissionResult(key ASKey, stream uint16, size, written int, err error) *DataWriteError {
	if err != nil {
		if written > 0 {
			return newDataSendIndeterminate(key, stream, err)
		}
		return newDataSubmissionError(key, stream, err)
	}
	if written != size {
		// RFC 4666 §3.3.1 defines one DATA message; a partially accepted
		// SCTP user message cannot be completed by sending the remainder.
		return newDataSendIndeterminate(key, stream, ErrPartialDataWrite)
	}
	return nil
}

// outboundDataActive reports whether DATA may leave this association at all.
//
// RFC 4666 Section 4.3.1 defines ASP-ACTIVE as the state in which "application
// traffic is active (for a particular Routing Context or set of Routing
// Contexts)", and Section 3.8.1 gives the receiving end of the same rule:
// "silent discard is used by an ASP if it received a DATA message from an SGP
// while it was in the ASP-INACTIVE state". In the Section 5.6.2 Double Exchange
// model the state that governs sending is the peer-directed one, which is what
// Association.State holds.
func (c *Association) outboundDataActive() bool {
	return c.State() == StateASPActive
}

// encodeDataFrame serializes one DATA message directly into a single buffer.
//
// It is the wire form of messages.NewData(...).MarshalBinary() for the same
// parameters — codec_test pins that equivalence — built without constructing
// the intermediate parameters. Building them would copy the payload a second
// time for every message on the busiest path in the library.
func (c *Association) encodeDataFrame(request *DataRequest) []byte {
	protocolDataValue := routingLabelOctets + len(request.ProtocolData.Data)
	// RFC 4666 Section 3.2: "If the length of the parameter is not a multiple
	// of 4 octets, the sender pads the Parameter at the end (i.e., after the
	// Parameter Value field) with all zero octets. The length of the padding is
	// NOT included in the parameter length field."
	protocolDataPadding := (4 - protocolDataValue%4) % 4

	length := commonHeaderOctets + parameterHeaderOctets + protocolDataValue + protocolDataPadding
	if request.AS.NetworkAppearanceSet {
		length += uint32ParameterOctets
	}
	if request.AS.RoutingContextSet {
		length += uint32ParameterOctets
	}
	if request.CorrelationIDSet {
		length += uint32ParameterOctets
	}

	frame := make([]byte, length)
	frame[0] = m3uaVersionRelease1
	frame[1] = 0
	frame[2] = messages.MsgClassTransfer
	frame[3] = messages.MsgTypePayloadData
	binary.BigEndian.PutUint32(frame[4:8], uint32(length))

	offset := commonHeaderOctets
	// Section 3.3.1 orders the parameters, and requires the Network Appearance
	// to be first when it is present.
	if request.AS.NetworkAppearanceSet {
		offset += putUint32Parameter(frame[offset:], params.NetworkAppearance, request.AS.NetworkAppearance)
	}
	if request.AS.RoutingContextSet {
		offset += putUint32Parameter(frame[offset:], params.RoutingContext, request.AS.RoutingContext)
	}

	binary.BigEndian.PutUint16(frame[offset:offset+2], params.ProtocolData)
	binary.BigEndian.PutUint16(frame[offset+2:offset+4], uint16(parameterHeaderOctets+protocolDataValue))
	offset += parameterHeaderOctets
	binary.BigEndian.PutUint32(frame[offset:offset+4], request.ProtocolData.OriginatingPointCode)
	binary.BigEndian.PutUint32(frame[offset+4:offset+8], request.ProtocolData.DestinationPointCode)
	frame[offset+8] = request.ProtocolData.ServiceIndicator
	frame[offset+9] = request.ProtocolData.NetworkIndicator
	frame[offset+10] = request.ProtocolData.MessagePriority
	frame[offset+11] = request.ProtocolData.SignallingLinkSelection
	offset += routingLabelOctets
	offset += copy(frame[offset:], request.ProtocolData.Data)
	// The padding octets are already zero: the buffer was allocated, not reused.
	offset += protocolDataPadding

	if request.CorrelationIDSet {
		putUint32Parameter(frame[offset:], params.CorrelationID, request.CorrelationID)
	}
	return frame
}

// putUint32Parameter writes one 32-bit parameter and reports its length.
func putUint32Parameter(b []byte, tag uint16, value uint32) int {
	binary.BigEndian.PutUint16(b[0:2], tag)
	binary.BigEndian.PutUint16(b[2:4], uint32ParameterOctets)
	binary.BigEndian.PutUint32(b[4:8], value)
	return uint32ParameterOctets
}

// writeRawData is WriteSignal's path for a caller-built DATA message. It runs
// the same admission and the same outcome classification as WriteData, and
// keeps WriteSignal's own return convention: the encoded message length.
//
// enforceTrafficScope is false only for the SGP distribution engine, which has
// already selected the Application Server and holds its delivery barrier;
// re-entering the barrier there would deadlock against itself.
func (c *Association) writeRawData(
	message messages.M3UA,
	frame []byte,
	encoded int,
	enforceTrafficScope bool,
) (int, error) {
	if enforceTrafficScope && !c.outboundDataActive() {
		return 0, newDataNotSent(ASKey{}, 0, ErrNotEstablished)
	}

	data, err := decodeOutboundData(frame)
	if err != nil {
		return 0, newDataNotSent(ASKey{}, 0, err)
	}

	// The same order as WriteData: the stream constraints of Section 1.4.7
	// before the Application Server binding.
	stream, err := c.outboundDataStream(data)
	if err != nil {
		return 0, newDataNotSent(ASKey{}, 0, err)
	}

	// The scope is resolved and admitted only where the caller owns the
	// selection. The distribution engine has already chosen the Application
	// Server and holds its delivery barrier, so re-resolving the scope here
	// would judge its message against this association's own configuration and
	// re-entering the barrier would deadlock against itself.
	var scope ASKey
	release := noDeliveryBarrier
	if enforceTrafficScope {
		scope, err = c.outboundDataScopeForMessage(data)
		if err != nil {
			return 0, newDataNotSent(scope, stream, err)
		}
		release, err = c.admitDataWrite(scope)
		if err != nil {
			return 0, newDataNotSent(scope, stream, err)
		}
	}
	defer release()

	// The signal seam belongs to the tests that observe decoded messages; in
	// production both write paths end at the same transport call.
	if c.signalWriter != nil {
		written, err := c.signalWriter(message)
		if err != nil {
			return 0, newDataSubmissionError(scope, stream, err)
		}
		return written, nil
	}
	if err := c.submitDataContext(context.Background(), frame, scope, stream); err != nil {
		return 0, err
	}
	return encoded, nil
}

// decodeOutboundData decodes a caller-built DATA so its scope and stream can be
// read from the message rather than from association-wide state.
func decodeOutboundData(frame []byte) (*messages.Data, error) {
	decoded, err := messages.Parse(frame)
	if err != nil {
		if errors.Is(err, messages.ErrMissingParameter) {
			return nil, ErrMissingProtocolData
		}
		return nil, err
	}
	data, ok := decoded.(*messages.Data)
	if !ok {
		return nil, ErrMissingProtocolData
	}
	if data.ProtocolData == nil {
		return nil, ErrMissingProtocolData
	}
	return data, nil
}

// outboundDataStream is the stream a caller-built DATA travels on: the one its
// own Signalling Link Selection maps to, validated like every other.
func (c *Association) outboundDataStream(data *messages.Data) (uint16, error) {
	payload, err := data.ProtocolData.ProtocolData()
	if err != nil {
		return 0, err
	}
	stream := c.streamFor(payload.SignallingLinkSelection)
	if err := c.checkDataStream(stream); err != nil {
		return 0, err
	}
	return stream, nil
}

// outboundDataScopeForMessage resolves the Application Server a caller-built
// DATA belongs to.
//
// Unlike DataRequest, whose ASKey is the scope, a raw message carries only what
// RFC 4666 Section 3.3.1 puts on the wire. An omitted Routing Context is
// resolved to the single coordinated one, which is what the same section
// permits — "Where multiple Routing Keys and Routing Contexts are used across a
// common association, the Routing Context MUST be sent to identify the traffic
// flow" — and an omitted Network Appearance, which is Optional there, resolves
// to the appearance of the Application Server the context names. The admission
// that follows is then identical.
func (c *Association) outboundDataScopeForMessage(data *messages.Data) (ASKey, error) {
	var key ASKey
	if data.RoutingContext != nil {
		contexts := data.RoutingContext.RoutingContexts()
		if len(contexts) != 1 {
			return key, NewInvalidRoutingContextError(contexts...)
		}
		key.RoutingContext, key.RoutingContextSet = contexts[0], true
	} else {
		switch configured := c.configuredRoutingContexts(); len(configured) {
		case 0:
			// No Routing Key coordinated: the contextless Application Server.
		case 1:
			key.RoutingContext, key.RoutingContextSet = configured[0], true
		default:
			return key, ErrMissingRoutingContext
		}
	}

	if data.NetworkAppearance == nil {
		if dynamic, ok := c.dynamicASKey(key.RoutingContext, false); ok && key.RoutingContextSet {
			key.NetworkAppearance, key.NetworkAppearanceSet = dynamic.NetworkAppearance, dynamic.NetworkAppearanceSet
			return key, nil
		}
		configured := c.contextlessASKey(false)
		if key.RoutingContextSet {
			configured = c.staticASKeyForRoutingContext(key.RoutingContext, false)
		}
		key.NetworkAppearance, key.NetworkAppearanceSet = configured.NetworkAppearance, configured.NetworkAppearanceSet
		return key, nil
	}
	if data.NetworkAppearance.Tag != params.NetworkAppearance || len(data.NetworkAppearance.Data) != 4 {
		return key, ErrInvalidNetworkAppearance
	}
	key.NetworkAppearance, key.NetworkAppearanceSet = data.NetworkAppearance.NetworkAppearance(), true
	return key, nil
}
