// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"os"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// DataMessage is one received DATA and everything the association knows about
// how it arrived.
//
// RFC 4666 Section 3.3.1 makes the Network Appearance "of local significance
// only, coordinated between the SGP and ASP" and Section 1.4.2.1 makes a
// Routing Context "an index into a sending node's Message Distribution Table",
// so what the peer put on the wire and the Application Server it resolves to
// are reported separately: an application distributing inbound traffic needs
// the resolved identity, and one answering in the scope the request arrived on
// needs the exact wire scope.
type DataMessage struct {
	// ProtocolData is the MTP3 routing label and user octets. It belongs to the
	// caller: the library keeps no reference to it after delivery, and no two
	// delivered messages share storage.
	ProtocolData *params.ProtocolDataPayload

	// Scope is the Network Appearance and Routing Context exactly as received,
	// including whether each was present at all.
	Scope WireScope

	// AS is the Application Server the message resolves to, which for an
	// omitted Routing Context is the single coordinated one.
	AS ASKey

	// Stream is the SCTP stream the message arrived on. RFC 4666 Section 1.4.7
	// rule 1 makes zero impossible for DATA.
	Stream uint16

	// CorrelationID is the Section 3.3.1 Correlation Id the peer attached.
	// CorrelationIDSet distinguishes an explicit zero from an omitted parameter.
	CorrelationID    uint32
	CorrelationIDSet bool

	// Association is the owning Endpoint's identity for the association that
	// received the message. Zero when the association has no Endpoint.
	Association AssociationID

	// Epoch is the SCTP association epoch the message arrived in. It changes
	// when the peer restarts the SCTP association, so traffic from before a
	// restart is distinguishable from traffic after it.
	Epoch uint64
}

// ReadData returns the next DATA message received on this association.
//
// Cancelling ctx ends this one read and nothing else: the association stays
// open, and neither the message that may have been arriving nor anything queued
// behind it is discarded. Several goroutines may read concurrently; each
// message is delivered to exactly one of them.
//
// The read deadline set by SetReadDeadline and SetDeadline still applies and
// still reports os.ErrDeadlineExceeded, which is recoverable. A closed
// association reports ErrNotEstablished.
func (c *Association) ReadData(ctx context.Context) (*DataMessage, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !c.inboundDataActive() {
		return nil, ErrNotEstablished
	}
	// Checked before the select so an already-cancelled read cannot take a
	// message it will not return.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	timeout, stop, expired := c.readTimeout()
	if expired {
		return nil, os.ErrDeadlineExceeded
	}
	defer stop()

	select {
	case message, ok := <-c.dataChan:
		if !ok {
			return nil, ErrNotEstablished
		}
		return message, nil
	case <-ctx.Done():
		// Nothing was taken from the queue, so nothing is lost: the next read
		// finds the same message waiting.
		return nil, ctx.Err()
	case <-timeout:
		// Recoverable, unlike every other error here: the association is
		// healthy and the caller may read again.
		return nil, os.ErrDeadlineExceeded
	case <-c.done:
		return nil, ErrNotEstablished
	}
}

// DataQueueStats is the observable state of the inbound DATA queue.
type DataQueueStats struct {
	// Capacity is the configured queue depth, from AssociationConfig.DataQueueSize.
	Capacity int
	// Queued is how many messages are waiting to be read.
	Queued int
	// Discarded is the cumulative number of received DATA payloads dropped
	// because the queue was full.
	Discarded uint64
	// Congested reports that the queue is currently overflowing: it became full
	// and has not accepted a message since.
	Congested bool
}

// DataQueueStats reports the inbound DATA queue's occupancy and its cumulative
// loss, so an application can see overload rather than infer it.
//
// Overload is never silent: the peer is also told with the SCON of RFC 4666
// Section 3.4.4, which "MAY also be sent from the M3UA layer of an ASP to an
// M3UA peer, indicating that the congestion level of the M3UA layer or the ASP
// has changed".
func (c *Association) DataQueueStats() DataQueueStats {
	if c == nil {
		return DataQueueStats{}
	}
	return DataQueueStats{
		Capacity:  cap(c.dataChan),
		Queued:    len(c.dataChan),
		Discarded: c.dataDiscarded.Load(),
		Congested: c.dataOverflow.Load(),
	}
}

// Epoch is the current SCTP association epoch, starting at one and advancing
// each time the peer restarts the SCTP association.
//
// RFC 4666 Section 4.3.3 treats an SCTP restart as a loss of the M3UA peer's
// state, so knowledge carried over from before one is not knowledge about the
// association that exists now.
func (c *Association) Epoch() uint64 {
	if c == nil {
		return 0
	}
	return c.epoch.Load() + 1
}

// DataQueueOverflowError reports one episode of inbound DATA loss and names the
// destination whose traffic was discarded.
//
// The Affected Point Code of an SCON is Mandatory (RFC 4666 Section 3.4.4) and
// the congested destination is this node, which is the Destination Point Code
// of the traffic being discarded — the routing label of the discarded message
// says so without the library having to be told its own point code.
type DataQueueOverflowError struct {
	DestinationPointCode uint32
	NetworkIndicator     uint8
}

func (e *DataQueueOverflowError) Error() string {
	return fmt.Sprintf("%s: destination %#06x", ErrDataQueueFull.Error(), e.DestinationPointCode)
}

// Is keeps the sentinel matchable.
func (e *DataQueueOverflowError) Is(target error) bool {
	return target == ErrDataQueueFull
}

// receivedDataScope is the Network Appearance and Routing Context exactly as
// the peer sent them.
func receivedDataScope(data *messages.Data) WireScope {
	var scope WireScope
	if data.NetworkAppearance != nil {
		scope.NetworkAppearance = data.NetworkAppearance.NetworkAppearance()
		scope.NetworkAppearanceSet = true
	}
	if data.RoutingContext != nil {
		scope.RoutingContexts = data.RoutingContext.RoutingContexts()
		scope.RoutingContextSet = true
	}
	return scope
}

// receivedASKey resolves the Application Server a received DATA belongs to.
func (c *Association) receivedASKey(data *messages.Data) ASKey {
	local := c.isIPSPDoubleExchange()
	var key ASKey
	if routingContext, ok := c.receivedDataRoutingContext(data.RoutingContext); ok {
		key.RoutingContext, key.RoutingContextSet = routingContext, true
	}
	if data.NetworkAppearance != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = data.NetworkAppearance.NetworkAppearance(), true
		return key
	}
	if key.RoutingContextSet {
		if dynamic, ok := c.dynamicASKey(key.RoutingContext, local); ok {
			key.NetworkAppearance, key.NetworkAppearanceSet = dynamic.NetworkAppearance, dynamic.NetworkAppearanceSet
			return key
		}
	}
	configured := c.contextlessASKey(local)
	if key.RoutingContextSet {
		configured = c.staticASKeyForRoutingContext(key.RoutingContext, local)
	}
	key.NetworkAppearance, key.NetworkAppearanceSet = configured.NetworkAppearance, configured.NetworkAppearanceSet
	return key
}
