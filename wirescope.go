// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// WireScope is the exact Network Appearance and Routing Context scope a peer
// put on the wire, before any resolution into local membership or canonical
// Application Server identity.
//
// RFC 4666 Section 3.3.1 makes Network Appearance the SS7 network context of a
// message, and Section 3.6.1 makes Routing Context the label one peer assigns
// to a Routing Key. Neither is globally meaningful: the same Routing Context
// value names different Application Servers on different peers, so the exact
// scope is kept separate from the canonical identity it resolves to.
//
// Both presence bits are load bearing. Zero is a legitimate explicit value for
// either parameter, and a Routing Context list may legitimately be empty, so
// neither value nor length substitutes for the flag.
type WireScope struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContexts      []uint32
	RoutingContextSet    bool
}

// clone returns a WireScope that shares no storage with the receiver, so a
// scope handed to an application cannot be mutated through the retained copy.
func (s WireScope) clone() WireScope {
	if s.RoutingContexts != nil {
		s.RoutingContexts = append([]uint32(nil), s.RoutingContexts...)
	}
	return s
}

// equal compares two wire scopes by value, including both presence bits and
// the exact Routing Context list order.
func (s WireScope) equal(other WireScope) bool {
	if s.NetworkAppearanceSet != other.NetworkAppearanceSet ||
		s.NetworkAppearance != other.NetworkAppearance ||
		s.RoutingContextSet != other.RoutingContextSet ||
		len(s.RoutingContexts) != len(other.RoutingContexts) {
		return false
	}
	for index, routingContext := range s.RoutingContexts {
		if other.RoutingContexts[index] != routingContext {
			return false
		}
	}
	return true
}
