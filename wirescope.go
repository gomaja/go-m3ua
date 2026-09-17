// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// WireScope is the exact Network Appearance and Routing Context scope a peer
// put on the wire, before any resolution into local membership or canonical
// Application Server identity.
//
// RFC 4666 Section 3.3.1 makes the Network Appearance "of local significance
// only, coordinated between the SGP and ASP", so "the same SS7 network context
// may be identified by different Network Appearance values, depending on which
// SGP a message is being transmitted/received". Section 1.4.2.1 makes a
// Routing Context "an index into a sending node's Message Distribution Table".
// Neither is globally meaningful, so the exact scope is kept separate from the
// canonical identity it resolves to.
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
