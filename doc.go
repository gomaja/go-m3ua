// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

/*
Package m3ua implements the M3UA protocol over SCTP.

An Endpoint has an explicit RFC 4666 role: ASP, SGP, or IPSP. Dial and
Listen/Accept describe only which endpoint initiates the SCTP association.
RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support either SCTP
orientation, so protocol behavior never follows from whether Dial or Accept was
used.

An IPSP Association must select the RFC 4666 Section 4.3 exchange model in
AssociationConfig.IPSP. Single Exchange is supported. Its ASPSM and ASPTM
initiation policies are independent from each other and from SCTP association
initiation, because either IPSP may initiate either exchange.

M3UA BEAT/BEAT Ack liveness is application-layer logic from RFC 4666 and is
configured with HeartbeatInfo. It is separate from SCTP HEARTBEAT chunks and
SCTP path-management timers, which remain transport/kernel behavior below this
package.

An endpoint accepting SCTP associations passes a ListenerConfig to Listen. Its
optional SelectAssociationConfig hook runs after SCTP accept and before M3UA
parsing so each association receives a separate immutable AssociationConfig.

This package uses github.com/gomaja/go-sctp for the underlying SCTP transport.

Specification: https://www.rfc-editor.org/rfc/rfc4666.html

# Security

RFC 4666 Section 6 carries one normative requirement, and it points elsewhere:

	Implementations MUST follow the normative guidance of RFC3788 [11] on
	the integration and usage of security mechanisms in SIGTRAN protocols.

RFC 3788 Section 7 states it plainly: "A SIGTRAN node MUST support IPsec and MAY
support TLS." A library that only implements M3UA over SCTP cannot satisfy that
node-level requirement by itself. IPsec ESP and IKE normally live in the host
network stack, outside this package; the SCTP association opened here is an
ordinary one to which host IPsec policy can apply.

Concretely, so that a deployment is not left to guess:

  - This package does NOT provide confidentiality, integrity, or peer
    authentication of its own. What it sends is exactly as protected as the IP
    path underneath it.

  - A conformant SIGTRAN node must provide the IPsec and IKE capabilities RFC
    3788 requires. This package does not provide them, so applications claiming
    node-level conformance must supply that support through the host or another
    layer. RFC 3788 does not require every association to have IPsec enabled;
    enabling protection is a deployment-policy decision based on the threats to
    that association.

  - TLS is permitted by RFC 3788 as an alternative but is not usable here: RFC
    3788 Section 6 requires TLS "on all bi-directional streams" of the SCTP
    association, which the underlying SCTP package does not implement. IPsec is
    the practical answer.

  - Nothing in M3UA authenticates a peer. An association that completes the SCTP
    handshake and sends ASP Up is treated as the ASP it claims to be, so network
    reachability is the authentication boundary: the M3UA port should be
    reachable only from the peers intended to use it.

Two related choices this package does make are documented where they are
implemented, and a security review will want to know about them: an ASP's SCON
is recorded apart from the SGP's own destination state, so a peer cannot inject
SS7 congestion into what other ASPs are told when they audit (see
Association.PeerCongestionLevel); and an ASP Active from a peer in ASP-DOWN is
refused rather than acknowledged, so an association that has not completed
ASPSM cannot be driven into carrying traffic.
*/
package m3ua
