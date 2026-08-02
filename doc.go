// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

/*
Package m3ua provides easy and painless handling of M3UA protocol in pure Golang.

The API design is kept as similar as possible to other protocols in standard net package.
To establish M3UA connection as client/server, you can use Dial() and Listen() / Accept()
without caring about the underlying SCTP association, as go-m3ua handles it together
with M3UA ASPSM & ASPTM procedures.

M3UA BEAT/BEAT Ack liveness is application-layer logic from RFC 4666 and is
configured with HeartbeatInfo. It is separate from SCTP HEARTBEAT chunks and
SCTP path-management timers, which remain transport/kernel behavior below this
package.

This package relies much on github.com/gomaja/go-sctp, as M3UA requires underlying SCTP connection,

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
Conn.PeerCongestionLevel); and an ASP Active from a peer in ASP-DOWN is refused
rather than acknowledged, so an association that has not completed ASPSM cannot
be driven into carrying traffic.
*/
package m3ua
