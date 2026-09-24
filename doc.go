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
AssociationConfig.IPSP and state which procedures it initiates in
AssociationConfig.ASPProcedures. Single Exchange and Double Exchange are
supported. The ASPSM and ASPTM initiation policies are independent from each
other and from SCTP association initiation, because either IPSP may initiate
either exchange.

DATA is carried by WriteData and ReadData. A DataRequest names the exact
Application Server scope and carries the whole MTP3 routing label of RFC 4666
Section 3.3.1, so nothing about a message is held on the association and
concurrent senders cannot take each other's scope. A successful send reports
local transport acceptance only; every failure is a *DataWriteError whose
Outcome distinguishes a message that never reached the transport from one whose
fate is unknown. ReadData is context-scoped: cancelling a read ends that read
and neither closes the association nor discards queued DATA.

Endpoint exposes the RFC 4666 Layer Management status boundary for
Associations, ASPs, Application Servers, MTP Routes, and destinations.
Association exposes explicit ASP Up, ASP Down, ASP Active, ASP Inactive, DAUD,
and optional ASP-to-SGP SCON procedures. Endpoint exposes SGP-originated SCON
and DUPU fan-out to the concerned active ASPs. ManagementIndications preserves
the exact Association, ASKey, destination, and local failure scope.

M3UA BEAT/BEAT Ack liveness is application-layer logic from RFC 4666 and is
configured with HeartbeatInfo. It is separate from SCTP HEARTBEAT chunks and
SCTP path-management timers, which remain transport/kernel behavior below this
package.

An endpoint accepting SCTP associations passes a ListenerConfig to Listen. Its
optional SelectAssociationConfig hook runs after SCTP accept and before M3UA
parsing so each association receives a separate immutable AssociationConfig.

This package uses github.com/gomaja/go-sctp for the underlying SCTP transport.

Specification: https://www.rfc-editor.org/rfc/rfc4666.html

# Ownership and shutdown

Three scopes own resources, and each closes exactly its own.

  - Association.Close closes one association: its SCTP association, its
    goroutines, and its streams. It deregisters that association from the
    Listener that accepted it and from the Endpoint that owns it. The Listener
    keeps listening and sibling associations keep carrying traffic. Peers still
    see it through M3UA, because an ASP leaving an Application Server can change
    that AS's state and produce a Notify to the others; that is RFC 4666 Section
    4.3.2, not teardown reaching sideways.

  - Listener.Close closes the listening socket and every Association that
    Listener accepted, because nothing else owns them. It leaves the Endpoint
    and its shared Application Server, NIF, destination and MTP3 restart state
    alone, and does not touch associations another Listener or Endpoint.Dial
    created.

  - Endpoint.Close closes every Listener and Association the Endpoint owns,
    dialled and accepted alike, and then the shared state none of them owns
    individually: Application Server timers and recovery queues first, any MTP3
    restart in progress, then the transports, then the MTPIndications channel
    and every open SSNM subscription.

All three release SCTP without sending ASP Inactive or ASP Down, which is RFC
4666 Section 4.9 option (b). Option (a) is Association.ShutdownContext, which
performs the procedures AssociationConfig.ASPProcedures marks automatic and then
closes. Nothing calls it for the application: an ASP that wants its peers told
before it goes calls it on each association before closing their owner.

Both options end in the SCTP SHUTDOWN procedure, which the peer acknowledges
and its SCTP layer reports as SHUTDOWN_COMPLETE. Association.Abort ends one
association with an SCTP ABORT instead (RFC 9260 Sections 9.1 and 11.1.4): it
discards whatever is still queued, does not wait for the peer, and the peer's
SCTP layer reports COMMUNICATION LOST. Locally it is Close, with the same
teardown and the same deregistration, except that Err reports
ErrAssociationAborted, which matches ErrAssociationClosed. It is for an
association that has to go at once, such as a misbehaving peer or one that is
not completing the shutdown, and like ShutdownContext only the application
calls it.

The ctx passed to Dial and Accept is the association's lifetime, not its
handshake. Cancelling it closes the associations it produced, and that makes it
the wrong context to derive from an interrupt signal if the application also
wants ShutdownContext to work: the association's monitor closes it as soon as
the context is done, so the withdrawal finds an association already in ASP-DOWN,
sends nothing, and returns the cancellation that closed it. An application that
wants option (a) gives the association a context of its own and cancels it only
after ShutdownContext has returned.

# Callbacks and snapshots

Application callbacks — ListenerConfig.SelectAssociationConfig,
AssociationConfig.AuthorizeASP, ASPRoutingConfig.CongestionPolicy and the
RoutingKeyManagementConfig authorizers and allocator — run without any Endpoint,
Listener, Association or registry lock held, and may run concurrently for
independent associations. A callback that blocks delays only the work that is
waiting on it; it cannot deadlock the library against itself. None of them may
assume how often it is called: congestion policy in particular is evaluated per
candidate during selection.

What crosses that boundary is copied in both directions. Configuration passed to
NewEndpoint, Dial and Listen is deep-copied, so a later mutation of the caller's
struct, slice or map changes nothing already running; an Association's
configuration is immutable once it exists. Values returned to the application are
owned by the application: status snapshots, SSNM snapshots and events,
ManagementIndications, MTPIndications and DataMessage payloads each own their
slices and their SCTP addresses, and may be retained or modified freely.

The exception is deliberate and narrow. Association.LocalAddr and
Association.RemoteAddr return the transport's own address values, because they
are the net.Addr accessors of the underlying association; treat them as
read-only. AssociationSnapshot.LocalAddr and AssociationSnapshot.RemoteAddr are
owned copies of the same addresses for callers that need to retain them.

# Peer identity, addresses and authentication

Nothing an M3UA peer says about itself is authenticated. ASPIdentity.RemoteAddr,
AcceptInfo.RemoteAddr and Association.PeerASPIdentifier are what the transport
observed and what the peer asserted, in that order of trustworthiness: the
address is where the packets came from, and the ASP Identifier is a number the
peer chose to send. AuthorizeASP is therefore an authorization hook over an
unauthenticated identity, and useful precisely because it runs after SCTP accept
and before any M3UA message is parsed. Peer authentication belongs below this
package; see the Security section.

An SCTP association is multi-homed, so its addresses are lists. AcceptInfo and
AssociationSnapshot carry *sctp.SCTPAddr values whose IPAddrs hold every address
the peer confirmed for that association, not one representative address. A
selector matching a peer by address matches against the whole list, because
which of those addresses a given SCTP packet arrives from is a transport
decision that can change during the association's life without M3UA noticing.

# Signalling domain isolation

Two labels that look global are not. RFC 4666 Section 3.3.1 makes a Network
Appearance "of local significance only, coordinated between the SGP and ASP",
and Section 1.4.2.1 makes a Routing Context "an index into a sending node's
Message Distribution Table". The same Routing Context value means different
things on two Signalling Gateways, and two SGPs of one Signalling Gateway may
label one Application Server differently.

The API keeps those apart rather than flattening them. ASKey is the exact wire
scope on one Association — a Network Appearance and a Routing Context, each with
its own presence bit, because zero is a legitimate value for either. WireScope is
the scope exactly as a peer sent it, before resolution. SGASKey and SSNMPartition
are the canonical identity — one Signalling Gateway and one Application Server —
that retained knowledge belongs to, so a report learned through one SGP survives
that SGP's association and is never confused with a same-numbered scope on
another Signalling Gateway. A bare Routing Context is not accepted as an identity
anywhere it would be ambiguous; the operation fails closed instead of guessing.

# Derived MTP status and per-path selection

An ASP that provisions ASPConfig.Routing gets two different answers about a
destination, and they are allowed to differ. MTPIndications, MTPDestinationStatus
and MTPDestinationStatuses are the MTP3-User's aggregate view over every
provisioned route, where RFC 4666 Appendix A.2.2 defines a Signalling Gateway's
capability negatively: established, activated, and no report of inaccessibility
or MTP restart. A destination nobody has reported on satisfies that, so the
aggregate reads Available. MTPTransfer decides about one candidate, reading the
canonical SSNM store where an absent availability record is absence, and refuses
with ErrDestinationStateUnknown unless
ASPRoutingConfig.AllowUnknownDestinations opts back into the Appendix A.2.2
reading. They disagree only for an established, activated, silent Signalling
Gateway.

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
SS7 congestion into what other ASPs are told when they audit (it is delivered by
Endpoint.SubscribeSSNM as an SSNMReport with PeerReported set, without changing
retained destination state); and an ASP Active from a peer in ASP-DOWN is
refused rather than acknowledged, so an association that has not completed
ASPSM cannot be driven into carrying traffic.
*/
package m3ua
