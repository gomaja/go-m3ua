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

Every NewEndpoint call must provide EndpointConfig.Congestion with an explicit
Default.Method: CongestionInternational, CongestionNationalWithPriorities or
CongestionNationalWithoutPriorities. Nil and zero methods are invalid for every
role, including network and association overrides. There is no library default:
the application knows the SS7 network's method, which cannot be inferred from NI,
MP or received SCON parameters (RFC 4666 Sections 3.3.1 and 3.4.4; Q.704 (07/96)
Sections 11.2.3.1, 11.2.4 and 14.2.2). Default is the application's explicitly
chosen fallback for networks without a selector, not an automatically chosen
method. National MaxLevel zero still selects 3 as an engineering level default.

ASP/IPSP local-user DATA and all SCON sends must suit the selected method. An
SGP preserves original MP0..3 on NIF transit, reporting a method mismatch through
the same bounded diagnostic as received DATA; MP above the supported ceiling is
refused before writing. The two-bit ceiling is the selected fallback, not full
ANSI/TTC certification. A diagnostic is not a protocol Error or a discard request.

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
DataMessage.Received retains the socket reader's monotonic timestamp, so
time.Since(message.Received) includes parsing, dispatch and receive queue wait;
time in the kernel's socket receive buffer is excluded.

WriteDataContext and MTPTransferContext cancel one send's admission and buffer
waits. A full buffer returns EAGAIN without an application write deadline; with
one, the transport waits until space is available, the deadline passes, or the
context ends. SendMsgContext errors with zero accepted bytes report DataNotSent;
unexpected partial submissions report DataSendIndeterminate. Full local
acceptance wins over concurrent cancellation. Application deadlines and other
sends are preserved.
Local SCTP shutdown or close wakes a send waiting for buffer space; a peer's
SHUTDOWN alone does not wake it. Shutdown and closed-transport refusals report
DataNotSent without changing the outcome of earlier accepted messages.
An ENOMEM refusal with no accepted bytes also reports DataNotSent and remains
eligible for pending AS recovery. Positive error counts remain
DataSendIndeterminate; ordinary sends also leave unclassified causes
indeterminate.

Admission and send locks use github.com/gomaja/go-ctxsync. Cancellable and
ordinary senders share its queue and starvation handoff; transfer readers
queued behind a routing-state writer are admitted before the next writer.
This policy does not impose a maximum wait: holders must release their locks
and waiting goroutines must be scheduled. A context ending before admission
returns DataNotSent with a cause matching ctx.Err(). Contexts with no Done
channel retain the ordinary submission path. Close and Abort bypass the send
lock so they can release a blocked transport write.

Endpoint exposes the RFC 4666 Layer Management status boundary for
Associations, ASPs, Application Servers, MTP Routes, and destinations.
Association exposes explicit ASP Up, ASP Down, ASP Active, ASP Inactive, DAUD,
and optional ASP-to-SGP SCON procedures. Endpoint exposes SGP-originated SCON
and DUPU fan-out to the concerned active ASPs. ManagementIndications preserves
the exact Association, ASKey, destination, and local failure scope.

StateChanges and ManagementIndications are independent optional subscriptions.
Calling either starts delivery of future events, without replaying earlier
ones; State gives the current ASP/IPSP state. To see the handshake, call them
from AssociationConfig.Subscribe, which Dial and Accept run before the handshake
starts. At an ASP, StateChanges then delivers ASP-INACTIVE and ASP-ACTIVE, the
M-ASP_UP and M-ASP_ACTIVE confirms of RFC 4666 Sections 4.3.4.1 and 4.3.4.3,
and ManagementIndications delivers the peer's NTFY and ERR. The opening ASP-DOWN
is the initial state (Section 4.3.1), not a transition. Applications must drain
each subscribed channel for the association's lifetime: a full queue closes it
with ErrIndicationQueueFull. Unsubscribed streams do not queue events.

M3UA BEAT/BEAT Ack liveness is application-layer logic from RFC 4666 and is
configured with HeartbeatInfo. It is separate from SCTP HEARTBEAT chunks and
SCTP path-management timers, which remain transport/kernel behavior below this
package.

An endpoint accepting SCTP associations passes a ListenerConfig to Listen. Its
optional SelectAssociationConfig hook runs after SCTP accept and before M3UA
parsing so each association receives a separate immutable AssociationConfig.

This package uses github.com/gomaja/go-sctp for the underlying SCTP transport.

Specification: https://www.rfc-editor.org/rfc/rfc4666.html

# Peer withdrawal and application recovery

ASPProcedureAutomatic controls startup and orderly-shutdown lifecycle points.
It never initiates recovery from an unsolicited acknowledgement. The library
adopts the peer's validated state assertion and reports the exact affected ASKeys
and numeric RoutingContexts to Layer Management. Subscribe through
AssociationConfig.Subscribe and drain ManagementIndications.

	Event                         Library action                         Application action
	Unowed Up Ack, ACTIVE          Error, INACTIVE, ASP-WITHDRAWAL        Re-register dynamic keys, restore approved scopes
	Unowed Up Ack, DOWN            Error, INACTIVE, ASP-WITHDRAWAL        Decide whether to ASPDown or remain inactive
	Unowed Up Ack, INACTIVE        Absorb                                None
	Unsolicited Down Ack           DOWN, ASP-WITHDRAWAL; no Error/Up      Decide whether to ASPUp; restore approved scopes
	Unsolicited Inactive Ack       Withdraw named active scopes          Decide whether to ASPActive
	Unsolicited Active Ack         Accept valid grant, ASP-ACTIVATION     Keep grant or ASPInactive
	Owed duplicate Ack             Absorb                                None

For unexpected ASP Up Ack, RFC 4666 §4.3.4.1 says the ASP "SHOULD send an
Error message and then initiate procedures to return itself to its previous
state". This library deliberately takes an exception to that uppercase SHOULD:
the Error is submitted before the withdrawal report, and the return is delegated
to Layer Management. On receipt of Up while active the SGP considers "all
registered Routing Keys ... deregistered" (§4.3.4.1); the library cannot restore
registrations or application policy alone. Re-register affected dynamic Routing
Keys before requesting activation for the reported Application Servers.
§4.3.4.3 permits an "M-ASP_ACTIVE request primitive from Layer Management".

RFC 4666 §4.3.4.2 uses lowercase "should" for return after unsolicited Down
Ack. §3.8.1 says Unexpected Message Error "MAY be sent"; this implementation
sends none and leaves Up/Active to the application. Solicited Down Ack handling
is unchanged. After an unsolicited ASP Inactive Ack the affected scopes stay
inactive even when the last scope is lost and startup policy is automatic
(§4.3.4.4, §4.3.4.4.1). Startup after a solicited ASP Up Ack is unchanged.

ManagementASPWithdrawal is an implementation indication named ASP-WITHDRAWAL,
not an M- primitive from §1.6.3. It excludes local confirmations and owed
duplicates. PreviousState records the prior local ASP procedure state;
PreviousIPSPState preserves both IPSP directions. Shared ASPSM Up/Down withdrawal
can remove active scopes in both directions, while independent Double Exchange
keeps the peer direction. ASPTM remains directional in Double Exchange
(§4.3.4.1.2, §4.3.4.3.1, §5.6).

A procedure retired by withdrawal exposes ErrASPProcedureSuperseded only after
state, per-AS membership and traffic gates have committed and ASP-WITHDRAWAL
has been published. This also applies to public initial writes. Internal
Up/Active entry, startup and restart actions only submit control work; they never
wait for a retired result, whose publication still follows the commit and
indication. Orderly shutdown retains its caller wait for the committed result.
ActivateRoutingContexts and DeactivateRoutingContexts normally
return after submission, but join a withdrawal that has retired their request.
Context cancellation and association failure remain independent outcomes; they
may end a caller's wait before withdrawal finishes draining earlier DATA.

Automatic entry actions serialize their state decision through initial control
submission, keeping ASP Up before ASP Active. Explicit procedures release the
state lock before writing or waiting for an Ack. A pending ASP Up or ASP Down
replaced by a newer local request of the same kind, and procedures cancelled by
orderly shutdown, now return ErrASPProcedureSuperseded instead of
ErrAssociationClosed on a live association. Local replacement leaves it open;
shutdown still completes its withdrawal and closes it.

ManagementASPActivation reports every scope in a validated Active Ack accepted
without an outstanding request or duplicate history. It is ASP-ACTIVATION, not
a new primitive from §1.6.3. A cancelled or timed-out ASPActive call can still
take effect later: a local cancellation or timeout does not retract an ASP
Active already received by the peer, and M3UA has no request identifier. A valid
late Ack can activate the named scopes. The application decides whether to keep
them or send ASPInactive. Existing validation and terminal-failure handling
still apply; this does not keep a failed association alive.

Use PreviousState to decide which lifecycle procedure, if any, to request.
In IPSP Double Exchange, partition the reported keys by the configured traffic
direction: ASPActive restores TrafficToLocal only. The peer's application decides
recovery for TrafficToPeer; sharing ASPSM does not share ASPTM (§4.3, §5.6.2).

For an ASP or Single Exchange IPSP, an empty withdrawal report has no active
memberships to restore. Omitting
ASPActive's scope selects every configured AS, so restoration of reported
memberships requires a non-empty ASKeys slice:

	if len(indication.ASKeys) > 0 {
		err := association.ASPActive(ctx, indication.ASKeys...)
		// Handle err.
	}

or start a known numeric scope without waiting:

	err := association.ActivateRoutingContexts(1)

Contextless membership beside dynamically registered numeric ASes cannot be
activated independently: omission represents the whole inventory (§4.3.4.3), so
the application decides whether ASPActive(ctx), with no ASKeys, is appropriate.

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
AssociationConfig.AuthorizeASP, ASPRoutingConfig.CongestionPolicy,
SGPCongestionPolicy and the
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
AssociationSnapshot carry *sctp.Addr values whose IPs hold every address
the peer confirmed for that association, not one representative address. A
selector matching a peer by address matches against the whole list, because
which of those addresses a given SCTP packet arrives from is a transport
decision that can change during the association's life.

AssociationStatus reports live local addresses, the primary peer path and all
current peer paths. Its separate socket reads are not an atomic snapshot.
SendBacklog reports the send-buffer memory charge, including overhead, and limit.
SCTPSendBacklog reads it alone.
SCTPAssocQueues is a separate optional sock_diag read.
SetSCTPPrimaryAddr, SCTPPathParams/SetSCTPPathParams and
SCTPPathThresholds/SetSCTPPathThresholds provide runtime path management.
Before passing an SCTPPathParams result to SetSCTPPathParams, clear
HeartbeatInterval when Heartbeat is false, PathMTU when PMTUD is true, and
SACKDelay when DelayedSACK is false.
Association-wide SetSCTPPathThresholds applies the association defaults for nil
fields to every path, overwriting differing per-path thresholds.
SCTPRTOInfo/SetSCTPRTOInfo and
SCTPAssociationMaxRetrans/SetSCTPAssociationMaxRetrans read and change the
association's RTO parameters and failure threshold (RFC 9260 Sections 6.3.1,
6.3.3 and 8.1; RFC 6458 Sections 8.1.1 and 8.1.2). AssociationStatus includes
both values. SCTPConfig.RTOInfo is applied before connect or listen; a
selector's overrides and runtime changes have Linux's per-path and
T5-shutdown-guard limits described by SetSCTPRTOInfo. AssociationMaxRetrans is
applied after path settings so Linux checks the live path limits against the
Note in RFC 9260 Section 8.2.
SCTPConfig applies path parameters and failure thresholds before each M3UA
handshake, to all current and future paths (RFC 6458 Section 8.1.12; RFC 7829
Section 7.2). ManagementIndications carries peer-address changes as
ManagementSCTPStatus with an owned PeerAddrChange (RFC 4666 Section 1.6.3;
RFC 6458 Section 6.1.2). Potentially Failed transitions require PFExposeEnabled
(RFC 7829 Section 7.3).

# SCTP bundling delay

Every association starts with SCTP_NODELAY on, including Dial, Accept and
SelectAssociationConfig selections (RFC 6458 §8.1.5). This reduces signalling
latency at the cost of potentially more packets. SCTPConfig.Nagle or
AssociationConfig.SetSCTPNagle(true) enables the Nagle-like bundling delay.
On Linux, small messages may then wait for earlier DATA to be acknowledged
unless enough data is queued to fill a packet (Linux v5.14 and v7.4-rc2,
net/sctp/output.c, sctp_packet_can_append_data). This can add the peer's
delayed-SACK delay: RFC 9260 §16 suggests 200 ms for SACK.Delay; §6.2 caps it
at 500 ms. Linux's net.sctp.sack_timeout defaults to 200 ms (same tags,
Documentation/networking/ip-sysctl.rst, sack_timeout).
Association.SetSCTPNoDelay changes the live socket; passing false enables
the delay.

# SCTP reliability

go-m3ua offers PR-SCTP (RFC 3758) only when SCTPConfig.PartialReliability is
true. By default Dial's INIT omits Forward-TSN-Supported and a Listener's INIT
ACK returns a peer's offer in an Unrecognized Parameter, overriding Linux's
net.sctp.prsctp_enable default, so a peer cannot abandon a message it sends
(RFC 3758 Sections 3.3.1–3.3.3 and 4.2; RFC 7496 Section 4.5), with the
Listener exception below. go-m3ua's own sends are always fully reliable. A peer
that requires PR-SCTP may decline the association; set PartialReliability
before setup to offer it. A peer can decline the offer without error, and
SCTPPRSupported and AssociationStatus.PRSupported report the negotiated result.

On Linux, a Listener's association answers an SCTP restart from
net.sctp.prsctp_enable rather than from PartialReliability
(sctp/lksctp-tools#81), and go-m3ua keeps the association across the restart
(RFC 4666 Section 4.3.3). With the sysctl at its default of 1, a restarted peer
is offered PR-SCTP and may abandon messages; this end rejects each FORWARD TSN,
later messages on that stream are not delivered, and nothing reports it. On
hosts that run Listeners, set the sysctl to match PartialReliability (0 for the
default). Dial's associations are not affected.

Opting in lets the peer abandon messages: a whole abandoned message never
arrives and nothing reports it, and abandonment after partial delivery ends the
association with a cause matching sctp.ErrMessageInterrupted, which
Association.Err keeps after Association.Done closes. The setting cannot change
a live association. A Listener fixes it at Listen, before its INIT ACK; every
selected config must carry the same value or Accept refuses that association
with ErrInvalidSCTPConfig and the Listener keeps serving. A fresh zero-value
selection is therefore refused under an opted-in default.

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

# Scoped SSNM loss

Oversized received reports discard only destination dimensions and retained
availability/congestion sources in the resolved AS and Network Appearance scope,
even when several ASes share a standalone partition. Admission-time resolution
survives source reconfiguration. An unresolved scope discards the selected
partition's destination knowledge. SSNMDestinationsInvalidatedEvent replaces the
removed SSNMPartitionInvalidatedEvent identifier. It routes by the
Endpoint-unique partition Epoch and leaves Partition empty.

SSNMEvent.Invalidated carries dimension-removal flags. The independent
CongestionSourcesInvalidated and AvailabilitySourcesInvalidated arrays carry
SSNMCongestionSourceInvalidation and SSNMAvailabilitySourceInvalidation IDs,
each with Destination, AS and Revision. ApplyInvalidation updates all three on a
matching caller-owned snapshot partition, including a destination that retains
only availability or congestion source history. It preserves unrelated
dimensions, sources and bindings. Each array is independently capped at 2048
entries per chunk: 16 accounted bytes per dimension ID and 48 per source ID. A
dimension-only event still costs at most 33440 bytes; all three arrays combined
cost at most 230048 bytes, including the 512-byte base and 160-byte reason
allowance. No RC lists or configured names are replayed. Larger custom
partitions use chunks; queued bursts and smaller custom queues can still require
Resync. Destination retention charges 128 bytes plus 4 per raw wire RC and 16
per resolved AS. Oversized DAUD removes nothing because it is a query (RFC 4666
§4.5.3).

# Received congestion validity and audits

CongestionConfig.Validity and CongestionConfig.Auditing independently enable local
received-SCON expiry and optional ASP audits. Nil disables each. A nonnil zero
configuration selects 30 seconds for After and Period, 256 for MaxPending and 10
for MaxMessagesPerSecond; negative fields are invalid. Auditing is ASP-only.
RFC 4666 §3.4.4 says a node may maintain a validity timer; §4.5.3 uses MAY for
periodic and isolation DAUD and lowercase must for its level-zero periodic
exception. These are optional procedures with engineering defaults.

Congestion-origin audits require the national-with-priorities method;
availability audits after DUNA/DRST are independent of method. OnActivation
configures isolation scopes after committed activation. A job retains its
original Association, epoch, wire scope and range, with independent per-AS
origins and deadlines. DAUD narrows its RC list to still-due origins. Separate
SCON(0) clears only congestion origins; DAVA clears availability/isolation
origins and resets surviving congestion work without clearing congestion.
Positive job replacements are staged; a budget refusal restores prior origins
and charges. There is no sibling fallback or automatic reactivation. Newer
applicable clear prefixes retire a masked-range cause only after covering every
AS still needing it, including interleaved reports. Partial coverage,
unrelated-AS clears and unknown congestion keep needed work. Coverage uses
bounded prefixes and three fixed uint64 per-job cause versions, without
per-point audit history or a change to the 256-byte job base charge. Received
statements apply to their resolved AS and Network Appearance; another AS's
Routing Context supplies no authority. Lookup and retirement use the same rule.
Auditing retains AvailabilitySources; validity or auditing retains
CongestionSources, preserving interleaved per-AS evidence. With both policies
nil, the scalar cache filters by AS and can displace old evidence to unknown; it
adds no source-history records and never borrows another AS's statement.

Canonical transfer selection, observed status and direct national-priority DATA
admission use the configured SG/AS aggregate. RFC 4666 §§1.2, 1.4.2.1 and
3.3.1 allow coordinated SGPs to use different local NA/RC labels for that
owner; a sibling's report is not filtered by another SGP's numeric labels.
Standalone projections and explicit source-scoped reads retain AS/network
filtering. Source history, audit origins and invalidation retain their
original reporter and scope; a shared route view does not widen them.

When validity or auditing is enabled, full source admission is rechecked under
submission-tracker, peer-dynamic binding and store locks in that order. Store
epochs advance monotonically; retention and job insertion use the same admitted
token. Registration removal, version/epoch replacement and source loss remove
only affected source history and audit origins and release their charges.
Unaffected static membership and the independent local registration direction
survive.

Association.ForgetDestinations reclaims the reporter's canonical availability,
congestion history and periodic audit causes as well as legacy destinations. It
preserves canonical siblings, peer congestion, transport observations and
configured isolation. Its integer result still counts only released legacy rows.
The unconditional forget-generation fence rejects pre-forget received admission
and legacy writes delayed after route resolution even when both policies are
nil; genuinely new reports can use the retained bindings. With both policies
nil, receive admission captures only one atomic forget-generation load and
checks it under the mutation locks already needed by the legacy and canonical
stores; it adds no tracker acquisition or source-history retention. The full
source-epoch and registration-token capture remains conditional on validity or
auditing.

The forget generation is also checked under the existing route-cache mutex
before a delayed report can publish derived indications. ForgetDestinations
does not reclaim pre-existing shared SG route-index allocations; its reclaim
contract covers the reporter-owned stores and periodic work described here.

A scalar destination dimension costs 128 + 4 per raw RC + 16 per resolved AS.
Each independent availability or congestion source adds one record and costs 144
+ 4 per raw RC per resolved AS. Auditing retains availability history; validity
or auditing retains congestion history. Nil policies preserve scalar record
counts and charges. The minimum MaxBytes is 400. A partition plus one single-RC
auditing availability statement or validity/auditing SCON costs 552 bytes; its
one-binding/one-partition job adds 472 before Info String, totaling 1024.
General jobs cost 256 + Info String bytes + 8 per raw RC + 144 per binding + 64
per unique partition. These portable charges are not heap or RSS measurements.

SSNMPublicationOutcomeEvent distinguishes kinds with PublicationKind. For DAUD,
it reports one aggregate per fixed reason per report, pass or retirement batch. PublicationCount counts every completed job outcome for
submitted, send-failed and transport send-deferred, including consecutive equal reasons and a same-key
refresh during transport. The refreshed job retains its newer causes and
deadlines. Every removed job contributes exactly once to reporter-retired,
regardless of retirement-pass order. Deferred and contended work counts outcome
transitions. Counts remain per job; one batched DAUD can complete several jobs.
PublicationScopeOmitted marks mixed original scopes or more than 64 raw RCs; zero
Report.Association marks mixed sources. No APC lists, Info String or configured
names are replayed. Repeated contention and rate deferrals of a retained job are suppressed.
Each diagnostic costs at most 928 conservative bytes (784 with fixed reasons);
seven diagnostics add at most seven slots to the actual report-observation and
report-patch count. Only an unfragmented observation adds one slot. Reports
have separate payload accounting; custom queues and bursts may still need
Resync. The seven reasons are queue-full, rate-deferred, reporter-retired, send-deferred,
contended, send-failed and submitted. A successful send also has a separate local
DAUD report once per wire batch rather than once per canonical partition. Its
Partition and Epoch are empty; Scope and Destinations describe the submitted
wire batch. Neither establishes peer receipt or response correlation.

SSNMCongestion and PeerCongestionState expose ReceivedAt, ExpiresAt and
Freshness. Expiry preserves severity/provenance as CongestionExpired history and
emits SSNMCongestionExpiredEvent. Its owned Expired slice contains compact
SSNMCongestionExpiry identities with partition Epoch, Destination, AS, original
Revision and Peer. ApplyExpiry on the owned full SSNMSnapshot expires only
matching records and their scalar copies, preserving values and provenance. It
leaves the snapshot revision for the caller to order. Expiry no longer replays
DestinationUpdates or PeerCongestion values; top-level Partition and Epoch are empty. Each
atomic store commit revalidates up to 256 candidates, changes freshness and
publishes before unlocking. Locks are released and cancellation checked between
chunks. Accounting charges 56 bytes per ID: the default record ceiling permits
at most 64 compact SSNM expiry events and 950272 bytes in one expiry-only pass.
This bound excludes other queued SSNM events and the separate MTP indication
channel; it does not remove either channel's overflow and resynchronization
contract.

Queued DestinationUpdates destinations now charge 320 fixed bytes, reflecting the
larger public destination shape. Each individual peer or availability/congestion
source row still charges 256 bytes, plus its Routing Context storage. Review
custom subscription byte budgets: report updates can reach them sooner. This
queued-payload change leaves source-removal IDs at 48, expiry IDs at 56, the
230,048-byte combined invalidation maximum and the 950,272-byte compact expiry
pass bound unchanged; retained record and job charges are separate.

Expiry may produce more MTP indications than receipt when transport pressure
masks the received level but expiry changes ReceivedCongestionUnknown. A
projection emits at most one indication per destination whose exposed status
changes; it does not suppress that uncertainty change. If the bounded indication
queue overflows, it emits ResyncRequired and the consumer must recover current
statuses with MTPDestinationStatuses. The compact expiry bound applies to SSNM
subscription events, not this MTP indication queue.

SSNMAvailability.AS identifies the resolved AS in auditing history, and
SSNMDestinationKnowledge.AvailabilitySources preserves independent statements
when auditing is enabled. CongestionSources preserves independent AS/source
history when validity or auditing is enabled. Snapshots own both histories;
public field additions break unkeyed literals. ApplyInvalidation consumers must
handle dimension IDs and both source-removal arrays, or use the helper.
MTPDestinationStatus.ReceivedCongestionUnknown means local uncertainty, not
abatement. A fresh independent contribution may remain congested. Newest
covering source statements are selected before freshness, so expiry cannot
reveal an older superseded range. Bare DAVA changes availability only. Validity
is not negotiated, does not expire independent transport observations, and emits
no invented SCON(0), DAVA or RESUME.

Endpoint.Close cancels and joins the worker through its operation wait after
Association closure, outside locks needed for completion. Passes check
cancellation between jobs and expiry commits. Association withdrawal and
teardown do not join the worker. Late REG/DEREG dispatcher/request processing
can hold RKM correlation/lifecycle locks while joining a cancelled tracked
optional SCON/DAUD. Completion needs only the tracker and optional submission never
acquires those RKM locks. One NoWait send avoids waiting for socket buffer space;
the pinned transport's internal mutex and application-deadline limits remain
(RFC 6458 §5.3.4; Errata 6111).

Restart and ForgetDestinations now gate new optional SCON/DAUD and settle any
cancelled optional attempt before changing the epoch or reclaiming knowledge.
Concurrent lifetime changes share counted admission closure and settle only the
admitted optional attempt, without waiting for another owner's cleanup. They
wait outside transport, destination, store and procedure locks, never for DATA
or synchronous SCON; at most one optional attempt holds the writer lock. The
restart receive path and state monitor can therefore wait for cancellation
completion. Ordinary received status admission rejects the gate or stale
generation instead of joining it. These joins add no universal elapsed-time
guarantee.

# SGP peer congestion

An SGP retains ASP-reported congestion independently from authoritative SS7
knowledge (RFC 4666 §§1.3.2.4, 1.4.2.4). SSNMSnapshot.PeerCongestion and
SSNMPeerCongestionEvent expose current records and removal deltas through the
existing SubscribeSSNM/Resync continuity barrier. Records share SSNM record and
byte limits. Explicit zero is known peer clear; an absent record is unknown.
Lifecycle retirement removes the affected association epoch and AS scopes.
Neither peer reports nor outbound transport samples change authoritative DAUD.

Endpoint.CongestionInputs returns copied AS membership, peer records, local
contributions and transport observations. A changing component version vector triggers one retry; Consistent
reports stability of the returned collection, and Revision covers SSNM only.
VersionsBefore and VersionsAfter expose membership and SSNM versions plus each
peer binding generation, opaque LocalGeneration, transport observation and
TransportPresent flag. PeerCongestionVersion.LocalGeneration versions local
lifetime/queue evidence and must be compared without interpreting its value.
CongestionInputSnapshot.Local contains queue and application contributions
separately. Queue overload also has its own management indication stream. A
fresh input snapshot does not replace that stream's loss contract.

SGPConfig.CongestionPolicy is an SGPCongestionPolicy application veto evaluated
outside Endpoint, AS and delivery locks before the write barrier. The decision
is revalidated once;
ErrCongestionDecisionStale is a known-not-sent result if it remains stale.
SGPConfig.PreferUncongestedPeers defaults to false and, when enabled, ranks only
loadshare alternatives by the maximum of the latest matching peer record by
Revision and transport pressure. A newer matching clear supersedes an older
range. Equal ranks preserve active-member order and SLS modulo selection.
Override retains its selected active member; broadcast reports a veto separately
for each required unsent member. Rerouting may missequence SLS traffic because
there is no cross-association changeover barrier (RFC 4666 §1.4.7).

The application/NIF determines represented SPMC capacity and whether SS7 TFC is
appropriate (RFC 4666 §1.4.6). No automatic TFC API or cluster congestion verdict
is supplied, and resync is never a replay of Concerned Destination triggers.

# Continued MTP status

RFC 4666 Section 1.4.6 describes MTP-STATUS according to current MTP3 procedures,
without an uppercase MUST in that service prose. Routed ASP MTPTransfer counts
one original validated offer before transport submission. While the destination
remains known congested, international and national-without-priorities methods
return status for offer 1 and no later than eight offers after each indication.
An uninterrupted interval reports at 1, 9, 17 and so on; resets can report
earlier (Q.704 (07/96) Sections 11.2.3.1 and 11.2.5). Failed transport attempts
count; invalid requests, gateway candidates, broadcast copies, repeated
policy evaluation and recovery retries do not add offers. WriteData and
WriteSignal do not count. UserData length is not the full level-2 MSU length
required by the octet alternative. National-with-priorities stays
transition-driven (Section 11.2.4).

MTPIndication.Cause uses MTPStatusSignallingNetworkCongested for known congestion
onset, repeat and clear; Destination carries the actual Congested and level
state. PAUSE and RESUME use the zero-value MTPStatusCauseNone. Reason defaults
to MTPStatusTransition; initial/repeated status uses MTPStatusOfferedTraffic.
Only explicit received-validity expiration uses MTPStatusValidityExpired with
MTPStatusCauseNone, reporting unknown knowledge rather than measured clear or
continuing congestion. Reason identifies the notification trigger for a coalesced current aggregate
snapshot; it carries no per-field causal history. Transport uncertainty uses
the normal transition reason, never received-validity expiry. Availability,
transport or lifecycle operations exposing already-expired knowledge also keep
MTPStatusTransition; an uncertain noncongested result uses MTPStatusCauseNone and does not
claim measured congestion clear.

As a conservative engineering policy, ReceivedCongestionUnknown or
TransportCongestionUnknown suppresses offered-traffic status and retires its
automatic interval, even if Congested stays true from a surviving contribution.

Each live exact-DPC LRU entry owns one restart mark in a single Endpoint
registry. Matching uses shared immutable routing configuration and the changed
input's scope; entries retain no association or source snapshots. Actual
availability, congestion or freshness changes, including changes selected by
the newest covering input, uncertainty, expiry and renewal after expiry,
commit marks before route projection. Binding admission/retirement, association
attachment, Active/Done/close and traffic-permission changes also mark affected
entries at commit. An eligible routed offer consumes its mark and resets the interval.
A later onset marks it again even if an offer already consumed the preceding
clear, so coalesced projection cannot hide that restart.

Equal renewals preserve the interval when all relevant newest inputs,
including ties, remain unchanged and fresh. Timestamp/revision-only changes
and changes outside the entry's point-code or configured SG/AS route scope
also preserve it. Canonical mark matching uses that configured owner, just like
canonical transfer and observation; peer-local NA/RC labels do not narrow it
(RFC 4666 §§1.2, 1.4.2.1, 3.3.1). Standalone reads and source-specific
invalidation keep their independent applicability rules. Association and lifecycle changes may conservatively mark
all provisioned routes, including initially inactive or
later-attached alternatives; per-AS permission changes mark the routes whose
permission changed. Relevant changes can therefore cause extra initial
reminders even while another contribution sustains congestion. The indication
still carries the current coalesced route projection, without recomputing a
fresh canonical aggregate for the offer. Marks retire with their LRU entries
and Endpoint close and retain no event history.

Mark matching is bounded by the entry budget, the change's own scope, the
24-bit point-code width and configured candidates. It does not scan retained
source history or recompute an aggregate per entry. A shared input index
resolves a received source's newest covering input with at most 25 prefix
checks per query. This also detects an equal broad reassertion replacing a
newer narrow clear, including equivalent wire-scope aliases. An expired input
tied for newest revision conservatively makes renewal restart the interval;
it does not change the canonical projection, status payload or Cause/Reason.
Removing that input lets unchanged fresh renewals preserve cadence again.
Shared per-prefix/per-revision counts track retained records and expirations.
For H retained received-source records, the index owns at most 2H links and
2H revision groups, released with their source keys. It retains no additional
statements, destination aggregate or interval-by-association table. These
structural bounds are engineering contracts, not portable heap-byte or RSS
measurements.

Availability history uses one shared incremental prefix index for ordinary
projection and offered-status marks. Each retained source has at most two links
(all-source and normalized scoped inputs); a query checks at most 25 prefixes.
Availability changes do not rebuild the separate congestion aggregate. Admission,
withdrawal and forget fence their actual availability and scalar removals with
marks, including when a remaining source supplies the same value after the
scalar copy disappears. Equal current-input renewals preserve cadence. Restart
settles optional DAUD before its existing epoch-reset fence; compact expiry
identities leave the final validity notification as the only expiry origin.
These operations add no wait to offered transfer beyond short bookkeeping locks.

Bounded atomic flags per association preserve semantic transport Known/Level
changes, including clear, later onset and positive-level changes, across
coalesced projection. A commit fence begins before snapshot publication and
stays visible until a pending reset has been posted. Consumers preserve the
Endpoint dirty hint while a commit is in progress, so an offer consuming a
clear cannot absorb a later onset or erase an in-flight snapshot change.
Storage is four booleans per association plus the Endpoint hint, with no event
history, additional sampler locks or user callbacks.

The next routed offer or projection conservatively retires counters for the
association's configured routes, covering activation before projection.
Ordinary observations reset only while the association is Active at
consumption; ordinary samples consumed while inactive preserve cadence.
Forced epoch retirement is independent of Active. Sampling is fenced even
when it starts inactive because activation can cross snapshot publication.
Another contribution may sustain congestion, so a reset can produce an extra
initial reminder without a false clear; the limit is no later than eight
offers, not exactly eight. Equal Known/Level observations, including changes
to diagnostic fields or timestamps alone, and unrelated peer timers preserve
the interval. Sampling never waits for route or AS locks.

CongestionConfig.MaxStatusDestinations bounds the Endpoint-wide exact-DPC LRU,
with route/method generation identity. Zero selects the engineering default of
4096 and negative values are invalid. Eviction restarts initial notification.
Effective clear, either uncertainty, lifecycle retirement and changed
route/method generation retire intervals. Equal renewal preserves cadence when
all relevant newest inputs, including ties, remain unchanged and fresh.
Endpoint.Close ends this state. Indication overflow follows the existing ResyncRequired snapshot rules;
MTPTransfer never waits for a consumer.

For IPSP/application-owned routing, NewMTPStatusProcedure constructs a procedure
for one destination at one local MTP3 boundary. It has no destination map, is
safe for concurrent calls and must not be copied after first use. Offered takes
the caller's effective aggregate status once per original validated offer and
returns (indication, error), with (nil, nil) when nothing is due, including
for national-with-priorities. A route name above 255 octets returns
IdentityLengthError wrapping ErrInvalidParameterValue before retention or any
cadence change.
A changed Destination resets automatically; clear or uncertain status supplied
to Offered resets the interval too. The helper sees snapshots only. Call Reset
on known clear, uncertainty, expiry or local lifecycle/method retirement between
offers, including a source ending masked by a surviving contribution. The
application must order source updates and resets with its offers at its own
routing boundary. Endpoint.SubscribeSSNM can supply ordered source updates;
handle subscription loss by resetting and resynchronizing. The coalesced
Endpoint.MTPIndications stream alone cannot reconstruct hidden source endings.
IPSP and SGP peer-input procedures have the same application-owned ordering
responsibility. Reset keeps the configured profile; construct a new procedure
for a changed method. Routed ASP MTPTransfer maintains restart marks and
fenced transport reset flags internally, so do not also call the helper for
those offers.

MTPIndications is one bounded consumer stream: the application fans each
primitive out to each registered MTP3-User; concurrent readers do not multicast. Level-4 users decide
when to resume transmission (Q.704 (07/96) Section 11.2.3.3).

# Local congestion publication and DATA admission

ReportLocalCongestion accepts a LocalCongestionRequest identifying Source,
Association, Epoch, AS, Destination and Congestion. Source is an identifier of
1–64 UTF-8 bytes of Unicode letters/digits or _, -, ., :, /. The $ namespace
is reserved; queue records use $queue:<OPC>. The association
epoch and active association member/binding of the exact AS must be current;
Destination is an exact 24-bit DPC. The AS has no authoritative local-destination
inventory, so admission validates the numeric value and current binding. This
input evidence does not prove aggregate AS readiness or authorize DATA/SPMC
delivery; optional wire admission checks applicable delivery readiness separately. National numeric
levels must agree with Congested; omitted levels denote unknown congestion.
International input uses LevelSet=false and Level=0, including local clear.
Contradictory states return ErrInvalidParameterValue. A contribution has no TTL;
WithdrawLocalCongestion removes only that source, while lifecycle retirement
and ForgetDestinations remove affected scopes. LocalCongestionContribution adds
copied Scope and a
library-assigned Revision. Retention shares SSNMStateConfig record/byte limits;
refusals wrap ErrSSNMResourceLoss and produce loss events. Each local source
costs one record and 320 plus Source bytes plus 8 per wire RC accounted bytes;
these portable units are not heap-size bounds. A changing or contended admission
can return ErrCongestionDecisionStale; retry the complete declaration with its
current scope. Withdrawing an absent source succeeds only with a current active
association member/binding.

CongestionInputSnapshot.Local exposes queue and application evidence separately.
Aggregation takes maximum or unknown congestion only within one association,
AS and destination. Queue drain does not clear an application contribution;
failed wire publication does not erase local evidence. An SGP retains these
inputs for its application/NIF without originating SGP-to-ASP SS7 SCON
(RFC 4666 §§1.4.6, 3.4.4).

CongestionConfig.Publication configures automatic local SCON and audit scheduling
through CongestionPublicationConfig. Nil or zero fields select MaxPending=64,
MaxPendingPerAssociation=32, MaxBytes=64 KiB, RetryInterval=100 ms and
MinReportInterval=1 s after successful submission. These are engineering defaults;
negative values and a
per-association ceiling above MaxPending are invalid. Equal desired states
coalesce. An unsent onset is cancelled on local clear. A submitted national onset
reserves its slot until clear submission or lifecycle retirement; audits consume
only unreserved transient capacity. A pending clear is never evicted to admit
new work. Distinct triggering OPCs keep distinct reservations; recovery has no
fresh trigger and omits Concerned Destination. International drain sends no
numeric zero and no periodic local SCON reassertion is installed.

The optional worker tries existing barriers without waiting for their locks,
rechecks state/epoch/revision and submits once with NoWait. It defers EAGAIN,
contention and pending mandatory control, with at most one optional attempt per
association per pass across both report kinds. A concurrently starting mandatory
write can race an already admitted attempt; absolute fairness is not promised.
No tracker or state lock crosses transport. Withdrawal, restart and teardown
fence old work without reactivating an AS; Endpoint.Close cancels and joins the
worker. Explicit SignallingCongestion and DestinationStateAudit remain
synchronous and return actual errors.

CongestionPublications returns owned CongestionPublicationState snapshots with
association/epoch, scope, destinations, kind, concerned destination, desired
congestion and revision, submitted revision, pending status, attempts, last
attempt and a LastError bounded to 256 bytes. Kinds are
CongestionPublicationLocalSCON and CongestionPublicationDAUD. Snapshots are bounded
by MaxPending; completed clears leave the snapshot and waiting audit deadlines
remain in their separate bounded store. A same-state coalesced update can leave
DesiredRevision ahead of SubmittedRevision with Pending=false. Local SCON
Attempts belongs to its reservation; DAUD Attempts describes its transient
batch, not cumulative deadline-store history.
CongestionPublicationCounters exposes Queued, Submitted, Coalesced, Deferred,
Refused and Failed. Queued counts reservation/transient admissions, not unique
logical audit jobs; deferral decisions can repeat on later passes. Failed
includes actual EAGAIN transport refusals. ManagementCongestionPublication carries the publication state
in ManagementIndication.CongestionPublication for new reservations, visible
capacity refusals, changed desired state for the updated key, and each actual
transport attempt. Equal desired state coalesces without another state-change
observation.
SSNMPublicationOutcomeEvent carries PublicationKind and logical PublicationCount;
CongestionPublication is owned exact state for a single local SCON outcome and
is nil for aggregated audits. PublicationScopeOmitted retains aggregate-scope
semantics. Each channel uses the common outcome once, without a second audit-only
notification. Logging is not required for local failure observability. The
existing management channel has 64 slots; subscribers must drain it for the
association lifetime, including actual retry attempts. Submission is not proof
of peer receipt. Lifecycle/pre-transport retirement does not promise a separate
publication outcome notification. SSNM subscriptions retain bounded
continuity-loss/Resync behavior: a legal multi-AS report or a combined
report/expiry burst can overflow an unread default queue. Individual events fit
the default byte budget. SG/AS and ASP/IPSP destination route names have a
255-octet cap; local source IDs retain their stricter 64-octet cap. Oversized
report histories use ReportContinues/ReportPatch.SourcesContinued fragments and ApplyReport.
A complete replica applies through the last fragment. Queue overflow exposes a
continuity-loss marker; Resync recovers exact retained state, then resumes the
stream. Discarded raw observations and diagnostics are not replayed. Defaults
remain 256 slots and 1 MiB; no whole-report-plus-pass default-fit guarantee is
made. The queue-setting GoDoc and endpoint-management design give conservative
budget formulas. MTP overflow continues to use ResyncRequired.

AssociationConfig.CongestionAdmission opts into an additional cached DATA gate;
nil disables it. CongestionAdmissionConfig.ChargedRatioLimit zero selects 0.9;
the ratio must be finite and strictly between zero and one, and requires the
backlog monitor. Every typed, raw and distributed DATA send reads the cached
sample under send serialization without a socket query. At or above the ratio,
before the first valid sample, or when evidence is invalid/stale, admission
returns a typed known-not-sent error wrapping ErrDataCongestionRefused. Control
bypasses the gate. It reserves no kernel memory and provides no control delivery
guarantee: large messages, retransmissions and external descriptor users can
consume remaining charge. ControlWriteTimeout still closes a permanently stalled
association (RFC 4666 §1.4.7; RFC 6458 §5.3.4; Errata 6111).

# Security

RFC 4666 Section 6 carries one normative requirement, and it points elsewhere:

	Implementations MUST follow the normative guidance of RFC3788 [11] on
	the integration and usage of security mechanisms in SIGTRAN protocols.

Transport congestion detection defaults to one periodic Endpoint scheduler with
bounded workers, independent per-boundary dwell, and charged SCTP memory ratios.
Pending dwell needs consecutive valid samples at most 1.5 sampling intervals
apart and no missed slot. State-lock contention skips only the affected
association's sample; a bounded coalescing publisher keeps AS state waits off
the sampling path. Destination projection uses MTPTransfer's ordered AS and
destination availability checks.
EndpointConfig.TransportCongestion configures or disables it. Association.TransportCongestion
reports unavailable/stale samples as unknown, retains raw evidence and sample age,
and stays independent of received SCON and local inbound overload. Routed ASPs and
explicit IPSPDestinations mappings project transport pressure to MTP-STATUS
(RFC 4666 §§1.4.6, 4.5.2.1–2). The implementation indications
ManagementTransportCongestion (TRANSPORT-CONGESTION) and ManagementDataQueueCongestion
(DATA-QUEUE-CONGESTION) report outbound transport pressure and independent inbound
queue onset, end and tracking loss to Layer Management.
An SGP leaves SS7 actions to its NIF. Outbound pressure sends no automatic SCON.
CongestionPolicy sees combined per-association pressure. Default selection remains
unchanged; PreferUncongestedTransport may reroute an SLS flow and missequence it
because there is no cross-association changeover barrier (RFC 4666 §1.4.7).
Raw transport levels are engineering severity; explicit congestion-method
profiles supply the local-user priority admission rule. At the IPSP
destination-record limit, refused SSNM updates
leave the previous retained state in MTPIndications and destination snapshots.
SubscribeSSNM reports SSNMResourceLossEvent; a new snapshot alone cannot recover
an update that was not retained. MTPIndications is a single consumer channel
with bounded overflow resynchronization; applications fan out to their MTP3-Users.

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
Endpoint.SubscribeSSNM as an SSNMReport with PeerReported set and as separate
retained peer records, without changing authoritative destination state); and an
ASP Active from a peer in ASP-DOWN is refused rather than acknowledged, so an
association that has not completed ASPSM cannot be driven into carrying traffic.
*/package m3ua
