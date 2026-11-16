# Endpoint Management and SSNM Operations

RFC 4666 remains the current M3UA specification. The RFC Editor and IETF
Datatracker both list it as a Proposed Standard that obsoletes RFC 3332, with no
RFC that updates or obsoletes it as of 2026-09-19. The SIGTRAN working group is
concluded and lists no active Internet-Draft that replaces M3UA.

The RFC Editor lists no Verified errata. Errata 2065 and 4475 are Held for
Document Update, and erratum 2518 is Rejected. Existing support for the routing
context behavior described by held erratum 2065 remains an explicit
interoperability decision; it is not described as a verified correction.

This design completes the Layer Management and SSNM application boundary. It
does not infer M3UA roles from which peer initiates SCTP: `RoleASP`, `RoleSGP`,
and `RoleIPSP` remain the protocol roles, while `Dial`, `Listen`, and `Accept`
describe only SCTP association initiation (RFC 4666 Section 1.4.8).

## Alternatives considered

### Replace every indication channel with one Endpoint event stream

This would centralize management delivery, but it would duplicate or replace
the existing association-scoped state and management streams, make one slow
consumer a failure domain for every association, and obscure which peer caused
an event. It is rejected.

### Keep only Association methods

This matches the wire connection but cannot answer node-wide AS, ASP, route, or
destination questions. It also leaves an SGP application responsible for
finding and iterating the correct concerned ASPs. It is rejected.

### Endpoint snapshots plus association-scoped procedures

The Endpoint owns stable identities and authoritative node-wide snapshots. An
Association owns procedures sent to one peer. SGP-originated SSNM operations
that must reach all concerned ASPs remain Endpoint operations. Existing
association indication streams remain fail-closed. This is the selected model.

## Stable association identity

Every association tracked by an Endpoint receives a monotonically increasing
`AssociationID`. Zero means that an Association was constructed outside an
Endpoint, which is supported only by internal and package tests.

```go
type AssociationID uint64

func (a *Association) ID() AssociationID
```

The identifier is local to one Endpoint lifetime. It is immutable, never
reused, and does not expose the kernel SCTP association identifier.

## Keyed status snapshots

RFC 4666 Sections 1.6.3 and 4.2 define M-SCTP_STATUS, M-ASP_STATUS, and
M-AS_STATUS as local Layer Management queries. They do not send peer messages.
Every returned slice and address is caller-owned. Association snapshots are
ordered by AssociationID; SCTP paths retain kernel order.

```go
type AssociationSnapshot struct {
    Association AssociationID
    Role        Role
    State       State
    IPSPState   IPSPState
    LocalAddr   *sctp.Addr
    RemoteAddr  *sctp.Addr

    PeerASPIdentifier    uint32
    PeerASPIdentifierSet bool

    SCTP      *AssociationStatus
    SCTPError error
}

func (e *Endpoint) AssociationStatus(AssociationID) (AssociationSnapshot, bool)
func (e *Endpoint) AssociationStatuses() []AssociationSnapshot
```

`SCTPError` makes a close racing a snapshot explicit instead of silently
dropping that association. A successful snapshot owns its SCTP status value, including live `LocalAddrs`,
`Paths` and `Primary`. These are separate socket reads rather than an atomic
snapshot. M-SCTP_STATUS indications carry owned peer-address-change events
through `ManagementIndications`; the query remains the confirm primitive.

An ASP status is keyed by association and exact AS traffic identity. The two
presence bits avoid conflating an omitted value with numeric zero. Local and
peer state are separate because RFC 4666 Sections 4.3 and 5.6.2 define two
independent IPSP Double Exchange directions.

```go
type ASPStatusKey struct {
    Association AssociationID
    AS          ASKey
}

type ASPStatus struct {
    Key ASPStatusKey

    LocalState    State
    LocalStateSet bool
    PeerState     State
    PeerStateSet  bool

    LocalASPIdentifier    uint32
    LocalASPIdentifierSet bool
    PeerASPIdentifier     uint32
    PeerASPIdentifierSet  bool
}

func (e *Endpoint) ASPStatus(ASPStatusKey) (ASPStatus, bool)
func (e *Endpoint) ASPStatuses() []ASPStatus
```

For an ASP Endpoint, `LocalStateSet` is true. For an SGP Endpoint,
`PeerStateSet` is true. IPSP Single Exchange sets both to the same state. IPSP
Double Exchange reports the local state governing `TrafficToLocal` and the peer
state governing `TrafficToPeer`.

Application Server state remains keyed by `ASKey`, never by bare Routing
Context (RFC 4666 Sections 1.2, 1.4.2.1, 3.1.1, and 4.3.2).

```go
type ApplicationServerStatus struct {
    AS                 ASKey
    State              ASState
    TrafficMode        uint32
    RequiredActiveASPs int
    ActiveASPs         []AssociationID
}

func (e *Endpoint) ApplicationServerStatus(ASKey) (ApplicationServerStatus, bool)
func (e *Endpoint) ApplicationServerStatuses() []ApplicationServerStatus
```

The ASP Endpoint exposes each configured MTP route independently from the
peer-specific AS keys used along its SGP paths (RFC 4666 Sections 1.4.2.5,
4.5.2.2, and 5.5.1.1.1).

```go
type MTPRouteStatus struct {
    MTPRoute     MTPRouteID
    Destinations []MTPDestinationStatus
    Associations []AssociationID
}

func (e *Endpoint) MTPRouteStatus(MTPRouteID) (MTPRouteStatus, bool)
func (e *Endpoint) MTPRouteStatuses() []MTPRouteStatus
```

An SGP Endpoint exposes the exact destination record used to answer DAUD. The
key retains Network Appearance and Routing Context presence, point code, and
mask. Congestion level is retained independently from reachability because
RFC 4666 Sections 3.4.4 and 4.5.3 make an explicit level zero an abatement and
permit the parameter to be omitted. Audit congestion knowledge is a separate
presence bit from the authoritative congestion record, carried through sweep
equality, complete-plan RC grouping and message construction. A zero-valued state,
LevelSet, availability, peer report, transport sample or configured method cannot
supply presence. National known clear sends explicit SCON(0) before DAVA or DRST
(RFC 4666 §§4.5.3, 5.4.1). Absent congestion sends availability alone (§5.4.3).
International known clear sends availability alone without a partial-response
management error; retained congestion that its method cannot express keeps the
[declared refusal policy](../rfc4666-conformance.md#daud-congestion-profile-deviation).
Bare DAVA still leaves an ASP's congestion dimension unchanged.

A masked audit uses a snapshot of those records and partitions its requested
union at narrower state boundaries, including active and completed restart
ranges that survive forgetting destination records (§4.6). Replies carry
aligned, non-overlapping
Affected Point Code ranges (RFC 4666 §§3.4.1, 4.5.3), so a DAVA never includes a
known unavailable exception and reply application order cannot change the
availability result. Superseded records do not fragment a uniform reply.
The planner copies state under the existing store/registry locks, then uses a
scope-local boundary sweep with separate newest-record heaps for availability
and congestion. Standard library sorts order events and boundaries; resolving
R relevant retained records and B input request/state/restart boundaries
(including restart ranges filtered out for that scope) costs
O((R + B) log(R + B)) per Routing Context scope. Event and boundary slices,
heaps, priority objects and counter arrays are reset and reused across scopes;
returned plans retain separate storage. RFC 4666 §3.4.1 aligned
ranges either nest or are disjoint. Restart isolation remains the union of each
epoch's affected ranges minus its own completed recovery ranges. No planning
lock is held during socket writes.

Replies group equal message type, encoded state and Routing Context sets into
Affected Point Code lists, split at the effective configured/default APC limit
and the existing message/TLV size limits. RFC 4666 §4.5.3 permits DUNA/DAVA
lists. Section 3.4.4 says the APC parameter can indicate congestion of multiple
destinations or ranges; §3.4.6 gives DRST the DUNA parameter descriptions. All
SCON lists precede the DAVA/DRST they qualify. Availability ranges remain
disjoint, including across chunks, and explicit audit boundaries are preserved.

Both total messages and encoded bytes, including national SCON(0), are bounded
before the first write; known-clear replies do not widen the limits.
For store limit N and Q requested ranges, the planning envelope allows
P = 48 × (3N + Q + 1) aligned pieces per scope: at most 24 dyadic cuts per
input range, with a factor of two allowance, and three record budgets for
retained records and affected/completed restart boundaries. Without forgetting
records those distinct boundaries fit the store's limit. Complete identical
scope plans share an RC list, so there are at most as many RC sets as scopes.
For each RC list chunk, let H be its worst SCON header size and L its effective
APC chunk capacity. The reply budget allows 2 × (ceil(P/L) + 48) messages and
8P plus that message count times H bytes. This covers availability and congestion
lists with more state-group headers than ordinary state needs. In particular,
256 isolated known points across the full 24-bit space receive all 4352 ranges,
in five messages at the default APC limit.

Restart history surviving repeated `ForgetDestinations` can exceed this
ordinary-state envelope. A refused plan sends no status prefix: the SGP sends
Destination Status Unknown ERR with the requested APCs and associated NA/RC
(including known scope inferred from configuration, RFC 4666 §3.8.1) and publishes a Layer Management error. The Association remains up.
An ASP receiving that ERR publishes its scoped management error without changing
traffic authorization or destination availability.
Destinations without retained state still answer DUNA. A known congestion-only
record keeps the initial available state; unavailable entries carry no SCON.

```go
type DestinationStatusKey struct {
    NetworkAppearance    uint32
    NetworkAppearanceSet bool
    RoutingContext       uint32
    RoutingContextSet    bool
    PointCode            uint32
    Mask                 uint8
}

type DestinationNetworkState struct {
    Availability DestinationAvailability
    Congestion   CongestionState
}

type DestinationStatusSnapshot struct {
    Key   DestinationStatusKey
    State DestinationNetworkState
}

func (e *Endpoint) DestinationStatus(DestinationStatusKey) (DestinationStatusSnapshot, bool)
func (e *Endpoint) DestinationStatuses() []DestinationStatusSnapshot
```

`DestinationNetworkState` carries both dimensions because an audit has to answer
both, whichever of them was reported last. The single `DestinationState` enum
this replaces could not: it had one `DestinationCongested` value, so a
congestion report silently made a destination reachable and a DAVA silently made
it uncongested. `DestinationAvailability` is what DUNA, DAVA and DRST move, and
`CongestionState` is what SCON moves, with `LevelSet` separating an omitted
Congestion Indications parameter from an explicit level zero.

Role-invalid status queries return no result and never consult a peer.

## ASP procedure policy

RFC 4666 Sections 4.3.4.1 through 4.3.4.4 allow Layer Management to request
ASP Up, ASP Down, ASP Active, and ASP Inactive. The M3UA layer may also start
the corresponding procedure automatically. The choice is independent for each
procedure and independent from SCTP association initiation.

```go
type ASPProcedureMode uint8

const (
    ASPProcedureAutomatic ASPProcedureMode = iota + 1
    ASPProcedureExplicit
)

type ASPProcedurePolicy struct {
    ASPUp       ASPProcedureMode
    ASPDown     ASPProcedureMode
    ASPActive   ASPProcedureMode
    ASPInactive ASPProcedureMode
}
```

`AssociationConfig.ASPProcedures` is deeply snapshotted. A nil policy selects
the historical policy: ASP endpoints automatically climb through ASP Up and
ASP Active and automatically perform ASP Inactive and ASP Down during
`ShutdownContext`; IPSP initiation follows its existing explicit exchange
configuration. A non-nil policy must specify all four modes. It is invalid on
an SGP Association because RFC 4666 makes the ASP or IPSP the initiator of
these requests.

The explicit request methods are:

```go
func (a *Association) ASPUp(context.Context) error
func (a *Association) ASPDown(context.Context) error
func (a *Association) ASPActive(context.Context, ...ASKey) error
func (a *Association) ASPInactive(context.Context, ...ASKey) error
```

They validate role, exact traffic scope, and current state before the first
write. Success means the matching acknowledgement completed the T(ack)
procedure. Cancellation removes the exact pending request and returns the
context error. A failed operation publishes a local M-ERROR indication as
required by RFC 4666 Section 4.2 without originating an Error message to the
peer.

For IPSP Double Exchange these methods operate on `TrafficToLocal`, because
that is the direction whose ASP Up and ASP Active requests the local IPSP
sends. `TrafficToPeer` changes only when the peer sends the corresponding
request.

Connection readiness follows the selected startup policy:

| Role and policy | `Dial` or `Accept` returns when |
| --- | --- |
| SGP | the peer reaches ASP-ACTIVE, unchanged |
| ASP/IPSP, explicit ASP Up | SCTP setup and the opening ASP-DOWN transition complete |
| ASP/IPSP, automatic ASP Up and explicit ASP Active | ASP Up Ack establishes ASP-INACTIVE |
| ASP/IPSP, automatic ASP Up and ASP Active | ASP Active Ack establishes ASP-ACTIVE |

`EstablishTimeout` bounds the selected readiness point. M3UA BEAT starts only
when the traffic direction becomes ASP-ACTIVE; returning an explicitly managed
ASP-DOWN or ASP-INACTIVE association does not falsely enable BEAT.

`ShutdownContext` runs only the procedures configured Automatic. Explicit
procedures are the application's responsibility; after the selected automatic
steps, SCTP is released. `Close` remains RFC 4666 Section 4.9 option (b): SCTP
release without M3UA withdrawal messages.

That distinction propagates to the two wider ownership scopes, which is worth
stating because it is the part applications get wrong. `Listener.Close` closes
the listening socket and every Association that Listener accepted;
`Endpoint.Close` closes every Listener and Association the Endpoint owns,
dialled and accepted alike, together with the shared Application Server, NIF,
destination and MTP3 restart state that none of them owns individually. Neither
performs a withdrawal: both use option (b) on every association. An application
that wants option (a) calls `ShutdownContext` on each association first. An
`Association.Close` on its own closes only that association and deregisters it;
its Listener keeps listening and its siblings keep carrying traffic, although
its departure from an Application Server is still visible to peers as the
Section 4.3.2 AS state change it is.

## Typed SSNM operations

The public API models the wire scope directly while preventing callers from
constructing malformed parameter combinations.

```go
type WireScope struct {
    NetworkAppearance    uint32
    NetworkAppearanceSet bool
    RoutingContexts      []uint32
    RoutingContextSet    bool
}

type PointCodeRange struct {
    PointCode uint32
    Mask      uint8
}

type DestinationStateAuditRequest struct {
    Scope        WireScope
    Destinations []PointCodeRange
    Info         string
}

type DestinationAvailabilityRequest struct {
    Scope        WireScope
    Destinations []PointCodeRange
    Availability DestinationAvailability
    Info         string
}

type SignallingCongestionRequest struct {
    Scope        WireScope
    Destinations []PointCodeRange

    CongestionLevel    uint8
    CongestionLevelSet bool

    ConcernedDestination    uint32
    ConcernedDestinationSet bool
    Info                    string
}

type DestinationUserPartUnavailableRequest struct {
    Scope       WireScope
    Destination PointCodeRange
    User        uint16
    Cause       uint16
    Info        string
}
```

The type is named `WireScope` rather than `SSNMScope` because the rename carries
a distinction the old name hid. `WireScope` is the exact Network Appearance and
Routing Context a peer put on the wire, before any resolution into local
membership. It is not an identity: RFC 4666 Section 1.4.2.1 makes a Routing
Context "an index into a sending node's Message Distribution Table", so the same
value on two Signalling Gateways names different Application Servers. What
retained knowledge is owned by is `SSNMPartition`, below. Both presence bits are
load bearing: zero is a legitimate explicit value for either parameter, and a
Routing Context slice may be empty in memory when `RoutingContextSet` is
false, representing omission. A present wire list must be nonempty; RFC 4666
§3.4.1 describes it as "Routing Context: n x 32 bits (unsigned integer)" and
says, "The conditional Routing Context parameter contains the Routing Context
values associated with the DUNA message." Neither value nor length substitutes
for the presence flag.

The operations are:

```go
func (a *Association) DestinationStateAudit(DestinationStateAuditRequest) error
func (a *Association) SignallingCongestion(SignallingCongestionRequest) error
func (e *Endpoint) ReportDestinationAvailability(DestinationAvailabilityRequest) error
func (e *Endpoint) SignallingCongestion(SignallingCongestionRequest) error
func (e *Endpoint) DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest) error
```

`Endpoint.ReportDestinationAvailability` is the SGP's DUNA, DAVA and DRST
publication (Sections 3.4.1, 3.4.2 and 3.4.6). It moves the availability
dimension alone, records the state a later Section 4.5.3 audit is answered from,
and stages destinations inside an MTP3 restart instead of publishing them. It
replaces the per-Association and per-Listener `SetDestinationState` and
`ReportDestinationState` families, because an SGP's view of the SS7 network is
shared by every ASP it serves and was never a property of one association.

`Association.DestinationStateAudit` is ASP-to-SGP (RFC 4666 Sections 3.4.3 and
4.5.3). `Association.SignallingCongestion` is the optional ASP-to-SGP report;
Concerned Destination is permitted only there, with exactly one Affected Point
Code whose mask is zero (Section 3.4.4). The same admission rule applies to
`Association.WriteSignal`, including generic caller-built SCON. Without that
parameter, SCON destination lists and ranges remain supported.
`Endpoint.SignallingCongestion` is SGP-to-concerned-ASPs and atomically updates
the SGP destination registry. `Endpoint.DestinationUserPartUnavailable` is
SGP-to-concerned-ASPs and enforces DUPU's one unmasked destination. User/Cause
admission uses the same structural envelope on typed origination, raw
`WriteSignal`, and reception: User ≤255 and Cause ≤15 (Q.704 (07/96) §15.17;
Q.Imp704 (12/00) §§2.2.3–2.2.4). RFC 4666 §3.4.5 defers assignments to the
applicable MTP3 variant; callers own those assignments. The codecs retain both
16-bit fields. The SG-ASP DUNA, DAVA, DAUD, DUPU, and DRST
procedures are not part of the library's endpoint-only IPSP model
(Sections 1.4.3.4 and 1.4.6). The SS7-connected IPSP restart variant is not
implemented. RFC 4666 §4.6 says, "In the IPSP case, MTP restart could be
considered if the IPSP also has connection to an SS7 network." It continues,
"In that case, the same behavior as described above for the SGP would apply
to the restarting IPSP." Managed MTP3 restart is available only on `RoleSGP`.
DUPU's single unmasked destination rule also applies at shared send admission,
including `Association.WriteSignal` and generic caller-built messages. The
message codecs remain general serializers for capture decoding and re-encoding.

Every operation validates role before constructing or writing a message. A
present Routing Context list must be non-empty and unique. Every Routing
Context must resolve to the request's exact Network Appearance. Point codes are
24-bit and masks are at most 24. Congestion levels are 0 through 3. Info String
is valid UTF-8 and at most 255 octets (Section 3.4.1).

SGP fan-out snapshots concerned active ASPs under the AS registry and writes to
each association without holding Endpoint or AS locks. Partial failures are
returned with the failed `AssociationID` values; successful peers are never
replayed automatically.

A received ASP-to-SGP SCON can retain congestion for a list or range even
when its Concerned Destination cannot identify a single-DPC TFC trigger
(RFC 4666 §3.4.4). `SubscribeSSNM` reports every destination, range and congestion
level unchanged. `SSNMReport.ConcernedDestinationSet` is true only for a mappable
trigger; otherwise its value is zero and `UnmappedConcernedDestination` owns the
raw received value, including an explicit zero. That optional scalar is a Layer
Management diagnostic, not permission to generate a TFC. Every subscriber owns
its copy. The receive path sends no Error for this mapping mismatch and changes
no DATA authorization. The existing direction, parameter framing, scope and
resource checks still apply.

The SSNM subscription is the management channel for this accepted report.
`ManagementIndications` has no accepted-message warning primitive: M-ERROR would
suggest a received peer Error or failed local operation. No additional management
primitive is emitted; consumers must inspect the diagnostic in the SSNM report
and honor that subscription's continuity-loss contract. TFC generation and
SPMC-wide policy remain application/NIF responsibilities.

## Retained peer congestion and SGP selection

RFC 4666 §1.3.2.4 describes maintaining remote ASP congestion;
§1.4.2.4 includes it among SGP distribution inputs. At an SGP, peer records
are keyed by association and epoch, resolved AS, exact wire scope and affected
range. Concerned Destination is a raw-report diagnostic and never part of a
persistent record key. Explicit zero remains known clear; absence remains
unknown. This is separate from canonical SS7 destination authority: peer SCON
cannot change DAUD replies, and DAVA cannot clear peer congestion.

`SSNMSnapshot.PeerCongestion` and `SSNMPeerCongestionEvent` participate in the
same store revision and snapshot/subscription barrier as destination knowledge.
An event's `PeerCongestion` supplies its record and `PeerCongestionRemoved`
selects removal. Retained deltas commit before the raw report event; snapshots
see the completed transaction. Association restart/teardown, AS withdrawal and
binding retirement remove the affected records without clearing unrelated peers.
Late old-epoch work cannot recreate retired state.

An oversized peer SCON retires records only for its resolved AS bindings and
overlapping wire network. Unresolved peer scope can retire the association
view; a resolved empty or stale admission cannot remove newer knowledge.
DAUD and non-peer reports preserve peer records. Individual removal deltas
precede scoped destination invalidation in one store transaction. Oversized
DAUD preserves destination knowledge too: it is a query (RFC 4666 §4.5.3).
Authoritative reports invalidate only overlapping AS/network dimensions;
unresolved scope can invalidate the selected partition whole.

Peer records consume the existing global, per-peer and per-partition record
and accounted-byte budgets. Expanded AS/range records and copied wire scope are
charged; varying Concerned Destination cannot multiply retention. Refusal is
atomic for the report, increments existing refusal totals and publishes the
existing resource-loss signal. Subscription queue accounting includes peer
event payloads. `Resync` restores current retained peer records, including clear,
but cannot restore refused information or the history of raw CD triggers.

`Endpoint.CongestionInputs(as)` returns copies of AS membership, matching peer
records and outbound transport observations; absent AS returns
`ErrNoConfiguredAS`. The membership generation, SSNM revision and each transport
component's epoch/configuration generation/sample timestamp are checked around
collection. One retry bounds collection work; `Consistent=false` exposes a
second change with component versions intact. `VersionsBefore` and
`VersionsAfter` are `CongestionInputVersion` vectors containing `Revision`,
`MembershipGeneration` and `Peers []PeerCongestionVersion`. Each peer version
identifies association, epoch, active binding and binding generation, an opaque
`LocalGeneration` for local lifetime/queue transitions, and carries the complete
comparable `TransportCongestionState` plus `TransportPresent`. Consumers compare
`LocalGeneration` as a version component and do not interpret its numeric value. Transport epoch,
configuration generation, sample timestamp, known status and missed samples
therefore remain observable independently. An in-progress epoch reset with an
old transport sample returns `Consistent=false`, even if both vectors observe
that same mismatch. `Revision` describes only the SSNM
component. This is an Endpoint observation, not an atomic cluster snapshot.
`Local` retains queue and application contributions separately. Queue overload
also uses its independent management stream; consumers must honor that stream's
loss contract independently of snapshots.

`SGPConfig.CongestionPolicy` has type `SGPCongestionPolicy` and receives
copied peer and transport inputs as a boolean application veto. It runs before
the write barrier, outside Endpoint, AS and delivery locks. Final delivery takes
`deliveryMu`, then `sendMu`, and checks the input versions briefly. A changed
version releases the barriers before reevaluating the callback once; a second
stale decision produces known-not-sent `ErrCongestionDecisionStale`.
New DATA queues behind the reserved in-flight delivery slot while recovery waits
for policy. Expired unsent recovery DATA is dropped, and indeterminate broadcast
recipients are never retried. Accepted DATA is never replayed through another
association because congestion changed.
A zero-byte `ENOMEM` is a proven whole-message transport refusal and retains
its recovery copy for retry. Positive error counts remain indeterminate, as do
unclassified causes on the ordinary send path; the no-replay rule applies to
those copies. The Linux
source and pinned transport boundaries are cited in `transportRefusedWhole`.

The receive retention lock order is `peerRetentionMu` → ordered `AS.mu` locks
→ `transportMu` → `muDynamicASKeys.RLock` → SSNM store `mu`. Registry lookups
finish before AS locks are acquired. The transport sampler uses `Endpoint.mu`
→ `transportMu` → dynamic-key reads, followed by nonblocking management
publication. Transport therefore always precedes the dynamic-key lock; a queued
dynamic-key writer cannot trap retention while it waits for the sampler.
AS, dynamic-binding and epoch lifecycle retirement takes the SSNM store lock
last. The store publishes to bounded nonblocking subscriber queues under its
lock; `Next` waits outside that lock and invokes no application callback.
Input collection takes separate AS, SSNM and dynamic-binding snapshots plus
atomic transport observations, without nesting their state locks.

`SGPConfig.PreferUncongestedPeers` is an opt-in engineering policy, default
false. For each member, the latest matching peer record by `Revision` supplies
its peer input, then loadshare ranks admitted alternatives by the maximum of
that input and transport severity. A newer matching exact/range or RC-subset
clear supersedes an older matching range. Present omitted-level peer reports
and observed unknown/stale transport rank above level 3; absent reports and
absent detector state add no pressure, while remaining unknown in the inputs.
These ranks are an engineering preference. Equal-rank alternatives retain their
original active-member order and SLS modulo selection, without a new SLS cache;
assignments change only with strict rank, admission or membership changes. The
existing ASP/IPSP route and SLS caches stay under `aspRoutes.mu`; transport
projection reaches them only after releasing Endpoint and sampler locks. SGP
peer retention never acquires that routing lock.
Override keeps its selected active member rather than promoting standby on congestion.
Broadcast preserves all required copies; each application veto is an explicit
unsent member result. Rank changes can move SLS flows and missequence traffic
without a cross-association changeover barrier (RFC 4666 §1.4.7).

The application/NIF owns SPMC mapping, alternate capacity assessment and any
SS7 TFC trigger/encoding/transmission. RFC 4666 §1.4.6 permits such triggering
and makes it implementation dependent; it does not require an embedded SS7
stack. No automatic TFC operation or SPMC boolean is provided, and resync must
never trigger historical TFC replay.

## Route-independent SSNM knowledge

The SSNM operations above are how a node *originates* a report. What a node
*retains* from the reports it receives is a separate store, deliberately not
keyed by anything on the wire.

```go
type SSNMPartitionKind uint8

const (
    SSNMCanonicalPartition SSNMPartitionKind = iota + 1
    SSNMStandalonePartition
)

type SSNMPartition struct {
    Kind              SSNMPartitionKind
    SignallingGateway SignallingGatewayID
    ApplicationServer RemoteASID
    Association       AssociationID // standalone partitions only
}

func (e *Endpoint) SSNMKnowledge() SSNMSnapshot
func (e *Endpoint) SubscribeSSNM() (SSNMSnapshot, *SSNMSubscription, error)

func (s *SSNMSubscription) Next(context.Context) (SSNMEvent, error)
func (s *SSNMSubscription) Resync() (SSNMSnapshot, error)
func (s *SSNMSubscription) Close() error
```

Four decisions are worth recording.

**Ownership is canonical, not wire.** A partition is one Signalling Gateway and
one Application Server. Section 1.4.2.1 makes a Routing Context an index into
the *sending* node's distribution table, so two SGPs of one Signalling Gateway
may label one Application Server differently and the same value at another
Signalling Gateway means something else. Keying retained knowledge by the label
would have merged two networks' reports whenever the numbers collided, and lost
a Signalling Gateway's knowledge whenever the association that happened to carry
it closed. A standalone partition covers an Association with no provisioned peer
identity, which is how an Endpoint without peer inventory still retains what it
is told.

Oversized received reports invalidate only dimensions overlapping the resolved
AS and Network Appearance. Standalone partitions can contain several ASes;
canonical partitions already identify an AS, so sibling RC labels are ignored
while network scope still matters. Admission captures immutable resolved AS
keys, charged with each dimension, so source deregistration and RC reuse cannot
change a retained statement's effective network. Omitted parameters inherit
configuration at admission;
an all-appearance AS with omitted NA overlaps every appearance of that AS.
A retained statement spanning several ASes/appearances is removed whole when
it overlaps, because the refused APC list is never expanded or partially applied.
If the report's AS cannot be resolved, the selected partition is invalidated
whole. DAUD is a query and invalidates nothing (RFC 4666 §4.5.3).
On oversized refusal, `PartitionsInvalidated` increments per nonempty removal
publication, including chunks for custom record limits. Last-binding retirement
still increments it as before.
The exact removal payload is owned and charged to subscription byte budgets;
queue overflow still requires Resync, which returns all surviving records.

**The snapshot and the subscription are atomic with respect to each other.**
`SubscribeSSNM` takes the snapshot and registers the subscription in one
critical section, so no report is both in the snapshot and in the stream, and
none is in neither. Reading a snapshot and then subscribing would have left a
window in which a change belonged to nobody, which is the kind of gap that shows
up as an unexplained stale destination months later.

**Bounds refuse; they do not evict.** The store is bounded in every dimension a
peer controls — records per partition, per peer and per Endpoint, accounted
bytes, partitions, subscribers and per-subscription queue depth and bytes.
Reaching a state-retention bound refuses the record or the report and counts it in
`SSNMSnapshot.RecordsRefused`, `ReportsRefused` or `PartitionsInvalidated`, with
`LastResourceLoss` naming the most recent. A subscription that falls behind is
told so with `SSNMEvent.ContinuityLost` and recovers with `Resync`. Silent
eviction would have handed the application a view that looked complete and was
not, which is worse than refusing: a peer chooses every Affected Point Code it
reports, so unbounded retention is peer-controlled memory, and silent eviction
is peer-controlled misinformation.

**Events are deltas; only the snapshot is whole.** Ordinary report events carry
`SSNMEvent.DestinationUpdates` destinations with both retained dimensions. Oversized
reports use one raw observation and bounded replacement/source-continuation
patches. Apply them once in order with `ApplyReport`, through
`ReportContinues=false`, to obtain the complete report effect. No unrelated
destination is replayed. Binding
lifecycle events carry no destinations. Retirement discards the partition.
`SSNMDestinationsInvalidatedEvent` replaces the removed whole-partition event
identifier. `Invalidated` carries exact destination keys and independent
AvailabilityRemoved/CongestionRemoved flags. `CongestionSourcesInvalidated` and
`AvailabilitySourcesInvalidated` carry independent
`SSNMCongestionSourceInvalidation{Destination, AS, Revision}` and
`SSNMAvailabilitySourceInvalidation{Destination, AS, Revision}` identities.
Losing a scalar dimension does not imply losing either source history, and a
history-only destination remains until its last source is removed. All three
arrays are owned by the consumer; cloning preserves their independence.

The event routes by the Endpoint-unique partition `Epoch`, with `Partition`
empty. `ApplyInvalidation` applies all three arrays to a matching caller-owned
snapshot partition and preserves other dimensions, sources and bindings. It is
idempotent and rejects an event of another kind or epoch. An independent replica
must remove a source only when destination, resolved AS and source report
revision match. Removal IDs replay no raw RC lists, severity or configured
identity names.

Each array is capped independently at 2,048 entries per publication. Dimension
IDs cost 16 accounted bytes and source IDs cost 48. The dimension-only bound
remains `512 + 160 + 2048*16 = 33,440` bytes; the combined bound is `512 + 160 +
2048*(16+48+48) = 230,048` bytes. Both are below the default 1 MiB queue for a
single event. Larger custom partitions use ordered chunks under the store
transaction. Each removal publication gets a revision and increments
`PartitionsInvalidated`; last-binding retirement retains its counter semantics.
Aggregate queued bursts, slow consumers and smaller custom queues can still
require `Resync`.

Scalar destination retention costs 128 fixed bytes, 4 per raw wire RC and 16 per
admission-time resolved AS entry. Auditing adds availability source history;
validity or auditing adds congestion source history. Each source adds one record
per exact association, resolved AS, original scope and range, at `144 + 4*rawRC`
bytes per AS. Nil policies preserve scalar record counts and costs. A partition
costs 256 bytes. Positive `MaxBytes` must be at least 400, which holds one
contextless resolved scalar dimension and its partition. A single-RC auditing
availability statement or validity/auditing SCON needs 552 bytes for its scalar,
source and partition; its one-binding/one-partition job adds 472 before Info
String, totaling 1,024 bytes for that statement and job. These accounting units
are portable reservations, not heap or RSS measurements. A successfully retained
report never removes a destination: DAVA and a level-zero SCON are retained as
statements, and bounds refuse rather than evict. Applying the events in order to
the snapshot the subscription started from reproduces the partition knowledge
`SSNMKnowledge` returns (its refusal tallies are counters no event carries); the
whole partition comes only from `SubscribeSSNM` and `Resync`. Carrying the
partition in every event had made a one-destination report cost the partition's
size once per subscriber and once more to build — about 24 MB per report with
16,384 retained records and 8 subscribers — so publication could not keep up
with a steady stream of one-destination reports.

`SSNMStateConfig.SubscriptionQueueBytes` defaults to 1 MiB independently of the
256-event default `SubscriptionQueueSize`. Positive byte limits must be at least
512. The portable accounting charges 512 bytes per queued event, 8 per report
destination, 320 per updated destination, 4 per Routing Context in the report,
both dimensions and each availability/congestion source of an update, 256 per
peer or source record, 16 per dimension-removal ID, 48 per source-removal ID, 56
per expiry ID, and the byte lengths of the reason and both event/report
partition identity strings. Every variable field is charged even when its
presence flag is false, because the queued copy retains it. These fixed charges
are accounting units, not a heap-size measurement; queue backing-array capacity
and allocator overhead are separate. Owned snapshot results and the fixed,
out-of-band continuity-loss marker are not charged against this queue budget.

Both queue limits are checked before cloning an event. Overflow preserves
already queued events and marks continuity lost; later reports are suppressed
until `Resync` atomically replaces the view and clears the queue. Taking an
event releases its accounting and clears its queue slot. `Close` preserves
queued events for delivery before the terminal result, so their accounting
remains until they are consumed.

`Epoch` is the binding generation, numbered across the whole store. A partition
that loses its last binding is retired; the next binding starts a new epoch, so
knowledge from before a source reset is never mistaken for knowledge after one.
`Revision` increases strictly across a subscription's events and is always
greater than the snapshot it started from.

Reports admitted during the RFC 4666 §4.5.1 activation window (DUNA, DRST,
and SCON) and §4.6 restart window (also DAVA) are retained for both explicit
Routing Contexts and a contextless AS with an outstanding ASP Active. Section
3.4.1 permits omitted RC for an uncoordinated Routing Key. An unconfigured RC
is rejected with Invalid Routing Context, including that RC, under §3.8.1;
without an outstanding activation, inactive SSNM remains rejected. An omitted-RC
ASP Active covers both the contextless AS and its configured and dynamically
registered RCs (§4.3.4.3). Ack, cancellation and T(ack) exhaustion close the
request's window. Completed ASP Inactive or ASP Down also stops competing ASPAC
requests whose transmitted scope overlaps the withdrawal; a partial Ack does
not shrink the message that would be retransmitted (§4.3.4.4).

Classification takes the ASP state and request inventory together under the
existing T(ack) lock, in state-before-request lock order. An admitted report
racing cancellation remains pending knowledge unless its AS has completed
activation. Classification does not hold an Endpoint-wide barrier through
report handling or socket writes.

A pre-Ack report whose Network Appearance differs from the activating scope is
discarded before mutation. This is this implementation's interpretation:
§3.8.1 permits silent discard of some unexpected messages, but does not prescribe
NA-mismatch discard. The Invalid Network Appearance ERR in that section is
SGP-originated. Active-state acceptance of Network Appearance is unchanged.
An accepted report resolving outside the classified scopes now has a pending
fallback binding, so its binding events and `TrafficAuthorized` can differ from
the former active fallback.

Accepted early reports remain the SGP's latest knowledge when an activation is
cancelled, refused or exhausts T(ack), as accepted active reports remain after
withdrawal. Request retirement does not roll back knowledge or clear derived
state. Later activation can receive fresh reports or audit with DAUD (§4.5.3).
Normal association/binding cleanup and configured retention limits still apply.
Permanent source retirement at Association close prevents late bind or apply
operations from recreating that source's canonical knowledge. The store tracks
only live source IDs, registered when the Endpoint takes ownership and removed
at permanent retirement, so association churn retains no departed source IDs.
It does not change retention on cancellation of an activation request.

Refusal ERR reaches Layer Management. Section 3.8.1 makes Diagnostic Information
optional and permits any information relevant to the error; M3UA supplies no
transaction identifier. The library does not correlate an ASPAC attempt by those
bytes or retire it on ERR. Its request and early window remain until normal
completion, cancellation or T(ack) exhaustion; Layer Management can cancel it.

These reports
authorize nothing on their own: `SSNMPartitionKnowledge.TrafficAuthorized` is
true only once at least one binding has completed activation, because Section
4.3.4.3 says "The ASP SHOULD NOT send Data or SSNM messages for the related
Routing Context(s) before receiving an ASP Active Ack message, or it will risk
message loss."

## Management indications and overflow

`ManagementIndication` gains:

```go
Association         AssociationID
ASKeys              []ASKey
AffectedDestinations []AffectedDestination
Cause               error
```

`ASKeys` is the complete explicit or configuration-implied scope. It preserves
Network Appearance and contextless AS identity instead of projecting to bare
Routing Context. `AffectedDestinations` retains masks and scope. `Cause` is set
for local M-ERROR and M-SCTP_RELEASE.

The projections they replaced are removed rather than kept alongside. The old
`RoutingContext` and `RoutingContextSet` fields reported only the first Routing
Context of an indication that may name several, and `AffectedPointCodes`
reported only the first unmasked destination. A field that is right for the
one-element case and silently wrong otherwise is worse than no field: it reads
as complete. The indication owns its slices, so an application may retain or
modify them.

The bounded `StateChanges` and `ManagementIndications` streams remain
association-scoped and subscribe independently on their first method call.
Earlier events are not queued or replayed; `State` provides the current state.
`AssociationConfig.Subscribe` runs once on the Dial or Accept goroutine after
socket setup and Endpoint tracking, and before the handshake and its
establishment timeout start, so accessors called there also receive the
handshake: at an ASP, the M-ASP_UP and M-ASP_ACTIVE confirms of RFC 4666
Sections 4.3.4.1 and 4.3.4.3 as ASP-INACTIVE and ASP-ACTIVE, and the peer's NTFY
and ERR. The opening ASP-DOWN is the initial state (Section 4.3.1), not a
transition. The field's documentation states the callback's restrictions.
Calling neither accessor queues nothing, and unsubscribed streams cannot
overflow. Each method returns the same channel on later calls, and applications must drain every
subscribed stream for the association's lifetime. Overflow closes it with
`ErrIndicationQueueFull`; `Done` closes, `Err` returns the cause, and no later
mandatory event is represented as delivered. MTP3-User destination indications
retain their explicit resynchronization marker because an authoritative
Endpoint destination snapshot exists. No mandatory management event is silently
dropped from a subscribed stream.

## Concurrency and ownership

- Endpoint maps and ID allocation are protected by the Endpoint mutex.
- Status snapshots copy pointers, maps, slices, and SCTP addresses before
  returning.
- No socket or Endpoint lock is held across a user callback or network write.
- Explicit procedures use the existing per-association T(ack) registry and are
  safe for concurrent requests in disjoint Routing Context scopes.
- Duplicate or overlapping requests retain the existing T(ack) serialization
  and acknowledgement matching rules.
- Association close removes the ID from future Endpoint snapshots only after
  the final M-SCTP_RELEASE indication has been published.

## Verification

The implementation requires failing tests first for:

- deterministic status enumeration and deep ownership;
- same Routing Context under different Network Appearances;
- contextless AS status and ambiguous legacy lookup rejection;
- each automatic and explicit procedure state path, timeout, cancellation, and
  role-invalid no-write behavior;
- IPSP Single and Double Exchange local/peer direction separation;
- DAUD, both SCON directions, congestion level zero/omitted/1-3, Concerned
  Destination directionality, and DUPU User/Cause validation;
- full management indication scope and local failure causes;
- state and management queue overflow;
- concurrent association tracking, querying, procedures, and close;
- parser fuzz seeds for every typed SSNM representation; and
- Linux SCTP integration for readiness, request/ack sequencing, shutdown
  policy, fan-out, and no post-cancellation retransmission.

## Congestion method profiles

`EndpointConfig.Congestion` selects immutable network methods from the procedures
read in Q.704 (07/96) §§11.2.3–11.2.5 and the encodings in RFC 4666 §§3.3.1 and
3.4.4. This does not certify ANSI, TTC or the unavailable Q.704 covering-note
erratum. Every role requires an explicitly chosen Default.Method. Nil Congestion,
a zero default method or zero method in either override table is invalid; there
is no auto-detection or permissive fallback. The application knows which SS7
network procedure applies. The three valid method numbers remain 1, 2 and 3.


An explicit International profile sends MP zero and omits Congestion Indications.
National-with-priorities allows MP 0 through MaxLevel; national-without-priorities
sends MP zero. National SCON may omit its level or carry 0 through MaxLevel.
National MaxLevel zero selects 3 as an engineering default, not a standards value.
Invalid ASP/IPSP DATA and any invalid SCON, typed or serialized, fail with known-not-sent
errors after the existing framing, stream, scope and state checks.

Profiles match the validated AS network, preserving omitted versus explicit NA
zero. A peer-qualified ASP selector precedes the unqualified selector and Default;
association overrides precede the Endpoint table and cannot specify PeerSGP.
Selectors are copied, immutable and unique within each table. One logical ASP
route must resolve to the same method and maximum across alternatives; static
configuration is checked at construction and resolved bindings before transfer.

Reception is lenient about well-formed profile mismatches. SSNM retains the raw
level unchanged and carries `ProfileMismatches` on its accepted report for Layer
Management. DATA is delivered with its original MP. Its first mismatch produces
one `ManagementCongestionProfileMismatch` indication per association lifetime;
`Association.CongestionProfileDiagnostics()` retains a cumulative count and the
latest owned sample, including exact wire scope. Subsequent mismatches update the
snapshot without queuing more primitives, even across valid traffic or SCTP
restart. This reporting limit is an engineering choice. The bounded diagnostic
never sends a protocol Error. Existing malformed
input, framing, scope and state errors are unchanged.

Only ASP/IPSP local-user offers use the national-with-priorities discard boundary
of Q.704 (07/96) §11.2.4. MP equal to effective congestion is permitted; lower MP
is refused before transport submission. Received levels and transport severity
are separate inputs; their effective national level is capped at MaxLevel.
Unknown present congestion refuses the candidate conservatively as an engineering
policy. No detector observation remains absent. The SGP NIF transit path validates
the supported MP width but does not acquire the local-user discard rule. It
relays NIF MP0..3 unchanged even when its profile does not allow that MP, and
uses the same lifetime mismatch count/latest sample/first indication as peer
DATA. Precheck is pure so a raw send cannot count twice. MP>3 is refused before
writing under the selected fallback bound; this is not a full ANSI/TTC-width
compliance claim. Application congestion
policy remains an additional veto, outside Endpoint/AS locks.

Transport pressure stays local and never originates SCON or clears received SS7
congestion. National receive-queue overload contributes MaxLevel; the mapping is
an engineering choice. Queue drain withdraws only that source, so an application
source can keep the aggregate congested. The common optional scheduler retains
current reports across full-buffer refusals. A submitted national onset reserves
capacity for its eventual clear; an onset never submitted is cancelled on local
clear. Recovery omits Concerned Destination because it has no fresh DATA trigger,
while retaining separate reservation keys for distinct triggering OPCs.
International queue episodes retain omitted-level onset and no numeric wire
clear. Reports preserve exact wire scope and live binding/epoch fences; local
recovery does not prove clear submission or peer receipt. SGP receive-queue
overload stays local. SPMC policy and SS7 TFC generation remain with the
application/NIF; [continued offered-traffic status](../../README.md#continued-mtp-status) procedures are separate.

Distribution passes its resolved AS network into the shared encoded admission,
without inserting an omitted wire NA. Unused values of absent scope fields are
normalized consistently with binding admission. Transport preference can rank an
unobserved member below a measured clear member; that ranking signal alone is
not present congestion for the mandatory priority gate. Required broadcast copies
try later AS and SGP alternatives before reporting an explicit unsent obligation;
a successful copy is never replayed when another required recipient is refused.


Received ERR and NTFY explanations reach Layer Management under RFC 4666 §4.2
as `ManagementIndication.DiagnosticInformation` and `InfoString`, respectively.
The publication boundary copies the bytes; nil means absent and a non-nil empty
slice means present and empty. `Description` keeps the code/status description.
Undefined Traffic Mode Type in ASPAC selects Error 0x05 under §3.8.1 without
activation or Ack. Other parameter validation and error precedence stay strict.

## Received congestion validity and audits

`CongestionConfig.Validity *CongestionValidityConfig` and
`Auditing *DestinationAuditConfig` independently opt into received-SCON validity
and ASP audit scheduling. Nil disables each. A nonnil configuration defaults
zero `After` and `Period` to 30 seconds, `MaxPending` to 256 and
`MaxMessagesPerSecond` to 10. Negative fields and auditing on SGP/IPSP are
invalid. Configuration is copied. RFC 4666 §3.4.4 optionally permits local
validity (lowercase may); §4.5.3 permits periodic/isolation DAUD (MAY). These
values and the scheduler representation are engineering choices.

Congestion-origin audits require `CongestionNationalWithPriorities`.
DUNA/DRST availability auditing works with any method. `OnActivation` supplies
configured isolation scopes after activation commits, including newly activated
AS scopes; it is not arbitrary path-isolation detection. RFC 4666 §4.5.3's
implementation note uses lowercase must for excluding level-zero SCON from the
periodic congestion procedure. SCON(0) cancels only its congestion origins.
DAVA ends corresponding availability/isolation origins and resets surviving
congestion deadlines without clearing retained congestion.

### Original source, independent origins and admission

Jobs retain the original Association, its admitted epoch, normalized source
scope key, original raw wire scope, compact destination range and Info String.
Each captured AS binding has its own origin: partition, cause flags and due
time. A grouped RC {1,2} report therefore remains auditable for RC2 after a
separate clear for RC1; clearing RC2 removes the remaining matching origin.
An outgoing DAUD includes only still-due RCs. The original source scope remains
available for accounting and diagnostics even when that outgoing list narrows.
There is no masked-PC expansion, sibling substitution or audit-driven activation.

A cause retires only when newer applicable clear prefixes collectively cover its
range for every AS still needing that cause, including interleaved reports. A
partial clear keeps work for the remaining range; expired or otherwise unknown
congestion still needs an audit. Coverage follows bounded retained prefixes,
without per-point audit history. Three fixed `uint64` report-version fields
distinguish congestion, availability and isolation causes; the job's 256-byte
base accounting is unchanged.

Received availability and congestion apply to their resolved AS and Network
Appearance. A report scoped to one AS supplies no authority for an unrelated AS.
Lookup and audit retirement use this same rule. Auditing retains independent
`AvailabilitySources`; validity or auditing retains independent
`CongestionSources`, so interleaved clears cannot overwrite another audited AS's
evidence. With both policies nil, the scalar cache still filters by AS; a later
scalar can displace older AS evidence, leaving that AS unknown rather than
borrowing the later statement. Nil policies add no source-history records.

Canonical transfer selection, observed status and direct national-priority DATA
admission use the configured SG/AS aggregate. RFC 4666 §§1.2, 1.4.2.1 and
3.3.1 allow coordinated SGPs to use different local NA/RC labels for that
owner; a sibling's report is not filtered by another SGP's numeric labels.
Standalone projections and explicit source-scoped reads retain AS/network
filtering. Source history, audit origins and invalidation retain their
original reporter and scope; a shared route view does not widen them.

Positive status updates stage the movement of matching origins into replacement
jobs under the store lock. If the replacement cannot fit `MaxPending` or the
byte budget, prior origins, job liveness and charges are restored. A refused
replacement does not silently discard previously admitted work. Independent
congestion, availability and isolation causes are updated separately.

With validity or auditing enabled, full source admission captures the
peer-direction registration token and resolved applicability before legacy route
resolution. Both publication and oversized-report invalidation consume that
original admission. The final order is submission tracker → dynamic bindings →
SSNM store; the same captured token fences retained history and job insertion.
An active change gate is discarded by the receive path, not joined. The store
rejects stale Association epochs and advances its remembered source epoch
monotonically. Projections and transport run only after these locks end.

Registration removal, version replacement, epoch change and source loss retire
the affected received history and audit origins and release their record/byte
charges. Static AS membership still serving that scope survives dynamic removal;
an IPSP Double Exchange local-direction change does not retire independent
peer-direction received state. Unaffected source/AS contributions survive.
Source-only destinations remain representable after their scalar copy disappears.
Admitted oversized refusals refresh ASP and IPSP projections after releasing the
admission fence, preserving unrelated AS and transport contributions.

`Association.ForgetDestinations` also releases the calling ASP/IPSP reporter's
canonical availability, congestion source history and periodic audit origins,
then refreshes projections. Canonical siblings, peer ASP congestion, independent
transport observations and configured isolation work survive. Its return value
still counts only released legacy destination rows; it does not include
canonical records or audit jobs. An unconditional forget-generation fence
rejects stale received admissions and legacy writes delayed after route
resolution, including when validity and auditing are both nil. Canonical
publication checks the same generation. Bindings stay live for genuinely new
reports.

With both policies nil, receive admission captures one atomic forget-generation
load and validates it under the existing legacy/canonical mutation locks. It
adds no submission-tracker acquisition or source-history records. Full source-
epoch and registration-token capture remains enabled only with validity or
auditing; the unconditional forget barrier does not extend that separate policy.

The forget generation is also checked under the existing route-cache mutex
before a delayed report can publish derived indications. `ForgetDestinations`
does not reclaim pre-existing shared SG route-index allocations; its reclaim
contract covers the reporter-owned stores and periodic work described here.

### Retention and diagnostic budgets

Scalar destination dimensions cost `128 + 4*rawRC + 16*resolvedAS` bytes. Each
independent availability or congestion source adds one record and `144 +
4*rawRC` bytes per resolved AS, alongside its scalar dimension. Availability
history is enabled by auditing; congestion history by validity or auditing. Nil
policies retain scalar record counts and charges. A partition costs 256. The
minimum nonzero `MaxBytes` is 400; a single-RC auditing availability statement
or validity/auditing SCON with one resolved AS needs 552 bytes. A job costs `256
+ len(info) + 8*rawRC + 144*bindings + 64*uniquePartitions`; each binding
includes an 80-byte origin and 64-byte lifetime token. One RC, one binding and
one partition therefore add 472 bytes before the Info String; together with that
statement and partition the total is 1,024 bytes. All jobs share the SSNM store
byte limit and `MaxPending` bounds jobs. These values are portable accounting
units, not allocator/heap/RSS bounds. The three cause report-version counters
fit the fixed job base charge; no per-point audit history is retained.

`SSNMPublicationOutcomeEvent` distinguishes kinds using `PublicationKind`. For
DAUD, it aggregates transitions into at most one event for each of seven fixed
reasons per report, automation pass or retirement batch:
`queue-full`, `rate-deferred`, `reporter-retired`, `send-deferred`, `contended`,
`send-failed` and `submitted`. `PublicationCount` counts each completed job outcome
for submitted, send-failed and transport send-deferred, even consecutive equal completions or a
same-key refresh during transport. The successor keeps its newer causes and
deadlines. Each removed job contributes once to `reporter-retired`, regardless
of whether source, registration, binding or epoch retirement runs first.
Contention and rate deferrals count outcome transitions; repeated identical
deferrals are suppressed. Counts remain per job: one batched DAUD can contribute
several completed outcomes. Its `ReportSet` is false. `Report.Association` identifies a homogeneous
source, or is zero for mixed sources. `Report.Scope` retains a homogeneous
original source scope only when it has at most 64 raw RCs; otherwise
`PublicationScopeOmitted` is true and the scope is empty. No APC list, Info String or
configured identity names are repeated in these diagnostics.

One diagnostic costs at most 928 accounted bytes using the general 160-byte
reason ceiling; the longest fixed reason makes the actual maximum 784. Seven
outcomes consume at most seven queue slots and 6,496 conservative bytes. With a
report, add the actual report-observation and report-patch event count;
only an unfragmented observation adds one slot. The defaults remain 256 slots
and 1 MiB. The validated/local report payload has separate destination/source-delta
accounting and is not capped at a diagnostic's size. Custom limits, report
payloads and aggregate bursts can still lose continuity and require `Resync`.
A successful completed submission also publishes one local DAUD report per
wire batch, rather than one per canonical partition. Its `Partition` and `Epoch`
are empty; `Scope` and `Destinations` describe the actual submitted batch. This
is a breaking observation-cardinality change, independent of logical-job outcome
counts. A later report refresh does not erase that observation. A failed attempt records its
actual outcome without publishing a successful-send observation. Submission proves neither peer receipt nor a
transaction-correlated response.

### Expiry and subscription state

`SSNMCongestion` and `PeerCongestionState` expose `ReceivedAt`, `ExpiresAt` and
`Freshness`; a zero deadline does not expire. `CongestionSources` retains each
source's resolved `AS` and original wire scope, including omitted RC
applicability. Validity or auditing preserves each congestion source's resolved
AS and wire network scope; auditing also preserves independent availability
sources. Canonical SG/AS owners combine contributions only for that AS and
applicable network scope. Other ASes cannot supply a displaced statement's
authority. A candidate expiry carries partition epoch and source report version;
each short commit rechecks those values, freshness and deadline before changing
state.

Expiry preserves severity, provenance and chronology as `CongestionExpired`
history and publishes `SSNMCongestionExpiredEvent`.

Expiry events carry owned `SSNMEvent.Expired` identities of type
`SSNMCongestionExpiry{Epoch, Destination, AS, Revision, Peer}`. Call
`event.ApplyExpiry(&snapshot)` on the owned full snapshot. Each identity names
the Endpoint-unique partition epoch and original statement version; only
matching source records, their matching scalar copy, or the identified peer
record become `CongestionExpired`. Values, severity, timestamps, provenance and
the snapshot's stream revision remain unchanged. The caller orders the event
stream. Expiry uses neither `DestinationUpdates` nor `PeerCongestion`; top-level
`Partition` and `Epoch` are empty because one event may span partitions. Each
commit rechecks at most 256 candidates and publishes their freshness changes
atomically under the store lock, then releases it before the next chunk and
cancellation check. Each ID costs 56 accounted bytes. At the default
16,384-record ceiling, an expiry-only pass queues at most 64 compact SSNM events
and 950,272 bytes in the default SSNM subscription. This bound excludes reports,
other queued bursts and the separate derived MTP indication channel; SSNM
subscription overflow still requires `Resync`.

Queued `DestinationUpdates` destinations now charge 320 fixed bytes, reflecting the
larger public destination shape. Each individual peer or availability/congestion
source row still charges 256 bytes, plus its Routing Context storage. Review
custom subscription byte budgets: report updates can reach them sooner. This
queued-payload change leaves source-removal IDs at 48, expiry IDs at 56, the
230,048-byte combined invalidation maximum and the 950,272-byte compact expiry
pass bound unchanged; retained record and job charges are separate.

Expiry may produce more MTP indications than receipt when transport pressure
masks the received level but expiry changes `ReceivedCongestionUnknown`. A
projection emits at most one indication per destination whose exposed status
changes; it does not suppress that uncertainty change. If the bounded indication
queue overflows, it emits `ResyncRequired` and the consumer must recover current
statuses with `MTPDestinationStatuses`. The compact expiry bound applies to SSNM
subscription events, not this MTP indication queue.

For each source, the newest covering
statement is chosen before freshness; expiry cannot reveal an older superseded
range. Other fresh received sources and independent transport contributions stay
effective. `MTPDestinationStatus.ReceivedCongestionUnknown` is uncertainty even
when another contribution remains congested. No current congestion plus that bit
is not measured abatement. National priority admission refuses received unknown;
international transfer policy remains distinct from its observability.

Lifecycle removal uses `SSNMDestinationsInvalidatedEvent` with independent
`Invalidated`, `CongestionSourcesInvalidated` and
`AvailabilitySourcesInvalidated` arrays. Source IDs carry `Destination`, `AS`
and `Revision`; availability IDs use `SSNMAvailabilitySourceInvalidation`.
`ApplyInvalidation` patches scalar and history-only destinations by
Endpoint-unique partition epoch, with `Partition` empty. Source IDs cost 48
bytes each and dimension IDs 16; each of the three arrays independently caps at
2,048/chunk. The combined maximum is 230,048 bytes; the dimension-only bound
remains 33,440. All arrays clone independently, without replaying source scopes.
Consumers must handle all three or use the helper, then resync after loss.
`SSNMAvailability.AS`, `AvailabilitySources` and the new removal array are
breaking public field additions; update unkeyed literals, snapshot replicas and
event consumers.

Bare DAVA is availability only; fresh SCON renews knowledge and actual national
SCON(0) can clear it. Expiry emits no SCON(0), DAVA or RESUME and does not impose
a TTL on independent transport observations. No validity is negotiated with the
peer and no DAUD response transaction ID exists. International local expiry is
not a promise of recovery at a peer retaining omitted-level congestion forever.

### Worker and submission lifetime

The worker registers through `Endpoint.beginOperation` before it starts and
calls `endOperation` on exit. `Endpoint.Close` cancels first, closes Associations,
then joins operations outside Endpoint, AS, state, route, store and tracker locks
needed for completion. Cancellation is checked between jobs and expiry commits.
Association withdrawal and teardown never join the shared worker.

Restart and `ForgetDestinations` now exclude new optional SCON/DAUD admission,
cancel an admitted optional attempt and settle it before changing the epoch or
reclaiming knowledge. Concurrent lifetime changes share admission closure without joining each
other’s cleanup. Activation reconciliation leaves its bookkeeping untouched
while admission is closed, so the final owner can schedule configured work. Completion joins occur outside transport, destination, store and
procedure locks and settle only optional SCON/DAUD, never DATA or synchronous SCON.
At most one optional attempt holds the Association writer lock. This introduces
a cancellation-completion wait reachable from restart receive handling and the
state monitor; ordinary received status processing rejects a changing or stale
admission instead of joining that gate. No universal elapsed-time bound is
claimed for the transport's internal mutex or application deadline.

Optional submission tries the traffic barrier, writer lock, brief state check,
submission tracker, dynamic bindings and acknowledged-context locks in that
order. No store, state, binding or tracker lock crosses transport; the traffic
barrier and writer lock cover one `NoWait` attempt. Contention and socket-space
pressure defer bounded work, with retry ordering that does not permanently
starve a healthy original reporter. `NoWait` does not promise a universal mutex,
wall-clock or application-deadline bound for the pinned transport.

Late REG/DEREG responses on the receive dispatcher or request goroutines can
hold RKM correlation/lifecycle locks while joining a cancelled optional SCON/DAUD
tracked by the submission tracker. The binding-change gate cancels it, releases
tracker/dynamic locks and waits before committing replacement. Optional
submission acquires neither RKM lock; completion needs only the tracker. This
cancellation-completion join is separate from waiting for peer socket progress.
SNDINFO/complete-record semantics retain RFC 6458 §5.3.4 and Verified Errata
6111; ordered stream use follows RFC 9260 §§6.5–6.6 and remains permitted by RFC
4666 §4.5.3.

### Retained-state work and derived indexes

The work bounds below count visited records, copied entries and prefix nodes;
there is no elapsed-time, heap or RSS guarantee. Let `R` be retained logical
records, `S` received source rows, `D` distinct destination prefixes, `A` the
report's resolved AS entries, `J` audit jobs, `O` their total origins, `B` retained
bindings, `S_a` and `S_c` the availability and congestion source counts,
`H` the source rows included in an owned event, and `W = 25` the fixed 24-bit prefix depth including its root. Scope
hashing, owned wire-list copies and publication also cost the bytes they process.
Sorting costs are stated separately from row visits.

`SSNMStateConfig` defaults bound `R` to 16,384 globally, 8,192 per peer and
2,048 per partition, and partitions to 256. `DestinationAuditConfig.MaxPending`
defaults to 256 jobs. Each admitted origin charges 144 bytes, so total origins
are also bounded by `MaxBytes / 144`, before other positive charges. The default
`MaxBytes` is 4 MiB. `MaxSubscribers` defaults to 16; each queue defaults to 256
slots and 1 MiB of accounted payload. `MaxAffectedPointCodes` defaults to 1,024.
These limits are configurable. `SSNMStateConfig` has no separate binding-count
limit: `B` follows admitted Associations and configured AS inventory. Configured
AS lists likewise have no independent SSNM count limit; processing or rejecting
an input still costs its actual AS/configuration size. Accepted source fan-out
must fit the existing record and byte limits.

| Path under the store mutex | Work retained by the implementation |
| --- | --- |
| Received congestion or availability report | Sort/deduplicate `D` destinations and `A` ASes, then update the `D*A` source identities the report actually names. Scalar resolution is copied once and shared immutably. With registered subscriptions, exact destination buckets visit only the `H` source rows included in `DestinationUpdates`, once each, plus `D` scalar lookups and source ordering. Bounded report patches publish oversized updates. Without subscriptions, no destination updates or patches are built. |
| Partition snapshot, Subscribe and Resync | Enumerate scalar/bucket destination keys once, sort them, and read each retained source once. Destination ordering costs `O(D log D)`; source ordering costs the sum of `h*log h` within each destination. Whole snapshots additionally sort partitions, bindings and peer records and copy owned scope bytes; binding enumeration is `O(B)` plus its sorting. |
| Canonical and standalone destination projection | Availability uses one incremental input index shared with offered-status marks: at most two links per retained availability source and at most `W` prefix probes per query, with no cold history pass. Congestion builds one aggregate in `O(W*S_c)` on the first read after a congestion mutation, then follows at most `W` nodes without scanning source rows. Availability-only writes keep this congestion cache warm. Combined history reads use at most `2*W` probes/nodes. Scalar dimensions without history retain their fixed-prefix lookups and captured-AS applicability checks. |
| Expiry discovery and commit | Discovery scans partitions and received/peer source rows once, bounded by `MaxPartitions` and `MaxRecords`. Each commit rechecks at most 256 candidate identities by key and publishes one chunk. Mutations invalidate the derived aggregate; subsequent projections rebuild once for each mutation interval they observe. |
| Scoped invalidation | Compile the incoming AS/network sets once, sharing an unpartitioned set or using each captured partition's subset. Matching then visits retained rows and their captured AS metadata without an affected-AS cross join. One job pass visits origins and clones each affected job once across all affected partitions. Inline retirement performs exact source/origin index probes for each removed source; its final staged flush visits each candidate job/origin once more at most. Removal sorting and existing bounded event chunks remain separate. |
| Partial binding withdrawal | Resolve the withdrawn partition set once. Visit the reporter's association-indexed jobs and all their origins once, cloning each affected origin slice at most once for the set; reconcile surviving jobs once afterward. Partition/source cleanup visits each selected partition once. Source deletion invalidates captured job liveness inline and shares one staged replacement flush across the event. Work is linear in selected partition/source rows plus the reporter's jobs and origins, with existing removal/output sorting separate. |
| Multi-context registration or deregistration | Acquire the event's selected direction/RC gates together, join each matching tracked submission once outside store/tracker/map locks, then scan partitions and received sources once. Filter reporter jobs/origins once for all contexts and flush source-origin changes once. Stale version-only removals acquire no gates and scan no store records. The scan is bounded by global `MaxPartitions`/`MaxRecords`, reporter jobs/origins and selected RCs, rather than their product. Existing registry/configuration lookups and projections remain separate. |
| Peer AS or context retirement | One derived identity bucket per reporter/AS selects only its retained peer records. Across per-AS registry commits, work is selected AS probes plus selected records, preserving the existing AS commit and event order. A multi-context registration event scans its reporter's active AS keys once and selects matching buckets once. Ordinary source synchronization may scan retained peer records once; it is not repeated per removed context. |
| Source or epoch retirement; forget and reclaim | Association-wide jobs are removed through the association index once before the source/partition sweep. Forget and scoped invalidation share one job/source retirement batch across partitions. Each retained source/record set is visited once per sweep; a concrete partition uses a direct lookup. Last-binding reclaim drops its buckets and aggregate with its records. |
| Legacy destination retirement | Filter all withdrawn RCs against each original record's compact RC list once under the destination mutex. Stage replacements before insertion so map iteration cannot revisit inserted keys; merge dimension stamps on collisions and reclaim each affected prefix once. Work counts records and their RC entries plus existing per-prefix reclamation/output work, rather than one full record scan/copy per removed RC. This is a separate mutex from the Endpoint SSNM store. |
| Audit clear coverage | One report shares AS membership and one sparse summary per original reporter/AS/dimension. Build and summarize in `O(W*S)` over the inspected partitions, then answer each cause/version threshold in at most `W` nodes. Availability and isolation reuse the same summary with separate thresholds. Prefixes represent evidence; masked ranges never expand into point codes. |
| Positive audit admission and refresh | Build a job-prefix index once and keep per-destination admission, replacement and rollback in the original input order. AS membership and old-binding joins use maps. A job prefix has at most `W` distinct covering report prefixes; even stale operation-local keys therefore add at most a fixed-depth `O(W*W*(J+D))` traversal, plus actual moved origins and new bindings. There is no unconstrained `D*J` scan. |
| Scheduler pass | Scan jobs once under the mutex; sort due jobs and group by maps outside it. Each selected job/origin is checked a bounded number of times. Work is `O(J log J + O)` plus per-job scope ordering and payload bytes. Transport remains outside the store lock; contention and rate limits retain the existing local outcomes. |

The peer identity index has one entry per retained peer record and one bucket
per nonempty reporter/AS group; removal, refusal, epoch change and partition
retirement remove the corresponding entries. It borrows immutable record keys
and adds no portable record/byte charge. Like the other derived indexes, this
is a cardinality bound rather than a heap measurement.

Availability has one index node and at most two list memberships per retained
availability source, shared by ordinary status reads and offered-status marks.
The congestion aggregate has at most two sparse insertions per retained
congestion source and at most `50*S_c` nodes across `S_c+1` query groups.
Arbitrary queries create no groups. Congestion insertion, replacement, expiry,
removal or partition reclaim invalidates that aggregate under the same mutex. With concurrent mutations,
projection work is the sum of the rebuilds for the intervals actually observed,
plus fixed-depth queries; it is not a promise that a continuously changing store
builds only once. ASP/IPSP refreshes, transfer selection and direct DATA admission
share this cache, so broad sources are not reduced again for each candidate or
destination. Latest covering statements are still selected before freshness.
An equal-version overlapping report can be partially expired between expiry
chunks; the previous unordered scan allowed either tied statement to win. The
derived view fixes one such winner until its next invalidation; events and
historical statements are unchanged.

Destination/source buckets have one identity per retained source. Job indexes
have one association/source key per job and one immutable identity per origin. Retained jobs belong to the reporter's current live epoch, so an exact association/scope/range source key names at most one job. Inline retirement costs one such key/origin membership probe per removed source; it does not scan job origins until the shared flush.
Temporary coverage, scope-match, retirement and admission indexes grow with their
input records, origins and compact prefixes, not the covered point-code domain.
The retained aggregate and these derived indexes add Go allocations outside the
portable logical record/job charges. `MaxBytes` remains accounted retention of
records, partitions and jobs; it does not bound total derived-index memory, live
bindings, snapshot copies, allocator overhead, heap or RSS. Queue limits and the
950,272-byte expiry-only payload bound are unchanged.

The existing ASP route/inventory traversal and SCTP association sampler still
follow their configured routes, candidates and live Associations. Their counts
are not bounded by the SSNM record limit; the new source aggregate adds one
bounded rebuild and fixed-depth reads rather than a history scan inside each
traversal step. Owned snapshot replay helpers run outside Endpoint locks. None
of these indexes adds a lock, channel wait, worker join, application callback or
transport call. The restart, forget, RKM and `NoWait` qualifications above remain
applicable.

Lifecycle work probes count only their named operations: `sourceRows`,
`jobRows` and `copiedOrigins` in the retirement tests execute under the SSNM
mutex; `peerRows` counts general record-scan visits or indexed identities selected,
and `peerASRows` counts active-key visits or direct AS probes under that mutex.
The legacy probe counts original record visits under its destination mutex.
Setup and post-event checks are excluded. These are neither all instructions
under a lock nor lock-duration measurements. The shared `asRows` probe also
counts static-index preparation outside the mutex and is excluded from these
store-locked assertions.

The batched SCON binding fence retains the existing completion joins and gate
waits. It owns no partial set while waiting on a competing event. For SGP peer
registration changes, after those joins it holds `peerRetentionMu` through
peer-record retirement and dynamic-map publication, then releases it before
registry notifications, projection or reopening the gates. This serializes
short bookkeeping with the existing peer admission path; map/acknowledgement
mutex waits remain inside that span. It adds no transport/subscriber/callback
wait under the store mutex. The existing order `peerRetentionMu` before
AS/transport/dynamic/store locks remains intact; registration mutation uses
`peerRetentionMu -> muAckedRCs -> muDynamicASKeys`. Acknowledgement mutators
release `muAckedRCs` before synchronizing peer retention. Optional completion
acquires none of these lifecycle locks.

Final per-Association binding/profile checks outside Endpoint-wide locks retain
their existing configured-inventory cost, conservatively one configuration-sized
lookup per due binding. These checks scan no retained source history and add no
end-to-end linear-time or latency guarantee.

## Local congestion publication and DATA admission

`Endpoint.ReportLocalCongestion(LocalCongestionRequest)` declares application
state. The request identifies `Source`, `Association`, `Epoch`, `AS`, an exact
local `Destination` and `Congestion`. The source identifier occupies 1–64 UTF-8
bytes of Unicode letters/digits or `_`, `-`, `.`, `:`, `/`. The `$` namespace
is reserved; queue records use `$queue:<OPC>`. Admission requires the
current association epoch, active association member/binding of the exact AS,
and an exact 24-bit DPC. The AS has no authoritative local-destination inventory;
this validates the numeric value and current binding. It does not prove aggregate
AS readiness or authorize DATA/SPMC delivery. Optional wire admission separately
checks applicable AS delivery readiness. The library assigns
`LocalCongestionContribution.Revision` and preserves its copied `Scope`; callers
cannot provide a revision. Contributions have no TTL.
`WithdrawLocalCongestion(association, epoch, as, destination, source)` removes
only that source. Explicit zero records only the reporting source as clear;
withdrawal removes it. Lifecycle retirement and `ForgetDestinations` remove
affected scopes.

National explicit levels are 0..MaxLevel and require `Congested == (Level != 0)`.
A national omitted level requires `Congested=true`, `LevelSet=false` and `Level=0`.
International states require `LevelSet=false`, `Level=0`, and can declare onset or
local clear through `Congested`. Contradictory states are rejected with
`ErrInvalidParameterValue`, rather than normalized. Contributions share
`SSNMStateConfig` record, byte and per-partition budgets. Resource refusal wraps
`ErrSSNMResourceLoss` and publishes the existing loss event; it does not erase
previous local evidence. Each local source costs one record and
`320 + len(Source) + 8*wireRC` portable accounted bytes, including the normalized
scope key, not a heap-size bound. A changing or contended admission can return
`ErrCongestionDecisionStale`; the caller retries the complete declaration with a
current scope. An absent withdrawal is an idempotent success only with a current
active association member/binding.

`CongestionInputSnapshot.Local` retains queue and application contributions
separately. The publication aggregate uses maximum or unknown congestion only
within one association, AS and local DPC. Queue drain, transport recovery and
received SS7 clear cannot withdraw an application contribution. Local evidence
at an SGP remains an application/NIF input and never authorizes SGP-to-ASP SS7
SCON (RFC 4666 §§1.4.6, 3.4.4). No OS or CPU detector is installed.

### Bounded optional scheduling

`CongestionConfig.Publication *CongestionPublicationConfig` controls one common
optional scheduler for local reports and automatic audits. Nil or zero fields
select engineering defaults:

| Field | Default | Validation |
| --- | --- | --- |
| `MaxPending` | 64 | Negative invalid |
| `MaxPendingPerAssociation` | 32 | Negative or above `MaxPending` invalid |
| `MaxBytes` | 64 KiB | Negative invalid |
| `RetryInterval` | 100 ms | Negative invalid |
| `MinReportInterval` | 1 s after successful submission | Negative invalid |

Capacity bounds are Endpoint-wide. Each reservation/transient slot charges
`768 + 8*wireRC + 8*destinations` portable metadata bytes; local-source retention
uses the separate SSNM budgets described above. These are not heap-size bounds. A publication key preserves association,
epoch, wire NA/RC and local DPC, plus Concerned Destination only for an actual
triggering OPC. Distinct CD origins never merge. Equal desired states coalesce.
The scheduler keeps desired and last-submitted states separately; an admitted
onset cannot be overtaken by its clear. Local clear cancels an onset that was
never submitted. A publication reservation exists before any onset is submitted.

A submitted national onset holds its slot/byte reservation until its current
clear is submitted or lifecycle retirement cancels the scope. The clear reuses
that reservation; it cannot fail capacity admission. A new onset can supersede
an unsent clear in place by revision. A new key without capacity is visibly
refused or deferred before submission. Audits use only unreserved transient
slots and otherwise stay in their independently bounded deadline store. Pending
clears may exhaust capacity; the scheduler surfaces that condition without
reclaiming their reservations for new work. International drain retires pending
reports and never manufactures numeric zero. No periodic local SCON reassertion
is installed.

The worker runs independently of receive processing, the sampler and mandatory
control. It tries the existing traffic barrier/send serialization in their lock
order, then rechecks live state, epoch and desired revision. It makes one NoWait
submission without holding tracker or state locks across transport. EAGAIN and
contention retain bounded work; permanent state errors retire it; other failures
remain observable and retry after the configured interval until superseded or
retired. Each pass permits at most one optional attempt per association across
both kinds, subject to rate limits and deferral while mandatory control is
pending or waiting. An already admitted NoWait attempt can race a newly starting
mandatory write; there is no preemption or absolute fairness guarantee.

Optional work never enters the mandatory queue or reactivates an AS. Restart,
withdrawal, binding replacement and teardown fence stale work. `Endpoint.Close`
cancels before joining the operation-owned worker outside the locks required for
completion. Explicit `SignallingCongestion` and `DestinationStateAudit` remain
synchronous and return actual errors, without implicit retry.

`Endpoint.CongestionPublications()` returns copied `CongestionPublicationState`
values; the bound is `MaxPending`; completed clears leave the snapshot. Only
transient admitted audits appear here; pending deadlines remain in their
separately bounded audit store.
Each state identifies association/epoch, scope, destination ranges, publication
kind, optional concerned destination, desired congestion/revision, submitted
revision, pending flag, attempts, last attempt and bounded `LastError` (256 bytes).
A same-state coalesced update can leave `DesiredRevision` ahead of
`SubmittedRevision` with `Pending=false`. Local SCON `Attempts` belongs to the
reservation; DAUD `Attempts` describes its transient batch, not cumulative
deadline-store history.
After an admitted DAUD attempt completes, its management state sets `Pending`
only when the error is retryable, the endpoint remains live and a live deadline
job survives completion reconciliation. A covering DAVA can remove the last cause during transport;
that attempt still reports its actual outcome once, with `Pending=false`.
A surviving destination or newer cause keeps its own deadline. This is an
observation at reconciliation; subsequent lifecycle changes can retire the job.
Kinds are `CongestionPublicationLocalSCON` and `CongestionPublicationDAUD`.
`Endpoint.CongestionPublicationCounters()` returns `Queued`, `Submitted`,
`Coalesced`, `Deferred`, `Refused` and `Failed`. `Queued` counts reservation or
transient admission, not unique logical audit jobs; deferral decisions can recur
on later passes. `Failed` includes actual EAGAIN transport refusals. Logging is not required for
failure observation. `ManagementCongestionPublication` carries
`ManagementIndication.CongestionPublication` for new reservations, visible
capacity refusals, changed desired state for the updated key, and each actual
transport attempt. Equal desired state coalesces without another state-change
observation.
The common `SSNMPublicationOutcomeEvent` uses `PublicationKind`,
`PublicationCount` and `PublicationScopeOmitted` for logical outcomes.
`SSNMEvent.CongestionPublication` carries owned exact state for a single local
SCON outcome and is nil for aggregated audits. No second audit-only outcome is
published alongside the common outcome in the same channel. The DAUD count is
per logical job, so one wire batch can complete several jobs. Failed wire
publication never clears retained local evidence. The existing association
management queue has 64 slots; a subscribed application must drain it throughout
the lifetime, including actual retry attempts. Submission is not a peer-delivery
result. Lifecycle or pre-transport retirement of local SCON does not promise a
separate publication outcome notification.

### Cached DATA admission

`AssociationConfig.CongestionAdmission *CongestionAdmissionConfig` is opt-in; nil
disables the gate. `ChargedRatioLimit=0` selects 0.9 as an engineering default.
Nonzero ratios must be finite and strictly between zero and one; the backlog
monitor must be enabled. At the common typed/raw/distributed DATA admission
boundary, after send serialization, the gate reads cached charged/limit evidence
without querying the socket. It refuses at or above the ratio, before the first
valid sample, and when the current sample is invalid or stale. Refusal uses the
existing typed known-not-sent DATA envelope wrapping `ErrDataCongestionRefused`;
no wire Error, SCON or DUNA is generated. The gate does not apply to control.

A cached ratio does not reserve kernel memory. A single large message,
retransmission or direct descriptor user may consume remaining charge between
samples. It therefore gives no hard headroom or control-delivery guarantee.
`ControlWriteTimeout` retains its close-on-expiry behavior for permanent stalls.
RFC 4666 §1.4.7 permits SSNM on any stream but does not specify buffer reservation;
transport SNDINFO semantics retain RFC 6458 §5.3.4 and Verified Errata 6111.

SSNM subscriptions keep their existing bounded continuity-loss/`Resync` contract.
A legal multi-AS report or a combined report/expiry burst can overflow an unread
default queue; optional scheduling supplies no whole-burst-fit guarantee.

### Individual events and burst sizing

Configured `SignallingGatewayID`, `RemoteASID` and ASP/IPSP destination
`MTPRouteID` values are limited to **255 octets**. ASP configuration failures
wrap `ErrInvalidASPConfig`; IPSP destination failures wrap
`ErrInvalidRoleConfiguration`. RFC 4666 defines no limit on these local names;
this engineering ceiling mirrors the INFO String ceiling in §3.4.1. It counts
bytes, not runes. SGP/path names are not carried in these SSNM events or MTP
indications. Local contribution source IDs retain their stricter 64-octet
validation. INFO String requests remain limited to 255 UTF-8 octets, resource
reasons to 160 bytes and publication error diagnostics to 256 bytes. Source,
association and AS wire identities in state/expiry/invalidation payloads are
numeric, not additional unbounded names.

RFC 4666 §3.2 uses a 16-bit parameter length including its four-byte header.
A Routing Context or APC parameter therefore contains at most `W=16382` words.
Explicit operations and activation-audit configuration reject larger lists with
`ErrInvalidParameterValue` before submission. A nonempty RC list with its
presence flag unset is also rejected; unencoded data cannot enter observations.

The accountant in `ssnm_subscription.go` charges, with counts D/M/U/I/CI/AI/X
for report destinations, profile mismatches, updates, dimension invalidations,
congestion/availability source invalidations and expiry identities:

```
A(E) = 512 + 8D + 32M + 320U + 16I + 48(CI+AI) + 56X + 4Rreport
       + len(Reason) + len(SGevent)+len(ASevent)+len(SGreport)+len(ASreport)
       + publication*(256+len(LastError)+4Rpublication+8Dpublication)
       + peer*(256+4Rpeer)
       + sum_updates[4Ravailability+4Rcongestion
                     + sum_sources(256+4Rsource)]
```

All sums are portable accounting units, not allocator or RSS measurements.
The following hard ceilings include every charged string. Independently
maximized components give conservative bounds where a legal joint wire or
retention shape can be smaller.

| Single event | Accounted ceiling (bytes) | Derivation |
| --- | ---: | --- |
| Ordinary complete report | 1,048,575 | Strict publication ceiling; larger reports are fragmented. Legal report charges are even, so the greatest complete-report charge is at most 1,048,574. |
| Raw-only report fragment | 722,340 | `512+4*255+8W+36W`; mismatches are at most `max(rawRC,1)`. With the default inbound APC cap, this conservative bound is 599,476. |
| Destination/source patch | 262,144 | Greedy accounting ceiling; scalars plus one maximal source cost at most `512+510+320+3*4W+256=198182`, so progress is bounded. |
| Binding admitted, activated, retired; partition retired | 1,022 | `512+2*255`. |
| Peer congestion add/remove | 66,806 | `512+2*255+256+4W`. |
| Dimension/source invalidation chunk | 230,048 | `512+160+2048*(16+48+48)`. Each of three arrays has its own 2,048-identity ceiling. |
| Expiry chunk | 14,848 | `512+256*56`; no names or scopes. |
| Resource loss | 672 | `512+160`. |
| Audit outcome | 784 | `512+4*64+16`; no configured names. |
| Individual local publication outcome | 132,104 universally; 65,808 with default publication metadata capacity | `768+8W+8+256+16`; also bounded by admitted reservation charge plus 272, hence `MaxBytes+272`. Local SCON has one destination. |
| Continuity marker | 512 notional | Fixed marker delivered outside the queued-byte accounting. |

Each ceiling is below the **1 MiB** default subscription budget. Oversized
reports preserve the raw report once (`ReportSet=true`) and then emit bounded
patches. `ReportPatch.SourcesContinued` appends history for one previously replaced
destination, preserving both scalar dimensions. `ReportContinues` marks every
fragment except the last. Each enclosing event has a fresh increasing
`Revision`; retained statement revisions stay unchanged. `ApplyReport` applies
owned replacements and source continuations once in stream order. Consumers
must finish the report before exposing its complete replica, or replace it
through `Resync` after continuity loss. Ordinary reports that already fit keep
their original shape. No source history or raw wire scope is truncated.

MTP indications have a count-only default channel of 256 slots, with **no byte
budget**. Each pause/resume/status indication occupies one slot and has at
most 255 variable string bytes (its route ID). This change adds no indication
burst: at most one per changed destination; overflow uses `ResyncRequired`
and `MTPDestinationStatuses` remains the resynchronization source.

A whole report plus one pass may exceed the default **256 slots / 1 MiB**.
For exact deployment sizing, both `sum_E A(E) <= SubscriptionQueueBytes` and
`number_of_events <= SubscriptionQueueSize` must hold, including initial queue
occupancy. Count raw observations, fragment patches, validity expiry, audit
outcomes, local publication and resource/refusal events, with their actual
fan-out and retained history. A constantly draining subscriber avoids loss due
to a single oversized event; this does not promise a scheduling/service rate
that can keep up with every burst.

A conservative configured-budget envelope is available without assuming that
independent maxima can coexist. Let `N=MaxRecords`,
`p=min(N,MaxRecordsPerPartition,MaxRecordsPerPeer)`, `F` be captured report
partition fan-out (`F<=W`), `I=min(N,F*ceil(p/2048))`,
`Q=Congestion.Publication.MaxPending`, `B=Congestion.Publication.MaxBytes`,
`L=1048575`, `J=262144`, `K=66806`. For report-attributed output:

```
reportSlots <= F*(2+p) + 3*N + I + 8
reportBytes <= F*(1022+L+J*p) + 3*N*K + 672*I + 48*N + 6160
passSlots   <= ceil(N/256) + Q + 7
passBytes   <= 512*ceil(N/256) + 56*N + B + 272*Q + 5488
```

The report bound includes binding/refusal, complete or fragmented reports,
peer synchronization/retirement, compact invalidation and audit diagnostics;
mutually exclusive branches are deliberately summed. Per-partition history can
change between canonical commits, so patches are bounded by `F*p`, not by one
instantaneous store snapshot. The pass bound includes successful DAUD raw
observations within the shared optional budget. At defaults the pass alone is
bounded conservatively by **135 events / 1,038,704 bytes**. Add the report and
pass expressions when provisioning lossless burst capacity, or use exact
charges for the deployment's admissible fan-out/history to avoid the loose
independent maxima. Separately add concurrent unrelated producers; no budget-only
bound covers an arbitrary wall-clock interval of unbounded external traffic.

If either queue bound is exceeded, already queued deltas remain readable,
followed by one `SSNMContinuityLostEvent`. Further deltas are suppressed until
successful `Resync`, which returns the complete owned `SSNMKnowledge` snapshot
and current revision, clears queued deltas and resumes the stream. The gap is
explicit. Resync recovers retained state exactly; it does not replay discarded
raw reports or diagnostic history, nor recover peer reports refused by state
admission. Such refusals have their own resource-loss observations. These
semantics are unchanged and are the supported recovery for legal bursts.

### Report compatibility and runtime identity entry

Raw observations and complete destination replacements use
`SSNMReportObservedEvent` and `DestinationUpdates`. Patches are a separate
`SSNMReportPatchEvent` with `ReportPatch.State` and
`ReportPatch.SourcesContinued`; that kind never populates the complete-update
field. The former `SSNMReportEvent`, `SSNMEvent.Updated` and top-level
`SSNMEvent.SourcesContinued` identifiers have no aliases. This requires source
migration, consistently with the destination-invalidation kind change, rather
than allowing an old replacement consumer to silently clear scalars/history.
`ApplyReport` handles both new kinds and rejects mixed shapes. Its ordered,
exactly-once and completion-boundary requirements remain unchanged.

All charged SG/AS/route identity admission paths share the octet check:
configured definitions and references, association peer configuration (including
no-inventory ASPs), registration requests, saved response metadata, atomic result
installation, canonical binding setter, congestion selectors and routed transfer
requests. `IdentityLengthError` carries only field/count/limit metadata and
unwraps to `ErrInvalidParameterValue`; config wrappers retain their category.
Source identifiers use the stricter 64-octet limit. REG RSP names come from the
validated request correlation record, not a wire echo. Lookup-only bool APIs
cannot insert names and keep their existing false result for unknown input.

Local SCON completion applies the same endpoint-liveness rule as DAUD: pending
work requires a live endpoint/store and current live submission job. Cancellation
retires capacity without changing the attempt's actual result or erasing local
contribution evidence. All completion reconciliation remains under the short
store mutex; notification follows unlocking. No new transport call or wait is
introduced.

### Integrated status and lifetime boundaries

`MTPStatusProcedure.Offered` validates its caller-supplied `MTPRouteID` before
locking, retaining a destination or changing its count. Its 255-octet cap uses
`IdentityLengthError` / `ErrInvalidParameterValue`; it returns
`(*MTPIndication, error)`, with `(nil, nil)` when nothing is due. Rejecting an
invalid identity does not advance or reset a valid interval. This engineering
limit covers the newly retaining application helper as well as configuration
and transfer requests; it adds no RFC local-name requirement.

Registration changes retire local queue lifetimes, contributions and publication
reservations against their whole admitted direction/RC set once, after inherited
submission settlement and before map publication. Main's received-history/audit
batch owns its own retirement; local cleanup does not repeat origin cloning.
Stale versions and opposite DE directions preserve unrelated inputs. The existing
`peerRetentionMu` span still covers peer retirement through Changed map commits.
Partition retirement scans publication capacity once for the selected partition
set, including reserved clears. The private `jobRows` probe additionally counts
publication rows in this retirement path; it measures those visits, not every
instruction or mutex duration. No history scan is nested per destination or
partition and no new transport/subscriber/worker wait is added.

Completion checks association lifetime as well as endpoint/store/current work.
It retires SCON capacity and DAUD deadlines before constructing a nonpending
observation, while retaining actual attempts/results once. A newer deadline under
the same key cannot survive an ended association. A successful admitted DAUD's
raw local observation remains an actual submission fact while the endpoint/store
are live. `Pending` describes the reconciliation point; later retirement can
obsolete an earlier live observation.

The routed offer hook remains before candidate selection and the cached DATA
gate remains under send serialization. A gate refusal counts once per validated
request, stays known not sent and is never retried through another frozen target.
Local contributions describe the local SPMC; publication tracks its wire work.
Neither enters the outbound received SS7/transport aggregate, so neither alone
resets offered cadence. Existing effective source, transport and eligibility
changes retain their commit-time restart marks and indexed aggregate reads.
