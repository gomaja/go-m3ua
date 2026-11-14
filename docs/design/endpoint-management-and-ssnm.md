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
permit the parameter to be omitted.

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

Both total messages and encoded bytes are bounded before the first write.
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
Routing Context list may legitimately be empty, so neither value nor length
substitutes for the flag.

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
SGP-to-concerned-ASPs and enforces DUPU's one unmasked destination and valid
User/Cause tables (Section 3.4.5). The SG-ASP DUNA, DAVA, DAUD, DUPU, and DRST
procedures are not extended to IPSP communication by Sections 1.4.3.4 or 1.4.6.
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

**Events are deltas; only the snapshot is whole.** A report event carries, in
`SSNMEvent.Updated`, the destinations that report wrote, each with both
dimensions as retained after it, and nothing else of its partition. Binding
lifecycle events carry no destinations, and a retired or invalidated partition
is discarded whole by its event kind. No report removes a single destination:
DAVA and a level-zero SCON are retained as statements, and bounds refuse
rather than evict. Applying the events in order to the snapshot the
subscription started from reproduces the partition knowledge `SSNMKnowledge`
returns (its refusal tallies are counters no event carries); the whole partition comes
only from `SubscribeSSNM` and `Resync`. Carrying the partition in every event
had made a one-destination report cost the partition's size once per
subscriber and once more to build — about 24 MB per report with 16,384
retained records and 8 subscribers — so publication could not keep up with a
steady stream of one-destination reports.

`SSNMStateConfig.SubscriptionQueueBytes` defaults to 1 MiB independently of
the 256-event default `SubscriptionQueueSize`. Positive byte limits must be at
least 512. The portable accounting charges 512 bytes per queued event, 8 per
report destination, 256 per updated destination, 4 per Routing Context in the
report and in both dimensions of each update, and the byte lengths of the reason
and both event/report partition identity strings. Every variable field is
charged even when its presence flag is false, because the queued copy retains
it. These fixed charges are accounting units, not a heap-size measurement;
queue backing-array capacity and allocator overhead are separate. Owned
snapshot results and the fixed, out-of-band continuity-loss marker are not
charged against this queue budget.

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
