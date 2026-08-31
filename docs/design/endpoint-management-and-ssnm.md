# Endpoint Management and SSNM Operations

RFC 4666 remains the current M3UA specification. The RFC Editor and IETF
Datatracker both list it as a Proposed Standard that obsoletes RFC 3332, with no
RFC that updates or obsoletes it as of 2026-08-31. The SIGTRAN working group is
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
Every returned slice and address is caller-owned and deterministically ordered.

```go
type AssociationSnapshot struct {
    Association AssociationID
    Role        Role
    State       State
    IPSPState   IPSPState
    LocalAddr   *sctp.SCTPAddr
    RemoteAddr  *sctp.SCTPAddr

    PeerASPIdentifier    uint32
    PeerASPIdentifierSet bool

    SCTP      *AssociationStatus
    SCTPError error
}

func (e *Endpoint) AssociationStatus(AssociationID) (AssociationSnapshot, bool)
func (e *Endpoint) AssociationStatuses() []AssociationSnapshot
```

`SCTPError` makes a close racing a snapshot explicit instead of silently
dropping that association. A successful snapshot owns its SCTP status value.

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

```go
type DestinationStatusKey struct {
    NetworkAppearance    uint32
    NetworkAppearanceSet bool
    RoutingContext       uint32
    RoutingContextSet    bool
    PointCode            uint32
    Mask                 uint8
}

type DestinationStatusSnapshot struct {
    Key                DestinationStatusKey
    State              DestinationState
    CongestionLevel    uint8
    CongestionLevelSet bool
}

func (e *Endpoint) DestinationStatus(DestinationStatusKey) (DestinationStatusSnapshot, bool)
func (e *Endpoint) DestinationStatuses() []DestinationStatusSnapshot
```

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

## Typed SSNM operations

The public API models the wire scope directly while preventing callers from
constructing malformed parameter combinations.

```go
type SSNMScope struct {
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
    Scope        SSNMScope
    Destinations []PointCodeRange
    Info         string
}

type SignallingCongestionRequest struct {
    Scope        SSNMScope
    Destinations []PointCodeRange

    CongestionLevel    uint8
    CongestionLevelSet bool

    ConcernedDestination    uint32
    ConcernedDestinationSet bool
    Info                    string
}

type DestinationUserPartUnavailableRequest struct {
    Scope       SSNMScope
    Destination PointCodeRange
    User        uint16
    Cause       uint16
    Info        string
}
```

The operations are:

```go
func (a *Association) DestinationStateAudit(DestinationStateAuditRequest) error
func (a *Association) SignallingCongestion(SignallingCongestionRequest) error
func (e *Endpoint) SignallingCongestion(SignallingCongestionRequest) error
func (e *Endpoint) DestinationUserPartUnavailable(DestinationUserPartUnavailableRequest) error
```

`Association.DestinationStateAudit` is ASP-to-SGP (RFC 4666 Sections 3.4.3 and
4.5.3). `Association.SignallingCongestion` is the optional ASP-to-SGP report;
Concerned Destination is permitted only there (Section 3.4.4).
`Endpoint.SignallingCongestion` is SGP-to-concerned-ASPs and atomically updates
the SGP destination registry. `Endpoint.DestinationUserPartUnavailable` is
SGP-to-concerned-ASPs and enforces DUPU's one unmasked destination and valid
User/Cause tables (Section 3.4.5). The SG-ASP DUNA, DAVA, DAUD, DUPU, and DRST
procedures are not extended to IPSP communication by Sections 1.4.3.4 or 1.4.6.

Every operation validates role before constructing or writing a message. A
present Routing Context list must be non-empty and unique. Every Routing
Context must resolve to the request's exact Network Appearance. Point codes are
24-bit and masks are at most 24. Congestion levels are 0 through 3. Info String
is valid UTF-8 and at most 255 octets (Section 3.4.1).

SGP fan-out snapshots concerned active ASPs under the AS registry and writes to
each association without holding Endpoint or AS locks. Partial failures are
returned with the failed `AssociationID` values; successful peers are never
replayed automatically.

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
for local M-ERROR and M-SCTP_RELEASE. Existing compatibility fields remain
derived projections.

The bounded `StateChanges` and `ManagementIndications` streams remain
association-scoped. Overflow closes that association with
`ErrIndicationQueueFull`; `Done` closes, `Err` returns the cause, and no later
mandatory event is represented as delivered. MTP3-User destination indications
retain their explicit resynchronization marker because an authoritative
Endpoint destination snapshot exists. No mandatory management event is silently
dropped.

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
