# ASP Multi-SG Routing Design

## Objective

Implement the ASP-side routing function required when one M3UA Application
Server Process reaches SS7 destinations through multiple Signalling Gateway
Processes and Signalling Gateways. The ASP Endpoint, rather than any one SCTP
Association, owns route state, derives the destination state presented to the
MTP3-User, and selects the Signalling Gateway Process, Association, and SCTP
stream for each MTP-TRANSFER request.

## Standards Baseline

The standards relationships were checked on 2026-08-29 against both the RFC
Editor and the IETF Datatracker.

- RFC 4666 remains the current Proposed Standard for M3UA. It obsoletes RFC
  3332 and is not obsoleted or updated by another RFC.
- The Datatracker reports RFC 4666 as an IETF Proposed Standard and reports no
  incoming `obs` or `updates` relationship.
- The SIGTRAN working group is concluded and lists no active M3UA Internet-Draft.
- RFC 4666 Errata 2065 and 4475 are Held for Document Update. Errata 2518 is
  Rejected. None changes the requirements implemented here.

Normative and explanatory requirements:

- RFC 4666 Section 1.2 defines ASP, SGP, SG, Association, and IPSP. Those are
  protocol entities and are the names used by this design.
- RFC 4666 Section 1.4.8 uses client and server only for initiating an SCTP
  association. SCTP initiation never selects the M3UA role or routing behavior.
- RFC 4666 Sections 1.3.2.1, 1.3.2.5, and 1.4.2.5 require an ASP to choose an
  SGP, maintain individual routes, account for route availability, restriction,
  and congestion, derive overall destination state, and minimize
  missequencing.
- RFC 4666 Section 4.3.4.3 requires the Routing Context used for transfer to be
  active on the selected Association.
- RFC 4666 Sections 4.5.2.1 and 4.5.2.2 require local Association loss and SSNM
  from an originating SG to update the affected route and emit MTP3-User
  indications only when the derived destination state changes.
- RFC 4666 Section 5.5.1.1.1 requires the ASP to determine the correct SGP,
  Association, and stream for an MTP-TRANSFER request.
- RFC 4666 Appendix A.2.2 describes primary/backup, loadsharing, and broadcast
  models between the SGPs of one SG, and says selection should minimize message
  missequencing.

## Naming Contract

Public protocol names use RFC entities and primitives:

- `RoleASP`, `RoleSGP`, `RoleIPSP`
- `SignallingGatewayID`
- `SignallingGatewayProcessID`
- `SGPIdentity`
- `Association`
- `MTPTransferRequest`, `MTPTransferResult`, and `MTPIndication`

`Dial`, `Listen`, and `Accept` remain transport-establishment verbs. New APIs do
not describe an ASP as a client or an SGP as a server. Documentation may use
client/server only when explicitly discussing RFC 4666 Section 1.4.8 SCTP
association initiation.

## Configuration Model

### ASP Endpoint policy

`EndpointConfig.ASP` contains immutable ASP-wide routing policy:

```go
type ASPConfig struct {
    SignallingGatewaySelection   RouteSelectionMode
    MTPRoutes                    []MTPRouteConfig
    SignallingGateways           []SignallingGatewayConfig
    CongestionPolicy             ASPCongestionPolicy
    TransferFlowCacheEntries     int
    MTPIndicationQueueSize       int
    MaxAffectedPointCodesPerSSNM int
    MaxSSNMStateRecordsPerRoute  int
    MaxSSNMStateRecords          int
}
```

`SignallingGatewaySelection` controls selection between provisioned SG routes.
`MTPRoutes` supplies the ASP's local MTP routing table. `SignallingGateways`
maps those routes to the peer-specific Network Appearance and Routing
Context values used by each SGP. The configuration is deeply copied by
`NewEndpoint`.

The three SSNM limits bound peer-controlled work and retained route state. A
route record belongs to one SG and MTP Route; availability and congestion are
independent records. Zero selects defaults of 1,024 Affected Point Codes per
message, 2,048 records per route, and 16,384 records per ASP Endpoint. A
message that would exceed a limit is rejected atomically with
`ErrASPRouteStateLimit`: neither the Endpoint route registry nor the
Association's diagnostic destination view is partially changed. RFC 4666
Section 3.8.1 defines no M3UA Error code for a receiver's local retention
budget, so the live Association is closed rather than sending a misleading
protocol Error.

When the last Association to an SG detaches, that SG's retained
availability and congestion records are discarded and their capacity is
reclaimed. Records remain while any Association to the SG exists, so losing one
redundant Association does not erase state still shared by the remaining SGPs.

### SG and SGP inventory

```go
type SignallingGatewayID string
type SignallingGatewayProcessID string

type SGPIdentity struct {
    SignallingGateway        SignallingGatewayID
    SignallingGatewayProcess SignallingGatewayProcessID
}

type MTPRouteID string

type MTPRouteConfig struct {
    ID                       MTPRouteID
    DestinationPointCode     uint32
    Mask                     uint8
    ServiceIndicators        []uint8
    OriginatingPointCodes    []uint32
}

type SignallingGatewayConfig struct {
    ID           SignallingGatewayID
    SGPSelection RouteSelectionMode
    SGPs         []SignallingGatewayProcessConfig
}

type SignallingGatewayProcessConfig struct {
    ID     SignallingGatewayProcessID
    Routes []SGPRoute
}

type SGPRoute struct {
    MTPRoute MTPRouteID
    AS       ASKey
}
```

The SGP list order is normative local provisioning for primary/backup selection
and a deterministic ordering input for loadshare and broadcast. An SGP
identifier is unique within its SG; the composite `SGPIdentity` is unique within
the ASP Endpoint. Each SGP route maps one local MTP Route to the `ASKey` used
on that peer relationship.

`AssociationConfig.PeerSGP` identifies the remote SGP for an Association owned
by a `RoleASP` Endpoint:

```go
type AssociationConfig struct {
    // existing fields
    PeerSGP *SGPIdentity
}
```

The Endpoint rejects an ASP Association whose peer identity is missing or is
not provisioned. An SGP Association rejects `PeerSGP`. The selected Association
configuration is snapshotted before any monitor or protocol goroutine starts.

### MTP Routes and provisioned SGP routes

```go
type SGPRoute struct {
    MTPRoute MTPRouteID
    AS       ASKey
}
```

The `MTPRouteConfig` destination and mask identify a local outbound point-code
range. Optional SI and OPC lists refine it using the MTP routing-label fields
that RFC 4666 Section 1.4.2.1 permits in a Routing Key. It is deliberately not
named a Routing Key: RFC 4666 defines a Routing Key within one SG, while one
local MTP Route here may use different peer Routing Keys across multiple SGs.
Empty SI and OPC lists match any value.

`SGPRoute.AS` is peer-specific. This is essential: a Routing Context is an index
into the sending node's distribution table, so two SGPs providing alternate
routes for the same local MTP Route may use different Routing Context or
Network Appearance values. The Endpoint aggregates them by `MTPRouteID` and
uses the selected SGP route's `ASKey` on the wire. It never treats a bare Routing
Context as a global route identity.

RFC 4666 permits Network Appearance and Routing Context to be omitted in
defined single-scope cases. When an Association is provisioned with exactly one
Network Appearance or Routing Context scope, an accepted SSNM message may infer
that omitted scope from the immutable Association configuration. An explicit
different value never matches another provisioned route, and an omitted scope
is never guessed when the Association has several possible values.

Mask is the number of wildcarded low-order bits and must not exceed 24. A route
with mask 24 is an explicit all-destinations route. Omission never silently
means all destinations.

Configuration validation rejects:

- duplicate or empty SG and SGP identifiers;
- duplicate or empty MTP Route identifiers;
- unsupported selection modes;
- an SG without an SGP or an SGP without a route;
- an SGP route that references an unknown MTP Route;
- duplicate MTP Routes or duplicate SGP route mappings;
- an MTP Route that has no SGP mapping;
- negative flow-cache or indication-queue sizes;
- invalid point-code masks or point-code values.

Overlapping MTP Routes are permitted because an application may name one
explicitly. An MTP-TRANSFER request that omits `MTPRoute` is rejected when its
routing label has more than one equally specific best match.

### Selection modes

```go
type RouteSelectionMode uint8

const (
    RouteSelectionPrimaryBackup RouteSelectionMode = iota + 1
    RouteSelectionLoadshare
    RouteSelectionBroadcast
)
```

The same explicit selection vocabulary is used at two levels:

1. `ASPConfig.SignallingGatewaySelection` chooses one or more SG routes.
2. `SignallingGatewayConfig.SGPSelection` chooses one or more SGPs within the
   selected SG.

Primary/backup chooses the first eligible configured item. Loadshare chooses a
stable item for the MTP traffic flow. Broadcast chooses every eligible item in
configuration order. Broadcast across separate SGs is supported only when the
operator explicitly provisions it; RFC 4666 Appendix A.2.2 says multiple-SG
models are subject to local SS7 provisioning constraints.

### Congestion policy

Availability and congestion are independent. An SCON does not overwrite DAVA,
DUNA, or DRST state, and a later DAVA does not erase a still-current congestion
report.

```go
type ASPCongestionPolicy func(messagePriority, congestionLevel uint8, levelSet bool) bool
```

The zero policy permits all reachable routes. Selection still prefers an
uncongested route, then the lowest known level, then an unknown congestion
level. A configured policy can exclude a route according to the national MTP
congestion procedure and the Protocol Data Message Priority. The policy is
evaluated without Endpoint locks, may be called concurrently, and must not
depend on invocation count. A route excluded by policy is ineligible for that
MTP-TRANSFER request.

## Runtime Ownership

A `RoleASP` Endpoint owns one `aspRoutes` registry containing:

- the immutable MTP Route, SG, SGP, route, and selection policy snapshot;
- all attached Associations grouped by `SGPIdentity`;
- route availability, restriction, and congestion state per SG and MTP Route;
- bounded SSNM availability and congestion records per route and Endpoint;
- per-SG capability and restart-related SSNM state derived from its
  Associations;
- the derived MTP destination state;
- stable MTP traffic-flow assignments;
- the bounded Endpoint-wide MTP indication channel.

Each ASP Association keeps its raw peer-reported destination state for
route-level diagnostics. It also forwards accepted SSNM changes and Association
state changes to the Endpoint registry. The MTP3-User consumes only the
Endpoint-wide derived state.

SG route capability follows RFC 4666 Appendix A.2.2. A route for one MTP Route
is capable when:

- at least one Association to an SGP of that SG is established;
- the SGP route's `ASKey` is active on that Association;
- the SG has not reported the destination unavailable;

An SG restart affects selection through its RFC 4666 Section 4.6 DUNA and DAVA
sequence; the ASP does not invent a separate peer-restart state.

An Association closing removes only that Association. The SG route remains
capable when another eligible Association to an SGP in the SG exists.

## Route State and Aggregation

Internal route state separates:

- availability: available, restricted, or unavailable;
- congestion present or clear;
- congestion level and whether the peer supplied a level;
- the SSNM sequence that produced the state.

An SG that has not reported a provisioned destination inaccessible is treated
as available while it has an eligible Association, matching RFC 4666 Appendix
A.2.2. An SG without an eligible Association is unavailable for selection.

For one MTP Route and destination, overall state is derived as follows:

1. If any capable SG route is available, overall availability is available.
2. Otherwise, if any capable SG route is restricted, overall availability is
   restricted.
3. Otherwise, overall availability is unavailable.
4. Overall congestion is the least severe congestion state among routes having
   the selected availability class. An uncongested route clears overall
   congestion; otherwise the lowest known level wins and an unknown level ranks
   after levels 1 through 3.

The destination range store uses aligned point-code prefixes. When an update
overlaps more-specific or less-specific records, the registry partitions the
affected prefix into canonical non-overlapping prefixes before comparing old
and new aggregate state. This prevents one SG's broad DUNA from hiding another
SG's more-specific DAVA.

Retained availability and congestion records are indexed in a persistent
24-bit point-code prefix tree. Route recomputation traverses that index and the
bounded prefix depth instead of rescanning every retained record for every
derived leaf. MTP-TRANSFER route-state lookup walks the same bounded prefix
path rather than scanning records belonging to unrelated SGs and MTP Routes.
Repeated updates to existing prefixes replace indexed records in place, so
overwrite-only SSNM traffic cannot restore the former quadratic Endpoint-lock
work or grow the index.

## MTP3-User Indications

```go
type MTPIndicationKind uint8

const (
    MTPPauseIndication MTPIndicationKind = iota + 1
    MTPResumeIndication
    MTPStatusIndication
)

type MTPIndication struct {
    Kind           MTPIndicationKind
    Destination    MTPDestinationStatus
    ResyncRequired bool
}

func (e *Endpoint) MTPDestinationStatus(destination MTPDestination) (MTPDestinationStatus, bool)
func (e *Endpoint) MTPDestinationStatuses() []MTPDestinationStatus
func (e *Endpoint) MTPIndications() <-chan *MTPIndication
```

The Endpoint publishes:

- MTP-PAUSE when derived availability becomes unavailable;
- MTP-RESUME when it changes from unavailable to available or restricted;
- MTP-STATUS when restriction or derived congestion changes;
- one `ResyncRequired` marker if the bounded queue overflows.

While the resynchronization marker remains unread, further deltas are dropped
instead of being queued behind stale state. Reading the marker resumes delivery;
the receiver must replace its view from `MTPDestinationStatuses` before applying
subsequent absolute destination statuses.

Only derived changes produce indications. A DUNA received through one SG emits
no pause while another SG still provides an available route. Closing one
Association emits no pause while its SG or another SG remains capable.

The channel closes only after `Endpoint.Close` has removed every Association
and completed route shutdown. `MTPDestinationStatuses` returns the canonical,
non-overlapping Endpoint snapshot used to resynchronize after an overflow.
`MTPDestinationStatus` returns no single value for a range containing mixed
statuses, rather than hiding a more-specific route change.

## MTP-TRANSFER API and Selection

```go
type MTPTransferRequest struct {
    MTPRoute     MTPRouteID
    ProtocolData *params.ProtocolDataPayload
}

type MTPTransferResult struct {
    UserDataOctets          int
    TransmittedAssociations int
}

func (e *Endpoint) MTPTransfer(request MTPTransferRequest) (MTPTransferResult, error)
```

The request is the RFC 4666 MTP-TRANSFER request primitive. `MTPRoute` may be
omitted when the MTP routing label uniquely matches one configured MTP Route.
An explicit value selects that key and is validated against the label. The
selected SGP route supplies the peer-specific Network Appearance and Routing
Context. Equal-specificity label matches are rejected as ambiguous rather than
guessed.

Selection order is:

1. Resolve the local MTP Route from the explicit identifier or DPC, OPC, and
   SI.
2. Find provisioned SGP routes for that MTP Route.
3. Exclude unavailable SG routes.
4. Prefer available routes over restricted routes.
5. Apply the configured congestion policy and prefer the least congested
   remaining route.
6. Apply the ASP-level SG selection mode.
7. Apply each selected SG's SGP selection mode.
8. Choose one eligible Association to each selected SGP.
9. Use that SGP route's `ASKey` and choose the SCTP stream from the Protocol
   Data SLS.

The stable flow key contains the MTP Route plus OPC, DPC, SI, NI, and SLS.
Message Priority is evaluated separately by congestion policy and does not
split a sequenced traffic flow. A bounded sticky-flow cache retains the
selected Association set while every selected Association remains eligible. A
change in candidate inventory does not move a healthy flow. When the assignment
becomes ineligible, the same deterministic hash and configuration order choose
its replacement.

Broadcast attempts every selected Association in deterministic order. A
partial failure is returned as a typed error containing the successful and
failed SGP identities; it is never reported as complete success. Non-broadcast
write failure is not retried blindly because an SCTP write error may not prove
that the peer received no DATA.

Concurrent MTP-TRANSFER requests for the same traffic flow are serialized
across the complete selected Association set. This gives every broadcast SGP
the same DATA order while independent flows remain concurrent. Sequence gates
exist only while calls for that flow are active and are then removed.

## Concurrency and Lifecycle

- ASP route registry mutation is serialized independently of Association state
  locks; callbacks occur after Association state commits to avoid lock cycles.
- MTP-TRANSFER selection takes a snapshot, releases the registry lock, and then
  writes, so peer-controlled socket backpressure cannot block SSNM or close.
- Association close releases the SCTP transport before waiting for an in-flight
  MTP-TRANSFER write barrier, so a blocked write cannot deadlock shutdown.
- Association removal invalidates affected sticky assignments and derives
  indications before the Association's route-level status channel closes.
- Endpoint close stops new transfers, closes Associations, publishes final
  derived changes, closes the MTP indication channel, and returns only after all
  operations finish.
- Caller configuration slices and peer identity objects are deeply copied.

## Validation

The implementation is test-first and includes:

- configuration and deep-snapshot tests;
- SG/SGP grouping and duplicate validation tests;
- availability, restriction, congestion, and range-partition aggregation tests;
- atomic per-message, per-route, and Endpoint-wide SSNM budget tests, including
  concurrent and repeated-overwrite cases;
- no-change suppression and bounded-indication overflow tests;
- primary/backup, loadshare, and broadcast tests at both SG and SGP levels;
- active Routing Context, Network Appearance, and route-key filtering tests;
- stable Association and SCTP stream tests per traffic flow;
- Association loss, SGP loss, SG loss, SSNM recovery, broadcast recovery, and
  failover tests;
- partial broadcast failure tests;
- concurrent SSNM, transfer, Association close, and Endpoint close tests under
  the race detector;
- Linux SCTP integration tests for both SCTP initiation orientations.

No dependency extension is required by this design. go-sctp v1.0.2 already
provides the Association lifecycle, address, stream, and status primitives that
go-m3ua needs; SG and SGP identity remain M3UA provisioning data.
