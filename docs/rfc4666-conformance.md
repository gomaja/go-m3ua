# RFC 4666 conformance matrix

Audit date: 2026-08-31. Rechecked 2026-09-19.

This matrix tracks the go-m3ua v1.2.0 conformance program against the current
[RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html). The governing document,
errata, IANA, and security decisions are defined in the
[standards contract](standards.md). Role, direction and state coverage per
procedure, and the concrete optional-procedure exclusions, are in
[procedure coverage](procedure-coverage.md).

RFC 4666 status was checked on 2026-11-21 against the
[RFC Editor relationship record](https://www.rfc-editor.org/rfc/rfc4666.json),
[Datatracker RFC page](https://datatracker.ietf.org/doc/rfc4666/), its
[document API](https://datatracker.ietf.org/api/v1/doc/document/rfc4666/?format=json)
and [filtered incoming relationships](https://datatracker.ietf.org/api/v1/doc/relateddocument/?target__name=rfc4666&relationship__slug__in=obs,updates&format=json).
Both databases identify the current Proposed Standard without an incoming
obsolete/update relationship. The
[SIGTRAN documents page](https://datatracker.ietf.org/wg/sigtran/documents/)
identifies a concluded working group; the complete
[M3UA draft query](https://datatracker.ietf.org/api/v1/doc/document/?name__contains=m3ua&type=draft&limit=100&format=json)
returned 16 historical drafts with past expiry dates; none was an unexpired
successor. No current successor was found.
The [errata records](https://errata.rfc-editor.org/search/?rfc_number=4666&presentation=records)
contain no Verified correction: Held 2065 (Notify Routing Context) and 4475
(SI alignment), and Rejected 2518 (RKM tags), are reported without applying a
new interpretation in this change.

## Procedure message ownership and shutdown registration

Breaking change: `WriteSignal` refuses ASPUP, ASPDN, ASPAC and ASPIA with
`ErrProcedureMessage`; use `ASPUp`, `ASPDown`, `ASPActive` and `ASPInactive`,
or `ActivateRoutingContexts` / `DeactivateRoutingContexts` for scoped traffic
procedures. ASPUP Ack, ASPDN Ack, ASPAC Ack and ASPIA Ack belong to the library's
procedures. For unsolicited ASPDN Ack use `SetNIFAvailable(false)`; for
unsolicited ASPIA Ack use `SetASAvailable(rc, false)` or
`SetASAvailableForAS(key, false)` on an SGP (RFC 4666 §4.7). REG REQ uses
`RegisterRoutingKeys`, DEREG REQ uses `DeregisterApplicationServers`; REG RSP
and DEREG RSP belong to the configured library RKM responder. All twelve raw
messages are refused in every role and state, including during shutdown, after
marshalling and before scope locks or transport submission. Refusal returns
zero, starts no T(ack), publishes no ManagementError and leaves the association
usable. BEAT, BEAT Ack, ERR, NTFY, DATA and SSNM retain their existing behaviour.
RFC 4666 §4.3.4.1–§4.3.4.4 put T(ack) with the request sender.

Raw per-call INFO String and Traffic Mode Type customization for these requests
is lost. The typed ASP procedures currently omit INFO String; they have no
per-call or configuration setting for it. Traffic mode comes from the existing
Application Server configuration (`ASConfig.TrafficMode`), or
`RoutingKey.TrafficMode` / `TrafficModeSet` for registration. Codecs remain
available for encoding and decoding messages independently of an Association.

`RegisterRoutingKeys` also refuses new registration once `ShutdownContext`
claims withdrawal, with `ErrASPProcedureSuperseded` wrapping the shared orderly
shutdown cause. Its final admission check runs under the control send lock, so
a call admitted earlier cannot send REG REQ after shutdown's first withdrawal.
Already outstanding REG waiters still await a response, caller cancellation or
association closure; shutdown's claim alone does not cancel them.
`DeregisterApplicationServers` stays allowed during withdrawal. RFC 4666 §4.9(a)
specifies "ASP-INACTIVE, DEREG (optionally whenever dynamic registration is
used), and ASP-DOWN" before SCTP Shutdown. Both typed RKM calls retain their
existing application write backpressure.

## Status vocabulary

- **Implemented**: the public behavior and negative cases are present and
  covered by repository tests.
- **Partial**: some behavior in the listed area is implemented; the row names
  the exclusions.
- **Deployment**: the requirement cannot be satisfied by this library alone.
- **Informative**: architecture, examples, or IANA process text does not create
  an independently testable protocol requirement.

`Mandatory`, `Recommended`, and `Optional` describe the strongest RFC 2119
requirement in the listed area. `Descriptive` marks sections that define the
model or show examples without creating a separate implementation requirement.

## Architecture and services

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [1.1-1.3.1 Scope, terminology, architecture](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.1) | Descriptive | Implemented | Public APIs use `RoleASP`, `RoleSGP`, `RoleIPSP`, `Association`, AS, SG, and SGP. SCTP initiation is independent from M3UA role. Reliable transport is the default: `SCTPConfig.PartialReliability` disables PR-SCTP unless opted in (RFC 3758 §§3.3.1–3.3.3, §4.2; RFC 7496 §4.5); go-m3ua's own sends are always fully reliable. A Listener's SCTP restart follows `net.sctp.prsctp_enable` on Linux (sctp/lksctp-tools#81), so the sysctl must match. |
| [1.3.2.1 MTP3-User transport](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.1) | Mandatory | Implemented | DATA transfer and delivery are implemented for ASP, SGP, and both IPSP exchange models, including independent directional Routing Context and Network Appearance scope. |
| [1.3.2.2 Native management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.2) | Mandatory | Implemented | Endpoint exposes keyed M-SCTP_STATUS (live local addresses, primary and peer paths, send-buffer memory charge and limit), M-ASP_STATUS, M-AS_STATUS, MTP Route, and destination snapshots. ASP procedures support explicit or automatic policy, and association-scoped management indications retain exact scope and local causes, including M-SCTP_STATUS peer-address changes. |
| [1.3.2.3 MTP3 network management interworking](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.3) | Mandatory | Implemented | DUNA, DAVA, DAUD, SCON, DUPU, and DRST operations retain exact Network Appearance, Routing Context, and destination scope. RFC 4666 §4.5.2.1 says, "Local management is informed." Accepted SSNM is exposed through `SubscribeSSNM`, including raw DUPU `SSNMReport.UserCause`; it is not all delivered through `ManagementIndications`. Derived MTP-PAUSE, MTP-RESUME, and MTP-STATUS require provisioned destinations. An ASP without configured routes consumes raw SSNM and owns its MTP3-User mapping. |
| [1.3.2.4 SCTP Association management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.4) | Mandatory | Implemented | Establishment, loss, restart, shutdown, management indications, timers, and concurrent lifecycle behavior are covered. SGP remote ASP congestion is retained separately by association epoch, AS, exact scope and destination range, with bounded SSNM snapshot/delta/resync support. The congestion sentence is service prose rather than an uppercase RFC 2119 requirement. |
| [1.3.2.5 Multiple SGPs](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.5) | Recommended | Implemented | An ASP Endpoint owns per-SG route state, derives destination status, and selects SG, SGP, Association, and SCTP stream. |
| [1.4.1 Signalling Point Code representation](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.1) | Mandatory | Implemented | Protocol Data OPC and DPC retain the RFC's 32-bit, Network-Appearance-dependent representation. Affected Point Code and route-prefix APIs validate their defined mask and 24-bit point-code components. |
| [1.4.1 Inter-SGP coordination](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.1) | Recommended | Deployment | Where an SG contains more than one SGP, the MTP3 routeset, SPMC and remote AS/ASP states of each SGP SHOULD be coordinated across all the SGPs, and the inter-SGP protocol is outside the RFC's scope. One SGP Endpoint shares state among its own associations; it does not coordinate SGP processes, so the deployment supplies that coordination. See the [inter-SGP boundary and existing observation/control APIs](design/endpoint-management-and-ssnm.md#inter-sgp-coordination-boundary). |
| [1.4.2.1-1.4.2.2 Routing Context and Routing Key model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.1) | Mandatory | Implemented | Static Routing Keys, contextless AS, Network Appearance, Routing Context authorization, ambiguity rejection, and ASKey isolation are implemented. |
| [1.4.2.3 Routing Key management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.3) | Optional | Implemented | Static provisioning and dynamic registration/deregistration share one collision-checked Endpoint registry with explicit SGP/IPSP policy and ASP/IPSP operations. |
| [1.4.2.4 Message distribution at the SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.4) | Mandatory | Implemented | Override, Loadshare, and Broadcast distribution use active per-AS ASP state and stable flow selection. Strict n+k startup gates DATA and SSNM until the AS is AS-ACTIVE. Copied peer/transport inputs support an application veto. Opt-in PreferUncongestedPeers ranks loadshare only, taking the latest matching peer record by Revision before combining transport pressure; newer matching clear supersedes older ranges and equal ranks preserve active order/SLS modulo selection; override retains its selected active member and broadcast reports vetoes per required recipient. Rank changes may missequence SLS traffic (§1.4.7). The congestion-aware ASP-list model is descriptive prose. |
| [1.4.2.5 Message distribution at the ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.5) | Mandatory | Implemented | Multi-SG route availability, restriction, congestion, failover, and missequencing controls are implemented. |
| [1.4.3.1-1.4.3.3 SG interworking and Application Server](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.1) | Descriptive | Implemented | The SGP Endpoint owns shared AS, NIF, destination, and MTP3 restart state independently of any Listener or Association. |
| [1.4.3.4 IPSP considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.4) | Mandatory when IPSP is used | Implemented | `RoleIPSP` requires an explicit Association-level exchange model. Single Exchange and Double Exchange are implemented independently from SCTP association initiation. |
| [1.4.4 Redundancy models](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.4) | Optional models | Implemented | Override, Loadshare, Broadcast, configurable n+k activation, explicit smooth start, recovery, shortage notification, restoration, and failover are implemented per exact ASKey. |
| [1.4.5 Flow control](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.5) | Optional | Implemented | Association traffic can be stopped through the ASP Inactive and ASP Down procedures; bounded queues and explicit overflow prevent silent mandatory-event loss. |
| [1.4.6 Congestion management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.6) | Service prose; local SCON and SG TFC MAY | Partial | Periodic SCTP charged-memory detection, ASP/IPSP MTP-STATUS, independent local queue indications, explicit method profiles and local-user priority admission are implemented. Received SS7 congestion, peer reports, transport pressure and local sources remain distinct. `CongestionInputSnapshot.Local` exposes queue/application contributions; optional local SCON and automatic DAUD share bounded retries, national clear reservations and lifecycle fences. `CongestionConfig.Publication` bounds optional capacity; failed publication never clears local evidence. `AssociationConfig.CongestionAdmission` optionally refuses DATA from cached pressure without querying the socket or generating SCON. SGP SPMC capacity and SS7 TFC remain application/NIF decisions: the section makes triggering implementation-dependent and uses MAY. No host detector, kernel-memory reservation or unconditional control-delivery guarantee is claimed. Routed ASP offered-traffic status and NewMTPStatusProcedure follow Q.704 (07/96) §§11.2.3.1/11.2.5/11.2.4 as applicable; applications fan out each primitive to each MTP3-User. See [continued status](#continued-mtp-status) and optional [validity/audit policy](#received-congestion-validity-and-audits). |
| [1.4.7 SCTP stream mapping](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.7) | DATA MUST NOT stream 0; sequenced traffic and ASPSM/MGMT/RKM SHOULD; listed exceptions permissive | Implemented | DATA uses an SLS-derived nonzero stream while preserving same-flow order. Library management and SSNM use stream 0. The RFC permits SSNM, ASPTM, BEAT, BEAT ACK and NTFY on any stream; that permission does not reserve send-buffer space. Optional publication defers pending mandatory control and uses one NoWait attempt per association per pass; `ControlWriteTimeout` still closes a stalled association. A different stream or cached DATA ratio gives no control delivery guarantee. |
| [1.4.8 SCTP initiation model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.8) | Recommended | Implemented | ASP and SGP Endpoints can each initiate or accept SCTP Associations. `Dial`, `Listen`, and `Accept` never select the M3UA role. |
| [1.5 Sample configurations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.5) | Descriptive | Informative | ASP/SGP examples exist, and IPSP Single and Double Exchange configurations and bidirectional transfer matrices are covered. |
| [1.6 M3UA boundaries](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.6) | Descriptive primitives | Implemented | Typed MTP-TRANSFER, Layer Management queries, ASP procedures, and SSNM operations expose the RFC boundaries. For SSNM, §4.5.2.1 says, "Local management is informed." `SubscribeSSNM` supplies raw reports, including DUPU User/Cause; derived destination indications require provisioned routes or IPSP destination mappings. Applications without ASP routes own the MTP3-User mapping. |

## Message formats

These rows assess wire codecs and message-specific validation in RFC 4666
Section 3. Procedure and public-operation gaps are assessed separately under
Section 4 below.

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [2 Conventions](https://www.rfc-editor.org/rfc/rfc4666.html#section-2) | Mandatory | Implemented | RFC 2119 requirements and network byte order are applied. |
| [3.1 Common header](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.1) | Mandatory | Implemented | Version, reserved octet, class/type, length, truncation, extension, and unsupported class/type behavior have strict and fuzz coverage. |
| [3.2 Variable-length parameters](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.2) | Mandatory | Implemented | TLV length, padding, cardinality, ordering, duplicate handling, unknown extension preservation, and bounded parsing are covered. Held Errata 4475 is handled only as documented in the standards contract. |
| [3.3 DATA](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.3) | Mandatory | Implemented | Parameter order, scope, Network Appearance, Routing Context, Correlation Id, Protocol Data, stream, and SLS behavior are covered. |
| [3.4 SSNM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.4) | Mandatory | Implemented | DUNA, DAVA, DAUD, SCON, DUPU, and DRST codecs and receive validation are implemented. DUPU send admission requires exactly one unmasked destination (§3.4.5); codecs retain general capture serialization. SSNM operation APIs are assessed under Sections 4.2 and 4.5. |
| [3.5 ASPSM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.5) | Mandatory | Implemented | ASPUP, ASPUP_ACK, ASPDOWN, ASPDOWN_ACK, BEAT, and BEAT_ACK codecs and message-specific validation are implemented. Role procedures are assessed under Section 4.3. |
| [3.6 RKM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.6) | Optional | Implemented | REG REQ, REG RSP, DEREG REQ, and DEREG RSP have strict typed codecs; mandatory cardinality, repeated groups/results, status cross-fields, ordering, lengths, masks, and malformed nesting have unit and fuzz coverage. Unknown nested Routing Key fields receive Registration Status 9 rather than being discarded, and contradictory duplicate results are rejected. Held Errata 4475 remains classified only as documented in the standards contract. |
| [3.7 ASPTM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.7) | Mandatory | Implemented | ASPAC, ASPAC_ACK, ASPIA, and ASPIA_ACK codecs, parameters, ordering, cardinality, and validation are implemented. Role procedures are assessed under Section 4.3. |
| [3.8 MGMT](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.8) | Mandatory | Implemented | Error and Notify codecs, diagnostic octets, parameter scope, cardinality, unknown-extension handling, and inactive-ASP targeting for `Insufficient ASP Resources Active in AS` are implemented. Every Section 3.8.1 Error code is decoded and named on receipt; two are never generated, and [procedure coverage](procedure-coverage.md#optional-procedures-concrete-exclusions) says which and why. Held Errata 2065 is a documented project interpretation. |

RKM receive validation follows RFC 4666 Sections 3.6.1, 3.6.2 and 4.4.1.
A correctly framed Routing Key with exactly one valid Local-RK-Identifier can
receive its own failure result without discarding valid sibling keys. Invalid
DPC masks receive Invalid DPC; undefined Traffic Mode values receive
Unsupported/Invalid Traffic Handling Mode; duplicate Network Appearance fields
receive Invalid Network Appearance. Other recoverable key semantic faults
receive Invalid Routing Key. Broken framing, invalid field widths, ambiguous or
missing identifiers, and nested RKM containers retain message-level ERR handling.
Unknown extension fields retain Unsupported RK parameter Field handling.
Strict parameter decoding and origination still reject semantic faults.

The all-Network-Appearances singleton rule counts both static and dynamic
membership, including static Application Servers without a provisioned RKM
entry. An omitted appearance still implies the sole configured appearance.
Successful and Already Registered results accept the peer's actual Routing
Context, including zero. Choosing nonzero allocated contexts and requiring
nonzero provisioned RKM contexts are local responder policy, not restrictions
on valid received results. Other unsuccessful results still require zero.

## Procedures

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [4.1 M3UA-User primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.1) | Mandatory | Implemented | Endpoint MTP-TRANSFER and bounded destination indications expose a resynchronizable boundary for provisioned destinations. RFC 4666 §4.5.2.1 says, "Local management is informed." Raw SSNM, including DUPU `SSNMReport.UserCause`, is delivered through `SubscribeSSNM`; an ASP without configured routes owns its MTP3-User mapping. |
| [4.2 Layer Management primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.2) | Mandatory | Implemented | Keyed local status queries, explicit ASP procedures, and bounded M-NOTIFY, M-ERROR, M-SCTP_RELEASE, and M-SCTP_RESTART indications retain Association, ASKey, destination, and cause scope. `AssociationConfig.Subscribe` installs independent readers before the handshake, including its M-ASP_UP and M-ASP_ACTIVE state confirms and peer NTFY/ERR indications. |
| [4.3.1 ASP/IPSP states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.1) | Mandatory | Implemented | ASP state is implemented per Association and per ASKey. IPSP Double Exchange maintains independent `TrafficToLocal` and `TrafficToPeer` state, including partial Routing Context activation. |
| [4.3.2 AS states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.2) | Mandatory | Implemented | AS-DOWN, AS-INACTIVE, AS-ACTIVE, AS-PENDING, configurable n+k thresholds, strict startup, explicit smooth start, T(r), recovery below n, and traffic gating are implemented. Both DistributeData handles use the Endpoint peer AS registry for SGP and IPSP (§4.3), including bounded recovery, expiry discard and Broadcast synchronization (§4.3.4.3). SE requests and acknowledgements establish shared state; DE uses TrafficToPeer under either ASPSM agreement. An IPSP Endpoint with no peer AS returns ErrNoConfiguredAS. See [managed IPSP distribution](compliance.md#managed-ipsp-data-distribution). |
| [4.3.3 Management procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.3) | Mandatory | Implemented | SCTP establishment/loss/restart, per-procedure automatic or explicit ASP policy, context-bounded acknowledgement waits, local failure indications, and policy-aware shutdown are implemented independently from SCTP initiation. Automatic ASP policy applies to startup and orderly shutdown; unsolicited acknowledgement recovery belongs to Layer Management. |
| [4.3.4.1 ASP Up](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.1) | Mandatory | Implemented with declared SHOULD exception | ASP/SGP and both IPSP exchange models cover version, identifier, independent and simplified ASPSM, duplicates, simultaneous messages, retry, timeout, and restart. At an SGP, ASP Up while active retires dynamic registrations before the Ack, through the successful DEREG path (§4.3.4.1; §4.4.2). AS-state Notify checks membership at the transport write, across both queues; retirement notifications follow the Ack (§4.3.4.5). An unowed Up Ack while ACTIVE or DOWN submits Error, adopts INACTIVE and reports exact prior active keys through `ManagementASPWithdrawal`; an inactive recipient absorbs it. The uppercase "SHOULD send an Error message and then initiate procedures to return itself to its previous state" (§4.3.4.1) has a declared exception: the Error is sent, and Layer Management decides the return (§4.3.4.3). Re-register affected dynamic Routing Keys before activating approved reported memberships. Owed library-copy duplicates remain inert. |
| [4.3.4.2 ASP Down](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.2) | Mandatory | Implemented | ASP/SGP and both IPSP exchange models cover directional quiescence, acknowledgement, timeout, restart, repeated procedures, and shutdown ordering. At an SGP, ASP Down retires dynamic registrations before the Ack (§4.3.4.2), preserving static membership, provisioned keys, and other ASPs. Every AS-state Notify checks current membership when written, including ready events behind a stalled Ack (§4.3.4.5). An unsolicited Down Ack adopts DOWN and reports prior state and exact active keys, with no Error or automatic Up/Active. The lowercase "should" return (§4.3.4.2) is application policy; Unexpected Message Error "MAY be sent" (§3.8.1), and this library sends none. Solicited Down handling is unchanged. |
| [4.3.4.3 ASP Active](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.3) | Mandatory | Implemented | ASKey authorization, omitted RC rules, traffic mode, state, retry, partial acknowledgements, configurable n+k activation, strict traffic withholding, explicit smooth start, and independent IPSP Double Exchange activation are covered. A valid Ack without an outstanding request or duplicate history retains its grant and adds `ManagementASPActivation` with exact keys and Routing Contexts. Unexpected Message Error "MAY be sent" (§3.8.1); this library sends none for that grant. Cancellation or timeout does not retract an accepted request; a late Ack can still activate its scopes. The application keeps the grant or sends Inactive. |
| [4.3.4.4 ASP Inactive](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.4) | Mandatory | Implemented | Scoped inactivity, partial acknowledgements, duplicate requests, transfer quiescence, and independent IPSP Double Exchange withdrawal are covered. Inactive Ack duplicate history ends per scope on an accepted Active Ack or successful peer-initiated IPSP Single Exchange reactivation (§4.3.4.3.1, §5.6.1). A fresh unsolicited Inactive Ack then withdraws the active scope (§4.3.4.4, §4.7); delayed solicited duplicates before reactivation remain ignored. Active scopes may be withdrawn independently of unrelated pending deactivation requests, which remain pending. Pending first activation alone does not authorize withdrawal. An omitted RC beside an outstanding explicit Inactive request retains main's ambiguity rejection without side effects. Recovery after the withdrawal is application-owned: no automatic ASP Active is sent, including for the last active scope. `ManagementASPWithdrawal` reports only previously active unsolicited ASKeys, distinct from local confirmations. Known inactive scopes are ignored individually, preserving pending application activation and startup; mixed inactive/active Acks withdraw the active portion. Unaccepted blocked sends are cancelled and fenced before reporting. If SCTP acceptance wins cancellation, a local M-ERROR immediately reports the message's ASKeys after the withdrawal report (§1.6.3). No state or wire request follows that outcome; the application chooses Inactive or Active. Later Active Acks retain ordinary handling (§4.3.4.3), with no cancellation evidence or inferred correlation. Application policy uses `ASPActive(ctx, indication.ASKeys...)` or `ActivateRoutingContexts(1)`; automatic startup after ASP Up Ack remains configured separately. This deliberately delegates the lowercase "should" return in §4.3.4.4 to Layer Management using §4.3.4.3. The report is an implementation indication, not an M- primitive from §1.6.3; both IPSP models follow the same policy (§4.3.4.4.1). |
| [4.3.4.5 Notify](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.5) | Mandatory | Implemented | ASP/SGP and both IPSP exchange models cover AS state, Other, Alternate ASP Active, ASP Failure, identifier, and Routing Context scope. Related acknowledgements precede ordered Notify delivery, and a newly ASP-INACTIVE peer receives current AS state. Held Errata 2065 is explicitly classified. |
| [4.3.4.6 Heartbeat](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.6) | Optional procedure | Implemented | M3UA BEAT is state-gated, echoes Heartbeat Data exactly, and is independent from SCTP HEARTBEAT path management. |
| [4.4 RKM procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.4) | Optional | Implemented | ASP, SGP, IPSP Single Exchange, and IPSP Double Exchange cover registration, deregistration, authorization, deterministic allocation/replay, static/dynamic coexistence, requested changes, collisions, permissions, bounded unresolved outcomes, partial/split results, active-AS rejection, cancellation, and Association loss. RFC 4666 defines no RKM T(ack), so local waits use caller contexts and responder retransmissions are idempotent. |
| [4.5.1 SSNM at an SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.1) | Mandatory | Implemented | SS7-side state, scoped distribution, DUNA/DAVA/SCON/DRST/DUPU, DAUD answers, typed SCON and DUPU operations, concerned-ASP fan-out, and partial-delivery reporting are implemented. |
| [4.5.2 SSNM at an ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.2) | Mandatory | Implemented | Single- and multiple-SG route derivation, loss, restriction, congestion, and MTP3-User indications are implemented. Canonical dynamic bindings match the complete registered NA/RC scope, including all-appearance registrations (§3.6.1). Once active, foreign appearances retain the static path's standalone knowledge outcome; §3.8.1 defines Invalid Network Appearance origination at an SGP, not an ASP. Outstanding explicit-RC and contextless ASP Active requests admit early DUNA/DRST/SCON under §4.5.1 and restart DAVA under §4.6; omitted-RC activation includes registered flows. Pending knowledge supplies no traffic authorization before Ack. Admitted reports remain latest knowledge after request cancellation or expiry; refusal ERR remains a Layer Management indication without transaction correlation. Pre-Ack NA-mismatch discard is local policy, not an explicit §3.8.1 requirement. |
| [4.5.3 ASP auditing](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.3) | Recommended | Implemented with declared deviation | An active ASP can originate typed DAUD on an Association; the SGP answers available, restricted, congested, unavailable, and unknown destination state with retained congestion level. Masked audits use a snapshot and scope-local sweep to preserve narrower exceptions and restart isolation/recovery boundaries in aligned, disjoint ranges. Lists group equal message state and Routing Context sets within configured APC and encoded-size limits (§§3.4.1, 3.4.4, 3.4.6). Reply message/byte budgets derive from the retained-record limit; an excess receives a scoped Destination Status Unknown ERR (§3.8.1) before any status reply, plus a Layer Management indication. Unknown destinations remain unavailable. Authoritative known congestion precedes available/restricted replies; national known clear emits SCON(0), while absent congestion and international known clear emit availability only (§§5.4.1, 5.4.3). A profile-refused SCON is omitted while all remaining replies continue, with one local management indication and no peer Error; see the [declared deviation](#daud-congestion-profile-deviation). |
| [4.6 MTP3 restart](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.6) | Mandatory | Implemented | SGP managed restart, ASP reception, SCTP_RESTART, state reset, ordered recovery, and affected destinations are covered. The library's endpoint-only IPSP model excludes the SS7-connected variant: "In the IPSP case, MTP restart could be considered if the IPSP also has connection to an SS7 network." (§4.6). |
| [4.7 NIF not available](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.7) | Mandatory | Implemented | Endpoint-wide and per-AS NIF availability, ASP Inactive, Notify, partial failure, and recovery are covered. |
| [4.8 Version control](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.8) | Mandatory | Implemented | Version `1`, ASP Up negotiation, unsupported version Error, and state preservation are covered. |
| [4.9 M3UA termination](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.9) | Mandatory | Implemented | Graceful shutdown, timeout, cancellation, transport closure, and resource release are covered. Once ShutdownContext claims withdrawal, all six explicit ASPSM/ASPTM procedure entry points are refused with ErrASPProcedureSuperseded; queued and pre-registration callers cannot send late requests. Shutdown-owned ASPIA/ASPDN still complete for ASP and both IPSP exchange models. `TestShutdownRefusesExplicitUpwardProceduresOnWire` covers both withdrawal policies against a raw Linux SCTP peer through orderly EOF. Peer release causes are retained when a control write notices transport failure before the reader. |

`Association.Err()` and the `ManagementSCTPRelease` indication retain the same
terminal cause. A peer ABORT matches `ErrSCTPNotAlive` and includes the cause
supplied by the SCTP layer; graceful peer shutdown matches `io.EOF`
(RFC 4666 §4.3.1; RFC 6458 §6.1.1; RFC 9260 §§9.1, 11.2.5). A control write
still returns its own write error to its caller, but that errno does not replace
the receive-side association-end indication. `Err()` remains nonblocking and
the cause is final when `Done()` closes. Explicit local release and owner
cancellation can complete teardown while a peer indication is pending.

## Examples, security, IANA, and appendices

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [5.1 Association and traffic examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.1) | Descriptive | Implemented | Static and dynamically registered 1+0, 1+1, configurable n+k, multiple AS, Override, Loadshare, Broadcast, and AS-ACTIVE notification-at-n cases are covered. |
| [5.2 Traffic failover examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.2) | Descriptive | Implemented | Withdrawal, insufficient-resource advisory, restoration at n, failover, failback, pending recovery, recovery queue, and flow stability are covered. |
| [5.3 Normal ASP withdrawal](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.3) | Descriptive | Implemented | ASP Inactive and ASP Down withdrawal, acknowledgement, state, and traffic quiescence are covered. |
| [5.4 Auditing examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.4) | Descriptive | Implemented with declared deviation | National known clear emits SCON(0) before DAVA (§5.4.1, "SG State: Uncongested/Available") or DRST (§4.5.3). Congested, unknown-congestion (§5.4.3), unavailable and restart-isolated results retain their distinct replies. International known clear emits availability only without a profile-refusal indication. Unrepresentable retained congestion remains subject to the [declared deviation](#daud-congestion-profile-deviation). |
| [5.5 M3UA/MTP3-User boundary examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.5) | Descriptive | Implemented | Typed transfer and provisioned-destination indications cover the M3UA/MTP3-User boundary. RFC 4666 §4.5.2.1 says, "Local management is informed." `SubscribeSSNM` provides raw DUPU `SSNMReport.UserCause` and route-free ASP reports; applications own their User Part mapping. Derived ASP indications require configured routes, and IPSP indications require explicit destination mappings. |
| [5.6 IPSP examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.6) | Descriptive | Implemented | Section 5.6.1 Single Exchange and Section 5.6.2 Double Exchange are covered across both SCTP initiation orientations, independent ASPSM/ASPTM initiators, one-way traffic, contextless AS, and directional withdrawal. |
| [6 Security](https://www.rfc-editor.org/rfc/rfc4666.html#section-6) | Mandatory node requirement | Deployment | The [standards contract](standards.md#library-and-deployment-boundary) defines library controls and the operator's RFC 3788 responsibility. go-m3ua alone is not an RFC 3788 deployment. |
| [7 IANA considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-7) | Mandatory/Recommended | Implemented | PPID `3` is sent, PPID `0` is accepted, other PPIDs are discarded, port configuration is not hard-coded, and unknown class/type/parameter behavior is covered. |
| [Appendix A architecture and redundancy](https://www.rfc-editor.org/rfc/rfc4666.html#appendix-A) | Informative | Implemented | AS and SG redundancy models include per-AS configurable n+k activation and Network-Appearance-aware AS identity. |

## Release evidence gate

The matrix records implemented behavior and explicit gaps. The national
§5.4.1 "SCON(0)" known-clear audit reply is implemented and tested, including
replay into an ASP with stale congestion, masked knowledge boundaries and
atomic reply budgets. International known clear uses availability only. Issue
[#20](https://github.com/gomaja/go-m3ua/issues/20) owns release-candidate
evidence only: authoritative standards revalidation, ecosystem regression
disposition, migration completeness, and exact-head validation. It does not
stand in for an unimplemented protocol capability.

The local release-candidate gate passed host build, test, race, vet, gopls,
staticcheck, golangci-lint, deterministic fuzz, workflow, vulnerability, and
secret checks. A privileged Linux SCTP environment separately passed the full
test and race suites, all exported fuzz targets, and focused multi-homing,
multi-Association, concurrent Accept, restart, timeout, cancellation, and
resource-release scenarios. The pull-request exact head remains the final CI
and review authority before merge.

## MTP3 management termination

RFC 4666 §1.4.3.2 requires the SG to terminate MTP3 management and generate
appropriate M3UA management messages. The deployment's Nodal Interworking
Function maps MTP-PAUSE, MTP-RESUME and MTP-STATUS indications to SSNM; the library
never forwards SI 0 management as DATA. DATA carries MTP3-User traffic (§3.3.1).

`Association.WriteData`, `WriteDataContext`, `WriteSignal` (including custom raw
encoders), `Endpoint.MTPTransfer`, `MTPTransferContext`, and both SGP and IPSP
Listener/Association `DistributeData` refuse SI 0 with an error matching `ErrMTPManagementData` through
`errors.Is`, before submission or queuing. These management refusals are
`*DataWriteError` values with `DataNotSent`, including both distribution entry
points for SGP and IPSP and built-in or custom messages passed to `WriteSignal`.
RFC 4666 §1.4.3.2 states the management prohibition for SG/ASP interworking;
§1.4.3.4 says that SS7 and M3UA interworking is unnecessary for IPSP. Since DATA
carries MTP3-User messages (§3.3.1), the library also excludes SI 0, MTP3's own
management, on managed IPSP sends. This uniform library admission policy keeps
every send path consistent with its direct-send and inbound SI-0 refusal.

The message and parameter codecs remain general-purpose serializers. They
preserve SI 0 when decoding and re-encoding captures for inspection or negative
tests. RFC 4666 §1.4.3.2 is enforced at send admission, not at serialization.

An omitted Routing Key SI selector matches every SI except SI 0, including in
registered-key distribution (RFC 4666 §3.6.1). RFC 4666 alone does not settle
whether SI 1 and SI 2 testing and maintenance messages fall within this
management prohibition; this implementation excludes only SI 0. SI 1, SI 2 and
SI 3 (SCCP) remain accepted.

For an otherwise admissible inbound DATA, ASP, SGP and IPSP discard SI 0 before
user delivery and send ERR Invalid Parameter Value (RFC 4666 §3.8.1), with the
received message in Diagnostic Information, bounded by the parameter's length
limit. A local `ManagementError` indication reports the same scope and cause.
Existing state, stream and scope checks retain their precedence. Decoding keeps
SI 0 intact so callers can inspect hostile frames without re-encoding them.

## Local inbound DATA queue overload

Capacity is association-wide, never per AS. The first discarded DATA in each
received NA/RC/DPC/OPC scope starts an episode. Omitted parameters and explicit
zero remain distinct. Up to 32 scopes are retained per episode; the first
untracked discard produces a management indication with `TrackingLost`, and
`DataQueueStats.ScopeTrackingLosses` counts every untracked discard. Untracked
scopes have no individual onset/end guarantee. The bounded management stream
retains its existing close-on-overflow policy.

ASP and IPSP onset sends SCON with the received NA/RC and one affected DPC;
ASP-to-SGP includes the triggering OPC as Concerned Destination. SGP queue
exhaustion produces local management only: it does not establish SS7 destination
congestion (RFC 4666 Sections 1.4.6 and 3.4.4). All roles report per-scope onset
and end to Layer Management independently of SCON send success.

Enqueue, dequeue and a 100 ms periodic check observe occupancy. An episode ends
only after occupancy stays at or below 50% of capacity for 1 s, including a drain
with no subsequent DATA. These are implementation defaults, not RFC thresholds.
A refill above 50% or another discard cancels the drain dwell.

Overflow SCON follows the explicitly configured congestion method (RFC 4666
Section 3.4.4). International onset omits Congestion Indications, and local end
clears local state and reports to management without a wire clear. A national
profile contributes its configured maximum level at onset; a numeric-zero clear
can follow recovery once other same-scope local sources clear. The bounded
optional scheduler retains current work across full-buffer refusals and reserves
a submitted national onset's capacity for clear. A superseded unsent onset is
cancelled. Reports are fenced against retired/replaced bindings and epochs.
Local recovery promises neither clear submission at a fixed time nor peer
receipt. The peer also relies on its validity timer or audit/current MTP
procedure. The queue detector covers inbound loss; application overload enters
through explicit local contributions, and SCTP send pressure alone emits no SCON.

## DAUD congestion profile deviation

RFC 4666 §4.5.3 says:

> The SGP SHOULD respond to a DAUD message with the MTP3 availability/congestion status of the routeset associated with each Destination Point Codes in the DAUD message.

It also says:

> Where the SGP does not maintain the congestion status of the SS7 destination, the response to a DAUD message should always only be a DAVA, DRST, or DUNA message, as appropriate.

Authoritative SS7 congestion knowledge is independent of its level and of
availability. National known clear emits SCON(0) before DAVA or DRST
(RFC 4666 §§4.5.3, 5.4.1); absent congestion knowledge emits availability only
(§5.4.3). International known clear also emits availability only, without a
partial-response management error. Neither peer reports nor transport pressure
supply this knowledge, and bare DAVA does not clear an ASP's retained congestion.

For known congestion that requires an SCON, if an association's
configured method cannot express that status, its DAUD response omits only the
profile-refused SCON, sends every remaining congestion and availability reply in
the planned order, and reports the partial response once per audit through
`ManagementError`, with the local refusal in `Cause` and the affected audit scope.
It sends no peer Error for this local mismatch and keeps the association open.
This is an engineering decision and a declared deviation from the congestion
status SHOULD for the affected destinations; the exception for an SGP that does
not maintain congestion does not apply. The authoritative congestion remains
retained. An audit delivers what a publication could deliver to this association.
Reply-budget refusal remains a separate whole-audit Destination Status Unknown
response before any status prefix. A real transport failure still stops writing.

`TestDAUDProfileRefusalPreservesAvailabilityAndOtherReplies` covers repeated
audits, permitted congestion, every availability class, exact scope, management
reporting and continued service.


RFC 4666 §4.2 peer explanations are delivered through owned
`ManagementIndication.DiagnosticInformation` (ERR) and `InfoString` (NTFY)
bytes. Nil denotes absence; a non-nil empty slice denotes a present empty
parameter. `Description` remains the code/status description. Undefined ASPAC
Traffic Mode Type selects Error 0x05 without activation or Ack (§3.8.1), while
other parameter checks and error precedence remain strict. DUPU admission uses
User ≤255/Cause ≤15 on typed, raw and inbound paths (§3.4.5; Q.704 (07/96)
§15.17; Q.Imp704 (12/00) §§2.2.3–2.2.4). These are structural bounds, not a
claim that every value is assigned; the caller owns its MTP3 variant's
assignments. Both codec fields remain 16 bits.

## Received congestion validity and audits

RFC 4666 §3.4.4 optionally permits local validity (lowercase may); §4.5.3 permits
periodic and isolation DAUD (MAY), with the lowercase must exclusion for numeric
zero congestion-origin work. Nil policies preserve explicit operations.
`Auditing` is ASP-only; congestion-origin auditing requires the national
multiple-level/message-priority method. Availability and configured activation
origins remain independent. Original source/epoch/scope/range jobs retain per-AS
deadlines and narrow outgoing RCs to still-due origins. Separate SCON(0)/DAVA
clears retire only their causes; a refused positive replacement restores prior
work and charges. No sibling fallback or automatic activation occurs.

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

With validity or auditing enabled, received history and job insertion use the
same captured registration token, with tracker → dynamic-binding → store fencing
and monotonic source epochs. RKM/version/epoch/source loss removes affected
history and charges while unaffected static AS membership and independent local
direction survive. `SSNMCongestionSourceInvalidation{Destination, AS, Revision}`
values in `SSNMEvent.CongestionSourcesInvalidated` independently remove
congestion history. `SSNMAvailability.AS` identifies resolved availability
applicability. `SSNMDestinationKnowledge.AvailabilitySources` carries the
independent statements retained when auditing is enabled; `CongestionSources` is
retained when validity or auditing is enabled. DestinationUpdates snapshot entries own both
histories. `SSNMEvent.AvailabilitySourcesInvalidated` adds independent
`SSNMAvailabilitySourceInvalidation{Destination, AS, Revision}` removal IDs.
Consumers must apply scalar dimension IDs and both source arrays, or use
`ApplyInvalidation`, which patches all three while preserving other knowledge
and history-only destinations. These public field additions break unkeyed
literals; update snapshot and event consumers as well as construction sites.
Removal events route by partition `Epoch`, with `Partition` empty. Each of the
three arrays independently caps at 2,048 IDs: dimension IDs cost 16 bytes,
availability and congestion source IDs each cost 48. The dimension-only maximum
remains 33,440 bytes; the combined maximum is `512 + 160 + 2048*(16+48+48) =
230,048` bytes. Custom queues/bursts can still need resynchronization.

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

Scalar retention is `128 + 4*rawRC + 16*resolvedAS` bytes. Each independent
availability or congestion source adds one record and `144 + 4*rawRC` bytes per
resolved AS. Availability history requires auditing; congestion history requires
validity or auditing. Nil policies retain the previous scalar record count and
cost. `MaxBytes` must be at least 400. A single-RC availability statement with
auditing, or a SCON with validity/auditing, plus its partition needs 552 bytes;
a one-binding/one-partition audit job adds 472 before Info String, totaling
1,024 for that statement and job. General jobs charge `256 + len(info) + 8*rawRC
+ 144*bindings + 64*uniquePartitions`. These are portable accounting units, not
measured heap/RSS limits.

Audit outcomes aggregate at most one event for each of seven fixed reasons per report,
pass or retirement batch. `PublicationCount` counts each completed job outcome
for `submitted`, `send-failed` and transport `send-deferred`, including consecutive equal reasons and a
same-key refresh during transport; each removed job contributes exactly once to
`reporter-retired` regardless of retirement-pass order. Deferred and contended
work still counts outcome transitions. Counts remain per job: one batched DAUD
can contribute several completed outcomes;
`PublicationScopeOmitted` marks mixed original scopes or more than 64 raw RCs, and zero
`Report.Association` marks mixed sources. No APC lists, Info Strings or names
are replayed. Repeated contention and rate deferrals are suppressed. A refreshed successor
keeps its newer causes and deadlines when the prior attempt completes. One event
costs at most 928 conservative bytes (784 with actual reasons). Seven diagnostics
consume at most seven slots and 6,496 conservative bytes, in addition to the
actual report-observation and report-patch events. Only an unfragmented
observation adds one slot. Report payloads and aggregate bursts retain their
separate limits against defaults of 256 slots/1 MiB.

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

Expiry publishes local unknown and historical severity, never measured clear,
a negotiated peer lifetime, a transport-observation TTL or an invented
SCON(0)/DAVA/RESUME. DAVA does not clear congestion. `Endpoint.Close` cancels and
joins the operation-owned worker after Association closure, outside locks needed
for completion; Association withdrawal/teardown never join it. Late REG/DEREG
processing may hold RKM correlation/lifecycle locks while joining a cancelled
tracked optional send. Completion needs only the tracker; optional submission
never takes those RKM locks. The single `NoWait` attempt keeps the pinned
transport's internal mutex/deadline limitations (RFC 6458 §5.3.4; Errata 6111).

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

See the
[design](design/endpoint-management-and-ssnm.md#received-congestion-validity-and-audits)
and [migration
guide](migration-v1.2.md#received-congestion-validity-and-audits). These
contract descriptions are not additional wire, timing or resource acceptance
results.

## Continued MTP status

RFC 4666 §1.4.6's reference to current MTP3 procedures is service prose without
an uppercase MUST; optional SCON/TFC triggering remains MAY. Its §4.5.2.2
aggregate-change procedure remains distinct from continued offered-traffic
status. Q.704 (07/96) §§11.2.3.1 and 11.2.5 prescribe initial and at least
every-eight-message notification for international and national-without-priorities
methods. Routed ASP `MTPTransfer` counts once per validated offer before
transport, so a failed send counts and broadcast copies do not. Status is due
on offer 1 and no later than eight offers after each indication. An uninterrupted
interval reports at 1/9/17; resets can report earlier. Raw writes do not count. The
full-level-2 octet alternative is not applied to UserData. National priority
status remains transition-driven under §11.2.4.

Conservative engineering policy suppresses repeats and retires automatic
intervals for either `ReceivedCongestionUnknown` or `TransportCongestionUnknown`,
even when a surviving contribution keeps `Congested` true. Supplying such status
to the helper resets it. Transport uncertainty uses a normal transition reason,
never received-validity expiry; this policy adds no Q.704 MUST.

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
the interval. Sampling never waits for route or AS locks. These are engineering
choices, not additional RFC or Q.704 requirements.

Only an explicit expiration operation sets `MTPStatusValidityExpired`.
Availability, transport or lifecycle operations that expose already-expired
knowledge retain `MTPStatusTransition`; an uncertain noncongested result uses
`MTPStatusCauseNone` and cannot indicate measured congestion clear. The RFC's
optional validity timer (§3.4.4) does not prescribe these Go trigger values.

IPSP AS selection and recovery use `DistributeData`. `MTPStatusProcedure`
exposes continued local-user status to applications supplying explicit status
at their own boundary. The managed peer AS distributor infers no SS7 routes
(RFC 4666 §1.4.3.4); the ASP offered-route procedure (§4.5.2) does not apply.
Peer/transport method admission and configured cached DATA admission still
apply to each selected IPSP (§1.4.6); SGP transit policy is SGP-only.
A changed destination resets automatically; clear or uncertain status supplied
to `Offered` also resets the interval. The helper sees snapshots only. Call
`Reset()` on known clear, uncertainty, expiry or local lifecycle/method
retirement between offers, including a source ending masked by a surviving
contribution. The application must order source updates and resets with its
offers at its own routing boundary. `Endpoint.SubscribeSSNM` can supply ordered
source updates; handle subscription loss by resetting and resynchronizing.
The coalesced `Endpoint.MTPIndications` stream alone cannot reconstruct hidden
source endings. IPSP and SGP peer-input procedures have the same application-owned
ordering responsibility. Reset keeps the configured profile; construct a new
procedure for a changed method. Routed ASP `MTPTransfer` maintains restart marks
and fenced transport reset flags internally, so do not also call the helper
for those offers.

`MTPIndications` is one bounded stream; the application fans each primitive to
each registered user. Cause/reason fields distinguish congestion onset, repeat
and clear from received-validity expiry, which is unknown knowledge rather than
abatement. Exact-DPC retention, resets, the Endpoint-wide engineering budget
and migration details are documented in [continued status](../README.md#continued-mtp-status)
and the [migration guide](migration-v1.2.md#continued-mtp-status).
Level-4 transmission resumption remains the user's decision (§11.2.3.3).
Q.Imp704 (12/00) changes none of these clauses; the separately advertised
17 September 1999 covering note remains unassessed. This is not a claim of
complete corrected-Q.704, ANSI or TTC compliance.

SSNM subscriptions keep their existing bounded continuity-loss/`Resync` contract.
Every individual event fits the default subscription byte budget: configured
SG/AS and ASP/IPSP destination route names are capped at 255 octets, and oversized
report histories use explicit bounded fragments (`ApplyReport`, `ReportContinues`
and `ReportPatch.SourcesContinued`). The stricter local-source limit remains 64 octets.
A legal multi-AS report or a combined report/expiry burst can still overflow an
unread default queue. Queued deltas precede an explicit continuity-loss marker;
`Resync` recovers exact retained state and resumes the stream, without replaying
discarded raw observations or diagnostics. Defaults remain 256 slots / 1 MiB;
there is no whole-report-plus-pass default-fit guarantee. MTP indication overflow
continues to use `ResyncRequired`, with at most one indication per changed
destination. See the endpoint-management design for per-event ceilings and queue
sizing with validity, auditing and publication enabled.

The application-owned `MTPStatusProcedure.Offered` returns `(indication, error)`.
It rejects route names above 255 octets with `IdentityLengthError` wrapping
`ErrInvalidParameterValue`, before retaining the name or changing cadence. This
local-name bound is engineering policy. Routed cached-DATA refusal still counts
as one original offer and stays known not sent. Local SPMC contributions and
publication results do not change outbound SS7/transport congestion by themselves.
Completion never retains a retry after association `Done` is observed; an actual
admitted result remains observable without implying a live retry.
