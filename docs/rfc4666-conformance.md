# RFC 4666 conformance matrix

Audit date: 2026-08-31.

This matrix tracks the go-m3ua v1.2.0 conformance program against the current
[RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html). The governing document,
errata, IANA, and security decisions are defined in the
[standards contract](standards.md).

## Status vocabulary

- **Implemented**: the public behavior and negative cases are present and
  covered by repository tests.
- **Deployment**: the requirement cannot be satisfied by this library alone.
- **Informative**: architecture, examples, or IANA process text does not create
  an independently testable protocol requirement.

`Mandatory`, `Recommended`, and `Optional` describe the strongest RFC 2119
requirement in the listed area. `Descriptive` marks sections that define the
model or show examples without creating a separate implementation requirement.

## Architecture and services

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [1.1-1.3.1 Scope, terminology, architecture](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.1) | Descriptive | Implemented | Public APIs use `RoleASP`, `RoleSGP`, `RoleIPSP`, `Association`, AS, SG, and SGP. SCTP initiation is independent from M3UA role. |
| [1.3.2.1 MTP3-User transport](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.1) | Mandatory | Implemented | DATA transfer and delivery are implemented for ASP, SGP, and both IPSP exchange models, including independent directional Routing Context and Network Appearance scope. |
| [1.3.2.2 Native management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.2) | Mandatory | Implemented | Endpoint exposes keyed M-SCTP_STATUS, M-ASP_STATUS, M-AS_STATUS, MTP Route, and destination snapshots. ASP procedures support explicit or automatic policy, and association-scoped management indications retain exact scope and local causes. |
| [1.3.2.3 MTP3 network management interworking](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.3) | Mandatory | Implemented | DUNA, DAVA, DAUD, SCON, DUPU, DRST, MTP-PAUSE, MTP-RESUME, and MTP-STATUS behavior and typed application operations are implemented with exact Network Appearance, Routing Context, and destination scope. |
| [1.3.2.4 SCTP Association management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.4) | Mandatory | Implemented | Establishment, loss, restart, shutdown, management indications, timers, and concurrent lifecycle behavior are covered. |
| [1.3.2.5 Multiple SGPs](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.5) | Recommended | Implemented | An ASP Endpoint owns per-SG route state, derives destination status, and selects SG, SGP, Association, and SCTP stream. |
| [1.4.1 Signalling Point Code representation](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.1) | Mandatory | Implemented | Protocol Data OPC and DPC retain the RFC's 32-bit, Network-Appearance-dependent representation. Affected Point Code and route-prefix APIs validate their defined mask and 24-bit point-code components. |
| [1.4.2.1-1.4.2.2 Routing Context and Routing Key model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.1) | Mandatory | Implemented | Static Routing Keys, contextless AS, Network Appearance, Routing Context authorization, ambiguity rejection, and ASKey isolation are implemented. |
| [1.4.2.3 Routing Key management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.3) | Optional | Implemented | Static provisioning and dynamic registration/deregistration share one collision-checked Endpoint registry with explicit SGP/IPSP policy and ASP/IPSP operations. |
| [1.4.2.4 Message distribution at the SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.4) | Mandatory | Implemented | Override, Loadshare, and Broadcast distribution use active per-AS ASP state and stable flow selection. Strict n+k startup gates DATA and SSNM until the AS is AS-ACTIVE. |
| [1.4.2.5 Message distribution at the ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.5) | Mandatory | Implemented | Multi-SG route availability, restriction, congestion, failover, and missequencing controls are implemented. |
| [1.4.3.1-1.4.3.3 SG interworking and Application Server](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.1) | Descriptive | Implemented | The SGP Endpoint owns shared AS, NIF, destination, and MTP3 restart state independently of any Listener or Association. |
| [1.4.3.4 IPSP considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.4) | Mandatory when IPSP is used | Implemented | `RoleIPSP` requires an explicit Association-level exchange model. Single Exchange and Double Exchange are implemented independently from SCTP association initiation. |
| [1.4.4 Redundancy models](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.4) | Optional models | Implemented | Override, Loadshare, Broadcast, configurable n+k activation, explicit smooth start, recovery, shortage notification, restoration, and failover are implemented per exact ASKey. |
| [1.4.5 Flow control](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.5) | Optional | Implemented | Association traffic can be stopped through the ASP Inactive and ASP Down procedures; bounded queues and explicit overflow prevent silent mandatory-event loss. |
| [1.4.6 Congestion management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.6) | Mandatory/Optional | Implemented | Inbound state, explicit level zero abatement, omitted and level 1-3 handling, route preference, Message Priority policy, ASP-to-SGP reporting, and SGP fan-out are implemented. |
| [1.4.7 SCTP stream mapping](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.7) | Mandatory/Recommended | Implemented | Management and SSNM use stream 0; DATA uses an SLS-derived nonzero stream while preserving same-flow order. |
| [1.4.8 SCTP initiation model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.8) | Recommended | Implemented | ASP and SGP Endpoints can each initiate or accept SCTP Associations. `Dial`, `Listen`, and `Accept` never select the M3UA role. |
| [1.5 Sample configurations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.5) | Descriptive | Informative | ASP/SGP examples exist, and IPSP Single and Double Exchange configurations and bidirectional transfer matrices are covered. |
| [1.6 M3UA boundaries](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.6) | Descriptive primitives | Implemented | Typed MTP-TRANSFER, destination indications, Layer Management status queries and indications, ASP procedures, and SSNM operations expose the RFC boundaries without requiring raw message construction. |

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
| [3.4 SSNM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.4) | Mandatory | Implemented | DUNA, DAVA, DAUD, SCON, DUPU, and DRST codecs, parameters, ordering, cardinality, and validation are implemented. SSNM operation APIs are assessed under Sections 4.2 and 4.5. |
| [3.5 ASPSM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.5) | Mandatory | Implemented | ASPUP, ASPUP_ACK, ASPDOWN, ASPDOWN_ACK, BEAT, and BEAT_ACK codecs and message-specific validation are implemented. Role procedures are assessed under Section 4.3. |
| [3.6 RKM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.6) | Optional | Implemented | REG REQ, REG RSP, DEREG REQ, and DEREG RSP have strict typed codecs; mandatory cardinality, repeated groups/results, status cross-fields, ordering, lengths, masks, and malformed nesting have unit and fuzz coverage. Unknown nested Routing Key fields receive Registration Status 9 rather than being discarded, and contradictory duplicate results are rejected. Held Errata 4475 remains classified only as documented in the standards contract. |
| [3.7 ASPTM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.7) | Mandatory | Implemented | ASPAC, ASPAC_ACK, ASPIA, and ASPIA_ACK codecs, parameters, ordering, cardinality, and validation are implemented. Role procedures are assessed under Section 4.3. |
| [3.8 MGMT](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.8) | Mandatory | Implemented | Error and Notify codecs, diagnostic octets, parameter scope, cardinality, unknown-extension handling, and inactive-ASP targeting for `Insufficient ASP Resources Active in AS` are implemented. Held Errata 2065 is a documented project interpretation. |

## Procedures

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [4.1 M3UA-User primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.1) | Mandatory | Implemented | Endpoint MTP-TRANSFER and bounded MTP-PAUSE, MTP-RESUME, and MTP-STATUS indications expose a resynchronizable MTP3-User boundary. |
| [4.2 Layer Management primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.2) | Mandatory | Implemented | Keyed local status queries, explicit ASP procedures, and bounded M-NOTIFY, M-ERROR, M-SCTP_RELEASE, and M-SCTP_RESTART indications retain Association, ASKey, destination, and cause scope. |
| [4.3.1 ASP/IPSP states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.1) | Mandatory | Implemented | ASP state is implemented per Association and per ASKey. IPSP Double Exchange maintains independent `TrafficToLocal` and `TrafficToPeer` state, including partial Routing Context activation. |
| [4.3.2 AS states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.2) | Mandatory | Implemented | AS-DOWN, AS-INACTIVE, AS-ACTIVE, AS-PENDING, configurable n+k thresholds, strict startup, explicit smooth start, T(r), recovery below n, and traffic gating are implemented. |
| [4.3.3 Management procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.3) | Mandatory | Implemented | SCTP establishment/loss/restart, per-procedure automatic or explicit ASP policy, context-bounded acknowledgement waits, local failure indications, and policy-aware shutdown are implemented independently from SCTP initiation. |
| [4.3.4.1 ASP Up](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.1) | Mandatory | Implemented | ASP/SGP and both IPSP exchange models cover version, identifier, independent and simplified ASPSM, duplicates, simultaneous messages, retry, timeout, and restart. |
| [4.3.4.2 ASP Down](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.2) | Mandatory | Implemented | ASP/SGP and both IPSP exchange models cover directional quiescence, acknowledgement, timeout, restart, repeated procedures, and shutdown ordering. |
| [4.3.4.3 ASP Active](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.3) | Mandatory | Implemented | ASKey authorization, omitted RC rules, traffic mode, state, retry, partial acknowledgements, configurable n+k activation, strict traffic withholding, explicit smooth start, and independent IPSP Double Exchange activation are covered. |
| [4.3.4.4 ASP Inactive](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.4) | Mandatory | Implemented | Scoped inactivity, partial acknowledgements, duplicate requests, transfer quiescence, and independent IPSP Double Exchange withdrawal are covered. |
| [4.3.4.5 Notify](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.5) | Mandatory | Implemented | ASP/SGP and both IPSP exchange models cover AS state, Other, Alternate ASP Active, ASP Failure, identifier, and Routing Context scope. Related acknowledgements precede ordered Notify delivery, and a newly ASP-INACTIVE peer receives current AS state. Held Errata 2065 is explicitly classified. |
| [4.3.4.6 Heartbeat](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.6) | Optional procedure | Implemented | M3UA BEAT is state-gated, echoes Heartbeat Data exactly, and is independent from SCTP HEARTBEAT path management. |
| [4.4 RKM procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.4) | Optional | Implemented | ASP, SGP, IPSP Single Exchange, and IPSP Double Exchange cover registration, deregistration, authorization, deterministic allocation/replay, static/dynamic coexistence, requested changes, collisions, permissions, bounded unresolved outcomes, partial/split results, active-AS rejection, cancellation, and Association loss. RFC 4666 defines no RKM T(ack), so local waits use caller contexts and responder retransmissions are idempotent. |
| [4.5.1 SSNM at an SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.1) | Mandatory | Implemented | SS7-side state, scoped distribution, DUNA/DAVA/SCON/DRST/DUPU, DAUD answers, typed SCON and DUPU operations, concerned-ASP fan-out, and partial-delivery reporting are implemented. |
| [4.5.2 SSNM at an ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.2) | Mandatory | Implemented | Single- and multiple-SG route derivation, loss, restriction, congestion, and MTP3-User indications are implemented. |
| [4.5.3 ASP auditing](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.3) | Recommended | Implemented | An active ASP can originate typed DAUD on an Association; the SGP answers available, restricted, congested, unavailable, and unknown destination state with retained congestion level. |
| [4.6 MTP3 restart](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.6) | Mandatory | Implemented | Local and peer restart, SCTP_RESTART, state reset, ordered recovery, and affected destination behavior are covered. |
| [4.7 NIF not available](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.7) | Mandatory | Implemented | Endpoint-wide and per-AS NIF availability, ASP Inactive, Notify, partial failure, and recovery are covered. |
| [4.8 Version control](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.8) | Mandatory | Implemented | Version `1`, ASP Up negotiation, unsupported version Error, and state preservation are covered. |
| [4.9 M3UA termination](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.9) | Mandatory | Implemented | Graceful shutdown, timeout, cancellation, transport closure, and resource release are covered. |

## Examples, security, IANA, and appendices

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [5.1 Association and traffic examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.1) | Descriptive | Implemented | Static and dynamically registered 1+0, 1+1, configurable n+k, multiple AS, Override, Loadshare, Broadcast, and AS-ACTIVE notification-at-n cases are covered. |
| [5.2 Traffic failover examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.2) | Descriptive | Implemented | Withdrawal, insufficient-resource advisory, restoration at n, failover, failback, pending recovery, recovery queue, and flow stability are covered. |
| [5.3 Normal ASP withdrawal](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.3) | Descriptive | Implemented | ASP Inactive and ASP Down withdrawal, acknowledgement, state, and traffic quiescence are covered. |
| [5.4 Auditing examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.4) | Descriptive | Implemented | Available, congested, unknown, and unavailable DAUD results are covered. |
| [5.5 M3UA/MTP3-User boundary examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.5) | Descriptive | Implemented | Typed transfer, destination, congestion, user-part unavailability, and Layer Management operations cover the M3UA/MTP3-User boundary. |
| [5.6 IPSP examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.6) | Descriptive | Implemented | Section 5.6.1 Single Exchange and Section 5.6.2 Double Exchange are covered across both SCTP initiation orientations, independent ASPSM/ASPTM initiators, one-way traffic, contextless AS, and directional withdrawal. |
| [6 Security](https://www.rfc-editor.org/rfc/rfc4666.html#section-6) | Mandatory node requirement | Deployment | The [standards contract](standards.md#library-and-deployment-boundary) defines library controls and the operator's RFC 3788 responsibility. go-m3ua alone is not an RFC 3788 deployment. |
| [7 IANA considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-7) | Mandatory/Recommended | Implemented | PPID `3` is sent, PPID `0` is accepted, other PPIDs are discarded, port configuration is not hard-coded, and unknown class/type/parameter behavior is covered. |
| [Appendix A architecture and redundancy](https://www.rfc-editor.org/rfc/rfc4666.html#appendix-A) | Informative | Implemented | AS and SG redundancy models include per-AS configurable n+k activation and Network-Appearance-aware AS identity. |

## Release evidence gate

The matrix contains no known missing RFC 4666 procedure. Issue
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
