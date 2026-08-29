# RFC 4666 conformance matrix

Audit date: 2026-08-29.

This matrix tracks the go-m3ua v1.2.0 conformance program against the current
[RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html). The governing document,
errata, IANA, and security decisions are defined in the
[standards contract](standards.md).

## Status vocabulary

- **Implemented**: the public behavior and negative cases are present and
  covered by repository tests.
- **Partial**: a conforming subset exists, but the linked v1.2.0 issue owns a
  remaining mandatory, recommended, or project-required capability.
- **Not implemented**: the capability is absent and the linked issue owns it.
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
| [1.3.2.1 MTP3-User transport](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.1) | Mandatory | Implemented | DATA transfer and delivery are implemented for ASP and SGP roles. IPSP transfer depends on [#15](https://github.com/gomaja/go-m3ua/issues/15) and [#16](https://github.com/gomaja/go-m3ua/issues/16). |
| [1.3.2.2 Native management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.2) | Mandatory | Partial | Automatic procedures and management indications exist. Complete typed Endpoint operations and status queries are tracked by [#19](https://github.com/gomaja/go-m3ua/issues/19). |
| [1.3.2.3 MTP3 network management interworking](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.3) | Mandatory | Partial | DUNA, DAVA, DAUD, SCON, DUPU, DRST, MTP-PAUSE, MTP-RESUME, and MTP-STATUS behavior exists. The remaining typed SSNM operations belong to [#19](https://github.com/gomaja/go-m3ua/issues/19). |
| [1.3.2.4 SCTP Association management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.4) | Mandatory | Implemented | Establishment, loss, restart, shutdown, management indications, timers, and concurrent lifecycle behavior are covered. |
| [1.3.2.5 Multiple SGPs](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.3.2.5) | Recommended | Implemented | An ASP Endpoint owns per-SG route state, derives destination status, and selects SG, SGP, Association, and SCTP stream. |
| [1.4.1 Signalling Point Code representation](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.1) | Mandatory | Implemented | Point codes, masks, affected ranges, and Protocol Data point codes are validated as 24-bit values. |
| [1.4.2.1-1.4.2.2 Routing Context and Routing Key model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.1) | Mandatory | Implemented | Static Routing Keys, contextless AS, Network Appearance, Routing Context authorization, ambiguity rejection, and ASKey isolation are implemented. |
| [1.4.2.3 Routing Key management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.3) | Optional | Partial | Static provisioning is implemented. Dynamic registration and deregistration are tracked by [#17](https://github.com/gomaja/go-m3ua/issues/17). |
| [1.4.2.4 Message distribution at the SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.4) | Mandatory | Implemented | Override, Loadshare, and Broadcast distribution use active per-AS ASP state and stable flow selection. Configurable n+k activation remains in [#18](https://github.com/gomaja/go-m3ua/issues/18). |
| [1.4.2.5 Message distribution at the ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.2.5) | Mandatory | Implemented | Multi-SG route availability, restriction, congestion, failover, and missequencing controls are implemented. |
| [1.4.3.1-1.4.3.3 SG interworking and Application Server](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.1) | Descriptive | Implemented | The SGP Endpoint owns shared AS, NIF, destination, and MTP3 restart state independently of any Listener or Association. |
| [1.4.3.4 IPSP considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.3.4) | Mandatory when IPSP is used | Not implemented | `RoleIPSP` rejects operation rather than guessing a model. Single Exchange is [#15](https://github.com/gomaja/go-m3ua/issues/15); Double Exchange is [#16](https://github.com/gomaja/go-m3ua/issues/16). |
| [1.4.4 Redundancy models](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.4) | Optional models | Partial | Override, Loadshare, Broadcast, recovery, and failover exist. Configurable n+k and smooth-start policy are tracked by [#18](https://github.com/gomaja/go-m3ua/issues/18). |
| [1.4.5 Flow control](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.5) | Optional | Implemented | Association traffic can be stopped through the ASP Inactive and ASP Down procedures; bounded queues and explicit overflow prevent silent mandatory-event loss. |
| [1.4.6 Congestion management](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.6) | Mandatory/Optional | Partial | Inbound congestion state, level handling, route preference, and Message Priority policy exist. Complete explicit SCON operations are tracked by [#19](https://github.com/gomaja/go-m3ua/issues/19). |
| [1.4.7 SCTP stream mapping](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.7) | Mandatory/Recommended | Implemented | Management and SSNM use stream 0; DATA uses an SLS-derived nonzero stream while preserving same-flow order. |
| [1.4.8 SCTP initiation model](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.4.8) | Recommended | Implemented | ASP and SGP Endpoints can each initiate or accept SCTP Associations. `Dial`, `Listen`, and `Accept` never select the M3UA role. |
| [1.5 Sample configurations](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.5) | Descriptive | Informative | ASP/SGP examples exist. IPSP examples depend on #15 and #16. |
| [1.6 M3UA boundaries](https://www.rfc-editor.org/rfc/rfc4666.html#section-1.6) | Descriptive primitives | Partial | DATA, management indications, and destination state APIs exist. Consolidated typed M3UA-User and Layer Management operations are tracked by [#19](https://github.com/gomaja/go-m3ua/issues/19). |

## Message formats

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [2 Conventions](https://www.rfc-editor.org/rfc/rfc4666.html#section-2) | Mandatory | Implemented | RFC 2119 requirements and network byte order are applied. |
| [3.1 Common header](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.1) | Mandatory | Implemented | Version, reserved octet, class/type, length, truncation, extension, and unsupported class/type behavior have strict and fuzz coverage. |
| [3.2 Variable-length parameters](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.2) | Mandatory | Implemented | TLV length, padding, cardinality, ordering, duplicate handling, unknown extension preservation, and bounded parsing are covered. Held Errata 4475 is handled only as documented in the standards contract. |
| [3.3 DATA](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.3) | Mandatory | Implemented | Parameter order, scope, Network Appearance, Routing Context, Correlation Id, Protocol Data, stream, and SLS behavior are covered. |
| [3.4 SSNM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.4) | Mandatory | Implemented | DUNA, DAVA, DAUD, SCON, DUPU, and DRST codecs and receive procedures are implemented. Typed operation completion is tracked by #19. |
| [3.5 ASPSM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.5) | Mandatory | Partial | ASP/SGP ASPUP, ASPUP_ACK, ASPDOWN, ASPDOWN_ACK, BEAT, and BEAT_ACK are implemented. IPSP behavior belongs to #15 and #16. |
| [3.6 RKM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.6) | Optional | Partial | RKM parameter codecs exist, but the four complete message codecs and procedures are tracked by #17. Until then RKM receives Error `Unsupported Message Class` as permitted by Section 4.4.1. |
| [3.7 ASPTM](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.7) | Mandatory | Partial | ASP/SGP ASPAC, ASPAC_ACK, ASPIA, and ASPIA_ACK are implemented per ASKey. IPSP directional state belongs to #15 and #16. |
| [3.8 MGMT](https://www.rfc-editor.org/rfc/rfc4666.html#section-3.8) | Mandatory | Partial | Error and Notify codecs, diagnostic octets, error mapping, Notify scope, and unknown-message behavior are implemented. Keyed management operations remain in #19. Held Errata 2065 is a documented project interpretation. |

## Procedures

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [4.1 M3UA-User primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.1) | Mandatory | Partial | MTP-TRANSFER and destination indications exist; the complete typed operation surface is #19. |
| [4.2 Layer Management primitives](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.2) | Mandatory | Partial | Automatic state management and bounded indications exist; explicit keyed operations and queries are #19. |
| [4.3.1 ASP/IPSP states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.1) | Mandatory | Partial | ASP state is implemented per Association and per ASKey. IPSP local/remote directional state is #15 and #16. |
| [4.3.2 AS states](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.2) | Mandatory | Partial | AS-DOWN, AS-INACTIVE, AS-ACTIVE, AS-PENDING, recovery, and Notify behavior exist. Configurable n+k activation is #18. |
| [4.3.3 Management procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.3) | Mandatory | Partial | SCTP establishment/loss/restart and automatic ASP procedures are implemented. Explicit policy selection is #19. |
| [4.3.4.1 ASP Up](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.1) | Mandatory | Partial | ASP/SGP version, identifier, duplicates, simultaneous messages, retry, timeout, and restart are covered. IPSP is #15/#16. |
| [4.3.4.2 ASP Down](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.2) | Mandatory | Partial | ASP/SGP quiescence, acknowledgement, timeout, restart, and shutdown ordering are covered. IPSP is #15/#16. |
| [4.3.4.3 ASP Active](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.3) | Mandatory | Partial | ASKey authorization, omitted RC rules, traffic mode, state, retry, and acknowledgements are covered. n+k is #18; IPSP is #15/#16. |
| [4.3.4.4 ASP Inactive](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.4) | Mandatory | Partial | Scoped inactivity and acknowledgements are covered. IPSP directional behavior is #15/#16. |
| [4.3.4.5 Notify](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.5) | Mandatory | Implemented for ASP/SGP | AS state, Other, Alternate ASP Active, ASP Failure, identifier, and Routing Context scope are covered, with Held Errata 2065 explicitly classified. |
| [4.3.4.6 Heartbeat](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.3.4.6) | Optional procedure | Implemented | M3UA BEAT is state-gated, echoes Heartbeat Data exactly, and is independent from SCTP HEARTBEAT path management. |
| [4.4 RKM procedures](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.4) | Optional | Not implemented | Full registration, deregistration, authorization, allocation, collision, partial result, and atomic registry mutation are #17. |
| [4.5.1 SSNM at an SGP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.1) | Mandatory | Partial | SS7-side state, scoped distribution, DUNA/DAVA/SCON/DRST/DUPU, and DAUD answers exist. Complete typed operations are #19. |
| [4.5.2 SSNM at an ASP](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.2) | Mandatory | Implemented | Single- and multiple-SG route derivation, loss, restriction, congestion, and MTP3-User indications are implemented. |
| [4.5.3 ASP auditing](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.5.3) | Recommended | Partial | DAUD receive and response behavior is implemented. An explicit Endpoint DAUD operation is #19. |
| [4.6 MTP3 restart](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.6) | Mandatory | Implemented | Local and peer restart, SCTP_RESTART, state reset, ordered recovery, and affected destination behavior are covered. |
| [4.7 NIF not available](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.7) | Mandatory | Implemented | Endpoint-wide and per-AS NIF availability, ASP Inactive, Notify, partial failure, and recovery are covered. |
| [4.8 Version control](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.8) | Mandatory | Implemented | Version `1`, ASP Up negotiation, unsupported version Error, and state preservation are covered. |
| [4.9 M3UA termination](https://www.rfc-editor.org/rfc/rfc4666.html#section-4.9) | Mandatory | Implemented | Graceful shutdown, timeout, cancellation, transport closure, and resource release are covered. |

## Examples, security, IANA, and appendices

| RFC 4666 area | Strength | Status | Evidence and remaining work |
| --- | --- | --- | --- |
| [5.1 Association and traffic examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.1) | Descriptive | Partial | Static 1+0, 1+1, multiple AS, Override, Loadshare, and Broadcast cases are covered. Dynamic RKM is #17 and configurable n+k is #18. |
| [5.2 Traffic failover examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.2) | Descriptive | Partial | Withdrawal, failover, failback, recovery queue, and flow stability are covered. General n+k policy is #18. |
| [5.3 Normal ASP withdrawal](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.3) | Descriptive | Implemented | ASP Inactive and ASP Down withdrawal, acknowledgement, state, and traffic quiescence are covered. |
| [5.4 Auditing examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.4) | Descriptive | Implemented | Available, congested, unknown, and unavailable DAUD results are covered. |
| [5.5 M3UA/MTP3-User boundary examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.5) | Descriptive | Partial | Transfer and destination state primitives exist; the consolidated typed boundary is #19. |
| [5.6 IPSP examples](https://www.rfc-editor.org/rfc/rfc4666.html#section-5.6) | Descriptive | Not implemented | Single Exchange is #15; Double Exchange is #16. |
| [6 Security](https://www.rfc-editor.org/rfc/rfc4666.html#section-6) | Mandatory node requirement | Deployment | The [standards contract](standards.md#library-and-deployment-boundary) defines library controls and the operator's RFC 3788 responsibility. go-m3ua alone is not an RFC 3788 deployment. |
| [7 IANA considerations](https://www.rfc-editor.org/rfc/rfc4666.html#section-7) | Mandatory/Recommended | Implemented | PPID `3` is sent, PPID `0` is accepted, other PPIDs are discarded, port configuration is not hard-coded, and unknown class/type/parameter behavior is covered. |
| [Appendix A architecture and redundancy](https://www.rfc-editor.org/rfc/rfc4666.html#appendix-A) | Informative | Partial | AS and SG redundancy models are implemented except configurable n+k activation in #18. |

## Remaining v1.2.0 work

| Issue | Normative scope |
| --- | --- |
| [#15 IPSP Single Exchange](https://github.com/gomaja/go-m3ua/issues/15) | Required for the RFC 4666 Single Exchange IPSP model. |
| [#16 IPSP Double Exchange](https://github.com/gomaja/go-m3ua/issues/16) | Required for the RFC 4666 Double Exchange IPSP model. |
| [#17 Dynamic Routing Key Management](https://github.com/gomaja/go-m3ua/issues/17) | Optional in RFC 4666; required by the v1.2.0 project scope. |
| [#18 Configurable n+k AS activation](https://github.com/gomaja/go-m3ua/issues/18) | Required to represent the redundancy and smooth-start policies described by RFC 4666 Sections 4.3.2 and 4.3.4.3. |
| [#19 Endpoint management and SSNM operations](https://github.com/gomaja/go-m3ua/issues/19) | Required to complete the RFC 4666 M3UA-User and Layer Management boundary contract. |
| [#20 Release conformance evidence](https://github.com/gomaja/go-m3ua/issues/20) | Re-audits every authority and closes all unexplained matrix gaps at the exact v1.2.0 release commit. |
