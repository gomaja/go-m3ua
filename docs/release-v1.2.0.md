# go-m3ua v1.2.0

These notes describe the v1.2.0 release candidate. They do not assert that a
tag or release artifact exists; publication is a separate release action.

## Protocol scope

v1.2.0 implements the M3UA library responsibilities defined by current
[RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.html) for:

- ASP, SGP, and IPSP roles independently from SCTP initiation;
- IPSP Single Exchange and Double Exchange, with independent directional
  ASPSM and ASPTM procedures where the exchange model requires them;
- static and dynamically registered Routing Keys, RKM procedures, Network
  Appearance, Routing Context, contextless AS, and unambiguous `ASKey` identity;
- Override, Loadshare, Broadcast, and configurable n+k AS activation;
- multiple SGs and SGPs, route state, stable flow selection, and failover;
- MTP3 restart, NIF availability, Layer Management status and indications;
- DATA, SSNM, ASPSM, ASPTM, RKM, MGMT, PPID, stream, and error handling; and
- typed MTP3-User transfer, destination, congestion, audit, and user-part
  unavailability operations.

The complete section-by-section disposition is in the
[RFC 4666 conformance matrix](rfc4666-conformance.md).

## Breaking API

v1.2.0 intentionally breaks the v1.1 API to make RFC entity ownership and
traffic identity explicit:

- `Endpoint` owns the M3UA role and shared state; `Dial`, `Listen`, and `Accept`
  describe SCTP transport orientation only.
- `Association` replaces `Conn`, and `AssociationConfig` replaces `Config` and
  `ConnConfig` without compatibility aliases.
- `ListenerConfig` selects an immutable per-Association configuration before
  M3UA parsing while shared AS, destination, and NIF state remains Endpoint-owned.
- `ASKey` includes Network Appearance presence/value and Routing Context
  presence/value; RC-only helpers fail closed when the result is ambiguous.
- ASP procedures, RKM, Layer Management, MTP3-User, routing, and SSNM operations
  have typed APIs with role and scope validation.

Every removed v1.1.1 declaration and its replacement is listed in
[Migrating to v1.2](migration-v1.2.md#complete-v111-symbol-disposition).

## Validation

The release candidate is gated at its exact commit by:

- build, unit, integration, vet, staticcheck, and golangci-lint checks;
- Go 1.23, 1.24, and 1.25 Linux tests plus the Go 1.25 race detector;
- deterministic execution of every exported Go fuzz target;
- Linux SCTP multi-homing, multi-Association, failure, restart, and lifecycle
  scenarios;
- Darwin, FreeBSD, Windows, and additional Linux architecture compile checks;
- dependency vulnerability, dependency-review, SAST, and secret-detection
  workflows; and
- exact-head pull-request review with no unresolved conversation.

The commands and CI jobs are recorded in the
[compliance and ecosystem audit](compliance.md#current-validation-commands).

## Deployment security boundary

go-m3ua implements M3UA and its SCTP integration. It does not itself provide
peer authentication, confidentiality, network-layer integrity, certificate or
key lifecycle, trust policy, firewalling, or operational monitoring. Those are
node and deployment responsibilities.

RFC 4666
[Section 6](https://www.rfc-editor.org/rfc/rfc4666.html#section-6) requires a
SIGTRAN implementation to follow RFC 3788. Current RFC 3788 still requires a
SIGTRAN node to support IPsec and permits TLS, but its concrete protocol profile
contains obsolete references. The exact literal-conformance and modern secure
deployment choices are separated in the
[standards and security contract](standards.md#security-modernization). Using
this library alone is not an RFC 3788 deployment-conformance claim.

## Known standards context

- RFC 4666 remains a Proposed Standard, obsoletes RFC 3332, and has no published
  update or replacement in the RFC Editor or Datatracker records checked for
  this release candidate.
- RFC 4666 has no Verified erratum. Errata 2065 and 4475 are Held for Document
  Update, and Errata 2518 is Rejected; their dispositions are documented without
  silently treating them as normative corrections.
- The live IANA registries assign SCTP PPID `3` to M3UA and service `m3ua` to
  port `2905/SCTP`. The IANA `DPU` label for SSNM type `5` differs from RFC
  4666's `DUPU` name without changing the numeric wire assignment.
- Active SCTP DTLS chunk and key-management Internet-Drafts are tracked as
  non-normative deployment context. They do not update RFC 4666 or replace the
  current SCTP base, RFC 9260.

Recheck the linked authority records before publishing a later release.
