# go-m3ua compliance and ecosystem audit

Audit date: 2026-08-29.

## Specification baseline

- Base protocol: RFC 4666, which obsoletes RFC 3332.
- RFC Editor errata for RFC 4666 on 2026-08-29:
  - Errata ID 2065 is Held for Document Update, Technical: Notify Routing Context is treated as Conditional for scoped Alternate ASP Active behavior.
  - Errata ID 4475 is Held for Document Update, Editorial: Service Indicator padding is interpreted as 32-bit alignment.
  - Errata ID 2518 is Rejected, Technical: RKM parameter tag values remain the RFC 4666 assignments.
- IANA SIGTRAN adaptation registry: M3UA message classes/types and parameters remain RFC 4666 assignments.
- IANA SCTP PPID registry: M3UA is Payload Protocol Identifier 3.
- SCTP baseline: RFC 9260 is applied where current SCTP behavior affects this library, including the 500 ms maximum for SACK.Delay.

The RFC Editor and Datatracker were cross-checked on the audit date. RFC 4666
remains an IETF Proposed Standard, has no `Updated by` or `Obsoleted by`
relationship, and the concluded SIGTRAN working group lists no active M3UA
Internet-Draft. Errata 2065 and 4475 remain Held for Document Update, and
Errata 2518 remains Rejected; no Verified erratum changes the implemented
behavior.

## ASP multi-SG routing

The ASP Endpoint implements the route function described by RFC 4666 Sections
1.3.2.5, 1.4.2.5, 4.5.2.2, and 5.5.1.1.1:

- MTP Routes are local routing-table identities; each SGP route maps one to
  the peer-specific Network Appearance and Routing Context in an `ASKey`.
- SSNM state is retained per originating SG and aggregated before MTP-PAUSE,
  MTP-RESUME, or MTP-STATUS is delivered to the MTP3-User.
- Availability, restriction, and congestion are independent selection inputs.
- Primary/backup, loadshare, and broadcast selection are supported between SGs
  and between SGPs of one SG, following Appendix A.2.2.
- Stable bounded flow assignments minimize missequencing; DATA uses the
  Protocol Data SLS to select a nonzero negotiated SCTP stream as required by
  Section 1.4.7.
- Association state and active Routing Context scope are revalidated at the
  write barrier. One Association loss does not remove a route still served by
  a sibling Association or SG.
- Both RFC 4666 Section 1.4.8 SCTP initiation orientations are covered by
  Linux SCTP integration tests. Protocol role never depends on which peer
  initiated SCTP.

## RKM scope

Dynamic Routing Key Management is intentionally not implemented. RFC 4666 Section 4.4.1 permits a node that does not support the registration procedure to answer RKM with Error "Unsupported Message Class". The repository has tests covering that behavior for REG REQ, REG RSP, DEREG REQ, and DEREG RSP across roles and states.

Static Routing Context and Routing Key behavior is implemented and tested separately. If dynamic RKM is later added, it must include full message codecs, AS/ASP registry mutation rules, permission/status handling, collision handling, and race tests.

## Go implementation survey

| Repository | Scope | Issue/PR surface | Result |
| --- | --- | --- | --- |
| Original Go M3UA library ancestor | Main Go M3UA library ancestor | 5 open issues, 40+ PRs inspected | This repository is ahead of it for RFC/error handling, concurrency, PPID, SACK, routing context, Network Appearance, SSNM, MTP3 restart, and test coverage. |
| `github.com/vazir/m3ua-go` | Old fork/library | No issues or PRs found | No stronger behavior found; it predates many fixes already present here. |

Application/plugin hits from GitHub code search were not treated as competing libraries: they either import an M3UA library, embed narrow app-specific handling, or implement probes rather than a reusable protocol stack.

## Upstream issue/PR mapping

| Upstream item | Risk | Local status |
| --- | --- | --- |
| Original upstream #62 | M3UA BEAT naming confused with SCTP heartbeat | Behavior is unaffected; docs distinguish M3UA BEAT from SCTP transport controls. |
| Original upstream #51 | Association data races and shared parameter mutation | Covered by parameter copying, atomics/mutexes, queue isolation, and `go test ./... -race`. |
| Original upstream #47 / PR #48 | Receive buffer reuse overwrote earlier SCTP messages | Covered by inbound copy handling and regression tests for received octets. |
| Original upstream #28 / PR #27 | Broken SCTP connections did not close correctly | Covered by close/reconnect/read-deadline tests. |
| Original upstream #25 / PR #26 | M3UA BEAT started before ASP Up and dropped connections | Covered by state-gated heartbeat tests and echo validation. |
| Original upstream #17 / PR #18 | M3UA length calculation errors | Covered by codec invariant and wire length tests. |
| Original upstream #3 | RKM not implemented | Deliberately answered as Unsupported Message Class per RFC 4666 Section 4.4.1. |
| Original upstream #59 / PR #60 | ASP Down handling from SGP to ASP | Covered by ASP Down state/quiescence tests. |
| Original upstream PR #61 | Optional SCTP SACK timer control | Implemented through `AssociationConfig.SetSCTPSACK` / `Association.SetSCTPSACK`, with RFC 9260 500 ms validation. |
| Original upstream PR #63 | SCTP dependency PPID handling | Implemented and tested with exported `M3UAPPID == 3` and inbound PPID filtering. |

## Current validation commands

- `go build ./...`
- `go test ./... -count=1`
- `go test ./... -count=1 -race`
- `go vet ./...`
- `staticcheck ./...`
- `golangci-lint run ./...`
- `go run github.com/rhysd/actionlint/cmd/actionlint@latest`

CI runs the test matrix on Go 1.23, 1.24, and 1.25, plus a Go 1.25 race job and pinned golangci-lint v2.
