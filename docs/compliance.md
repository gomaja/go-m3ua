# go-m3ua compliance and ecosystem audit

Audit date: 2026-08-01.

## Specification baseline

- Base protocol: RFC 4666, which obsoletes RFC 3332.
- RFC Editor errata for RFC 4666 on 2026-08-01:
  - Errata ID 2065 is Held for Document Update, Technical: Notify Routing Context is treated as Conditional for scoped Alternate ASP Active behavior.
  - Errata ID 4475 is Held for Document Update, Editorial: Service Indicator padding is interpreted as 32-bit alignment.
  - Errata ID 2518 is Rejected, Technical: RKM parameter tag values remain the RFC 4666 assignments.
- IANA SIGTRAN adaptation registry: M3UA message classes/types and parameters remain RFC 4666 assignments.
- IANA SCTP PPID registry: M3UA is Payload Protocol Identifier 3.
- SCTP baseline: RFC 9260 is applied where current SCTP behavior affects this library, including the 500 ms maximum for SACK.Delay.

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
| Original upstream #51 | `Conn` data races and shared parameter mutation | Covered by parameter copying, atomics/mutexes, queue isolation, and `go test ./... -race`. |
| Original upstream #47 / PR #48 | Receive buffer reuse overwrote earlier SCTP messages | Covered by inbound copy handling and regression tests for received octets. |
| Original upstream #28 / PR #27 | Broken SCTP connections did not close correctly | Covered by close/reconnect/read-deadline tests. |
| Original upstream #25 / PR #26 | M3UA BEAT started before ASP Up and dropped connections | Covered by state-gated heartbeat tests and echo validation. |
| Original upstream #17 / PR #18 | M3UA length calculation errors | Covered by codec invariant and wire length tests. |
| Original upstream #3 | RKM not implemented | Deliberately answered as Unsupported Message Class per RFC 4666 Section 4.4.1. |
| Original upstream #59 / PR #60 | ASPDN server-to-client handling | Covered by ASP Down state/quiescence tests. |
| Original upstream PR #61 | Optional SCTP SACK timer control | Implemented through `SetSackConfig` / `SetSctpSackConfig`, with RFC 9260 500 ms validation. |
| Original upstream PR #63 | SCTP dependency PPID handling | Implemented and tested with exported `M3UAPPID == 3` and inbound PPID filtering. |

## Current validation commands

- `go test ./... -count=1`
- `go test ./... -count=1 -race`
- `golangci-lint run --timeout=5m`
- `go run github.com/rhysd/actionlint/cmd/actionlint@latest`

CI runs the test matrix on Go 1.23, 1.24, and 1.25, plus a Go 1.25 race job and pinned golangci-lint v2.
