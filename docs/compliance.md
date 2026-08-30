# go-m3ua compliance and ecosystem audit

Audit date: 2026-08-29.

## Specification baseline

The authoritative publication, errata, IANA, active-draft, SCTP, and security
baseline is maintained in [standards.md](standards.md). The section-by-section
implementation status and remaining v1.2.0 issues are maintained in the
[RFC 4666 conformance matrix](rfc4666-conformance.md).

RFC 4666 remains the current M3UA Proposed Standard and RFC 9260 remains the
current SCTP base specification. The standards contract also records the
obsolete security protocol references embedded in current RFC 3788 and keeps
library conformance separate from deployment-provided peer authentication,
integrity, confidentiality, and replay protection.

## ASP multi-SG routing

The ASP Endpoint implements the route function described by RFC 4666 Sections
1.3.2.5, 1.4.2.5, 4.5.2.2, and 5.5.1.1.1:

- MTP Routes are local routing-table identities; each SGP route maps one to
  the peer-specific Network Appearance and Routing Context in an `ASKey`.
- SSNM state is retained per originating SG and aggregated before MTP-PAUSE,
  MTP-RESUME, or MTP-STATUS is delivered to the MTP3-User.
- Peer-controlled SSNM work and retained route state have configurable
  per-message, per-route, and Endpoint-wide bounds; an over-limit message is
  rejected atomically without inventing an RFC 4666 protocol Error code.
- A persistent point-code prefix index keeps derived-route recomputation
  bounded by retained prefixes and the 24-bit path depth, including when a peer
  repeatedly overwrites existing records without consuming new-record budget.
  MTP-TRANSFER route-state lookup uses that same bounded path rather than
  scanning Endpoint-wide records.
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

RFC 4666 Sections 3.6 and 4.4 Routing Key Management is implemented for ASP,
SGP, and IPSP Endpoints:

- REG REQ, REG RSP, DEREG REQ, and DEREG RSP use strict typed codecs with
  message and nested-parameter fuzz coverage.
- The SGP/IPSP registry applies authorization, deterministic Routing Context
  allocation, provisioned/dynamic coexistence, duplicate replay, overlap
  rejection, resource bounds, and inactive-only deregistration atomically.
- Registration batches return one independent result per Routing Key. REG RSP
  and DEREG RSP may be split across messages as RFC 4666 permits.
- An unsupported nested Routing Key field produces Registration Status 9 for
  that key instead of silently widening its traffic selector. Identical result
  replay is tolerated, while contradictory REG RSP or DEREG RSP results for
  one correlation value are rejected without applying that response.
- Routing Keys with an omitted Network Appearance use the Association's single
  configured appearance when available. Otherwise the key applies to all
  Network Appearances and is enforced as the only key registered on that
  Association, as required by Section 3.6.1.
- SGP DATA distribution resolves an omitted Routing Context from the registered
  DPC, SI, OPC mask, and Network Appearance traffic selector; ambiguity is
  rejected rather than guessed.
- IPSP Single Exchange shares one RKM traffic scope. IPSP Double Exchange keeps
  locally and remotely registered traffic directions independent.
- RFC 4666 defines no RKM acknowledgement timer. Local waits use caller
  contexts, while responder replay state makes peer retransmissions
  deterministic without inventing an RKM T(ack).
- Unresolved requester outcomes share a 1,024-result Association budget. New
  REG/DEREG procedures fail before transmission with `ErrRKMOutcomeLimit` when
  accepting the new request would exceed that budget, and delayed responses
  release capacity.

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
| Original upstream #3 | RKM not implemented | Full strict codecs, registration/deregistration procedures, policy, allocation, collision handling, and race coverage are implemented. |
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
