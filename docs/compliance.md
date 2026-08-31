# go-m3ua compliance and ecosystem audit

Audit date: 2026-08-31.

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

## Application Server n+k activation

SGP and IPSP Endpoints implement the n+k Application Server model described by
RFC 4666 Sections 1.4.4.1, 3.8.2, 4.3.2, 4.3.4.3, 4.3.4.5, 5.1.4, and 5.2.3:

- immutable default and exact-ASKey activation policies preserve Network
  Appearance and contextless-AS identity;
- strict startup withholds DATA and SSNM until n ASPs are ASP-ACTIVE, while an
  explicit smooth-start option enables the RFC exception;
- an already active AS remains active while at least one ASP is active, and
  pending recovery resumes with any active ASP;
- Loadshare and Broadcast shortages notify only ASP-INACTIVE members, without
  duplicate advisories during one continuous shortage;
- restoration to n notifies all non-DOWN members that the AS is AS-ACTIVE;
- related ASP procedure acknowledgements precede ordered Notify delivery; and
- Override is rejected unless the effective activation threshold is one,
  including provisioned and dynamically registered Routing Keys.

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
| Original upstream | Reusable Go M3UA library ancestor | 18 issues, including 5 open, and 44 pull requests inspected | Every behaviorally relevant issue and proposal has the disposition below. Counts and repository metadata were refreshed through the GitHub API on the audit date. |
| [`github.com/vazir/m3ua-go`](https://github.com/vazir/m3ua-go) | Stale reusable library | No issues or pull requests | No behavior absent here was found. Its last repository update predates the current parser, lifecycle, routing, and role work in this project. |
| [`github.com/wspdev-go/m3ua-client`](https://github.com/wspdev-go/m3ua-client) | Application scaffold | No issues or pull requests | Not a reusable M3UA stack and therefore not an implementation baseline. |
| [`github.com/rumalg123/telco-signalling-lab`](https://github.com/rumalg123/telco-signalling-lab) | Application-specific signalling lab | No issues or pull requests | Its narrow in-application implementation is not adopted as a library design. In particular, go-m3ua permits pre-ASP-Active-Ack DUNA, DRST, and SCON only in the RFC 4666 Section 4.5.1 activation window; it does not generalize that exception to DAVA. |

Other application and probe hits from GitHub code search were not treated as
competing libraries because they import a stack or embed task-specific protocol
handling rather than exposing a reusable implementation.

## Upstream issue/PR mapping

| Upstream item | Risk | Local status |
| --- | --- | --- |
| Original upstream #1 / PR #16 / issue #38 | Parser robustness and native Go fuzzing | `messages.TestParseNeverMisreportsClassOrType`, `messages.TestParseMalformed`, `messages.FuzzParse`, `messages.FuzzParseParams`, message-specific fuzz targets, and `scripts/fuzz-smoke.sh` cover the untrusted input boundary. |
| Original upstream #2 / PR #14 | Missing SSNM codecs and procedures | Strict DUNA, DAVA, DAUD, SCON, DUPU, and DRST codecs plus `TestSSNMIsDispatchedNotRejectedAsUnsupported`, `FuzzSSNMHandlers`, typed operations, scope, route-state, and Linux round-trip tests cover both codec and procedure behavior. |
| Original upstream #3 / PR #53 | RKM absent or incomplete | REG REQ/RSP and DEREG REQ/RSP codecs, authorization, allocation, replay, collision, Network Appearance, contextless AS, concurrency, and operation APIs are covered by the `rkm_*_test.go` suites. The closed proposal is not used as a substitute for those tests. |
| Original upstream #4 / #5 | Point-code helpers and point-code routing | `AffectedPointCode` validation, masked route-prefix state, `MTPRoute`, ASP multi-SG routing, and SGP distribution implement protocol-aware point-code handling rather than a detached utility API. |
| Original upstream #17 / PR #18 | Encoded length calculation | `messages.TestHeaderMarshalUsesCurrentPayloadLength`, parameter boundary tests, codec invariants, and `TestWriteOfLargePayloadReportsItsOwnLength` verify header, TLV, message, and user-payload lengths. |
| Original upstream #25 / PR #26 | M3UA BEAT started before ASP Up and caused Association loss | `TestHeartbeatObservesASPActiveBeforeItStarts`, `TestAssociationConfigWithoutHeartbeatInfoDials`, state-gated BEAT handling, exact echo tests, and expiry lifecycle tests keep T(beat) independent from SCTP path heartbeat. |
| Original upstream #28 / PR #27 | Transport interruption did not close the M3UA object | `TestCloseIsIdempotentAndConcurrencySafe`, `TestMonitorExitsOnDirectClose`, Listener/Endpoint shutdown tests, read-error handling, and `Done`/`Err` tests cover prompt, observable resource release. |
| Original upstream #31 | Read API returned transport-sized or ambiguous results | `Read`, `ReadPD`, `ReadData`, typed `DataMessage`, MTP3-User indications, and `TestWriteReturnsThePayloadLength` preserve application payload semantics while exposing message metadata explicitly. This is an API-design disposition, not an RFC defect. |
| Original upstream #33 | SCTP multihoming support | `TestMultihomedAssociationKeepsEveryAddress`, `TestMultihomedAssociationCarriesPayloadBothWays`, and `TestMultihomedListenerServesSeveralASPs` exercise owned SCTP address sets and live Linux Associations. |
| Original upstream PR #10 | Notify Status cardinality | Typed Notify validation requires Status and covers malformed, scoped, and state-transition cases; no optional-Status compatibility path is exposed. |
| Original upstream PR #11 | Lock leaks and deadlocks | Association, Listener, Endpoint, RKM, activation, and distribution concurrency tests run under `go test ./... -race`; dedicated shutdown tests cover blocked Accept, deregistration, and concurrent close paths. |
| Original upstream PR #41 | Unknown messages disrupted receive state | `TestUnparseableOrUnknownEventsDoNotFailTheRead`, `TestUnknownParameterDoesNotDiscardTheMessage`, and unsupported class/type Error tests preserve state while applying RFC 4666 extension rules. |
| Original upstream PR #42 | Error-interface panic | Typed error conversion, `TestHeartbeatAckWithoutDataDoesNotPanic`, malformed-message Error generation, and Error diagnostic ownership tests cover nil and unexpected error shapes without panics. |
| Original upstream PR #43 / #44 / #46 / #55 / #57 | Stream selection, explicit writes, and stream-ID races | `TestDataIsNeverSentOnStreamZero`, `TestOutboundSignalStreamUsesProtocolDataSLS`, `TestDataOnOneStreamIsDeliveredInOrder`, and concurrent traffic tests use negotiated streams and stable SLS mapping. |
| Original upstream #47 / PR #48 | Receive-buffer reuse overwrote an earlier message | Every received message owns its bytes; `TestDiagnosticInformationSurvivesTheNextReceivedMessage`, `TestDiagnosticErrorsOwnTheirReceivedBytes`, and multi-message read tests prevent later reads from changing retained data. |
| Original upstream #50 | Unsupported-platform installation failure | The portability workflow cross-compiles tests for Darwin, FreeBSD, Windows, and Linux architectures while socket-backed SCTP tests remain Linux-only. Unsupported hosts compile without pretending they can create SCTP Associations. |
| Original upstream #51 / PR #54 | Association state and parameter data races | State, configuration snapshots, parameters, queues, routing tables, and procedure outcomes are isolated or synchronized. `TestConcurrentTrafficOnTwoASPsIsRaceFree`, `TestConcurrentAcceptsAreIndependent`, and the full race gate cover the public concurrency contract. |
| Original upstream #39 / PR #52 | Functional options and upper-layer API proposals | v1.2 intentionally uses immutable typed `EndpointConfig`, `ListenerConfig`, and `AssociationConfig`, plus typed MTP3-User and Layer Management operations. These are API proposals, not protocol defects, and no compatibility alias is retained. |
| Original upstream PR #56 | Dynamic Protocol Data writes | `WritePD`, `WriteSignal`, and `Endpoint.MTPTransfer` accept per-message Protocol Data; `TestConcurrentFlowsOnOneAssociationKeepTheirRoutingContexts` and SLS-stream tests keep concurrent message scope independent. |
| Original upstream PR #58 | Exact negotiated stream limit | `TestStreamSelectionHandlesEveryNegotiatedCount`, `TestCheckDataStreamEnforcesNegotiatedBounds`, and the zero-stream refusal test use the Association's negotiated outbound stream count. |
| Original upstream #59 / PR #60 | ASP Down sent from the SGP to an ASP | ASP, SGP, IPSP Single Exchange, and IPSP Double Exchange procedures cover peer-initiated ASP Down, acknowledgement, directional quiescence, state reset, and shutdown ordering. |
| Original upstream PR #61 | Optional SCTP SACK timer control | `AssociationConfig.SetSCTPSACK` and `Association.SetSCTPSACK` validate and apply the per-Association policy, including the RFC 9260 500 ms ceiling, with Linux socket tests. |
| Original upstream #62 | M3UA BEAT confused with SCTP HEARTBEAT | Public documentation and `ErrHeartbeatExpired` identify RFC 4666 M3UA T(beat); SCTP HEARTBEAT remains kernel transport path management and is configured separately. |
| Original upstream PR #63 | SCTP dependency PPID handling | `M3UAPPID == 3`, host-order send metadata, acceptance of PPID `0` or `3`, and discard of every other PPID are covered by `ppid_test.go`. |
| Original upstream #36 | Unrelated request | The issue contains no M3UA protocol, API, interoperability, security, or implementation change to reproduce or adopt. |

## Current validation commands

- `go build ./...`
- `go test ./... -count=1`
- `go test ./... -count=1 -race`
- `FUZZTIME=2s scripts/fuzz-smoke.sh`
- `go vet ./...`
- `staticcheck ./...`
- `golangci-lint run ./...`
- `go run github.com/rhysd/actionlint/cmd/actionlint@latest`

CI runs the test matrix on Go 1.23, 1.24, and 1.25, plus Go 1.25 race and
fuzz-smoke jobs and pinned golangci-lint v2. The fuzz runner discovers every
exported `Fuzz*` target in every package, so adding a target automatically adds
it to the gate.

## Release-candidate validation evidence

The v1.2.0 readiness branch passed the complete host gate with Go 1.25.10:
formatting, `gopls check`, build, unit and integration tests, vet, staticcheck,
golangci-lint, the race detector, two seconds of fuzzing per target, module
tidiness, actionlint, govulncheck, and gitleaks. The fuzz gate discovered and
executed all 20 exported targets.

A privileged Linux/arm64 container with kernel SCTP support and go-sctp v1.0.2
then passed:

- `go test ./... -count=1 -timeout=900s`;
- `go test ./... -race -count=1 -timeout=900s`;
- `FUZZTIME=1s FUZZ_PARALLEL=1 scripts/fuzz-smoke.sh`; and
- focused live SCTP tests for multihomed address retention, bidirectional DATA,
  several ASPs on one multihomed Listener, concurrent Accept, SCTP restart,
  one-shot INIT timeout, prompt context cancellation, and repeated-cancellation
  resource release.

The TEST-NET-1 timeout target was routed through an isolated Linux test
interface so Docker transport translation could not reject SCTP before the
kernel exercised the intended silent-peer timeout path.
