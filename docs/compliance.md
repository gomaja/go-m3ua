# go-m3ua compliance and ecosystem audit

Audit date: 2026-08-31. Standards and API statements rechecked 2026-09-19.

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

## Inter-SGP deployment duty

RFC 4666 §1.4.1 recommends coordinating MTP3 routeset, SPMC and remote AS/ASP
states across the SGPs of one SG and leaves the inter-SGP protocol outside its
scope. One SGP `Endpoint` shares state among the associations it serves. The
library does not coordinate several SGP processes; the deployment must do so.
Divergence can produce inconsistent SSNM or SS7 management actions and traffic
offered through an SGP whose view is stale. The
[inter-SGP coordination boundary](design/endpoint-management-and-ssnm.md#inter-sgp-coordination-boundary)
lists the consequences and existing observation/control APIs for each state
class, including the absence of aggregate SPMC and remote AS/ASP state setters.

## SCTP reliability

`SCTPConfig.PartialReliability` defaults to false for Dial and Listen, preventing
peer abandonment through PR-SCTP (RFC 4666 §1.3.1; RFC 3758 §§3.3.1–3.3.3,
§4.2; RFC 7496 §4.5). Opting in allows silent whole-message loss; abandonment
after partial delivery ends the association with `sctp.ErrMessageInterrupted`.
go-m3ua's own sends are always fully reliable. A peer requiring PR-SCTP may decline
the new default. `SCTPPRSupported` and `AssociationStatus.PRSupported` report
negotiated support. A Listener fixes the value at Listen and refuses a
selector mismatch with `ErrInvalidSCTPConfig` while continuing to serve. On
Linux, a Listener's association answers an SCTP restart from
`net.sctp.prsctp_enable` instead (sctp/lksctp-tools#81), so hosts that run
Listeners need that sysctl to match `PartialReliability`. Persistent settings
also need the [module-load ordering](../README.md#enabling-sctp-on-rocky-linux--rhel-9)
described in systemd's [sysctl.d(5)](https://www.freedesktop.org/software/systemd/man/latest/sysctl.d.html)
and [modules-load.d(5)](https://www.freedesktop.org/software/systemd/man/latest/modules-load.d.html).
See [message boundaries and reliability](standards.md#sctp-message-boundaries-and-reliability)
for Linux behavior and the standards recheck.

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

## Managed IPSP DATA distribution

`Listener.DistributeData` and `Association.DistributeData` use the Endpoint's
peer Application Server registry for SGP and IPSP. IPSP traffic follows the same
Override, SLS Loadshare, Broadcast synchronization, and bounded AS-PENDING
recovery queue as SGP traffic (RFC 4666 §§4.3, 4.3.2 and 4.3.4.3).

| IPSP model and side | Either distribution entry point |
| --- | --- |
| Single Exchange, ASP Active requester or acknowledger | Uses shared peer AS state; ASP Active Ack also activates peer state (§4.3.4.3.1). |
| Double Exchange, either ASPSM agreement, with `TrafficToPeer` | Uses peer-directed AS state; `TrafficToLocal` is independent (§4.3). |
| Double Exchange, Endpoint with only `TrafficToLocal` | Returns `ErrNoConfiguredAS`; local activation cannot authorize outbound distribution (§4.3). |
| A local-only Association whose Endpoint has a sibling peer AS | Uses that Endpoint's peer registry, just as the Listener does. |

`ASPProcedures` and the SCTP initiator do not change this ownership (§1.4.8).
A configured inactive AS returns `ErrNoActiveASP`; AS-PENDING queues until
reactivation flushes it or T(r) expiry discards it (§4.3.2). Network Appearance
and Routing Context select an exact ASKey; other Endpoints and the DE local
direction cannot supply outbound members. Direct point-to-point DATA is unchanged.

Every send path refuses SI 0, including both `DistributeData` entry points for
SGP and IPSP, before selection or queuing, with `ErrMTPManagementData` wrapped
in a `*DataWriteError` with `DataNotSent`. RFC 4666 §1.4.3.2 forbids MTP3
management encapsulation between SG and ASP; §1.4.3.4 says SS7 and M3UA
interworking is unnecessary for IPSP. M3UA DATA carries MTP3-User messages
(§3.3.1). The library therefore excludes SI 0, MTP3's own management, consistently
with direct sends and inbound DATA admission. This uniform admission policy is
the library's application of those boundaries. Received peer congestion and
transport pressure still participate in congestion-method admission, and the
configured cached DATA gate remains effective (§§1.4.6 and 3.4.4). SGP-only
transit veto/ranking settings do not become IPSP configuration. No SS7 route
state, SPMC capacity verdict, or TFC generation is inferred. Automatic
offered-traffic status at the ASP multi-SG routing boundary (§4.5.2) does not
apply to this peer AS distributor; applications needing continued local-user
status use `NewMTPStatusProcedure` with explicit status (§1.4.6).
`Endpoint.MTPTransfer` and `MTPTransferContext` remain ASP routing APIs.

## ASP multi-SG routing

The ASP Endpoint implements the route function described by RFC 4666 Sections
1.3.2.5, 1.4.2.5, 4.5.2.2, and 5.5.1.1.1:

- MTP Routes are local routing-table identities and name provisioned
  `MTPRoutePath` candidates; each candidate names a Signalling Gateway and its
  canonical Application Servers, and the peer-specific Network Appearance and
  Routing Context in an `ASKey` is resolved per SGP at selection time.
- Outbound routing is optional. `ASPConfig.Routing` left nil keeps peer and
  Application Server inventory, procedures and authorization, and gives the
  application outbound candidate selection with no dummy route and no adoption
  of `Endpoint.MTPTransfer`.
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
  rejection, resource bounds, and inactive-only explicit deregistration atomically.
- At an SGP, ASP Down retires dynamic registrations (RFC 4666 §4.3.4.2), as
  does ASP Up while active (§4.3.4.1), through the successful DEREG path
  (§4.4.2) before the acknowledgement. Static membership, provisioned keys,
  and other ASPs' memberships are preserved. Retirement notifications follow
  the Ack. Every AS-state Notify checks current membership at the actual write,
  including events waiting in either notification queue (§4.3.4.5).
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
| [`github.com/rumalg123/telco-signalling-lab`](https://github.com/rumalg123/telco-signalling-lab) | Application-specific signalling lab | No issues or pull requests | Its narrow in-application implementation is not adopted as a library design. In particular, go-m3ua permits pre-ASP-Active-Ack DUNA, DRST, and SCON only in the RFC 4666 Section 4.5.1 activation window; restart DAVA is also admitted in that activation window under §4.6: "In addition to DUNA messages, SCON, DRST, and DAVA can also be sent." |

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
| Original upstream #31 | Read API returned transport-sized or ambiguous results | `ReadData`, the typed `DataMessage`, MTP3-User indications, and `TestWriteDataReturnsThePayloadLength` preserve application payload semantics while exposing message metadata explicitly. `DataMessage.Received` retains the socket reader's monotonic timestamp for measuring parsing, dispatch and receive queue wait, excluding the kernel receive buffer. This is an API-design disposition, not an RFC defect. |
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
| Original upstream PR #56 | Dynamic Protocol Data writes | `WriteData`, `WriteSignal`, and `Endpoint.MTPTransfer` accept per-message Protocol Data; `TestConcurrentFlowsOnOneAssociationKeepTheirRoutingContexts` and SLS-stream tests keep concurrent message scope independent. |
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
- `FUZZTIME=10000x scripts/fuzz-smoke.sh`
- `go vet ./...`
- `staticcheck ./...`
- `golangci-lint run ./...`
- `go run github.com/rhysd/actionlint/cmd/actionlint@latest`

CI runs the test matrix on Go 1.23, 1.24, and 1.25, plus Go 1.25 race and
fuzz-smoke jobs and pinned golangci-lint v2. The fuzz runner discovers every
exported `Fuzz*` target in every package, so adding a target automatically adds
it to the gate. Each newly interesting input receives a bounded minimization
budget so corpus reduction cannot overrun the smoke job's execution budget.

## Release-candidate validation evidence

The v1.2.0 readiness branch passed the complete host gate with Go 1.25.10:
formatting, `gopls check`, build, unit and integration tests, vet, staticcheck,
golangci-lint, the race detector, 10,000 generated inputs per fuzz target, module
tidiness, actionlint, govulncheck, and gitleaks. The fuzz gate discovered and
executed all 20 exported targets.

A privileged Linux/arm64 container with kernel SCTP support and the pinned
go-sctp release then passed:

- `go test ./... -count=1 -timeout=900s`;
- `go test ./... -race -count=1 -timeout=900s`;
- `FUZZTIME=10000x FUZZ_PARALLEL=1 scripts/fuzz-smoke.sh`; and
- focused live SCTP tests for multihomed address retention, bidirectional DATA,
  several ASPs on one multihomed Listener, concurrent Accept, SCTP restart,
  one-shot INIT timeout, prompt context cancellation, and repeated-cancellation
  resource release.

The TEST-NET-1 timeout target was routed through an isolated Linux test
interface so Docker transport translation could not reject SCTP before the
kernel exercised the intended silent-peer timeout path.

## SGP peer congestion boundary

RFC 4666 §§1.3.2.4 and 1.4.2.4 describe retaining remote ASP congestion and
considering it in distribution. The SGP provides bounded independent peer
retention, `SSNMSnapshot.PeerCongestion`, peer upsert/removal deltas and
`Resync` through the existing SSNM continuity barrier. Existing record and byte
budgets include these records; over-budget reports are refused atomically with
visible resource-loss accounting. Peer clear is distinct from unknown, and
lifecycle retirement removes only the affected association epoch/AS scopes.
Peer reports never replace authoritative SS7 state or change DAUD replies.

`Endpoint.CongestionInputs` exposes copied membership, peer, local and transport
components with their versions and a bounded-retry `Consistent` result;
`Revision` is SSNM-only. `Local` retains separate queue/application evidence;
this collection makes no cluster-wide atomicity claim.
`PeerCongestionVersion.LocalGeneration` is an opaque local lifetime/queue version;
compare it as a component without interpreting its numeric value. `SGPCongestionPolicy` is an application veto outside Endpoint,
AS and delivery locks, revalidated once before delivery; a still-stale decision
is known-not-sent `ErrCongestionDecisionStale`.

Optional `SGPConfig.PreferUncongestedPeers` ranks admitted loadshare members by
maximum peer/transport severity after choosing the latest matching peer record
by `Revision`; newer matching clears supersede older ranges. Equal-rank SLS
selection retains the original active-member order. The default is false;
override retains its active member and broadcast exposes each required member's veto explicitly.
Rerouting can missequence SLS traffic (RFC 4666 §1.4.7). RFC 4666 §1.4.6 makes
TFC triggering implementation dependent: SPMC assessment, alternate capacity,
SS7 encoding/transmission and inter-SGP coordination remain application/NIF
responsibilities. No automatic TFC API or SPMC verdict is claimed.

## Declared DAUD congestion profile deviation

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


Peer management explanations are delivered as owned `DiagnosticInformation`
(ERR) and `InfoString` (NTFY) bytes, with absent and present-empty distinguished
(RFC 4666 §4.2). Undefined ASPAC Traffic Mode Type selects Error 0x05 without
activation or Ack (§3.8.1); strict parsing and other fault precedence remain.
DUPU uses structural User ≤255/Cause ≤15 admission on typed origination, raw
`WriteSignal`, and reception (§3.4.5; Q.704 (07/96) §15.17;
Q.Imp704 (12/00) §§2.2.3–2.2.4). The caller owns variant assignments; the
16-bit codecs and one-unmasked-destination, role and scope restrictions remain.

## Optional received congestion validity and audit policy

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

## Continued MTP status boundary

RFC 4666 §1.4.6 describes MTP-STATUS according to current MTP3 procedures;
that service prose contains no uppercase MUST. Optional peer SCON and SG TFC
triggering remain MAY. For international and national-without-priorities
methods, routed ASP `MTPTransfer` implements Q.704 (07/96) §§11.2.3.1/11.2.5
using message counts: initial offer and then no later than eight offers after
each indication, including failed transport attempts. An uninterrupted interval
reports at 1/9/17; resets can report earlier. Invalid requests and additional
broadcast copies, candidates or retries do not add offers. Raw writes do not
count. National-with-priorities remains transition-driven (§11.2.4).

Conservative engineering policy suppresses repeats and retires automatic
intervals when either `ReceivedCongestionUnknown` or `TransportCongestionUnknown`
is true, even if another contribution keeps `Congested` true. Supplying uncertain
status to the helper resets it. Transport uncertainty uses a normal transition
reason, never received-validity expiry; this policy adds no Q.704 MUST.

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
the interval. Sampling never waits for route or AS locks.

Only explicit expiration sets `MTPStatusValidityExpired`. Availability,
transport or lifecycle operations exposing already-expired knowledge retain
`MTPStatusTransition`; an uncertain noncongested result uses `MTPStatusCauseNone` and cannot
claim measured congestion clear. RFC 4666 §3.4.4 permits a validity timer but
does not prescribe these Go trigger values.

IPSP AS selection and recovery use `DistributeData`. Applications needing
continued local-user status use `NewMTPStatusProcedure` with actual
aggregate status, once per destination/local MTP3 boundary. A changed
destination resets automatically; clear or uncertain status supplied to `Offered`
also resets it. The helper sees snapshots only. Call `Reset()` on known clear,
uncertainty, expiry or local lifecycle/method
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

Applications fan every primitive from the single bounded stream or explicit helper to each
registered MTP3-User; stream overflow requires snapshot resynchronization.
Received-validity expiry is unknown knowledge, not congestion clear or
continuing known congestion. Cause/reason fields expose this distinction.
Endpoint-wide exact-DPC LRU retention and its zero-value 4096 limit are
engineering choices, not standard-mandated resource values. See
[continued status](../README.md#continued-mtp-status) and its
[migration notes](migration-v1.2.md#continued-mtp-status).

Level-4 resumption stays with the user (Q.704 (07/96) §11.2.3.3). Q.Imp704
(12/00) changes none of the relied-on clauses; the 17 September 1999 covering
note remains unassessed. No complete corrected-Q.704, ANSI or TTC compliance
is claimed.

## Local publication and cached DATA resource admission

RFC 4666 §1.4.6 describes implementation-dependent congestion detection and uses
MAY for ASP/IPSP local SCON and SG SS7 TFC triggering. Bounded queue/application
contributions and optional publication implement local reporting without claiming
SPMC-wide congestion or installing an SS7 TFC sender. `ReportLocalCongestion` and
`WithdrawLocalCongestion` identify independent sources; `CongestionInputSnapshot.Local`
keeps them separate from received SS7, peer and transport evidence. No TTL or host
load detector is inferred. Queue drain removes only queue evidence. SGP local
contributions never authorize SGP-to-ASP SS7 SCON.

`CongestionConfig.Publication` bounds one optional scheduler for local SCON and
automatic audits. Defaults are engineering choices: 64 slots, 32 per association,
64 KiB of accounted metadata, 100 ms retry and a 1 s interval after successful
submission.
Repeated intent coalesces; an onset that clears before submission is cancelled.
Submitted national onsets reserve their clear capacity; blocked clears are not
evicted to admit new onsets or audits. Distinct triggering CD origins retain
separate keys. International recovery produces no numeric zero. Explicit SCON
and DAUD operations remain synchronous and return actual errors. Successful
automatic DAUD is observed once per submitted wire batch, with empty canonical
`Partition`/`Epoch`, rather than once per resolved owner; logical-job counts are
unchanged by that observation-cardinality break.

Optional attempts try existing traffic/send barriers, recheck live epoch/state/
revision and use NoWait once, without tracker/state locks across transport. Each
pass permits at most one attempt per association across both kinds, subject to
rate limits and pending mandatory control. Lifecycle fences prevent retired work
from being reused; no report activates an AS. `Endpoint.Close` cancels and joins
the worker. Publication snapshots, counters and management indications preserve
local failure observability without logging; failed publication never clears a
local contribution. A submitted result is not proof of receipt at the peer.

RFC 4666 §1.4.7 makes stream-zero DATA prohibition a MUST NOT, sequenced traffic
assignment and the listed management mapping SHOULDs, and SSNM's any-stream
mapping permissive. These stream rules do not reserve send-buffer memory.
`AssociationConfig.CongestionAdmission` therefore offers a separate cached DATA
resource gate, disabled by nil. The engineering default ratio is 0.9; finite
0<ratio<1 and the backlog monitor are required. All typed, raw and distributed
DATA uses cached admission under send serialization, with no socket query.
At/above the ratio, invalid/stale samples and startup before the first valid
sample return a typed known-not-sent error wrapping `ErrDataCongestionRefused`.
Control bypasses the gate; admission never manufactures SCON or DUNA.

Cached admission reserves no kernel memory and gives control no delivery
guarantee. A large message, retransmission or other descriptor user can consume
remaining charge. Mandatory control retains `ControlWriteTimeout` and closes a
permanently stalled association. Optional NoWait admission can still race newly
starting mandatory work and retains the pinned transport's internal-lock and
application-deadline limits; there is no universal latency or fairness claim
(RFC 6458 §5.3.4; Verified Errata 6111). See the [publication design](design/endpoint-management-and-ssnm.md#local-congestion-publication-and-data-admission)
and [migration contract](migration-v1.2.md#local-congestion-publication-and-data-admission).

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
