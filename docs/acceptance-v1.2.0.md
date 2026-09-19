# Cross-slice acceptance campaign

This is the evidence record for the v1.2.0 application-managed routing contract.
It records what was run, where it ran, what each run showed, and — separately —
which gates could not be measured here and what they would need. It is a record
of measurements, not a release authorization: readiness is not permission to
publish, and nothing here moves or creates a tag.

Every command below was executed for this record. Exit statuses are quoted as
observed. Where a gate could not be met, the reason is stated instead of the
threshold being moved; no numerical threshold in the approved budgets was
changed to fit a result.

## Heads and revisions

| Role | Commit | What it is |
| --- | --- | --- |
| Assessed baseline | `d097e191d879efc95e36c0254814933f01aa9aee` | The contract's assessed baseline. It predates the traffic fixture and the canonical DATA API, so it cannot be *measured* with this fixture; see [Measurable baseline](#measurable-baseline). |
| Measurable baseline | `35647ff67768b933d9d83b65146390cc7643bd4f` | The last commit before the first implementation slice, and the first commit carrying `internal/cmd/perftraffic`. |
| Campaign candidate | `64113c7ce1edf971d5ace9b8204b398ba430b552` | `140b3e4` (the last merged slice, #43/PR #67) plus the fixture fix in [Tooling defect](#tooling-defect-found-and-fixed). |

Dependency: `github.com/gomaja/go-sctp v1.0.4`, identical in the baseline and
the candidate, so the ratios below isolate library changes rather than a
transport change.

## Reference environment

Two sibling containers on one user-defined Docker bridge, not loopback and not
nested Docker. Loopback bypasses the qdisc layer, so a shaped or blackholed path
on `lo` shapes nothing; a bridge veth is a real path in both directions.

| | |
| --- | --- |
| Image | `sctp-wire:latest` — Debian 13, Go 1.25.14 linux/arm64, tshark 4.4.18, iproute2 6.15.0 |
| Network | `docker network create --driver bridge m3ua-i44` |
| Containers | `i44-a`, `i44-b` (functional suites); `i44-perf-asp` (`--cpuset-cpus 0-3 --cpus 4`), `i44-perf-sgp` (`--cpuset-cpus 4-7 --cpus 4`), both `--memory 2g`, `GOMAXPROCS=4`, `GOGC=100`, `GOMEMLIMIT=1536MiB` |
| Privilege | `--privileged`, for `sysctl net.sctp.*`, extra loopback addresses and `iptables` |
| Kernel SCTP | `/proc/net/sctp/snmp` present, `net.sctp.auth_enable=1`, `net.sctp.intl_enable=1` |

**Absolute figures from this environment are not hardware figures.** Docker here
runs inside a Linux VM on macOS, so throughput and latency magnitudes carry the
VM and its virtual NIC. They are reported as environment-qualified absolute
limits. The ratio bounds are reported separately, because both members of a
matched pair run on the same VM within seconds of each other and the environment
largely cancels.

### The test network preparation is part of the environment, not optional

The CI job performs a named preparation step before the Linux suite. Reproducing
it exactly settles three failures that had been carried as environmental noise:

```sh
modprobe sctp
sysctl -w net.sctp.auth_enable=1
sysctl -w net.sctp.intl_enable=1
iptables -A OUTPUT -d 192.0.2.1 -j DROP
ip addr add 127.0.0.2/8 dev lo   # and 127.0.0.3, 127.0.0.4
```

`TestDialGivesUpOnItsOwnTimeout`, `TestDialAbandonsOnContextCancellation` and
`TestRepeatedCancelledDialsDoNotLeak` dial `192.0.2.1` (TEST-NET-1) and require
it to be a silent blackhole. Without the `iptables` rule the container's NAT
answers the SCTP INIT, the dial fails immediately, and all three fail:

```
dial_test.go:197: Dial error = dial sctp4 192.0.2.1:2905: protocol not available, want ErrInitTimeout
--- FAIL: TestDialGivesUpOnItsOwnTimeout (0.00s)
--- FAIL: TestDialAbandonsOnContextCancellation (0.00s)
--- FAIL: TestRepeatedCancelledDialsDoNotLeak (1.03s)
```

With the rule applied in the same container, same image, same commit, no other
change:

```
--- PASS: TestDialGivesUpOnItsOwnTimeout (2.00s)
--- PASS: TestDialAbandonsOnContextCancellation (0.50s)
--- PASS: TestRepeatedCancelledDialsDoNotLeak (2.32s)
ok  	github.com/gomaja/go-m3ua	4.849s
```

They are neither defects nor noise: they are tests that state a requirement on
the host network and fail honestly when it is absent. An environment that cannot
blackhole an address cannot run them, and a green run on such a host would be
the wrong answer, not a better one.

## Standards checkpoint

Rechecked against both authorities on the date of this campaign, because an RFC
that was current when code was written can be obsoleted or amended afterwards.

| Document | RFC Editor | Datatracker incoming relationships | Errata |
| --- | --- | --- | --- |
| [RFC 4666](https://www.rfc-editor.org/rfc/rfc4666.json) | Proposed Standard, obsoletes RFC 3332, `obsoleted_by` and `updated_by` both empty | 16 incoming: 10 `refnorm`, 5 `refinfo`, 1 `became_rfc`. No `obs`, no `updates`. | 0 Verified. 2065 and 4475 Held for Document Update, 2518 Rejected. |
| [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.json) | Proposed Standard, obsoletes RFC 4460, 4960, 6096, 7053, 8540; `obsoleted_by` and `updated_by` both empty | 80 incoming, none `obs` or `updates` | 5 Verified — 7148 §3.3.3, 7387 §5.2.4.1, 8402 §5.1.6, 7147 §3.2, 7852 §8.5. None touches Section 16. |

Two consequences for this campaign:

- The transport-stall threshold cites RFC 9260 Section 16 (`RTO.Min` and
  `RTO.Initial` of one second). No verified erratum touches Section 16, so the
  citation stands as written.
- The only two remaining `RFC 4960` mentions in the tree are historical — both
  name RFC 9260 as the current document and RFC 4960 as the one it obsoleted.
  There is no obsolete-RFC dependency to report.

Endpoint note for whoever repeats this: `https://www.rfc-editor.org/errata/rfcNNNN`
now answers **HTTP 302** to `https://errata.rfc-editor.org/rfcNNNN`. A fetch
without redirect following returns a zero-byte body, which parses as "no errata"
and silently inverts the conclusion. Follow the redirect.

## 1. Per-slice failing-first regressions and passing validation

Each implementation slice is shown in its failing-first state: the slice's own
tests against the code as it was immediately before the slice landed. The tree
is built by taking the slice commit and restoring every non-test file to its
parent, so the regressions are exactly the slice's, run against the previous
contract.

| Slice | PR | Commit | Parent | Failing-first result |
| --- | --- | --- | --- | --- |
| #37 ASP peer and AS inventory | #54 | `8b5f260` | `35647ff` | `go vet` exit 1 — `not enough arguments in call to NewRoutingKeyPayload` (grouped Routing Key representation absent) |
| #38 bounded SSNM state | #55 | `b811d95` | `8b5f260` | `go vet` exit 1 — `undefined: WireScope`, `AffectedPointCodeCount undefined` |
| #39 typed per-message DATA | #56 | `a1c2eaf` | `b811d95` | `go vet` exit 1 — `undefined: DataSendOutcome` |
| #39 typed ApplicationServers | #57 | `17d990e` | `4784141` | `go vet` exit 1 — `undefined: ASConfig` |
| #40 optional routing projection | #62 | `fe787e6` | `0de90e2` | `go vet` exit 1 — `undefined: MTPRoutePathID` |
| #41 SGP publication and restart | #61 | `0de90e2` | `17d990e` | `go vet` exit 1 — `undefined: DestinationAvailability` |
| #42 codec/management export cleanup | #58 | `4784141` | `a1c2eaf` | compiles; `go test ./...` exit 1 — `TestDeprecatedCodecWrappersAreRemoved`, `TestNoDeprecationLoggingSurvivesInTheCodec`, `TestManagementIndicationHasNoCompatibilityProjections` all FAIL |
| #43 GoDoc, migration, coverage | #67 | `140b3e4` | `ac15503` | compiles; `go test ./...` exit 1 — `TestGoDocDescribesImplementedBehaviour`, `TestPublishedGoExamplesNameOnlyCurrentExports`, `TestEveryProcedureCoverageCitationExists` all FAIL |

Two distinctions worth keeping. A slice whose failing-first state is a *compile*
failure proves the contract did not exist; a slice whose failing-first state is
an *assertion* failure proves the behaviour was different and is now pinned. The
second is the stronger form, and #42 and #43 — the two slices whose changes are
removals and documentation rather than new types — are exactly the two that have
it.

### Defect slices

| Slice | What it was | Failing-first evidence | Passing evidence |
| --- | --- | --- | --- |
| #59 — the DATA allocation ceiling missed a staging regression | A coverage defect in the test suite, not a production defect | With `Data.MarshalTo` mutated to take the staged branch unconditionally: `TestWriteDataAllocations` and `TestWriteDataAllocatedBytes` both **PASS** (the ceiling is too far above the real cost to notice), while the #63 pin `TestDataMarshalToWithoutExtensionParametersStagesNothing` **FAILS** with `Data.MarshalTo allocates 1 times for a 128-octet message with no extension parameters, want 0` | Unmutated head: both pins pass |
| #60 — ASP state observable before the AS registry records it | A production race, reported as reproducing on `main` | Parent `17d990e`, 30 iterations, `-race`: **2 subtest failures**, `write IPSP DATA "A-to-B": m3ua: DATA write failed (not sent): routing context is not active for this association`, exit 1 | Head, 30 iterations, `-race`: **0 failures**, 0 occurrences of the refusal, `ok github.com/gomaja/go-m3ua 188.709s`, exit 0 |
| #64 — flaky progress-sampler deadline | A test-stability defect, not caused by the change it appeared under | Fixed in PR #66 (`ac15503`) by waiting on the sampler rather than on the wall clock | Head: the sampler tests pass in every suite run recorded here, including under `-race` and at `GOMAXPROCS=1` |

### Negative controls: the detector is live

Three tests in the suite are opt-in controls whose purpose is to prove the race
detector fires on the patterns the production tests claim are safe. They skip by
default. Run deliberately, under `-race`, all three fire:

| Control | Env | Data races reported | Result |
| --- | --- | --- | --- |
| `TestConcurrentAcceptResolutionRaceControl` | `M3UA_RACE_DETECTOR_CONTROL=1` | 3 | FAIL, as designed |
| `TestSSNMRaceDetectorControl` | `M3UA_SSNM_RACE_CONTROL=1` | 1 | FAIL, as designed |
| `TestUnsynchronizedWithdrawalIsDetected` | `M3UA_RACE_CONTROL=1` | 1 | FAIL, as designed |

Without these the clean `-race` suite would be consistent with a detector that
never fires. With them, the clean run is evidence.

### Classification

The acceptance asks for four categories to stay distinct. Flattening them would
turn "we kept this working" into "we fixed this", which is the claim the
contract exists to prevent.

- **Pre-existing supported behaviour, retained.** ASP/SGP/IPSP roles, both IPSP
  exchange models, Override/Loadshare/Broadcast and n+k activation, MTP3
  restart, NIF, Layer Management, heartbeat, stream and PPID handling. These
  predate the backlog; the slices moved their API surface without changing the
  procedures. Their evidence is that the full suite passes at the candidate head
  and the corresponding failing-first trees fail only on the *new* contract's
  tests, not on the retained ones.
- **Confirmed fixed defects.** #59 (test coverage gap, closed by PR #63), #60
  (ASP-state/registry race, closed by PR #61), #64 (wall-clock flake, closed by
  PR #66), and the tooling defect found by this campaign and fixed in
  `64113c7`. Each has a failing-first artefact above.
- **Application duties.** Outbound candidate selection, inbound application
  policy, persistence, orchestration, bounded discovery and audit scheduling,
  retry policy, cross-association ordering, and destination policy for direct
  writes. The library validates role, binding, authorization, active state,
  message structure and stream constraints on a direct write and stops there.
  This is stated in the migration map, and the examples exercise it.
- **Optional enhancements.** Routing Key Management, `Endpoint.MTPTransfer` as
  an optional routing mode, `AllowUnknownDestinations`, DAUD auditing,
  Correlation Id, Network Appearance, contextless AS, Destination Restricted,
  and the Message Priority congestion policy. `docs/procedure-coverage.md`
  separates the optional procedures that are implemented as procedures from the
  concrete exclusions where only a codec exists.

## 2. Real Linux SCTP, both initiation directions

The full suite on the candidate head, on Linux, against the kernel SCTP stack:

```
go test ./... -count=1 -timeout=900s          exit 0
  5656 tests passed, 0 failed, 7 skipped
go test ./... -count=1 -race -timeout=1800s   exit 0
  ok github.com/gomaja/go-m3ua 452.545s; 0 "WARNING: DATA RACE"
```

**No socket-backed test was skipped.** All seven skips are accounted for, and
none of them is a socket test opting out on an unsupported host:

| Skipped | Why |
| --- | --- |
| `TestDataWithoutProtocolDataIsRejected/{bare_header,NetworkAppearance_only,RoutingContext_only,CorrelationID_only}` | `t.Skipf("not parseable: %v")` — the codec rejects these at parse time, so the deeper association-level check is unreachable. The stricter behaviour is what makes the subtest vacuous. |
| `TestConcurrentAcceptResolutionRaceControl`, `TestSSNMRaceDetectorControl`, `TestUnsynchronizedWithdrawalIsDetected` | Opt-in race-detector controls, run separately above. |

Both SCTP initiation directions were exercised, at the library level and on the
wire:

- Library: `TestTypedDataCrossesBothSCTPInitiationDirections`,
  `TestIPSPDoubleExchangeDirectionsAreIndependentForTypedData`,
  `TestIPSPDoubleExchangeContextlessAcrossSCTPInitiationOrientations`,
  `TestExplicitASPProcedurePolicyControlsDialReadinessAndWireSequence` — 15
  results, 0 failures, 0 skips.
- Fixture: `perftraffic` supports `asp-dial` and `sgp-dial` and records the
  direction in `spec.initiation`; the direct-traffic rows below ran across two
  separate containers, so every association crossed a real veth pair.

### Independent-peer interoperability: not met here

Both ends of the traffic fixture are the same binary, and the fixture says so in
its own output: `unsupported_modes.independent_peer_validation = "unavailable:
both endpoints use this binary"`, and `independent_peer: false` on every record.
No third-party M3UA implementation was available in this environment, so the
acceptance bullet's independent-peer half is **not evidenced**. It needs a
separate implementation — an Osmocom `osmo-stp`, a vendor SG, or an equivalent
peer — and a run against it. Nothing here should be read as interoperability
evidence, and the fixture deliberately refuses to emit a zero-valued success in
its place.

## 3. Routing-inventory behaviours

Each topic was run as a named set against real Linux SCTP on the candidate head
(`go test . -count=1 -v -run '^(…)$'`), so the claim is a list of tests that ran,
not a summary.

| Topic | Tests | Results |
| --- | --- | --- |
| Zero-route traffic | 6 | 8 pass, 0 fail, 0 skip |
| Route reference 0→1→many→0 | 6 | 32 pass, 0 fail, 0 skip |
| Concurrent message tuples | 5 | 5 pass, 0 fail, 0 skip |
| Shared AS and associations | 5 | 5 pass, 0 fail, 0 skip |
| Different per-peer policies | 6 | 6 pass, 0 fail, 0 skip |
| Scoped identifier reuse | 6 | 7 pass, 0 fail, 0 skip |
| Ambiguous-wire rejection | 7 | 18 pass, 0 fail, 0 skip |

Every named test was checked to have actually run: one of them,
`TestRoutingKeyReceiverResetOnFailureAndReuse`, lives in `messages/params` and
was initially selected by a root-package run that could not match it. It was
re-run against its own package and passes. A `-run` pattern that matches nothing
reports success, so the check matters.

## 4. SSNM and recovery behaviours

| Topic | Tests | Results |
| --- | --- | --- |
| SSNM before route creation | 3 | 3 pass, 0 fail, 0 skip |
| Alternative-path failures | 6 | 6 pass, 0 fail, 0 skip |
| Pending and last-binding invalidation | 7 | 7 pass, 0 fail, 0 skip |
| Overload and resync | 9 | 9 pass, 0 fail, 0 skip |
| Event-only loss | 5 | 8 pass, 0 fail, 0 skip |
| Resource teardown | 8 | 8 pass, 0 fail, 0 skip |

## 5. Performance

Reported in three separate parts, because they answer different questions and
one cannot stand in for another: [ratio bounds](#ratio-bounds-candidate-versus-baseline)
(candidate against baseline, where the environment largely cancels),
[absolute limits](#absolute-limits-environment-qualified) (what this environment
did, qualified by what this environment is), and
[sustainable capacity](#sustainable-capacity-what-the-instrument-can-and-cannot-resolve)
(what the predeclared decision rule could and could not resolve).

## 6, 7. Gates on the exact head

See [Gate results](#gate-results).

## 8. Review findings

Every merged slice PR was checked for unresolved review conversations through
the GitHub GraphQL API:

| PR | State | Review threads | Unresolved |
| --- | --- | --- | --- |
| #54, #55, #56, #57, #58, #61, #62, #63 | MERGED | 0 | 0 |
| #66 | MERGED | 1 | 0 |
| #67 | MERGED | 2 | 0 |

No merged slice carries an unresolved conversation. PR #65 is still open and is
a **superseded duplicate**: it proposes a different fix for issue #64, which was
closed by PR #66. It is listed under [Remaining release actions](#remaining-release-actions).

## 9. Campaign completeness

This document is the campaign record. Evidence is above; the actions that remain
are below, separately, because a readiness record that mixes them invites the
second to be read as the first.

## 10. Transport dependency

No go-sctp capability gap or defect was found in this campaign, and no
dependent work was paused. The one ~1 second transport stall recorded here was
deliberately induced by blackholing the receiver's SCTP input; see
[The stall detector against real data](#the-stall-detector-against-real-data).
The dependency is `v1.0.4` on both sides of every matched pair, so it cannot be
the source of a candidate-versus-baseline difference.

## Tooling defect found and fixed

Running the traffic fixture and the acceptance CLI together against a real
record — for the first time — showed they could not be connected.

`internal/cmd/perfcapacity` requires `capped` and `send_errors` on every run and
rejects a record without them:

```
{"decision":"invalid-input","error":"probe 1 run: capped and send_errors are required"}
```

The fixture marked both `omitempty`. A loss-free run is exactly the run whose
counters are zero, so **every passing run produced a record the acceptance tool
refused to decide**, while a failing run — with non-zero counters — was accepted.
The tool's own tests supply hand-written JSON that spells the zeros out, so the
gap survived both sides' test suites.

Fixed in `64113c7` by serializing both counters unconditionally, with a
regression on the record's encoding (`TestSenderRecordKeepsTheAcceptanceRequiredCountersAtZero`),
failing first:

```
acceptance_input_test.go:29: run record omits "capped" when it is zero; internal/cmd/perfcapacity requires it and rejects the record as invalid input
--- FAIL: TestSenderRecordKeepsTheAcceptanceRequiredCountersAtZero (0.00s)
```

The tool's strictness was kept: an absent counter is still invalid input. What
changed is that the fixture now states the zero it counted.

## Measurable baseline

Issue #36 requires the campaign to name any baseline it cannot measure rather
than substituting one. The assessed baseline `d097e19` is one of those. It
predates `internal/cmd/perftraffic` entirely, and the fixture's send path calls
`Association.WriteData`, which does not exist there. A "baseline" run at
`d097e19` would have to be a different fixture measuring a different API, which
is not a matched pair, so it is reported as an unavailable baseline rather than
approximated.

The measurable baseline is `35647ff`: the commit that introduced the fixture and
the last commit before the first implementation slice. Between it and the
candidate the fixture's measurement path is unchanged — `progress.go` gained
named constants for values it already used, and `sender.go` changed
`WritePDWithRoutingContext` to `WriteData` and `ReadData()` to `ReadData(ctx)`.
Those two calls are the change under test. The window accounting, progress
sampling, ledger, payload identity and counters are identical, so the two
records mean the same thing.

Both binaries were built from clean clones at their exact commits, so each
manifest names its own revision (`35647ff67768…`, `64113c7ce1ed…`) with
`vcs_modified: false`, and both carry `assessed_baseline_revision`
`d097e191d879…` as a separate field. The fixture never lets those two be
confused, and neither does this record.

## Ratio bounds: candidate versus baseline

Twenty independent matched pairs. Each pair runs the baseline and the candidate
back to back so that machine drift is shared by both members, and the order
inside a pair alternates so neither revision always runs first. Row: eight
associations, `mix` payload, 40,000 messages/s offered, 10 s warm-up as a
separate cohort, 30 s measurement window, 2 s drain, deterministic seed per
pair.

All 40 runs were loss-free and fixture-valid: `fixture_verdict: pass`, zero
missing, duplicate, invalid, reordered and late-after-stop, zero cap refusals
and zero send errors. Every run uniquely validated all 1,200,000 scheduled
messages. A small tail landed in the two-second drain rather than inside the
measurement window — median 25 messages for the baseline and 12 for the
candidate, at most 256 — which is why the in-window count below is a few
messages short of the schedule while the missing count is zero.

`internal/cmd/perfratio`, geometric mean of the run-level log ratios with a
two-sided 95% Student-t interval on 19 degrees of freedom:

| Metric | Direction | Boundary | Geometric mean | 95% interval | Decision | Baseline spread |
| --- | --- | --- | --- | --- | --- | --- |
| **CPU-seconds per validated delivery** | upper | **1.10** | **0.9594** | **[0.9533, 0.9655]** | **pass** (exit 0) | 3.73% |
| Allocations per validated delivery | upper | 1.10 | 0.4741 | [0.4740, 0.4742] | pass (exit 0) | 0.11% |
| Allocated bytes per validated delivery | upper | 1.10 | 0.6066 | [0.6065, 0.6067] | pass (exit 0) | 0.12% |

The first row is the approved CPU-efficiency gate from #36: the candidate over
baseline upper bound must not exceed 1.10. The observed upper bound is 0.9655 —
the candidate costs about 4% *less* CPU per validated delivery than the
pre-slice baseline, and the whole interval sits below one.

The two allocation rows are **not** approved ratio gates; the approved
allocation budget is absolute and is reported below. They are included because
they are the clearest single measurement of what the typed DATA API changed:
per-delivery allocations fell to 47% and allocated bytes to 61% of baseline,
with intervals four decimal places wide because allocation counts are
deterministic rather than timing-dependent.

Throughput deliberately has no ratio row. At a fixed 40,000/s offered load both
revisions delivered the entire schedule with nothing lost, so a throughput ratio
would be 1.000 by construction and would say nothing. Differentiating throughput
means running both to saturation, which needs the capacity decision the
instrument cannot resolve here; see below.

## Absolute limits (environment-qualified)

These are what this environment did. Docker here runs in a Linux VM on macOS, so
the magnitudes carry the VM and its virtual NIC and are not quotable as hardware
figures. Allocation counts and byte counts are the exception: they are
properties of the code, not of the machine.

### Nominal row, 8 associations, mix payload, 40,000 messages/s

| | Baseline `35647ff` | Candidate `64113c7` |
| --- | --- | --- |
| Uniquely validated, of 1,200,000 scheduled | 1,200,000 in all 20 runs | 1,200,000 in all 20 runs |
| Validated inside the measurement window (median) | 1,199,976 | 1,199,988 |
| Missing / duplicate / invalid / reordered / late | 0 / 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 / 0 |
| Cap refusals, send errors | 0, 0 | 0, 0 |
| Sender-window achieved-rate lower bound | 39,985.6 – 39,987.3 /s | 39,985.0 – 39,987.1 /s |
| CPU-seconds per validated delivery (median) | 2.470 × 10⁻⁵ | 2.371 × 10⁻⁵ |
| Allocations per validated delivery (median) | 15.22 | 7.22 |
| Allocated bytes per validated delivery (median) | 1,094 | 664 |
| Send-call p99 | 65,536 ns (one run 131,072) | 65,536 ns (all 20 runs) |
| Longest send call | 11.0 – 26.4 ms | 11.4 – 42.4 ms |

The allocation figures are whole-process sender observations: they include the
fixture's own payload generation, validation, HTTP control and library work.
They are compared against the **end-to-end** budget stated in
`write_allocation_test.go` — 16 allocations and `2P + 1024` bytes per message —
and not against the tighter isolated-package budget, which the fixture cannot
measure.

For the `mix` workload the exact 90/9/1 schedule gives a mean payload of
202.24 octets, so the byte budget is 1,428.5. The candidate uses 7.22 of 16
allocations (45%) and 664 of 1,428 bytes (46%). The baseline used 15.22 of 16
allocations — inside the budget with 5% of headroom. That margin is what the
typed DATA API bought back.

### Payload and association matrix, 20,000 messages/s

The #36 fixture matrix, one offered rate across all six rows so they stay
comparable:

| Associations | Payload | Fixture | Validated | Allocs/msg | Bytes/msg | Byte budget `2P+1024` | CPU-s/msg | Longest send |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 128 | pass | 600,000 | 7.35 | 518 | 1,280 | 1.691 × 10⁻⁵ | 0.58 ms |
| 1 | 512 | pass | 599,983 | 7.43 | 1,309 | 2,048 | 2.286 × 10⁻⁵ | 0.40 ms |
| 1 | 4096 | **invalid** | 187,764 | 7.76 | 9,213 | 9,216 | 3.720 × 10⁻⁵ | **1,015 ms** |
| 8 | 128 | pass | 600,000 | 7.40 | 523 | 1,280 | 4.056 × 10⁻⁵ | 14.4 ms |
| 8 | 512 | pass | 599,997 | 7.41 | 1,308 | 2,048 | 4.313 × 10⁻⁵ | 14.5 ms |
| 8 | 4096 | pass | 599,980 | 7.46 | 9,184 | 9,216 | 5.921 × 10⁻⁵ | 11.7 ms |

Two absolute limits fall out of this table, and both are worth stating plainly.

**One association cannot carry 20,000 × 4,096-octet messages per second here.**
That is 82 MB/s on a single SCTP association over a bridge veth. The transport
saturated, 12,219 messages were refused by the fixture's outstanding cap, and
one send call blocked for 1.0155 s. Eight associations carry the same offered
load loss-free. This is an environment limit reported as one, not a library
finding.

**The allocated-byte budget is nearly exhausted at the largest payload.** At
P = 4096 the budget is 9,216 bytes and the measurement is 9,184 — 99.65% of it,
32 bytes of headroom, whole process. Nothing fails, and the smaller payloads sit
at 40% and 64% of their budgets, but a future change that adds one
message-sized copy on this path would breach it at 4,096 octets first and
nowhere else. That is a budget observation for the next change to know about,
not a result of this one.

### Round-trip latency, echo mode

Three repetitions each, eight associations, mix payload, 5,000 requests/s,
20 s window. RTT is measured on the sender's own monotonic clock from scheduled
dispatch to validated echo reply; it is never halved and is never presented as
one-way latency.

| | Baseline | Candidate |
| --- | --- | --- |
| Requests / validated | 100,000 / 100,000 | 100,000 / 100,000 |
| Cap refusals, deadline expiries, invalid replies | 0, 0, 0 | 0, 0, 0 |
| p50 | 1.049 ms | 1.049 ms |
| p95 | 2.097 ms | 2.097 ms |
| p99 | 16.777 ms | 16.777 ms |
| Max | 22.6 – 32.7 ms | 21.9 – 25.7 ms |

The p50, p95 and p99 figures are power-of-two bucket upper bounds, so they are
conservative over-estimates within a factor of two and identical between the two
revisions at that resolution. They do not distinguish the revisions and are not
claimed to; only the exact maxima differ, and they differ by run-to-run noise on
both sides. A latency comparison sharper than this needs a finer histogram, not
more runs.

### Retained memory

Live heap across the measured cohort, from the sender's `runtime.MemStats`
before and after, over 1,200,000 messages and roughly 335 garbage collections
per run:

| | Median change | Range over 20 runs |
| --- | --- | --- |
| Baseline sender | +338 KB | −2.36 MB to +2.36 MB |
| Candidate sender | +107 KB | −2.24 MB to +2.68 MB |
| Receiver (candidate pair) | +169 KB | — |

The change is noise around zero in both directions and in both revisions: the
median is well under one byte per message and individual runs go negative as
often as positive. There is no retention trend to report.

### Failure load

A real path failure was injected mid-measurement at the 40,000/s row by
blackholing arriving SCTP on the *receiver's* INPUT chain, so the drop happens
past the sender's capture point and the sender sees a genuinely unacknowledged
path. With a 3-second block:

| | |
| --- | --- |
| Scheduled | 1,200,000 |
| Submitted / sent | 895,668 |
| Refused by the outstanding cap | 304,332 |
| Send errors | 0 |
| Uniquely validated | 895,668 |
| Missing | 304,332 |
| Duplicate / invalid / reordered / late-after-stop | 0 / 0 / 0 / 0 |
| Outstanding work after drain | 0 |

Every message the library accepted was delivered exactly once and in order. The
whole loss is the fixture's own outstanding-cap refusals — work never handed to
the library — and it matches the missing count exactly. Under a three-second
path failure at 40,000 messages per second, nothing submitted was lost,
duplicated or reordered, and the association recovered and drained to zero.

The run's acceptance decision is `inconclusive`, not `pass`, because a stall was
detected; that ordering is the rule working, and it is covered next.

## Sustainable capacity: what the instrument can and cannot resolve

The predeclared rule compares the sender-window backlog-change interval against
`BacklogResolution`, one message — the fixture's own least count, derived from
its integer message arithmetic. A run passes only when the interval's upper
bound is at or below one message.

The bounded search was executed with the approved semantics and adjudicated by
`internal/cmd/perfcapacity`:

| Probe | Rate | Decision | Backlog | Longest send |
| --- | --- | --- | --- | --- |
| 1 | 1,000 /s | pass | not-growing | 7.4 ms |
| 2 | 2,000 /s | pass | not-growing | 15.3 ms |
| 3 | 4,000 /s | inconclusive | indeterminate — `backlog-change-interval-spans-instrument-resolution` | 21.6 ms |

Campaign decision: **inconclusive**, `search-did-not-refine-a-pass-fail-bracket`
(exit 2). An inconclusive probe terminates the search without retry, which is
the predeclared behaviour and was not overridden.

This is the campaign's most important negative result, so it is worth being
exact about what it does and does not mean.

The rule is **satisfiable on real data**: probes at 1,000 and 2,000 messages per
second reached `not-growing`, which the `perfstats` README expected to be
unreachable at the approved rates and did not claim was unreachable everywhere.
It is also **falsifiable in the other direction**: the failure-load run above
produced an interval of `[303,944.7, 304,004.6]` messages, decided `growing`.
The rule can return all three answers on measured data.

What it cannot do is resolve the region in between at useful offered rates. The
interval's width is the offered schedule's advance during one progress round
trip, and that round trip is roughly 1.2 ms here:

| Offered rate | Backlog-change interval | Width | Rule |
| --- | --- | --- | --- |
| 500 /s | [0, 0] | 0 | not-growing |
| 1,000 /s | [−0.29, +0.29] | 0.57 | not-growing |
| 2,000 /s | [−1.43, +1.00] | 2.4 | not-growing (upper exactly at the resolution) |
| 4,000 /s | wider than the resolution on both sides | ~5 | indeterminate |
| 40,000 /s | median [−30, +31] | ~60 | indeterminate |

So above roughly 2,000 messages per second the instrument cannot tell a backlog
that grew by one message from one that did not, and every row above that rate is
`indeterminate` **whatever the library does**. That is a resolution limit of the
measurement, not a property of this environment and not a candidate result: a
faster machine would have a shorter round trip but the same arithmetic, and the
limit would move, not disappear.

The correct response is not to raise `BacklogResolution` — the budgets forbid
exactly that, and raising it to, say, 60 messages would make the 40,000/s row
report `not-growing` by definition rather than by measurement. The fix is to
narrow the bracket: sample outstanding work locally in the sender, where the
count is exact, instead of inferring it across an HTTP round trip. That is a
fixture change, not a threshold change, and it is listed as a remaining action.

**Sustainable capacity is therefore unmeasured above 2,000 messages/s, and the
approved SSNM 90% and churn 80% sustainable-capacity ratios are unmeasured.**
They are ratios against a sustainable capacity that this instrument cannot
establish, so there is no denominator to take 90% or 80% of. Nothing here should
be read as a capacity claim, and no capacity number was invented to fill the
gap.

## The stall detector against real data

`internal/perfstats` predeclares the transport-stall signal —
`send_duration.max_ns` — and a one-second threshold taken from RFC 9260
Section 16's `RTO.Min` and `RTO.Initial`. Until this campaign the rule had only
ever been applied to hand-written values. Two things were open: whether the
signal reaches the threshold on real data, and whether `DecideRun` actually
fires when it does.

**Nominal runs do not stall.** Across every clean run recorded here — offered
rates from 500 to 40,000 messages per second, one and eight associations, 128 to
4,096-octet payloads — the longest observed send call was between 0.40 ms and
42.4 ms. That is one to three orders of magnitude below the threshold. The
`perfstats` README records that its reference environment "intermittently stalls
the SCTP transport for about a second"; on two sibling containers over a bridge
veth, no nominal run did.

**It fires when a stall is real.** Adjudicated by `perfcapacity`, not asserted:

| Run | Longest send | Decision | Reason |
| --- | --- | --- | --- |
| Injected 3 s blackhole, 40,000/s | 4,027,899,960 ns | inconclusive | `transport-stall-detected` |
| Natural saturation, 1 association × 4,096 B × 20,000/s | 1,015,512,084 ns | inconclusive | `transport-stall-detected` |

The first of those had 304,332 missing deliveries and a `growing` backlog, and
was still reported `inconclusive` rather than `fail` — the documented evaluation
order putting a detected stall ahead of every other gate, working as written on
a real record. The stall is carried into the output either way, so it reaches
this report.

**The threshold is not merely derived correctly; it sits at the floor of the
phenomenon.** The open worry was a stalled repetition that measures, say,
900 ms and escapes the rule. That was tested directly by shortening the injected
blackhole:

| Blackhole | Longest send call | Fired |
| --- | --- | --- |
| 3 s | 4.028 s | yes |
| 0.7 s | 1.073 s | yes |
| 0.4 s | 1.066 s | yes |
| 0.15 s | 1.074 s | yes |
| 0.05 s | 1.063 s | yes |

A 50-millisecond blackhole still produces a 1.06-second send block, because
recovery is gated by the retransmission timeout, and `RTO.Min` is one second.
An SCTP path failure cannot block a send for much less than a second, which is
precisely the quantity the threshold was taken from. Every injected failure
landed just above the threshold rather than below it.

**The residual gap is a different failure mode.** What this shows is that
*path* disturbances cannot hide under the threshold. A disturbance that blocks
a send without an SCTP retransmission — a long garbage collection, CPU
starvation of the sender process, a hypervisor scheduling stall — could block
for a few hundred milliseconds, leave counted failures behind, and be decided on
those counters as a `fail` charged to the candidate. No such run occurred here,
and the rule names an SCTP stall specifically, so it is not misapplied. It is
stated so that the next campaign knows which failure mode the rule does not
cover rather than assuming it covers all of them.

## Gate results

Every gate below ran at commit `f46faa3ba5faa14fb0bcf4442d47892dfe71665f`, the
head that carries the fixture fix and this record. Because a gate result belongs
to the commit it was measured at, the results are recorded afterwards; every
commit after `f46faa3` on this branch changes this document only and adds no Go
code, so none of them can alter a Go gate. `go build ./...`, `go vet ./...`,
`go test ./... -count=1`, `gofmt -l`, `staticcheck ./...`, `golangci-lint run`
and `actionlint` were re-run on the later head and were green there too. Linux
gates ran in the prepared privileged container; host gates ran on darwin/arm64.

### Linux, container `i44-a`, Go 1.25.14 linux/arm64, prepared SCTP test network

| Gate | Command | Exit |
| --- | --- | --- |
| build | `go build ./...` | 0 |
| vet | `go vet ./...` | 0 |
| test | `go test ./... -count=1 -timeout=900s` | 0 — root package `ok` in 175.0 s |
| race | `go test ./... -count=1 -race -timeout=1800s` | 0 — root package `ok` in 452.5 s, zero `WARNING: DATA RACE` |
| GOMAXPROCS=1 | `GOMAXPROCS=1 go test ./... -count=1 -timeout=1800s` | 0 — root package `ok` in 370.8 s |
| bench | `go test ./... -run '^$' -bench . -benchmem -timeout=900s` | 0 |
| examples | `go build ./examples/...` | 0 |
| fuzz | `FUZZTIME=10000x FUZZMINIMIZETIME=1x FUZZ_PARALLEL=1 scripts/fuzz-smoke.sh` | 0 — 28 targets discovered and executed, 10,000 inputs each, no failing input |
| cross vet | `GOOS=linux GOARCH={386,arm,s390x,mips} CGO_ENABLED=0 go vet ./...` | 0, 0, 0, 0 |
| cross compile | `GOOS={darwin,freebsd,windows,linux} GOARCH={amd64,amd64,amd64,386} CGO_ENABLED=0 go test ./... -run '^$' -count=0 -exec=true` | 0, 0, 0, 0 |
| vulnerability | `govulncheck ./...` (v1.6.0, Go 1.25.14) | 0 — **No vulnerabilities found** |

### Host, darwin/arm64, Go 1.25.4

| Gate | Command | Exit |
| --- | --- | --- |
| format | `git ls-files -z '*.go' \| xargs -0 gofmt -l` | 0 unformatted files |
| whitespace | `git diff-tree --check --root --no-commit-id -r HEAD` | 0 |
| module tidiness | `go mod tidy` then `git diff --exit-code -- go.mod go.sum` | 0, 0 |
| vet | `go vet ./...` | 0 |
| staticcheck | `staticcheck ./...` (2026.2.1 / 0.8.1; CI pins v0.7.0) | 0 |
| golangci-lint | `golangci-lint run --timeout=10m ./...` (2.13.2; CI pins v2.12.2) | 0 |
| actionlint | `actionlint` | 0 |
| secret detection | `gitleaks detect --no-banner --redact --source .` | 0 |
| scoped gopls diagnostics | `gopls check ./internal/cmd/perftraffic/result.go ./internal/cmd/perftraffic/acceptance_input_test.go` | 0 — the two files this campaign changed |
| unsupported-host portability | `go test ./... -short -count=1 -timeout=900s` on darwin | 0 |
| vulnerability | `govulncheck ./...` | **3** — see below |

The host `govulncheck` exit is a property of the host toolchain, not of the
module. All 18 reachable findings are Go standard-library issues in **go1.25.4**,
each already fixed in a 1.25.x patch between 1.25.5 and 1.25.13, and every
reported call trace starts in `internal/cmd/perftraffic`,
`internal/cmd/perfcapacity` or `examples/sgp` — the fixtures and examples, not
the library packages. Run on the toolchain CI uses, Go 1.25.14, the same command
reports **no vulnerabilities at all**. Nothing here is a dependency finding:
`govulncheck` also lists 6 vulnerabilities in imported packages and 11 in
required modules, and reports that this code does not call any of them.

### Periodic security reporting is preserved

`.github/workflows/security.yml` keeps its weekly schedule
(`cron: "17 3 * * 1"`) and all four jobs — `dependency-review`,
`dependency-scanning`, `sast` and `secret-detection`. The only change to it
since the assessed baseline `d097e19` is three `github/codeql-action` version
bumps from `v4.37.6` to `v4.37.9`, landed by the independent dependency PR #9
that #44 places outside this work. `.github/workflows/go.yml` is byte-identical
to the baseline. This campaign redesigned no workflow and changed no schedule;
its two commits touch `internal/cmd/perftraffic` and `docs/` only.

### Unmeasured gates

Stated here rather than waived, as bullet 7 requires.

| Gate | Status | What it would need |
| --- | --- | --- |
| Independent-peer interoperability | **Not measured.** Both ends of the fixture are the same binary; the fixture reports `independent_peer: false` and names the mode unavailable. | A second, independently written M3UA implementation as the peer, in both initiation directions and both roles. tshark's dissector agreeing with our encoding (below) is an independent *decoder*, not an independent *peer*. |
| Sustainable capacity above 2,000 messages/s | **Not measured.** The backlog interval is wider than the instrument's one-message resolution at higher offered rates, so the decision is `indeterminate` regardless of the library. | A narrower sampling bracket — outstanding work counted locally in the sender rather than inferred over an HTTP round trip. Not a threshold change. |
| SSNM-storm load | **Not measured.** No fixture implements an SSNM workload. | An SSNM workload in the fixture. Note the fixture's `unsupported_modes` still says "requires future routing and state APIs"; those APIs shipped in #38 and #40, so the reason string is now stale even though the mode really is unimplemented. |
| Slow-consumer load | **Not measured.** No fixture implements a slow or stalled consumer. | A receiver that applies backpressure deliberately, and a definition of the pass condition under it. |
| Churn load and the 80% churn ratio | **Not measured.** No fixture implements association churn, and the ratio needs a sustainable capacity to be a ratio of. | Both of the above. |
| SSNM 90% sustainable-capacity ratio | **Not measured**, for the same reason. | A resolvable capacity number first. |
| Absolute throughput and latency as hardware figures | **Not claimed.** Measured here, but inside a Linux VM on macOS. | Bare-metal Linux with the same fixture. The ratio bounds above do not need it. |

### Wire evidence

A capture was taken on the ASP container's `eth0` during a clean two-association
run and decoded by tshark 4.4.18, an independent dissector:

```
class/type counts:  2000 × (1,1) DATA   128 × (0,1) NTFY
                       2 × (3,1) ASPUP   2 × (3,4) ASPUP Ack
                       2 × (4,1) ASPAC   2 × (4,3) ASPAC Ack
```

and one decoded DATA:

```
Message class: Transfer messages (1)      Message Type: Payload data (DATA) (1)
Network appearance (7)                    Routing context (1 context): 100
Protocol data (SS7 message of 128 bytes)
    OPC: 1114112   DPC: 2228224   SI: SCCP (3)   NI: International network (0)
    MP: 0          SLS: 0
```

The ASPUP → ASPUP Ack → ASPAC → ASPAC Ack → NTFY → DATA order is on the wire in
that order, per association, and the per-message tuple varies as the fixture
scheduled it (the next frame carries an ISUP service indicator with a different
point-code pair). A struct populated correctly and an octet transmitted
correctly are different claims; this is the second one.

## Remaining release actions

Separate from the evidence above, and none of them performed here. Nothing in
this campaign creates, moves or authorizes a tag.

1. **Independent-peer interoperability run.** The one acceptance bullet with no
   evidence at all. It needs a second implementation, not more runs of this one.
2. **Narrow the backlog sampling bracket** in `internal/cmd/perftraffic` so the
   capacity rule can resolve above 2,000 messages/s, then re-run the bounded
   search and its five validation repetitions. Do not touch
   `BacklogResolution`.
3. **Implement the missing load profiles** — SSNM storm, slow consumer, churn —
   and refresh the fixture's `unsupported_modes` reason string, which still
   attributes the gap to routing and state APIs that have since shipped.
4. **Close PR #65**, a superseded duplicate of the #64 fix that landed as
   PR #66.
5. **Re-run the standards checkpoint** immediately before any publication. It
   was current on the date of this campaign; that is not a permanent property.
6. **Publication itself remains unauthorized.** This record establishes
   readiness evidence for the gates it covers and states the gates it does not
   cover. It is not permission to tag or release.
