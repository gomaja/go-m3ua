# Matched-pair performance ratios

This package implements the approved fixed-sample comparison for 20 independent
matched run pairs. It computes each run-level log ratio from a finite quotient,
using `log1p` near one, and falls back to `log(candidate) - log(baseline)` only
when the quotient overflows or underflows. It then reports the geometric-mean
ratio and the two-sided 95% Student-t interval using
`t(0.975, 19) = 2.093024054408263`.

The mean interval follows the [NIST one-sample mean confidence interval](https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm), applied to paired run-level log ratios and transformed back to ratio units.

Zero baselines are not assigned artificial ratios. Twenty matched zero-cost
pairs pass only when every candidate is also zero; any positive candidate is a
failure. Mixing zero-baseline pairs with positive-baseline pairs is invalid
because it cannot produce the required 20 log ratios.

The CLI at `internal/cmd/perfratio` reads one strict JSON request from standard
input and writes one JSON result. Its exit statuses are 0 for pass, 1 for fail,
2 for inconclusive, and 3 for invalid input. Input is limited to 64 KiB and uses
the exact, case-sensitive top-level fields `direction`, `boundary`, and `pairs`.
Each of the exactly 20 pair objects uses only `id`, `baseline`, and `candidate`.

A passing result covers only this numerical comparison. It does not establish
that runs were independent, that log ratios met the interval's distributional
assumption, that measurements were environmentally valid, or that every
absolute performance and correctness gate passed.

## Predeclared sustained-backlog decision method

`backlog.go` fixes the sustained-backlog decision method **before** any
capacity campaign run, as required by the approved budgets. The rule is pure
interval arithmetic over the sender-window backlog-change interval
`[lower, upper]` (first versus last measurement-quarter means, computed by the
traffic fixture) and admits no numeric tolerance. Both comparisons are made
against `BacklogResolution`, the instrument's own least count, which is derived
below rather than chosen.

Validity is decided first and the two comparisons are applied only to what
survives it, so an unusable interval can never reach either of them:

- an interval is usable evidence only when neither bound is NaN, both bounds
  are finite, and `lower <= upper`. Missing evidence and any interval failing
  that test are `indeterminate`, and neither comparison below is applied. In
  particular `[1, +Inf]` is `indeterminate` because it is not finite, not
  `growing` on `lower > resolution`;
- a usable interval is `not-growing` iff `upper <= resolution`;
- otherwise it is `growing` iff `lower > resolution`;
- otherwise it is `indeterminate`.

### Why the comparison is against the instrument's resolution

Comparing `upper` against zero made the rule unfalsifiable in one direction. A
steady-state run's true mean backlog change is exactly zero, so its measured
interval brackets zero from both sides and `upper <= 0` can never hold. Under
that comparison `not-growing` is unreachable for precisely the runs the rule
exists to accept, and no capacity or throughput row can ever reach `pass`. That
is not a strict rule; it demands proof of a strict negative about a quantity
whose correct value is zero.

`BacklogResolution` is one message, and it is derived from the fixture's
counting, not from any campaign result. The fixture's `analyzeProgress` brackets
outstanding work as `[max(0, offered(before) - unique), offered(after) - unique]`
over `uint64` message counts; `offeredAt` is `floor(elapsed*rate/second) + 1`,
the same integer expression the dispatcher uses to decide what is due, so its
least count is exactly one message; and `describeBacklogChange` differences
means of those counts, which rescales the bounds but cannot manufacture a
distinction the counted quantity does not carry. The bounds describe a level
difference and not a rate: "the backlog grew" means at least one more message
is outstanding at the end of the window than at its start, and below that the
two levels are the same count. The fixture offers, transports
and validates whole messages and holds no sub-message state, so the smallest
backlog change it can tell apart from no change is one message. The fixture's
own coarse series diagnostic, `assessBacklog`, independently treats a
first-to-last-quarter mean difference of at most one outstanding message as
"not growing"; its additional `1.10` proportional allowance is exactly the kind
of percentage tolerance this rule forbids and is not adopted.

The value does not depend on the offered rate, the run duration, the sample
count or any observed interval. The full derivation is in `backlog.go` next to
the constant.

The sampling method's own uncertainty is deliberately not used as the
resolution. Each sample's bracket is as wide as the offered schedule advances
during one progress round trip, and `describeBacklogChange` already carries
that width into the interval: it is exactly `upper - lower`. Using it as the
threshold as well would make the rule self-referential, because
`upper <= upper - lower` is only `lower <= 0`, and would leave `indeterminate`
with nothing to cover. The window length and the number of samples in a quarter
scale the arithmetic but put no floor on how finely two message counts can
differ either. The threshold has to be a property of the instrument that does
not vary with the run, and the message granularity is the only such property
the fixture has.

One message is a small threshold, not a generous one. At the offered rates the
approved rows use, a progress round trip advances the offered schedule by more
than one message, so those intervals stay wider than the resolution and those
rows stay `indeterminate`. The change makes the rule falsifiable; it does not
make runs pass.

A capacity or throughput run may pass only with `not-growing`, no detected
transport stall, observed stall evidence, zero counted failures (missing,
duplicate, invalid, reordered, late-after-stop, capped, send errors, echo
deadline failures) and a fixture-valid run. A growing interval or any counted failure is a failure;
everything else is inconclusive. The counted failures are summed with
saturating addition, so counters large enough to wrap a `uint64` sum still fail
the run rather than presenting a zero total.

The rule is fixed in advance and must not be relaxed after observing results:
no repeat-until-pass, no percentage allowance for boundary uncertainty, and no
tuning of `resolution`. Raising `resolution` so that a growing or unresolved
row reports `not-growing`, and lowering it so that an inconvenient row reports
`growing`, are both the post-hoc threshold change the budgets forbid. The only
admissible reason to change it is a change in how the fixture counts, and that
change must be stated in the derivation. An interval that spans the resolution
stays inconclusive rather than being interpreted as stability.

## Predeclared transport-stall detection

`stall.go` fixes the transport-stall detection **before** any capacity campaign
run, on the same terms as the backlog rule: the signal and the threshold are
named in advance and neither may be changed to move a result.

The reference environment intermittently stalls the SCTP transport for about a
second. It reproduces on both go-sctp v1.0.2 and v1.0.4, so it is a property of
that environment and not a candidate regression. The response is not to drop
the affected rows, not to widen a threshold for them, and not to exclude them
as outliers. A run whose evidence a stall contaminated is reported
`inconclusive` with the stall named, and the stall is carried in the decision
so it reaches the report.

- **Signal**: `send_duration.max_ns` from the fixture's sender record. The
  fixture already records it: each `WritePDWithRoutingContext` call is timed
  and fed to the send-duration histogram, whose `Max` is the exact observed
  maximum rather than a power-of-two bucket bound (only p50, p95 and p99 are
  bucket bounds). It measures the transport blocking the sender directly. The
  offered schedule is open loop, so a transport block shows up first, and
  unambiguously, as a send call that does not return; the outstanding-cap
  refusals and missing deliveries that follow are its consequences. Dispatch
  lag and echo round-trip time also rise when the fixture is merely loaded, so
  no derived counter is used in its place.
- **Threshold**: one second, taken from SCTP's own retransmission floor.
  [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html) Section 16 recommends
  `RTO.Min` of 1 second and `RTO.Initial` of 1 second, so no SCTP
  retransmission timeout can expire in less than a second, and a single send
  call blocked that long spans at least one whole minimum retransmission
  timeout. Nothing in the fixture's send path accounts for it: the scheduler
  releases work in 100 microsecond quanta, Nagle is disabled, SACK delay is
  zero, and measured send calls run three to four orders of magnitude shorter.
  RFC 9260 is the current SCTP Proposed Standard; the RFC Editor record and the
  IETF Datatracker agree that no RFC obsoletes or updates it, and none of its
  errata touch Section 16.

`DecideRun` applies a fixed evaluation order:

1. a **detected** stall is `inconclusive`, ahead of every other gate, because
   the fixture offers its schedule open loop and everything measured through
   the block sits downstream of it. The cost of that order is stated rather
   than hidden: a stall the candidate itself caused is reported inconclusive
   too. It is never reported as a pass, and the stall always reaches the
   report, so a campaign of such runs certifies nothing;
2. fixture validity, then the loss counters: failures. A stall that was merely
   never observed cannot excuse demonstrated loss;
3. **missing** stall evidence is `inconclusive`: a run whose freedom from
   stalls was never observed is not credited with a sustained rate;
4. missing or invalid backlog evidence is `inconclusive`;
5. the predeclared interval rule above.

The threshold must not be tuned after observing results. Raising it so a
stalled row reports clean, and lowering it so an inconvenient row can be
dismissed as environmental, are both the post-hoc threshold change the budgets
forbid.

`perfstats.DecideRun` is the boundary at which this rule produces a result.
The traffic fixture at `internal/cmd/perftraffic` answers a different and
narrower question and deliberately stops short of it: its `fixture_verdict`
reports loss-free fixture validity only, so it stays `pass` alongside a
`growing` backlog diagnostic while leaving `capacity_verdict` unavailable and
the overall `verdict` inconclusive. A `growing` backlog becomes a `Fail` here,
in `DecideRun`, and nowhere earlier. The two are therefore not in conflict: a
fixture-valid run is an input to this rule, never a substitute for it, and no
fixture output may be read as a sustained-rate result.

A passing run decision covers only this rule. It does not establish that the
underlying fixture run was environmentally valid, that its latency or CPU
gates passed, or that any campaign-level repetition requirement was met.

## Bounded capacity search

`capacity.go` mirrors the predeclared bounded search of the local
`capacity.py` probe driver: integer message-per-second rates, a pass/fail
bracket refined to within five percent (`100*upper <= 105*lower`), downward
and upward halving/doubling between bounds, no probe retries after an
inconclusive outcome, and a fixed probe budget. Rates are bounded by
`MaximumSearchRate` so that the products those comparisons form stay exact;
a maximum above it is rejected by the constructor rather than allowed to wrap
a comparison and report a bracket that was never refined. Probes must be recorded in
execution order at exactly the selected rate. Only a refined bracket proceeds
to validation: exactly five full repetitions at the lower passing rate must
all pass, and that lower rate is the result. `lower-bound-only`,
`integer-resolution-limit` and `probe-budget-exhausted` are inconclusive,
never widened into a pass; `no-passing-rate` is a failure.

The CLI at `internal/cmd/perfcapacity` reads one strict JSON request with the
search parameters, per-run fixture sender records in execution order, and the
validation repetitions. Each run is decided by the predeclared backlog and
stall rules above; a missing or unbounded sender window, an insufficient-sample
backlog change or absent interval bounds is missing evidence and stays
inconclusive. Exit statuses are 0 pass, 1 fail, 2 inconclusive, 3 invalid
input.

Each run record must carry `send_duration.max_ns` and a `manifest`. Neither is
optional: a record without the send-duration maximum cannot show whether a
stall contaminated it, and a record without a manifest cannot say where it ran
or which baseline it was assessed against. A record missing either is invalid
input rather than a run decided on the fields that happen to be present.

The response states the environment the campaign ran in, in `environments`:
the toolchain, platform, `GOMAXPROCS`, go-sctp module and version, the
fixture's own `vcs_revision`, and `assessed_baseline_revision`, the baseline
commit required by issue #36. The last two are different commits and are never
interchangeable. Distinct environments are listed in first-appearance order
rather than merged, so a campaign whose runs did not all come from one
environment shows that instead of presenting one it cannot support. Each run
decision also reports its `stall`, so the measured longest send call reaches
the report whether or not it decided the run.

A pass covers only the search and repetition rules; it does not establish
environmental validity, latency or CPU budgets, or independent-peer behavior.
Stating the environment is a record of where the runs happened, not a
certification that the environment was fit.
