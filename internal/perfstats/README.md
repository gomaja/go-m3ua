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
traffic fixture) and admits no numeric tolerance.

Validity is decided first and the two comparisons are applied only to what
survives it, so an unusable interval can never reach either of them:

- an interval is usable evidence only when neither bound is NaN, both bounds
  are finite, and `lower <= upper`. Missing evidence and any interval failing
  that test are `indeterminate`, and neither comparison below is applied. In
  particular `[1, +Inf]` is `indeterminate` because it is not finite, not
  `growing` on `lower > 0`;
- a usable interval is `not-growing` iff `upper <= 0`;
- otherwise it is `growing` iff `lower > 0`;
- otherwise it is `indeterminate`.

A capacity or throughput run may pass only with `not-growing`, zero counted
failures (missing, duplicate, invalid, reordered, late-after-stop, capped,
send errors, echo deadline failures) and a fixture-valid run. A growing
interval or any counted failure is a failure; everything else is
inconclusive. The counted failures are summed with saturating addition, so
counters large enough to wrap a `uint64` sum still fail the run rather than
presenting a zero total. The rule is fixed in advance and must not be relaxed
after observing results: no repeat-until-pass, no percentage allowance for
boundary uncertainty. An interval that straddles zero stays inconclusive
rather than being interpreted as stability.

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
validation repetitions. Each run is decided by the predeclared backlog rule
above; a missing or unbounded sender window, an insufficient-sample backlog
change or absent interval bounds is missing evidence and stays inconclusive.
Exit statuses are 0 pass, 1 fail, 2 inconclusive, 3 invalid input. A pass
covers only the search and repetition rules; it does not establish
environmental validity, latency or CPU budgets, or independent-peer behavior.
