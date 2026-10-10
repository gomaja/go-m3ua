# Paired performance ratio CLI

Build with `go build -o perfratio ./internal/cmd/perfratio`. The tool reads one
strict JSON object on stdin and writes one JSON result on stdout. Exit statuses
are 0 (pass), 1 (fail), 2 (inconclusive), and 3 (invalid input).

The top-level fields are `direction` (`upper` or `lower`), positive finite
`boundary`, `pairs`, and optional `pair_count`. The count must be a JSON integer
**5 or 20**. When omitted it defaults to 20, even if the array contains 5 pairs;
the count is never inferred from the array. Null, zero, unsupported counts,
non-integer values, duplicate keys, unknown fields and case aliases are invalid.
Input is limited to 64 KiB. Each of exactly the declared number of matched pairs
must have a unique nonempty `id` and finite nonnegative `baseline` and `candidate`;
positive baselines require positive candidates.

For example, a capacity row can request five matched pairs:

```json
{
  "direction": "lower",
  "boundary": 0.95,
  "pair_count": 5,
  "pairs": [
    {"id": "1", "baseline": 100, "candidate": 100},
    {"id": "2", "baseline": 100, "candidate": 100},
    {"id": "3", "baseline": 100, "candidate": 100},
    {"id": "4", "baseline": 100, "candidate": 100},
    {"id": "5", "baseline": 100, "candidate": 100}
  ]
}
```

This passes with a geometric-mean ratio of 1, lower and upper ratios of 1,
`pair_count: 5`, and `t_critical_975: 2.7764451051977943`. Twenty-pair results
report `t_critical_975: 2.093024054408263`; their existing numerical fields and
decisions are unchanged. This new output field is additive; recorded outputs
are not rewritten.

For positive pairs the method is the geometric mean of candidate/baseline
run ratios, with a two-sided 95% Student-t interval on their mean log ratio
using `n-1` degrees of freedom, then exponentiated. See the
[method, critical-value derivation and width example](../../perfstats/README.md).
An upper gate passes when the upper interval bound is at or below the boundary
and fails when the lower bound is above it. A lower gate passes when the lower
bound is at or above the boundary and fails when the upper bound is below it.
Otherwise the result is inconclusive. Baseline spread above 10% of the median
also makes the comparison inconclusive.

For either count, all-zero baselines pass only when all candidates are zero.
A positive candidate paired with any zero baseline fails regardless of gate
direction. Mixed zero/positive baselines without such a regression are invalid.
Zero-cost results report undefined ratios and the selected critical value as
metadata, with no Student-t calculation.

## Campaign driver revision required (not implemented here)

The owner decision of 2026-10-10 predeclares **5 matched pairs per capacity
row**, using warm-started searches; fixed70 and echo70 retain **20**. Tracking:
[issue #44](https://github.com/gomaja/go-m3ua/issues/44).

Plan-revision comment URL: **PLACEHOLDER — insert the owner's issue #44 comment URL.**

The local `local/claude-2026-09-23/campaign/plan.py` and `plan_b.py` need these
changes before generating the revised acceptance plan:

1. Keep `PAIRS = 20` for fixed70/echo70. Introduce `CAPACITY_PAIRS = 5` and
   persist the policy in generated plan metadata, for example a
   `pair_counts` map with `capacity: 5`, `fixed70: 20`, `echo70: 20`.
   Analysis must use the plan's recorded policy; older plans without this
   metadata retain 20 for all ratio roles. Never reinterpret an old 20-pair
   plan as a 5-pair plan or silently truncate its observations.
2. In `plan.py.generate`, choose the count by `kind`. Generate 5 capacity
   pairs (10 searches) and 20 pairs for each fixed70/echo70 row (40 runs).
   The order list currently uses `PAIRS // 2` for both sides: use
   `count // 2` and `count - count // 2`, giving 2/3 orders for 5, shuffled
   with the seeded plan RNG. Otherwise changing 20 to 5 silently generates
   only 4 pairs. Preserve matched pair IDs, run order and search evidence.
3. In `plan.py.analyze`, use the recorded kind-specific count in pair-ID
   iteration, completeness checks and report metadata. Extend
   `perfratio(pairs, direction, boundary, pair_count)` to pass `pair_count`
   in the JSON request. Pass 5 for capacity and 20 for every fixed70/echo70
   metric, including per-delivery allocation ratios. Keep the current
   lower-capacity gate at 0.95 and upper fixed/echo metric gates at 1.10.
   Keep fixed70 allocation completeness and allocation-report iteration at
   20; the five absolute repetitions are a separate policy.
4. In `plan_b.py.build_tasks`, use 5 seeds in all four capacity seed lists:
   `f1-router-capacity`, `ssnm-steady-capacity`, `ssnm-large-capacity`, and
   `f6-route-references-capacity`. Its `paired_tasks` already splits orders
   with `count - count // 2`, so 5 produces 2/3. Record the capacity policy,
   and revise capacity gate descriptions/docstrings to say 5, preserving
   each row's existing ratio boundary and absolute target. Keep independent
   fixed repetitions and alloc70's five-pair difference method unchanged.
5. Extend `plan_b.py.perfratio(binary, values, direction, boundary,
   pair_count)` to include the count in JSON. `analyze_capacity` must check
   completeness against the plan's acceptance capacity count and pass it
   to the tool, replacing `len(values) == PAIRS`. The `f1-router` absolute
   capacity check needs 5 completed routed searches for acceptance.
   Preserve the smoke requirement of 1 search per side as diagnostics:
   smoke's one pair never invokes perfratio as an acceptance comparison.
   Update smoke messages to report the acceptance requirement of 5.
6. In `plan_b.py.part_a_tasks`, choose the count by kind instead of
   `range(PAIRS * 2)` so runtime estimates include 10 capacity searches
   and 40 fixed70/echo70 runs per applicable row. Generated Part B estimates
   must likewise reflect the actual 5-pair capacity tasks.
7. Rebuild and repin perfratio's path and SHA-256, record the revised runner
   identities and policy, and generate a new plan. Preserve warm-start
   hints as search hints only: each capacity must still come from a passing
   search with fresh probes and the existing final validation repetitions.
   Missing or failed capacity searches remain incomplete/inconclusive and
   their diagnostic `selected_rate` must not become a ratio observation.

Neither driver is changed by this patch. The count reduction does not change
the geometric-mean estimator, confidence level, zero-baseline rule or interval
gate. It increases uncertainty: the controlled five-versus-twenty example in
the method README has **2.9074631999781765 times** the ratio-unit interval width.
Passing this numerical comparison does not establish independent runs or
validate the rest of the acceptance campaign.
