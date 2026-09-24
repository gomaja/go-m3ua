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

`backlog.go` fits the trend of the sender-window backlog over the measurement
window and compares the implied growth with a materiality floor.

The rule replaced a first-to-last-quarter mean comparison with zero tolerance
on 2026-09-23 ([#44](https://github.com/gomaja/go-m3ua/issues/44)). That
comparison cannot tell a stationary queue from a growing one: the difference
between two quarter means of a flat but noisy series is positive about half
the time. Replayed through it, 101 of 200 synthetic loss-free stationary runs
were classified as growing, and a recorded loss-free 120 s run at 25,000/s
(3,000,000 deliveries) measured +0.633 messages and failed.

Method:

- **Samples.** Observations taken strictly inside the window, each placed at
  the midpoint of its request bracket with backlog bounds `[lower, upper]`. At
  least eight are required.
- **Slope.** The least-squares slope of the per-second backlog. Because the
  slope is linear in the observations, its exact range over every backlog path
  inside the brackets is computed first. That range is then widened on each
  side by the one-sided 99% Newey–West standard error of the slope fitted to
  the bracket midpoints (Bartlett kernel, lag `floor(4*(n/100)^(2/9))`,
  `n/(n-2)` correction), so autocorrelated noise does not overstate precision.
- **Growth bounds.** The slope bounds multiplied by the window length, in
  messages.
- **Floor.** The traffic offered in 10 ms: `rate * 0.010`, which is 250
  messages at 25,000/s and 50 at 5,000/s.

Verdicts:

- `not-growing` when the upper growth bound is at or below the floor;
- `growing` when the lower growth bound exceeds the floor; and
- `indeterminate` otherwise, and for missing, non-finite or reversed bounds.

The deliberately under-served control (5,000/s offered, 4,998/s served, about
240 messages of growth over 120 s against a 50-message floor) remains
`growing`. The floor applies to growth over the observed window; it is not a
loss allowance. Nominal runs still require zero loss, and every numerical
budget is unchanged. The fit weights each observation by its leverage, so a
transient late spike is not what this rule detects; the latency, loss and
outstanding-cap gates cover that.

`perfcapacity` recomputes the trend from the sender window's raw observations
and rejects a reported status or bound those observations contradict. Missing
or incomplete trend evidence is inconclusive, never a pass.

A passing run additionally requires valid fixture evidence, no detected
transport stall and zero delivery/submission failures. Counter totals saturate
rather than overflow. A later drain cannot erase measured-window backlog
growth. This finite-run comparison covers the observed window only; it does
not prove indefinite queue stability or replace the remaining acceptance gates.

Historical campaigns evaluated under the quarter-mean or one-message rules must
be re-evaluated under this rule before claiming current acceptance.

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
  fixture already records it: each `WriteData` call is timed
  and fed to the send-duration histogram, whose `Max` is the exact observed
  maximum rather than a histogram bucket bound (only p50, p95 and p99 are
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
5. the predeclared trend rule above.

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

`capacity.go` implements the predeclared bounded search for the budget's
"highest sustained offered rate": integer message-per-second rates, a bracket
between the highest rate that demonstrated a sustained, loss-free,
not-growing run and the lowest rate that did not, refined to within five
percent (`100*upper <= 105*lower`), downward and upward halving/doubling
between bounds, no probe retries, and a fixed probe budget. Rates are bounded by
`MaximumSearchRate` so that the products those comparisons form stay exact;
a maximum above it is rejected by the constructor rather than allowed to wrap
a comparison and report a bracket that was never refined. Probes must be recorded in
execution order at exactly the selected rate. Only a refined bracket proceeds
to validation: exactly five full repetitions at the lower passing rate must
all pass, and that lower rate is the result. `lower-bound-only`,
`integer-resolution-limit` and `probe-budget-exhausted` are inconclusive,
never widened into a pass.

Each probe contributes one of four outcomes:

- `pass`: the run demonstrated the rate.
- `fail`: the run failed at the rate, for example with delivery or
  submission loss.
- `not-demonstrated`: the run was inconclusive only because of a detected
  transport stall or a backlog trend straddling the floor. Near and above
  capacity these are the observed outcomes. On the reference environment, with
  one association and 128-byte payloads, a 120,000 messages/s probe was
  loss-free but its backlog trend straddled the floor; probes at 140,000 and
  160,000 messages/s filled the outstanding cap within their first second and
  each blocked one send for about 1.03 seconds. The rate was not shown to be
  sustained, so it bounds the bracket from above exactly as a failure does and
  the search continues. Under the former rule, which ended the search at any
  inconclusive probe, none of these searches could converge.
- `inconclusive`: evidence was missing or invalid. It says nothing about the
  rate and ends the search.

`no-passing-rate` is a failure when every probe failed, and inconclusive when
any probe was only not demonstrated.

A probe whose warm-up could not sustain the rate never reaches measurement; its
failed warm-up cohort is accepted as that probe's evidence, and it can only fail
or be not demonstrated. It qualifies only when the warm-up offered its whole
schedule, no record carries a fatal read or control failure, the only error is
the fixture's own validity failure, and the sender shows outstanding-cap
refusals, missing deliveries, submitted work still undelivered when the drain
deadline passed (the sender record's `drain_timeout`), scheduled work the
sender itself could not submit by then (`sender_drain_timeout`), or a stall.
Work left undelivered at the drain deadline is how a probe above capacity
usually ends when the receiving library discards what it cannot queue: the
sender waits for those messages until the deadline. The fixture records both
outcomes as delivery failures, not fixture faults, and the CLI requires each to
reconcile with its record (its cause and drain; submitted and accounted counts
no higher than the receiver's final counters; outstanding work within the run's
limit and cut-off sends that account for the send errors) before counting it.
A bidirectional warm-up is evidence when either direction shows this and no
record in either direction carries a fault. In either phase a cohort one of
whose records carries a fatal error, or whose error names anything but its
directions' validity failures, is refused as invalid input rather than
decided: a fixture fault says nothing about the rate. Loss counts alone are not enough: an
abort for another reason, such as a failed control request or a receiver read
failure, also strands messages but says nothing about the rate, and is rejected
as invalid input. The same failed warm-up in a validation repetition is a failed
repetition, never a pass. Campaign identity compares a warm-up cohort with the
measurement workload in every field except its duration; the fixture runs its
warm-up with the measurement's drain, outstanding limit, payload and
instrumentation, and a warm-up that differed in any of them would be rejected.

The CLI at `internal/cmd/perfcapacity` reads one strict JSON request with the
search parameters, per-run fixture sender records in execution order, and the
validation repetitions. Each run is decided by the predeclared backlog and
stall rules above; a missing or unbounded sender window, or a missing,
incomplete or insufficient-sample backlog trend, is missing evidence and stays
inconclusive. Exit statuses are 0 pass, 1 fail, 2 inconclusive, 3 invalid
input.

While the search is still running, the response carries `next_probe_rate`,
the rate the search selected for its next probe. A campaign driver runs one
probe at that rate, appends it and asks again, so the probe order is always
the search's own; once the search terminates the field is absent and
`selected_rate` names the rate for the five validation repetitions.

Each run record must carry `send_duration.max_ns` and a `manifest`. Neither is
optional: a record without the send-duration maximum cannot show whether a
stall contaminated it, and a record without a manifest cannot say where it ran
or which baseline it was assessed against. A record missing either is invalid
input rather than a run decided on the fields that happen to be present.

The response states the environment the campaign ran in, in `environments`:
the toolchain, platform, `GOMAXPROCS`, go-sctp module and version, the
fixture's own `vcs_revision`, and `assessed_baseline_revision`, the baseline
commit required by issue #36. The last two are different commits and are never
interchangeable. Every probe and repetition must name the same clean candidate
revision, assessed baseline, workload and fixture-provided environment.
Missing fields, dirty builds and mixed identities are invalid input rather
than a capacity result. Each run decision also reports its `stall`, so the
measured longest send call reaches the report whether or not it decided the run.

The measured `spec.rate` must match the outer declared rate, and `spec.expected`
must match rate multiplied by duration using the fixture's integer arithmetic.
The top-level `measurement_duration_ns` and any `sender_window.duration_ns`
must match `spec.duration_ns`. Sender records must include one
`negotiated_outbound_streams` entry per declared association, so a different
measured connection count cannot be relabeled as the campaign topology.
The record-level `expected` must match `spec.expected`, and sender scheduling
and submission totals must reconcile. Delivery unique plus missing must equal
expected for every completed record, including failed probes; an invalid record
with a fatal error may lack complete receiver totals. The measurement and drain
unique-delivery counts must sum to the
reported unique total without overflow. Fatal errors, outstanding work and all
fixture-invalid counters must agree with `fixture_verdict`; legitimate invalid
probe records remain accepted evidence of a failed run, while a claimed pass
with those failures is invalid input. Duplicate JSON member names are rejected
using the same case-insensitive matching that `encoding/json` uses for struct
fields. Duplicated outstanding limits and initiation fields must agree between
the specification and manifest. Payload must be one of the producer workloads:
`128`, `512`, `4096` or `mix`. Echo-mode sender records must include their
fixed round-trip scope and two-second deadline, outstanding limit, requests,
validated, capped, deadline, invalid and post-drain outstanding counters; the
echo outstanding limit must equal `spec.outstanding`, and requests must equal
sender submissions. Direction must be `asp-to-sgp` or `sgp-to-asp`; initiation
must be `asp-dial` or `sgp-dial`. Non-echo sender records must not carry echo
evidence.

Standalone sender records remain supported only for unaligned throughput and
echo runs. A sender with `spec.shared_clock` requires a complete cohort.
Shared-clock unidirectional throughput uses the fixture's actual `measurement`
cohort object as `run`, containing exactly `sender` and `receiver` records. Its
phase must be `measurement`, its sender must use the producer's throughput
ASP-to-SGP specification, and both records must agree on the complete run
specification, delivery counters, verified clock domain and window. Raw
`reverse_sender` or `reverse_receiver` members are forbidden even when their
value is `null`; SGP-to-ASP sender records are produced only by the
bidirectional reverse driver and require the complete four-record contract.

The recorded `spec.peer_control` comes from the producer's optional
`-control-url`, not its required `-peer-control` destination. It may be omitted
for unidirectional throughput; bidirectional forward records need it so the
reverse driver can reach the ASP. A nonempty value must be a valid control URL.

A valid two-record decision reports one `directions` entry for `asp-to-sgp`.
Bounded achieved-rate evidence is reported on that entry; producer-shaped
unavailable accounting remains inconclusive with omitted bounds rather than an
invented zero. Unidirectional entries do not report aggregate offered or
achieved-rate fields. A detected transport stall takes precedence as
inconclusive. Otherwise a reconciled cohort error fails the probe while the
single directional decision remains available as diagnostic evidence.
Completed loss is retained as a failed probe, while missing records,
contradictory verdicts or reverse members are invalid input. This consumer
contract still depends on the aligned producer from issue #84, and actual
producer-to-consumer Linux round-trip evidence remains a merge gate for
[issue #97](https://github.com/gomaja/go-m3ua/issues/97).

Bidirectional entries use the fixture's actual `measurement` cohort object as
`run`, not the outer combined result and not a forward sender alone. The cohort
must carry `sender`, `receiver`, `reverse_sender`, and `reverse_receiver`. The
forward pair uses bidirectional ASP-to-SGP specifications; the reverse pair uses
the producer's throughput SGP-to-ASP specification and `-reverse` cohort suffix.
Both sender manifests must identify the same clean environment and revision.

All four records must carry one exact, verified same-host `CLOCK_MONOTONIC`
domain and measurement window. Each sender must also carry the producer's
bounded one-millisecond watchdog proof for translating that window's drain
deadline. Sender-window delivery bounds must match the corresponding receiver
boundary counters, contain the receiver's measurement-delivery count, and, for
a bounded window, come from a resolution-safe capture after the window ended.
Each direction is decided separately from both its sender and receiver validity;
both must pass, so receiver failures or backlog shrinkage in one direction
cannot be hidden by the other. Completed loss remains a failed probe, while a
missing reverse record or contradictory cohort verdict is invalid input.
Per-direction achieved-rate bounds exclude drain deliveries. A bounded zero is
reported explicitly as measured zero. When the producer reports its
`inconclusive` unavailable-window accounting instead, that direction's bounds
are omitted because zero-valued unavailable accounting does not prove zero
throughput. Aggregate achieved-rate bounds are reported only when both
directions have bounded evidence; the overflow-checked aggregate offered rate
remains separate and is still reported. A detected transport stall in either
direction makes the cohort decision inconclusive ahead of cohort or directional
failures, while the per-direction decisions retain those failures as diagnostic
evidence. The outer rate and selected rate remain per-direction offered rates.
Neither a capacity result nor those observations replace a separate absolute
achieved-throughput target.

Cohort and seed may vary between independent runs. Offered rate may vary during
the search; associations, duration, drain, outstanding limit, payload, mode,
direction, initiation and peer-control endpoint must otherwise stay fixed.
The manifest must also preserve socket options, flow count, outstanding limit,
initiation and accounting scope. Its required strings must be nonempty. Shared
clock and HTTP-progress instrumentation are distinct campaign identities and
must not be mixed.

A pass covers only the search and repetition rules; it does not establish
environmental validity, latency or CPU budgets, or independent-peer behavior.
Stating the environment is a record of where the runs happened, not a
certification that the environment was fit.
