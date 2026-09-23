# Direct M3UA traffic baseline

`perftraffic` is a two-process Linux SCTP fixture for receiver-validated DATA
throughput. The direct modes measure the association API without Endpoint
routing: the ASP Endpoint is constructed with a nil `ASP` configuration and
each DATA call uses a newly constructed Protocol Data parameter plus
`WriteData`; the SGP validates messages returned by `ReadData`.

The fixture implements five modes: one-way `throughput` (the default), `echo`
for round-trip latency, `bidirectional` for simultaneous two-way DATA, and the
optional-router pair `routed` and `routed-direct` described under
[Routed modes](#routed-modes). Throughput mode also runs the DATA overload
trial described under [DATA overload mode](#data-overload-mode). The direct modes support both SCTP initiation
directions: ASP-dial to SGP-listen (the default) and SGP-dial to ASP-listen.
Throughput runs can add the opt-in SSNM load workload described under
[SSNM load, overflow and resynchronization](#ssnm-load-overflow-and-resynchronization).
Workloads a run does not exercise (SSNM load outside that workload, the router
outside the routed modes, reference churn and independent peers) are reported
as unavailable; they are never emitted as zero-valued successful measurements.
Both processes run this binary, so `fixture_verdict: pass` establishes
loss-free fixture validity only, not independent-peer, sustainable-capacity,
or candidate acceptance.

## Initiation direction

`-role` selects the M3UA endpoint role and `-transport` the SCTP initiation
side; the traffic roles are unchanged (the ASP always drives the offered
load, the SGP always hosts the control endpoint). For SGP-dial runs the SGP
passes `-transport=dial` with `-sctp-address` naming the ASP, and the ASP
passes `-transport=listen` with `-sctp-address` as its listen address. The
run specification and manifest record the direction as `initiation`:
`asp-dial` or `sgp-dial`, following RFC 4666 Section 1.4.8's separation of
SCTP initiation from the client/server role. Performance role repetitions
under both initiation directions use the same mixed-payload row; the fixture
reports them as separate runs and never merges their results.

Startup is readiness-gated in both directions: an association serves traffic,
readiness reporting, or `ReadData` only after it reaches AS-ACTIVE (the
library's Accept/Dial wait for that policy-selected readiness state; the
SGP-dial side additionally verifies it explicitly and fails with
`association did not reach AS-ACTIVE before serving traffic` otherwise). A
refused SCTP dial while the ASP listener is still coming up is retried for a
bounded 30-second window and then fails with the named
`peer did not accept SCTP dials within the retry window` error rather than a
generic fatal. The ASP listener stays open for the whole run: closing it
closes every association it accepted. The accept wait is bounded externally
and the run's root context is passed to Accept untouched, because the library
runs every accepted association's monitor on the Accept context for the
association's whole lifetime — cancelling a derived context, or letting a
derived timeout fire, tears every accepted association down.

Startup failures are phase-labelled in the record's `fatal_error`
(`startup control-bind`, `startup endpoint`, `startup listen`,
`startup dial`) and read failures name the receiver phase (`idle`, `armed`,
`measuring`, `stopped`). Because one teardown fails every association's read
loop at once and the record keeps only the first reported error, each
read-loop failure also emits one JSON line to stderr
(`startup_diagnostic: "read-fatal"` with the association index and receiver
phase), preserving the failure order that separates a first cause from a
teardown cascade. The receiver's cgroup `cpu` observation on a process that
died before any cohort is expected to report missing counters; it is not a
startup abort cause.

## Bidirectional mode

`-mode=bidirectional` drives `-rate` messages per second in each direction
simultaneously over the same associations, so the aggregate offered load is
twice the flag value (the approved 40,000/s aggregate row uses
`-associations=8 -rate=20000`). The ASP drives the forward cohort exactly as
in throughput mode and additionally serves its own control endpoint
(`-control-address`, advertised to the peer with `-control-url`). On each
cohort start the SGP drives the reverse cohort against that endpoint with the
identical sender-side measurement path, the cohort name suffixed `-reverse`,
and reversed point codes; both sides validate everything they receive.

The cohort result carries per-direction records: `sender`/`receiver` cover
ASP-to-SGP and `reverse_sender`/`reverse_receiver` cover SGP-to-ASP, each
with its own counters, series, sender-window bounds and backlog interval. The
cohort passes only when all four records are loss-free and fixture-valid.
In the default HTTP-interval mode, each direction's measurement window is anchored by its own driving side; the
two windows start within one control round-trip of each other and are not
claimed to be identical. With `-same-host-clock`, both directions instead use
the same declared future start and end. The reverse sender record's CPU and allocation
observations cover the same whole process as the SGP's forward receiver
record, not a separate allowance.

## Echo mode

`-mode=echo` measures scheduled-request to validated-echo-reply round-trip
time on the sender's own monotonic clock. Arrivals are open-loop on the same
100 microsecond scheduler as throughput mode, and waiting from the scheduled
dispatch time is included in every RTT, so overload shows up as latency
rather than being hidden. Outstanding requests are capped by `-outstanding`
(at most 8,192) and each request carries a fixed two-second deadline; cap
refusals and deadline expirations are counted failures, never omitted from
the report and never credited to the RTT percentiles.

Echo requests are ordinary validated DATA deliveries and appear in the
receiver's offered-load counters. Echo replies are built from the request
identity with reversed point codes and the same deterministic size; they are
RTT evidence only and are never counted as useful deliveries on either side.
A reply that arrives after its request was swept, matches no cohort, or fails
deterministic validation is counted invalid. `sender.echo.rtt` reports
p50/p95/p99/max over validated replies only. RTT is never one-way latency and
is never divided by two. A passing echo run requires every scheduled request
to be delivered and answered loss-free within the deadline; the percentiles
are measurements, not proof that the approved latency budgets were met.

The receiver never writes replies from its read loop. Each association gets a
dedicated reply writer fed by a bounded queue (the outstanding bound shared
across associations, so at most 8,192 replies are queued in total); the read
loop only enqueues the validated identity and size, and the writer builds the
payload and performs the SCTP write with one write deadline per cohort
generation. A full queue drops the job and counts `replies_dropped`; write
failures count `reply_errors`; both fail fixture validity. This keeps
reply-write backpressure from stalling request validation, the control HTTP
endpoint or drain: a wedged peer costs one blocked writer goroutine per
association, and the dropped replies die on their two-second deadline at the
sender as counted `deadline_exceeded` failures. The sender's outstanding map
is likewise bounded by the cap and emptied by the deadline sweep even when
the peer never answers, so neither side amplifies a wedge.

Throughput mode does not use echo traffic.

## SSNM load, overflow and resynchronization

`-ssnm-rate` adds an SSNM disturbance to a shared-clock throughput run
(performance budgets section 4: SSNM steady updates, large SSNM updates,
indication overflow and resynchronization). It is off by default; without it
flags, specifications, control routes and records are unchanged. SSNM load
requires `-mode=throughput` and `-same-host-clock` on both processes, because
its schedule, receipts and delays are shared-clock timestamps.

| Flag | Process | Meaning |
| --- | --- | --- |
| `-ssnm-rate` | both | generated DUNA/DAVA messages per second (1 to 10,000) |
| `-ssnm-apcs` | both | Affected Point Codes per message, 1 to 1,024 (default 1) |
| `-ssnm-records` | both | distinct destinations cycled, at most 16,384 and a multiple of `-ssnm-apcs` (default 16,384) |
| `-subscribers` | ASP | `SubscribeSSNM` consumers on the ASP Endpoint (default 8, at most 16) |
| `-pause-subscriber=<offset>/<duration>` | ASP | F3: subscriber 0 stops reading `offset` into the measurement window for `duration`, then recovers by `Resync` |
| `-ssnm-apply-p99-budget` | ASP | p99 apply-time budget of 1,024-APC messages (default 100ms) |
| `-ssnm-resync-budget` | ASP | F3 `Resync` snapshot and subscription acquisition budget (default 100ms) |
| `-ssnm-recovery-budget` | ASP | F3 budget to consume the retained queued indications and the snapshot (default 1s) |

Both processes must pass the same `-ssnm-rate`, `-ssnm-apcs` and
`-ssnm-records`; the ASP declares them in every cohort specification (`spec.ssnm`)
and the SGP refuses a cohort that differs from its own flags. The budget flags
default to the section 4 contract values and are recorded in the ASP
manifest's `ssnm_budgets`.

**Generator (SGP).** Before any cohort the ASP opens its subscriptions and asks
the SGP (`POST /ssnm/preload`) to report every destination Unavailable once,
1,024 per message, then waits until every subscriber has consumed the preload
and the ASP store holds `associations x records` records: DATA always starts
against a full store. The generator then calls
`Endpoint.ReportDestinationAvailability` open-loop at `-ssnm-rate`, anchored at
the first cohort's shared start and running through warm-up, the gap between
cohorts and the measurement window until the measurement end. Message `m` is
scheduled at `anchor + floor(m * 1s / rate)`; a late generator catches up in
order and never skips, and stops issuing at the measurement end plus drain.
Each message names destinations `0x400000 + d`, a contiguous run of
`-ssnm-apcs` explicit point codes (mask 0) cycling through `-ssnm-records`,
alternately Unavailable and Available on successive passes, in Routing Context
100 with Network Appearance 7. DATA uses DPCs `0x220000`-`0x221f1f`, so the
disturbance never names a DATA destination and cannot make DATA ineligible.
The SGP keeps each message's report start and completion (`GET /ssnm/reports`)
and fan-out failures (`SSNMDeliveryError`).

**Subscribers (ASP).** The ASP Endpoint keeps its nil `ASP` configuration and
adds an `SSNMState` sized for the workload: a standalone ASP Association is one
partition and its own retention peer, and every association serves Routing
Context 100, so each partition holds `-ssnm-records` records, the store
`associations x records`, subscription queues keep the approved 256 events, and
`MaxAffectedPointCodes` is 1,024. The chosen limits are recorded in
`sender.ssnm.store.limits`. For the 16,384-retained-record rows use
`-ssnm-records=16384` with one association or `-ssnm-records=2048` with eight.
Each subscriber checks every delivered report against the deterministic plan:
per partition every position exactly once and in order, counting gaps,
duplicates, unexpected content, continuity loss, resource loss and
invalidation, and records its shared-clock receipt time of each measurement
message.

**F3 pause and recovery.** The paused subscriber stops calling `Next` at the
offset, sleeps for the duration, then drains what its queue retained, observes
`SSNMContinuityLostEvent`, calls `Resync`, consumes the snapshot and continues.
`sender.ssnm.pause` records the events retained at loss against the count cap
(`count_cap_enforced`), the partition states those events carried, the drain
time, the `Resync` acquisition time (`resync_ns`), the snapshot consumption
time and `recovery_ns` from resumption to a consumed snapshot after the
retained queue. The snapshot is validated destination by
destination against the plan at the first report after `Resync`, so a stale or
partial snapshot is a failure. A retained-byte cap is reported as not
observable: this library's `SSNMStateConfig` bounds a subscription by event
count only.

**Records.** The SGP record's `ssnm.generator` covers the cohort window:
`offered` (scheduled in the window), `reported_in_window`, `late`, `unsent`,
`failed`, `fanout_failures`, offered and actual rates, dispatch lag and report
call duration. The ASP measurement record's `ssnm` carries the workload, the
store limits and end counters, the preload, every subscriber's accounting, the
final generator view recomputed from the complete per-message log, `delay`
(report start to healthy-subscriber receipt, p50/p95/p99/max; an upper bound on
apply-and-publish time), `pause`, `budgets`, and `verdict`: `fail` for any
healthy indication loss, store refusal, generator failure, F3 contract
violation, exceeded time budget, final store that does not hold exactly the
plan's state, or a binding or partition lifecycle event reaching a subscriber;
`inconclusive` when the generator did not hold the intensity (a window message
unsent or failed, or a report starting more than `dispatch_tolerance_ns`, one
scheduling interval and at least 100 ms, after its schedule) or a gated budget
could not be measured; otherwise `pass`.

**Time budgets.** `budgets` lists each section 4 budget with its value,
measurement, whether it gates this run, and its outcome (`within`, `exceeded`,
`not measured`, or `recorded` where no budget applies). With `-ssnm-apcs=1024`
the report-to-receipt p99 gates the large row's "p99 apply time within 100
ms": that delay runs from the SGP's report call to the subscriber's receipt, so
it bounds apply time from above, and a p99 over the budget fails the run as a
budget not demonstrated. Other APC counts, the one-APC steady row included,
have no apply-time budget in section 4 and record the delay only. With
`-pause-subscriber` the F3 `resync_ns` gates "snapshot/subscription acquisition
within 100 ms" and `recovery_ns` gates "consume retained snapshot and 256
queued indications within 1 s".

**Final store.** After every subscriber has consumed every reported message,
the ASP reads `SSNMKnowledge` once more and requires `associations x records`
records holding exactly the plan's state after the last reported position,
destination by destination, the same check a Resync snapshot gets
(`store.state_validated`, `store.state_mismatches`).
`association_errors` names any association that ended during the run with the
library's close cause, which is also written to stderr as an
`ssnm_diagnostic` line, together with the subscribers' progress when they close.

The SGP sets no write deadline on its associations. Destination state
publications are writes the library makes on its own behalf, so when the ASP
falls behind and the SCTP send buffer fills, each one waits for space for up to
`AssociationConfig.ControlWriteTimeout`, which the fixture leaves at the library
default (`DefaultControlWriteTimeout`, 5 s): a slow ASP shows up as generator
lag and report duration. Only a wait longer than that closes the association
with `ErrControlWriteTimeout`, which `association_errors` then names. A socket
write deadline would replace that bound with its own and close the association
on any library write once it passed. Existing fields keep their meaning: the
DATA verdicts do not include SSNM, and `sender.ssnm.verdict` is the SSNM
result.

**Capacity comparison.** `perfcapacity` reads `spec.ssnm` into the workload
identity, so a campaign cannot mix SSNM-loaded probes with no-update probes or
with a different SSNM intensity. An SSNM-loaded probe needs `sender.ssnm` and
`receiver.ssnm.generator`; an SSNM `fail` fails the probe and an SSNM
`inconclusive` turns a passing probe inconclusive. A warm-up that failed from
overload is probe evidence here as in any throughput campaign: its
`spec.ssnm.phase` is `warmup`, its SGP record carries `ssnm.generator`, and
its ASP record carries no `ssnm` result, since the SSNM verdict belongs to the
measurement cohort; it keeps the SSNM workload identity. The matched no-update
control is the same pair of commands without the SSNM flags on either process:

```sh
# SSNM-loaded campaign probe (steady row; large row: -ssnm-rate=10 -ssnm-apcs=1024)
perftraffic -role=sgp ... -same-host-clock -ssnm-rate=1000 -ssnm-apcs=1 -ssnm-records=16384
perftraffic -role=asp ... -same-host-clock -ssnm-rate=1000 -ssnm-apcs=1 -ssnm-records=16384 -rate=<probe>
# Matched no-update control probe: identical except the SSNM flags
perftraffic -role=sgp ... -same-host-clock
perftraffic -role=asp ... -same-host-clock -rate=<probe>
```

Run each campaign through `perfcapacity` separately and compare the selected
rates: the steady row needs at least 90% and the large row at least 80% of the
control's capacity. The SSNM verdict each probe folds in includes the time
budgets, and `perfcapacity` adds the manifest's `ssnm_budgets` to the workload
identity, so one campaign cannot mix verdicts judged against different
budgets. Bidirectional and legacy single-record evidence carrying any SSNM
evidence is refused.

## Routed modes

`-mode=routed` and `-mode=routed-direct` are the optional-router row of the
approved performance budgets (section 2): 1,000 routes over 8 associations with
the mixed payload, and the matched direct-send control it is compared with.
Both run the same fixed topology, traffic, scopes, queues and resolved paths;
only the timed send call differs.

- **Topology.** One ASP Endpoint is configured with two SGs (`sg-a`, `sg-b`),
  two SGPs per SG (`p0`, `p1`) and two associations per SGP: eight
  associations in total. Each SGP serves two AS scopes (`primary` and
  `secondary`, Network Appearance 7, one Routing Context each) and each SG path
  lists them in that deterministic preference order. 1,000 exact-DPC routes
  (`0x220000` to `0x2203e7`) use both SG paths with load sharing. The SGP
  process hosts the four peer SGP Endpoints, one per SGP, each accepting two
  associations.
- **Preparation, before any cohort.** After all eight associations are
  ASP-Active in both scopes, each peer SGP reports every destination Available
  in each AS scope with DAVA (RFC 4666 Section 3.4.2). The ASP verifies every
  resulting SSNM report and the final knowledge, so destinations are
  explicitly Available rather than unknown. The ASP then sends one 128-byte
  preflight DATA per route through `Endpoint.MTPTransfer`, the MTP-TRANSFER
  request of RFC 4666 Section 5.5.1.1.1; the receiver returns what it received
  and the ASP checks each receipt against the path MTPTransfer reported. That
  resolved path map is frozen and must use all eight associations. The
  receiver freezes its own copy from the same receipts.
- **`routed`** sends every timed message with `Endpoint.MTPTransfer` on its
  route and fails the send if MTPTransfer used any path other than the frozen
  one.
- **`routed-direct`** keeps the same routing inventory configured but performs
  an already-resolved lookup in the frozen map and writes with
  `Association.WriteData` on that path's association, AS scope and stream. It
  is the matched control for the router comparison, not the zero-route direct
  baseline, and implements no routing engine.
- **Traffic.** Message index `i` belongs to route `i mod 1000`, so route hits
  are uniform, and each route is one ordered flow with its own OPC, DPC, SI,
  NI, priority and SLS. Every route maps to one sender worker, so a route's
  messages are submitted in order. Payload generation happens before the timed
  call. The send duration covers the same work in both variants: Protocol Data
  construction and one library call, MTPTransfer for `routed` and WriteData
  for `routed-direct`. Everything else runs outside it in both: `routed`
  checks the path MTPTransfer reported after the second timestamp, and
  `routed-direct` takes its admission slot, checks the context and
  revalidates the frozen path's association epoch and stream bound before the
  first timestamp and again after the second. Payloads use a route-aware
  header (version 2) that carries the route instead of the direct fixture's
  association and flow bytes.
- **Validation.** The receiver checks every arrival against its route's
  frozen transport, association epoch, AS scope, stream and label plus the
  deterministic payload, and keeps a 1,000-flow ledger with the same rolling
  8,192-sequence duplicate window per flow. A message on any other transport
  or scope is invalid.

Both processes pass the same `-mode`. The routed modes require
`-associations=8`, `-payload=mix` on the ASP, and ASP-dial initiation
(`-transport=dial` on the ASP, `-transport=listen` on the SGP). The SGP's
`-sctp-address` names one concrete address and the first of four consecutive
ports, one per SGP endpoint (`sgp-host:2905` listens on 2905 to 2908); a
wildcard address is refused. The ASP dials the same `-sctp-address` and must
bind one concrete local address with `-local-address=asp-host:0`, because the
fixture pairs every association by its exact transport addresses. A name must
resolve to exactly one address. `-same-host-clock` works as in throughput
mode.

The receiver serves the routing preparation under `/routing/` beside the
ordinary control endpoints and reports `/ready` only once the paths are
frozen; the cohorts then use the unchanged `/reset`, `/start`, `/progress`,
`/stop` and `/results` contract. Both records use the throughput schema,
including `sender_window`, backlog evidence, the delivery ledger,
`send_duration`, `cpu`, `allocations` and the manifest, whose `flow_count` is
1000. `internal/cmd/perfcapacity` accepts these cohorts for the fixed shape
only. A campaign runs each variant separately; the router ratio compares the
two capacities. The routing allocation increment is the difference between
the variants' allocations per validated delivery; both are whole-process
observations, so the fixture work common to both cancels, but the difference
is not an isolated-library measurement.

If the ASP fails at any point before its first cohort, it sends
`/routing/stop`, so the receiver ends with that reason instead of waiting for
a sender that has gone. A canceled preparation request never closes the SGP
endpoints on its own.

The routed modes do not cover alternate AS preference, partial path failures,
SSNM storms or reference churn; those remain separate workloads and are
reported as unavailable in `unsupported_modes`.

## DATA overload mode

`-overload-profile` turns a throughput sender into the DATA overload trial of
performance budgets section 4: "Offer twice the matched required rate for
60 s, then 50% for 60 s; all configured bounds hold, every
rejection/loss/indeterminate outcome accounted for, admitted healthy
throughput recovers within 2 s." The matched required rate is the section 2
mixed row, so the approved trial is

```sh
perftraffic -role=asp ... -associations=8 -payload=mix -rate=40000 \
  -overload-profile=2x:60s,0.5x:60s -warmup=30s -same-host-clock
```

The profile is a comma-separated list of `MULTIPLIERx:DURATION` phases. A
multiplier is a plain decimal (`2x`, `0.5x`, `1.25x`, at most `16x`) applied
to `-rate`; each phase rate must come out a whole number of messages per
second. Durations are whole seconds, so every phase boundary falls on a
per-second progress observation. The profile must contain a phase above 1x
followed by a phase at or below 1x, must end at or below 1x, and each such
recovery phase must last at least 12 s (the 2 s allowance, a one-second
window and eight trend observations). It schedules at most 2^26 messages, the
bound of the per-message outcome ledgers. The flag is an ASP sender flag for
throughput mode only; the receiver learns the profile from each cohort's run
specification. It is refused together with `-ssnm-rate`, and a receiver
refuses a specification carrying both: the overload row runs DATA alone and
judges its deliberate losses by its own contract, while the SSNM rows judge
SSNM delivery against loss-free DATA at a fixed nominal load, so a combined
run would belong to neither contract. Without it every mode, including `routed` and
`routed-direct`, is unchanged: no field below is emitted and the scheduler's
single-rate arithmetic is exactly the historical one.

- **Window and schedule.** The measurement window is the concatenation of the
  phases; `-duration` may be omitted or must equal it. The open-loop scheduler
  is the same 100 microsecond scheduler on the same clock (`-same-host-clock`
  included); only its schedule switches rate at each phase boundary. The drain
  is raised to at least 3 s so every request deadline falls a second before
  the drain deadline.
- **Offered shape.** The trial is only evidence if the offered load followed
  the phased schedule, so the scheduler records how long past its scheduled
  instant it emitted the first message of each batch (the message of the batch
  that waited longest), and each per-second sample records the offered count.
  The offered load followed the schedule when the scheduler was never more
  than 10 ms late (the trend rule's floor duration) and every in-window sample
  lies between `schedule.due` at the sample instant less 10 ms of the current
  phase's traffic (rounded up) and `schedule.due` itself, the upper bound taken
  at the end of the millisecond the sample offset is truncated to. The
  scheduler never emits early, so any excess is a fixture fault. A deviation,
  or no in-window sample at all, makes the trial `invalid`; the observations
  are in `overload.offered_shape`.
- **Warm-up.** The warm-up is an ordinary loss-free throughput cohort at the
  profile's last-phase rate, the nominal level the trial returns to (0.5x of
  the row: section 4's "mixed traffic at 50% of its target"). A warm-up that is
  not loss-free ends the run as in every other mode.
- **Per-request deadline and the cap.** Section 2's open-loop rules apply:
  scheduled but unfinished sends are capped at `-outstanding` (8,192), each
  request has a two-second deadline from its scheduled instant, and refusals
  are counted, never omitted. A message whose deadline passed while it waited
  in the fixture queue is not offered to the library. Otherwise the sender sets
  the association write deadline to the request's deadline (never beyond the
  drain deadline) for exactly the one `WriteData` call and then restores the
  cohort's drain deadline, so a request deadline never bounds a write the
  library makes on its own behalf. A full send buffer is therefore waited out
  until the request's deadline.
- **Outcome classes.** Every offered message ends in exactly one class,
  counted per phase, in total and in a per-second series: `accepted` by
  `WriteData`; `not_sent` (`m3ua.DataNotSent`) by cause, `write_deadline`
  (`os.ErrDeadlineExceeded`), `send_buffer_full` (`EAGAIN`/`EWOULDBLOCK`,
  which cannot occur while a write deadline is installed but is still
  classified), `not_established` and `other`; `indeterminate`
  (`m3ua.DataSendIndeterminate`); fixture cap refusals
  (`cap_refused_outstanding` for the 8,192 cap, `cap_refused_queue` for a full
  per-association queue); `deadline_expired`; and the fixture-failure classes
  `unclassified` (a result outside the `*m3ua.DataWriteError` contract or a
  short write), `fixture_errors` and `aborted`. The legacy `submitted`,
  `send_errors` and `capped` fields keep counting accepted messages,
  `WriteData` failures, and fixture refusals plus deadline expiries. The
  fixture never resends a refused or indeterminate message. The per-second
  sampler stops at the end of the measurement window while requests still
  complete during the drain, so a final point (`final: true`) is taken once
  every sender worker has finished; the series must end at the cohort totals
  or the trial is `invalid`. The per-association fixture queue maximum is read
  at every enqueue, immediately before (counting the message being added) and
  immediately after it: a worker can dequeue in between and a peak between two
  enqueues is not seen, so it is a sampled observation, and the channel
  capacity itself enforces the bound.
- **Receiver observations.** Validated unique deliveries and duplicates are
  counted by the phase the message was scheduled in, a scope failure (wrong
  routing label, Network Appearance, Routing Context or association) is counted
  as `misscoped` within `invalid`, and a per-message delivered ledger records
  every validated delivery. Library discards are observable through the public
  API: `Association.DataQueueStats` reports each inbound DATA queue's capacity,
  occupancy, cumulative discards and congestion, the same observation
  `TestDataQueueOverloadIsObservable` pins. The receiver polls it every 10 ms
  during the cohort for the maximum occupancy and congestion episodes, reports
  each association's discards, state and SCTP epoch at the end, and brackets
  every `/progress` snapshot's unique count with the discard total read just
  before and after it. `GET /overload/delivered?generation=N` serves the
  stopped cohort's delivered ledger (one bit per scheduled message,
  little-endian 64-bit words).
- **Admitted backlog.** Every progress observation is bracketed by the
  sender's attempt accounting, so the admitted-but-undelivered backlog is
  bounded below by accepted-before minus unique minus discards-after and above
  by started-after minus refused-before minus unique minus discards-before.
  Refused messages are never part of it. `sender_window` keeps its nominal
  meaning over the phased offered schedule; its backlog includes refused
  messages and is not an acceptance input here. With `-same-host-clock` an
  observation's instant is the receiver's capture plus or minus the clock
  resolution on the shared clock, as for the nominal sender window, not the
  sender's HTTP request envelope; a missing capture invalidates the trial.

The sender record's `overload` object carries the per-phase and total classes,
the per-second series, the refusals by scheduled second, the per-message
`reconciliation`, the observed `bounds`, the per-interval `windows` (delivered,
offered and admitted-backlog bounds between consecutive progress
observations), the `recovery` evaluation at each switch, and the `acceptance`
decision; the receiver record's `overload.receiver` carries the receiver
observations above. Acceptance is machine-readable, one entry per criterion
with the numbers in its detail:

1. **fixture** — no fixture or clock failure, bounded progress observations,
   receiver observations present, the offered load following the phased
   schedule and the per-second series ending at the totals. A failure makes
   the trial `invalid`.
2. **accounting** — every offered message reached exactly one class and the
   whole schedule was offered; the accepted and indeterminate ledgers agree
   with the counts and never overlap; the delivered ledger agrees with the
   receiver's unique count; no delivered message was neither accepted nor
   indeterminate (`phantom`); accepted messages the receiver never delivered
   are all explained by the counted library discards (`unexplained_missing`
   bounds, `discard_excess`); and there are no duplicates, invalid or
   mis-scoped, reordered or late deliveries. Discards are counted per
   association, not per message, so they explain undelivered messages by count;
   an indeterminate message may or may not have been delivered, and the bounds
   say how many of either kind there are.
3. **bounds** — fixture outstanding never above the cap, no fixture queue above
   its capacity, every receiver library DATA queue at the configured capacity
   (1,024) and never above it, every association on both sides still
   ASP-ACTIVE on the SCTP epoch it started on, no `not_established` refusal and
   no receiver read failure. The observed maxima are reported.
4. **recovery** — at each switch from a phase above 1x to one at or below it,
   the first one-second window between consecutive progress observations that
   starts after the switch and no later than 2 s after it (the latest instant
   its start could have been), that delivers at least the offered rate over the
   longest interval it could have spanned less 1% and less the trend rule's
   floor (10 ms of offered traffic), and after which the admitted backlog does
   not grow. Non-growth is the trend rule's `not-growing` verdict from the
   window to the end of the phase; because that rule's autocorrelation-robust
   bounds straddle the floor across the drain of the backlog an overload phase
   leaves behind, a window whose trend is undecided also counts when no later
   admitted backlog exceeds the backlog at the window's start by more than the
   floor (`non_growth_by: envelope`). A `growing` verdict is never excused. The
   recovery time is the window's latest possible start minus the switch. An
   observation whose bracket begins before the switch cannot start a window,
   because the deliveries it counts may include the overload phase's: with
   `-same-host-clock` that is the observation taken at the switch whenever
   the clock resolution exceeds its capture delay, and the next observation's
   window is then the first candidate.
   Refusals scheduled after the window and the end of the phase's last refused
   second are reported but are not part of the criterion.

The acceptance `verdict` is `pass` only when all four pass, `fail` when any
fails, `inconclusive` when one lacks evidence, and `invalid` on a fixture
failure. The sender record's `verdict` follows it (`fail` is reported as
`invalid` with the failed criteria in `reasons`); the receiver record passes
only its own checks. `capacity_verdict` is always `unavailable`: every cohort
of an overload trial, warm-up included, carries `spec.overload` (`role`
`warmup` or `measurement`), and `internal/cmd/perfcapacity` refuses any run
that carries that identity or an `overload` object before any other check.

## Protocol basis

The protocol basis was rechecked against both the RFC Editor and IETF
Datatracker before this fixture was added. RFC 4666 remains the current
Proposed Standard and obsoletes RFC 3332; it has no updating or obsoleting RFC.
Errata 2065 and 4475 are Held for Document Update and Errata 2518 is Rejected,
so none is applied silently here.

- Endpoint role names follow RFC 4666 Sections 1.2 and 1.4.8. SCTP initiation is
  reported separately and is not called a client/server role.
- DATA Protocol Data, Network Appearance, Routing Context, and traffic-flow
  validation follow RFC 4666 Section 3.3.1.
- The ASP Up and ASP Active procedures needed before DATA transfer follow RFC
  4666 Sections 4.3.1 and 4.3.4.3.

## Build and run

Build once; do not include compilation in the measured process:

```sh
docker volume create go-m3ua-perf-bin
docker run --rm \
  -v "$PWD":/src -v go-m3ua-perf-bin:/out -w /src \
  golang@sha256:154bd7001b6eb339e88c964442c0ad6ed5e53f09844cc818a41ce4ecb3ce3b43 \
  go build -trimpath -o /out/perftraffic ./internal/cmd/perftraffic
docker network create go-m3ua-perf
```

Start the SGP receiver in one terminal:

```sh
docker run --rm --name go-m3ua-perf-sgp \
  --network go-m3ua-perf --cpuset-cpus 4-7 --cpus 4 --memory 2g \
  -e GOMAXPROCS=4 -e GOGC=100 -e GOMEMLIMIT=1536MiB \
  -v go-m3ua-perf-bin:/out:ro \
  golang@sha256:154bd7001b6eb339e88c964442c0ad6ed5e53f09844cc818a41ce4ecb3ce3b43 \
  /out/perftraffic \
    -role=sgp -transport=listen \
    -sctp-address=0.0.0.0:2905 -control-address=0.0.0.0:8080 \
    -associations=8
```

Run the ASP sender in a second terminal:

```sh
docker run --rm --name go-m3ua-perf-asp \
  --network go-m3ua-perf --cpuset-cpus 0-3 --cpus 4 --memory 2g \
  -e GOMAXPROCS=4 -e GOGC=100 -e GOMEMLIMIT=1536MiB \
  -v go-m3ua-perf-bin:/out:ro \
  golang@sha256:154bd7001b6eb339e88c964442c0ad6ed5e53f09844cc818a41ce4ecb3ce3b43 \
  /out/perftraffic \
    -role=asp -transport=dial \
    -sctp-address=go-m3ua-perf-sgp:2905 \
    -peer-control=http://go-m3ua-perf-sgp:8080 \
    -associations=8 -payload=mix -rate=40000 \
    -warmup=30s -duration=120s -drain=2s \
    -cohort=baseline-direct-8-mix -seed=1
```

Stop the receiver after collecting the sender result. Its final stdout value is
the receiver-side JSON record:

```sh
docker stop go-m3ua-perf-sgp
docker network rm go-m3ua-perf
docker volume rm go-m3ua-perf-bin
```

Use `-associations=1` or `8` for the approved direct rows. Supported payload
workloads are `128`, `512`, `4096`, and `mix`; `mix` repeats an exact 100-message
90/9/1 schedule. The parser bounds associations to 32, the complete requested
window to 10 minutes, the offered rate to 1,000,000 messages/s, and scheduled
but unfinished sends to at most 8,192. The scheduler uses 100 microsecond
batches and records the resulting dispatch lag instead of delaying the offered
schedule to match achieved throughput.

## Control contract

The receiver exposes five bounded HTTP operations (a routed receiver also
serves its preparation operations under `/routing/`, and a stopped overload
cohort's delivered ledger is served at `/overload/delivered`):

- `GET /ready` reports established association count, fatal read errors, phase,
  and the minimum negotiated outbound stream count.
- `POST /reset` accepts one JSON run specification only while no measurement is
  active and replaces all bounded cohort state.
- `POST /start` changes an armed cohort to measuring and snapshots whole-process
  allocation and cgroup counters.
- `POST /stop` freezes measurement/drain timing and final observations.
- `GET /results` returns the current raw receiver record.

Warm-up is a separate cohort. The sender waits until every successfully
submitted warm-up message is accounted for, stops that cohort, and only then
resets the receiver for measurement. A reset during measurement and a start or
stop in the wrong phase returns HTTP 409. Messages racing a cohort generation
change are invalid rather than credited to the new cohort, and so is work
scheduled from them: an echo reply carries the generation its request was
validated under, so a reply written, dropped or failed after a reset is
accounted to the cohort that requested it and never to the new one. A reverse
cohort that completes after a reset is discarded for the same reason.

## Result contract

### Sender-aligned boundary accounting

`sender.sender_window` brackets validated delivery in the sender's actual
monotonic measurement interval. Before traffic begins, an atomic `/progress`
snapshot verifies an empty active cohort. One-second periodic snapshots, an
additional probe targeted 10 ms before the end, and post-window
observations retain the sender's request-start and response-completion offsets,
the full cohort specification, generation, phase and cumulative delivery counters.
No serialized receiver clock is treated as synchronized with the sender. Timing
uses the same-process monotonic subtraction described by the
[Go time package](https://pkg.go.dev/time#hdr-Monotonic_Clocks).

The measurement boundary stops new periodic requests without canceling one
already in flight. An active request retains its one-second timeout and caller
cancellation, following the [Go context contract](https://pkg.go.dev/context).
The sender joins the sampler before taking subsequent snapshots; this wait does
not extend the measurement window or the absolute drain deadline. A request
crossing the boundary remains recorded but cannot narrow the delivery bounds.

At the snapshot instant between offsets `before` and `after`, cumulative unique
delivery `U` bounds all scheduled-but-not-yet-validated work by
`max(0, offered(before)-U)` through `offered(after)-U`. The offered schedule is
ideal open-loop demand, not actual dispatcher progress or completed local writes.
These bounds include scheduler, library, transport and receive-side waiting;
they do not identify which queue holds the work. The legacy scalar outstanding
fields are local counters only, as identified by `outstanding_scope`.

For the fixed sender end boundary, snapshots completed before that boundary
provide lower delivery bounds; snapshots begun after it provide upper bounds.
A snapshot straddling the boundary does not narrow either side. Drain completion
never raises the lower measurement-window count. `sender.validated_per_second`
is now the conservative lower rate bound; use `sender_window.status` before
interpreting it. Failed, inconsistent or incomplete observations yield
`inconclusive`, not a measured zero or a passing result. Raw observations remain
in `sender.progress_observations` even when analysis cannot use them.

In the default HTTP-interval mode, the receiver's first-arrival-based `delivery.unique_measurement` and
`validated_per_second` remain explicitly receiver-window diagnostics, not
sender-aligned acceptance measurements. Older fixture results must not be mixed
with these bounds as if the measurement definitions were identical.

The paired `backlog_trend` fits the sustained-backlog trend of the observations
strictly within the measurement window (at least eight), as described in
[internal/perfstats](../../perfstats/README.md). Its `status` is `not-growing`,
`growing` or `indeterminate` against a floor of 10 ms of offered traffic, or
`insufficient-samples` or `invalid-samples` when no trend can be fitted. The
raw `samples` are retained so `perfcapacity` can recompute the trend before it
applies the predeclared decision; the fixture does not decide capacity itself.
Capacity remains unavailable pending paired-series calibration and the full
campaign. HTTP observation overhead remains in whole-process
CPU/allocation accounting; it is not silently subtracted.

### Opt-in shared Linux clock

Set `-same-host-clock` on both processes for throughput or bidirectional runs
on the same controlled Linux host. Echo mode rejects this
option; its scheduled RTT remains a separate, process-local measurement.
The default HTTP-interval mode is unchanged, and unsupported or mismatched
shared-clock configurations fail rather than silently falling back.

Before traffic, both sides compare `CLOCK_MONOTONIC`, kernel boot identity,
monotonic time-namespace offset, and clock resolution. A time namespace's
`CLOCK_MONOTONIC` is the boot's clock plus that namespace's monotonic offset
([time_namespaces(7)](https://man7.org/linux/man-pages/man7/time_namespaces.7.html)),
so the offset, read from `/proc/self/timens_offsets`, identifies the clock;
the namespace inode does not, because container runtimes give each container
its own time namespace. The sender declares a common
future start and end; the receiver acknowledges that exact window before the
sender schedules traffic. Missing the start during preparation invalidates the
run. Both domains are checked again after the run. This is a controlled-host
measurement assumption, not cryptographic proof that remote hosts are shared.
Container placement must independently establish the common Linux host.

Shared integer timestamps are authoritative for scheduled dispatch lag,
measurement-end waits, diagnostic sample offsets, and receiver drain duration.
Go timers are wake-up hints followed by another shared-clock read; no translated
Go start timestamp defines these measurements. Samples before the future start
are omitted, and both sides retain at most 601 diagnostic series points.
The send-call duration remains a separate process-local Go monotonic interval.

Socket and context watchdogs require Go deadlines. Their translation brackets
`time.Now()` between two shared reads and places the watchdog conservatively
after the shared end-plus-drain boundary. The bracket width plus twice the clock
resolution must be at most 1 ms, otherwise preparation fails. The sender retains
the bracket, target, translation-lateness bound, and budget in
`shared_clock_evidence.watchdog`. This bounds translation uncertainty, not OS wake-up
latency. Sender write completion and receiver delivery commit must independently
fit within the shared drain boundary including clock resolution; an uncertain
or late completion invalidates the run rather than receiving extra drain credit.
The shared drain duration includes final control/stop observation time and can
therefore exceed the delivery allowance without extending that allowance.
Go's Linux runtime uses `CLOCK_MONOTONIC` for its monotonic reading
([Go 1.25.10 arm64 runtime](https://github.com/golang/go/blob/go1.25.10/src/runtime/sys_linux_arm64.s));
[`time.Time.Add`](https://pkg.go.dev/time#Time.Add) preserves the local monotonic
reading used by these watchdogs. Host and monotonic-offset identity checks remain
mandatory; translating a timestamp does not establish a shared clock domain.

Receiver progress timestamps and unique counters are captured under the same
commit mutex. HTTP envelopes remain consistency checks, but HTTP transit time
does not widen these receiver-local observations. Each timestamp still carries
the reported clock-resolution uncertainty; boundary-adjacent deliveries remain
bounded rather than being rounded into the measurement window. The sender's
`sender_window` and receiver's `shared_clock_boundary` retain those bounds.
`validated_per_second` is the conservative lower bound in this mode. Nominal
`delivery.unique_measurement` remains accompanied by these explicit bounds.

The instrument retains at most 604 progress observations and no per-message
timestamp history. Clock reads add no allocation; observer CPU and contention
must still be measured on Linux and remain in whole-process accounting. Run
`go test ./internal/cmd/perftraffic -run TestSharedClock` and
`go test ./internal/cmd/perftraffic -run '^$' -bench BenchmarkSharedClockLinuxRead -benchmem`
on an otherwise idle Linux reference environment before calibration. Compare
the complete instrumented workload with a pristine-base run separately.

Aligned clocks do not establish sustainable capacity or excuse positive
backlog growth. The fixture still reports capacity as unavailable. Retain the
full time series, clock evidence, boundary counts, and post-drain counts; a
growing trial that drains completely afterward must not become a passing trial.
A trend fitted over a finite window cannot prove indefinite stability.

Sender stdout is one JSON object containing the active `phase`, top-level
`sender`, `receiver`, `verdict`, an optional `error`, and retained `warmup` and
`measurement` phase records. A warm-up failure returns both raw warm-up records
instead of replacing them with an empty result. Each side retains raw
configuration and observations:

- `scheduled`, `sent`, `submitted`, `send_errors`, `capped`, and outstanding counts at
  the start, measurement end, and end of drain;
- receiver `unique`, `unique_measurement`, `unique_drain`, `missing`,
  `duplicate`, `invalid`, `reordered`, and `late_after_stop` counts;
- bounded one-second series and bounded histogram-derived send-duration and
  scheduled-to-worker dispatch-lag percentiles (p50/p95/p99/max), where each
  percentile is a conservative bucket upper bound, capped by the observed
  maximum. Durations below 128 ns are exact; larger durations use 64 buckets
  per power-of-two interval, limiting over-estimation to 1/64 of the value.
  Each histogram retains less than 32 KiB and recording does not allocate;
- raw cgroup v2 `cpu.stat` maps, `usage_usec` delta, and CPU seconds per final
  unique validated delivery;
- runtime allocation counters spanning the cohort through drain, including
  asynchronous work;
- toolchain, revision when available, dependency version, fixed socket options,
  flow count, queue cap, and negotiated outbound stream counts;
- `assessed_baseline_revision`, the baseline commit the campaign is assessed
  against (issue #36). It is fixed in the fixture source, not read from the
  build stamp: the binary is built from the candidate head, so `vcs_revision`
  records the candidate and can never name the baseline. The two are separate,
  separately labelled fields and are never interchangeable.

`send_duration.max_ns` is the exact observed maximum send-call duration, unlike
the `p50`/`p95`/`p99` fields beside it, which are conservative histogram bucket bounds.
The acceptance decision in `internal/perfstats` reads it as the predeclared
transport-stall signal: a send call blocked for at least one second has spanned
at least one SCTP minimum retransmission timeout (RFC 9260 Section 16), and the
run it came from is reported inconclusive with the stall named rather than
dropped or charged to the candidate. The fixture itself does not apply that
rule; it records the measurement.

Payload identity is `(cohort, association, flow, sequence)`. Thirty-two logical
flows map stably to the configured associations and to 16 SLS values. The
receiver validates deterministic payload bytes plus per-flow OPC, DPC, SCCP or
ISUP SI, NI, priority, SLS, Routing Context, and Network Appearance. Duplicate
tracking uses a fixed rolling 8,192-sequence bitmap per flow. A missing
submission does not invalidate later correctly ordered deliveries or count
them as reordered. An arrival older than the retained window is unclassifiable
and earns no unique-delivery credit; it increments the fixture-invalid count.
Payloads outside the scheduled identity space are also invalid and cannot hide
a missing delivery.

Allocation and CPU fields are explicitly labelled whole-process observations:
they include fixture generation, validation, HTTP control, and library work.
They do not independently satisfy the isolated-library allocation budget.
Receiver measurement time begins on the first valid cohort arrival; sender and
receiver monotonic clocks are not assumed synchronized, and no one-way or RTT
latency claim is made. A bounded tail may drain after the offered window, but it
does not increase `unique_measurement` or achieved-window throughput.

The application-level backlog assessment compares the first and last quarters
of the measurement-only sender series. A material increase is `growing`; a
marginal increase is `uncertain`. These are capacity diagnostics and do not turn
otherwise loss-free delivery into a fixture failure. The fixture retains paired sender and receiver
series but has not yet calibrated them into the sustainable-capacity decision,
so `capacity_verdict` remains `unavailable` and the overall `verdict` remains
`inconclusive` even when the fixture is loss-free. This is a tooling limitation,
not a dependency API limitation.

`fixture_verdict: pass` requires every scheduled DATA message to be submitted
and uniquely validated before the single absolute drain deadline, with no caps,
send failures, invalid scope/payload, duplicates, reordering, fatal read error,
or final application outstanding work. A `growing` backlog is an inconclusive
capacity diagnostic, not a fixture-validity failure.
Missing or regressed cgroup counters, CPU throttling, too few series samples, or
unavailable capacity observability produce an overall `inconclusive` result;
protocol/data failures produce `invalid`. No field is named or treated as
application-routing acceptance.
