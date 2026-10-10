# Capacity search decision tool

`perfcapacity` reads one strict JSON request on standard input and writes one
JSON response. Build it on the host with
`go build -o perfcapacity ./internal/cmd/perfcapacity`. Exit codes are 0 pass,
1 fail, 2 inconclusive (including a search awaiting its next run), and 3
invalid input. It evaluates recorded evidence; it does not run traffic.

The [method and evidence contract](../../perfstats/README.md#bounded-capacity-search)
define the 5% search bracket, fixed probe budget, up to three validation
rounds, and five counted passing repetitions at the selected rate.

## Same-rate repeats

The [owner decision of 2026-10-10](https://github.com/gomaja/go-m3ua/issues/44#issuecomment-6100413906)
amends “no probe is retried” in the
[#105 note](https://github.com/gomaja/go-m3ua/issues/44#issuecomment-5792719087)
and “Nothing is retried” in the
[#140 note](https://github.com/gomaja/go-m3ua/issues/44#issuecomment-5819247631).
A probe or validation repetition undecided solely because its 99%
backlog-growth interval straddles the 10 ms floor is run again at the same
rate, up to two more times. The first decided outcome counts once. If all
three attempts straddle, count one not-demonstrated outcome and apply the
existing upper-bracket rule. Each new validation repetition has its own
three-attempt limit; deferred attempts do not count toward its five passes.
The probe budget counts decided probes: a re-measured window is the same
probe measured again.

Pass still requires the whole growth interval at or below the floor. A
transport stall immediately contributes not-demonstrated, including when
another direction straddles. A failure immediately contributes fail.
Missing evidence ends the search inconclusive, and malformed evidence is
invalid input. Failed warm-up handling is unchanged.
Every contributing DATA direction, SSNM and route-reference verdict must
pass or be a backlog straddle to qualify. Any other non-pass verdict rules
out a repeat even when the final decision retains the DATA backlog reason.

## Driver protocol

Requests contain `initial`, optional `maximum` and `max_probes`, and `probes`
and `repetitions` arrays. Every array entry is `{"rate": N, "run": evidence}`.
The tool replays the lists in the order selected by the search. Append every
run, including deferred attempts, to its list and submit the complete history
again. Do not supply an outcome or attempt number in the request.
Each run must be a distinct measurement. Reusing the same cohort and window
at the same rate and phase is invalid, including reuse from a probe in
validation. Shared-clock windows are identified by their clock domain and
start/end timestamps; for legacy unaligned evidence, use a fresh cohort for
each window. Changing a seed, counters or verdict does not create a new
measurement. Re-submitting the complete history remains valid.

While a run is pending, exactly one of `next_probe_rate` and
`next_repetition_rate` names its rate. The response also supplies
`next_attempt` and `max_attempts`; a repeat supplies
`repeat_reason: "backlog-growth-bounds-straddle-floor"`. For example, after
one straddle at a probe rate of 28,000/s, the response includes:

```json
{
  "decision": "inconclusive",
  "next_probe_rate": 28000,
  "next_attempt": 2,
  "max_attempts": 3,
  "repeat_reason": "backlog-growth-bounds-straddle-floor"
}
```

Each entry in `probe_decisions` and `repetition_decisions` retains its run's
decision, reason and `search_outcome`, and adds `attempt` and `max_attempts`.
Attempts 1 and 2 that straddle report `search_outcome: "backlog-undecided"`
and `repeat_reason`; attempt 3 that straddles reports
`search_outcome: "not-demonstrated"`. `probes` and `validation_rounds` contain
only counted outcomes. Attempt numbering resets for every new probe or
validation repetition, including repetitions at the same rate.

A driver already following the next-rate fields needs no protocol change:
run the named rate again, append the new evidence, and ask again. Stop when
neither rate field is present. A different rate or phase that skips a pending
repeat, malformed evidence, or a fourth attempt after three straddles is
rejected. With the default 24-probe budget the tool requests at most 117
runs; in general the bound is `3*max_probes + 45`.
