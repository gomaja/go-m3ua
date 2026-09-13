# Direct M3UA traffic baseline

`perftraffic` is a two-process Linux SCTP fixture for receiver-validated direct
DATA throughput. It measures the current association API without using Endpoint
routing. The ASP Endpoint is constructed with a nil `ASP` configuration and
each DATA call uses a newly constructed Protocol Data parameter plus
`WritePDWithRoutingContext`; the SGP validates messages returned by `ReadData`.

The fixture currently implements only ASP-dial to SGP-listen, one-way
throughput. SGP-dial, ASP-listen, bidirectional traffic, echo RTT, router/state
workloads, and independent-peer interoperability are reported as unavailable;
they are never emitted as zero-valued successful measurements. Both processes
run this binary, so `fixture_verdict: pass` establishes loss-free fixture
validity only, not independent-peer, sustainable-capacity, or candidate
acceptance.

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

The receiver exposes five bounded HTTP operations:

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
change are invalid rather than credited to the new cohort.

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

The receiver's first-arrival-based `delivery.unique_measurement` and
`validated_per_second` remain explicitly receiver-window diagnostics, not
sender-aligned acceptance measurements. Older fixture results must not be mixed
with these bounds as if the measurement definitions were identical.

The paired `backlog_change` reports first-to-last-quarter mean change bounds
using only observations strictly within the measurement window and at least
eight samples. Its `increase-demonstrated`, `nonincrease-demonstrated` and
`unresolved` statuses describe those sampled quarters only. They are not a
statistical stationarity test, proof of no intervening backlog, or a capacity
acceptance rule. Uncertainty is not resolved by adding a percentage allowance.
Capacity remains unavailable until the sustained-growth decision rule and full
campaign are established. HTTP observation overhead remains in whole-process
CPU/allocation accounting; it is not silently subtracted.

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
  scheduled-to-worker dispatch-lag percentiles;
- raw cgroup v2 `cpu.stat` maps, `usage_usec` delta, and CPU seconds per final
  unique validated delivery;
- runtime allocation counters spanning the cohort through drain, including
  asynchronous work;
- toolchain, revision when available, dependency version, fixed socket options,
  flow count, queue cap, and negotiated outbound stream counts.

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
