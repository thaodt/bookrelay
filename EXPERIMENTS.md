# Connection experiments - Extension 4

This report investigates connection selection and TCP source ports for market-data
collection. The implementation provides a bounded measurement tool; the results
below are preliminary observations, with no validated transport optimization.
The recorded experiments ran on 2026-11-06 and the TCP diagnostic on 2026-11-07.
The later full-collector validation did not repeat these E4 experiments.

## Measurement method

Two connections observe the same Kalshi market in one process using a shared
monotonic time origin. `A − B < 0` means reference A delivered the matched event
to the application earlier. These are signed differences in application arrival,
not exchange-to-NIC or one-way network latency. Timing includes WebSocket delivery
and local scheduling.

Each feed independently initializes its book and validates its subscription
sequence. Matching groups ordered payloads by market and comparable clock,
requires equal complete payload sequences and uses an ordinal for repeated
identical deltas. Subscription sequence numbers are not cross-connection IDs.
The first possibly partial group is discarded; a later clock closes a group.

The matcher retains bounded items/estimated bytes, enforces TTL and counts
unmatched, ambiguous, evicted and boundary-censored observations separately.
Unclosed final groups are censored. Unmatched arrivals alone do not establish
exchange-side data loss. Percentiles use nearest rank; results stay separate
per trial. Candidate selection uses a training window and is checked on a later
holdout window.

The current default keeps reference A while replacing candidate B sequentially.
Generation tags isolate trailing candidate frames. `--fresh-reference` selects
the older mode that opens both feeds afresh for each trial. This is sequential
candidate search, not a concurrent pool. A reference failure ends the experiment;
candidate failures remain in the report.

## Initial BTC trials

Three trials used two seconds of warm-up and 15 seconds of observation, opening
two fresh sockets each time. Candidate local ports were 51371–51373. Destination
IP was not fixed, the peers differed and a full-universe collector was also
bootstrapping on the same host/account. The run cannot isolate a source-port
effect from backend choice, network path or competing workload.

All skew values below are milliseconds of `A − B`.

| Trial | Matched pairs | Unmatched | Whole-trial p50 | Training p50 | Holdout p50 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 0 | 16,055 | 12 | -2.810 | +1.460 | -3.753 |
| 1 | 15,717 | 11 | +2.674 | +2.705 | +2.625 |
| 2 | 16,692 | 9 | -2.077 | -2.084 | -2.065 |

There were **48,464 matched pairs**, 32 unmatched and 23 boundary-censored
arrivals, with no payload ambiguity or detected sequence gaps. Both feeds closed
cleanly in every trial. Trial 0 selected B during training, but A was earlier in
holdout. Selecting a connection from a short initial window can therefore give
the wrong choice for the next window. The other two short trials do not establish
a durable connection advantage.

## Persistent-reference ETH trials

Three later trials used two seconds of warm-up and 20 seconds of observation.
A remained at generation 1 across all candidate replacements. Source ports and
destination resolution used their defaults. Collector workloads were stopped;
local regression tests ran during part of the experiment.

| Candidate generation | Matched | Unmatched | Whole-trial p50 A − B, ms | Holdout pairs | Holdout p50, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 7,114 | 429 | -33.153 | 3,088 | -33.465 |
| 2 | 7,509 | 31 | -30.689 | 3,686 | -30.593 |
| 3 | 6,382 | 307 | -35.897 | 2,726 | -35.480 |

There were **21,005 matched pairs**, 767 unmatched and 12 boundary-censored
arrivals, with no payload ambiguity or cap eviction. All feeds closed cleanly;
the CLI exited zero with no detected sequence gaps or feed failures.

A was earlier at the median in these windows. Peers/ports were not fixed or
crossed over and connection age differs by design. The run validates reference
persistence and reporting; it does not establish that older connections are
generally faster or explain the observed difference causally.

## Unsuccessful attempts and a measurement cost

| Attempt | Observation | Interpretation |
| --- | --- | --- |
| Market expiry | Replacement snapshot after 742 matches; subsequent trials had no updates | Failed/no-data attempt; choose markets that remain active for the whole experiment |
| Observer overload | Persistent-reference trial counts 14,911 / 339 / 0; reference observer queue overflowed during concurrent collector bootstrap | Invalid experiment; retained samples do not rescue the failed reference |
| Fixed-peer port trial | Ports 52471–52473; only 596 retained matches, all in training; reference TCP reset and no usable holdout | No conclusion about which port is better |

The original matcher scanned every pending group for expiry on every arrival.
With a lagging feed this repeated work could starve its observer. The current
code sweeps at most once per 100 ms, or TTL/4 when smaller, while checking exact
TTL on each match. A regression test rejects expired pairs between sweeps.
The change removes repeated scanning, but the live attempts differed in load
and network paths; they do not quantify an isolated performance improvement.

## Ashburn TCP diagnostic

Seventeen collector-scoped Linux TCP smoothed-RTT snapshots had per-sample
medians between **11.7345 and 12.022 ms**. These samples included REST connections.
They are a kernel transport diagnostic, not paired market-data arrival timing.
Application command-ACK timing was not instrumented in that run. No connection
or port optimization follows from this RTT range.

## Running a new comparison

Build the release executable and configure Kalshi credentials as described in
[README.md](README.md). Use active regular markets with enough lifetime, new
report/sample paths and a declared background workload. The CLI's `latency`
subcommand currently supports Kalshi.

```bash
# Keep reference A while testing three sequential candidates.
cargo run --release --locked -- latency --symbols LIVE_TICKER \
  --trials 3 --warmup-seconds 120 --trial-seconds 120 \
  --sample-cap 500000 --max-runtime 15m \
  --report /tmp/connections.json --samples /tmp/connections.jsonl

# Replace WS_IPV4 with a freshly resolved address for the configured WS hostname.
# The original hostname is retained for TLS verification.
cargo run --release --locked -- latency --symbols LIVE_TICKER \
  --remote-ip WS_IPV4 --candidate-ports 51371,51372,51373 \
  --trials 3 --warmup-seconds 120 --trial-seconds 120 \
  --sample-cap 500000 --max-runtime 15m \
  --report /tmp/ports.json --samples /tmp/ports.jsonl
```

Explicit ports bind before TCP connection. A local bind does not prove which
source port the exchange observes through NAT. A fixed peer removes one
confounder but does not reveal the backend behind that address.

Reports retain endpoint tuples, generations, per-trial distributions, failures
and stop reasons. Inspect matched/unmatched counts and training/holdout sample
sizes. Sample, byte, time and observer limits can end capture early; a capped run
is not a completed intended-duration comparison. A gap, clock reset, replacement
snapshot or observer overflow invalidates a trial rather than silently stitching
different epochs. No-data windows have no latency distribution.

## Remaining questions and data availability

- Repeat candidate comparisons over longer independent time blocks and check
  whether training choices retain an advantage in holdout.
- Compare source ports with fixed peer/market sets, declared background load and
  randomized or crossover order. Retain unfavorable and failed trials.
- Observe a real maintenance interval with pre/post measurements. This branch
  has not been executed. A reference reset ends its epoch; automatic measurement
  across successive maintenance epochs is not implemented.
- Add queue/stage instrumentation to distinguish network arrival from local
  waiting and output work before attributing an improvement.

Historical summary reports were retained in my development workspace outside
this submission. The tables are historical report summaries; fresh captures are
needed for independent statistical reanalysis. Source-port superiority,
maintenance advantage and a validated default optimization remain unproven.

After submission, I'll continue the E4 experiments and use what I learn to improve `bookrelay`.
