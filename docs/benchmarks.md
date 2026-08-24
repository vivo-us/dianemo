# Benchmarks

```bash
npm run bench                                   # every default scenario
npm run bench -- --list                         # what is available
npm run bench -- clientTypes queue              # just these
npm run bench -- --all                          # including the opt-in ones
npm run bench -- --backend=memory               # no Redis required
npm run bench -- --json > results.json          # machine-readable
```

`REDIS_URL` is required unless you pass `--backend=memory`.

```bash
docker run -d -p 6399:6379 redis:7-alpine
REDIS_URL=redis://localhost:6399 npm run bench
```

## What is actually being measured

Every request is served by a local HTTP server on loopback. There is no network and no vendor, so
what is being timed is the coordination layer: backend round-trips, Lua execution, queue ordering,
leader election.

That framing has a consequence worth internalising before reading any number here. **The loopback
upstream tops out around 18,000 req/s**, and most scenarios run right up against that ceiling. When
two configurations both saturate the upstream, the benchmark is measuring the upstream, not them.
This is why `backendOps` exists, and why it is the only scenario that can tell the two backends
apart.

Numbers below were measured on an 18-core workstation with a co-located Redis. They are
hardware-dependent — reproduce them on hardware resembling your production before relying on them.

## Results

### Overhead over a raw HTTP client

The baseline is the same HTTP client at the same concurrency, after the same warmup.

| Concurrency | Through the handler | Raw HTTP client | Overhead |
| ----------- | ------------------- | --------------- | -------- |
| 10          | 14,022 req/s        | 15,186          | 8%       |
| 50          | 15,619 req/s        | 16,699          | 6%       |
| 200         | 16,197 req/s        | 17,615          | 8%       |

**Uncontended requests skip the queue.** The queue exists to order requests competing for a budget;
when nothing is contending there is nothing to order. One atomic operation decides admission —
refusing if anything is queued, if the client is frozen, or if the budget cannot cover the cost —
and otherwise spends the tokens directly. That takes a request from nine Redis round-trips down to
two. The fallback is exact: the moment anything queues, new arrivals queue behind it, so ordering
under contention is unchanged.

### Sustained throughput

500,000 requests at concurrency 200:

```
overall    17,044 req/s over 29.3s, 0 failed
latency    p50 11.6 ms  p99 16.4 ms  p99.9 18.7 ms  max 24.4 ms
memory     144 MB start, 234 MB peak, flat after the first window
drift      -0.5% second half vs first
```

Throughput is reported per window rather than as one average, so decay, GC pauses and memory growth
stay visible instead of being averaged away.

### Memory vs Redis: 20× on coordination, ~8% end to end

The backends differ enormously at the operation level and barely at all in practice, because a real
request spends almost all its time in HTTP rather than in coordination.

Coordination only, no HTTP in the path (20,000 ops, concurrency 50):

| Operation                       | `memoryBackend` | `redisBackend` | Ratio |
| ------------------------------- | --------------- | -------------- | ----- |
| `tryAdmitImmediately` fast path | 2,468,539/s     | 112,539/s      | 21.9× |
| `acquireTokens` queued path     | 2,857,415/s     | 121,709/s      | 23.5× |
| enqueue → dequeue → remove      | 207,517/s       | 33,782/s       | 6.1×  |

The same two backends, driving real requests:

| Scenario                               | `memoryBackend` | `redisBackend` | Ratio |
| -------------------------------------- | --------------- | -------------- | ----- |
| Sustained 100k, concurrency 50         | 18,340 req/s    | 16,966 req/s   | 1.08× |
| Admission under a binding 1000/s limit | 13,717 req/s    | 9,688 req/s    | 1.42× |
| `noLimit`, 15 ms upstream              | 1,653 req/s     | 1,524 req/s    | 1.08× |
| `requestLimit`, 15 ms upstream         | 361 req/s       | 358 req/s      | 1.01× |

A 20× advantage collapses to 8% because the loopback upstream tops out near 18,000 req/s — both
backends are pinned against _that_, not against each other. And once a rate limit actually binds,
the two are indistinguishable: the limiter sets the pace, and the backend is not the thing you are
waiting for.

So the memory backend is not the fast option to reach for under load. It is the option for programs
that have no second process to coordinate with. See
[choosing a backend](backends/README.md).

### Refill granularity dominates tail latency

A vendor limit quoted as "1400 per 10 seconds" can be configured as one lump or as a trickle. The
average rate is identical; the tail is not:

| `interval` / `tokensToAdd` | Throughput | p99        |
| -------------------------- | ---------- | ---------- |
| 10,000 ms / 1400           | 177/s      | 4,790 ms   |
| 1,000 ms / 140             | 142/s      | 602 ms     |
| 100 ms / 14                | 142/s      | **199 ms** |

With a lump the budget is spent early and everything then waits for the next one. Prefer the
smallest interval that still expresses the vendor's limit. See
[`requestLimit`](rate-limits/request-limit.md).

### Client count is nearly free

200 registered clients instead of 1 costs about 40 ms of extra startup and nothing per request — the
hot path is a keyed lookup.

### Replicas scale, given cores

With total offered load held constant:

| Replicas (40 concurrent total) | Combined     | p50    |
| ------------------------------ | ------------ | ------ |
| 1                              | 10,941 req/s | 3.3 ms |
| 2                              | 14,268 req/s | 2.5 ms |
| 4                              | 15,605 req/s | 2.2 ms |
| 8                              | 18,998 req/s | 1.7 ms |

Throughput rises and latency falls, because the fast path removes the per-request broadcasts that
would otherwise make coordination grow with replica count.

The dependence on hardware is steep here. The same benchmark on a 4-core VM shows throughput
_falling_ from 1 to 8 replicas: eight busy processes on four cores spend their time
context-switching. Size replicas to available cores.

## Layout

```
bench/
  index.mjs              runner: selection, upstream lifecycle, output
  lib/
    format.mjs           percentiles and number formatting
    harness.mjs          handler construction, client registration, load driver
    upstream.mjs         the controllable fake vendor
  scenarios/             one file per scenario
  replicaWorker.mjs      forked child for the replicas scenario
```

A scenario is a file in `bench/scenarios/` exporting a default object:

| Field          | Purpose                                                      |
| -------------- | ------------------------------------------------------------ |
| `name`         | selector on the command line                                 |
| `title`        | printed header                                               |
| `summary`      | one line for `--list`                                        |
| `run(ctx)`     | performs the measurement and **returns plain data**          |
| `report(data)` | prints that data for a human                                 |
| `optIn`        | excluded from a bare run; name it explicitly, or `--all`     |
| `needs`        | scenarios to run first; results land in `ctx.shared`         |
| `backends`     | restrict to certain backends; `skipReason` explains the skip |

Keeping `run` free of printing is what makes `--json` possible: the same data either goes through
`report` or straight out as JSON. Adding a scenario means dropping a file in that directory — the
runner discovers it, and `ORDER` in `index.mjs` decides where it appears.

## The scenarios

### baseline

The same HTTP client the handler uses, at the same concurrency levels, with no handler in the path.
Publishes the floor that `throughput` compares against.

Both details are load-bearing. A baseline that uses a different HTTP client, or a different
concurrency, is not a baseline — it will happily report the handler running at more than 100% of its
own upstream.

### throughput

One unthrottled client at rising concurrency, reported as a percentage of the baseline. This is the
headline overhead figure.

### latency

Handler cost as the upstream slows to realistic speeds — 0, 10, 50 and 200 ms.

At zero latency the coordination overhead dominates and looks alarming. Real vendors answer in tens
to hundreds of milliseconds, and the question that actually matters becomes how many requests the
handler can keep in flight. The overhead column falls to roughly nothing by 50 ms, which is the
honest summary of what the handler costs in production.

### realistic

Three vendors at once, each with a different published limit, with 20% bulk traffic at `cost: 2` and
lower priority competing against interactive traffic. This is the shape a real integration layer
sees — not one client saturated with identical requests.

Two details of the reporting matter:

- **"Of budget" is measured in tokens, not requests.** The limiter meters tokens, so with cost-2
  traffic in the mix a request count can never reach 100%, and reporting it that way makes a healthy
  client look throttled.
- **Each vendor is timed over its own window.** Sharing one wall clock divides every vendor's count
  by the slowest vendor's drain, so a lumpy 10s-interval client with a multi-second tail makes its
  well-behaved neighbours look throttled.

The `store` vendor legitimately exceeds 100%: its `maxTokens` is twice its refill, so a full bucket
at t=0 is real burst allowance that a short window still counts.

### clientTypes

All four rate-limit strategies against identical load. See
[rate limits](rate-limits/README.md) for what each one does.

`sharedLimit` is addressed by its parent's name, which is the whole point of the type and worth
exercising rather than asserting.

### burst

Idle, then far more than the budget at once. Checks the thing a rate limiter exists for: that a
burst is absorbed and released at the configured rate rather than dropped or let through. The drain
time is compared against what the budget mathematically implies.

### clients

The cost of registering 1, 10, 50 and 200 clients on one handler. Build time is reported separately
from throughput because they answer different questions: how long a cold start takes, and whether a
large client map slows the request path afterwards. It does not.

### queue

Admission cost when a rate limit is actually binding, so every request takes the queued path rather
than the uncontended fast path most scenarios measure.

### replicas

N separate OS processes sharing one budget — the arrangement dianemo exists for, and the one a
single-process benchmark cannot say anything about. Two framings: each replica offering its own load
(what a growing deployment does), and total load held constant (which isolates coordination cost
from added load).

**Redis only, deliberately.** On the memory backend each replica would enforce its own private copy
of the limit, so the combined figure would look excellent and mean nothing. The runner skips it and
says so rather than printing a flattering number.

### sustained _(opt-in)_

A long run — 100,000 requests by default — reporting throughput per window rather than one average,
so decay, GC pauses and memory growth are visible instead of averaged away.

```bash
SUSTAINED_TOTAL=500000 SUSTAINED_CONCURRENCY=200 npm run bench -- sustained
```

The drift check compares the **median of each half**, discarding the first window. Comparing means
against window 1 instead would report a large spurious "rise" on a run that was flat throughout,
because window 1 is still JIT-warming.

### backendOps _(opt-in)_

Coordination cost with **no HTTP in the path** — the only scenario that compares the backends
against each other rather than against the upstream ceiling. Runs both backends regardless of
`--backend`, because a one-sided number answers nothing.

## Reading the results honestly

Four ways to get a meaningless number out of this suite:

- **Comparing unlike things.** The baseline must use the same HTTP client at the same concurrency,
  after the same warmup.
- **Measuring a stale build.** `npm run bench` runs against `packages/*/dist`. A number measured
  against an unbuilt or half-built `dist` is meaningless — and TypeScript will happily emit output
  alongside errors unless `noEmitOnError` is set, which it is.
- **Trusting a ratio near the ceiling.** If both sides are within a few percent of the baseline, you
  have measured the loopback server.
- **Forgetting Little's law.** A worker pool sized from _upstream_ latency is too small once queue
  wait is included, and the driver, not the limiter, becomes what caps throughput.
