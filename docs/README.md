# dianemo documentation

This is the manual. The [package README](../packages/core/README.md) is the
short version — enough to install and make a first request.

New here? **[Getting started](getting-started.md)** walks through a working
setup and explains what each piece is for.

## Guides

- [Getting started](getting-started.md) — a working setup, explained in the
  order the pieces actually matter
- [Writing a plugin](writing-plugins.md) — packaging an integration
- [Benchmarks](benchmarks.md) — what each scenario measures, how to read it, and
  the numbers

## Concepts

- [Concepts](concepts.md) — client names, sub-clients, grants, overrides,
  leader election, freeze/thaw, cost and priority, errors
- [Design notes](design-notes.md) — for contributors: clock ownership, queue
  score bands, the two-backend contract and the invariants behind them

## Rate limits

A client picks a strategy per budget it has to respect — usually one, sometimes
several. That choice decides how the client behaves under contention, so it is
worth matching to the limit the vendor actually publishes rather than to the one
that is easiest to configure.

- [Choosing a rate limit](rate-limits/README.md) — the decision, side by side
- [`noLimit`](rate-limits/no-limit.md) — no budget to coordinate
- [`requestLimit`](rate-limits/request-limit.md) — a token bucket, the common case
- [`concurrencyLimit`](rate-limits/concurrency-limit.md) — N in flight at once
- [`sharedLimit`](rate-limits/shared-limit.md) — several credentials, one budget
- [Several limits at once](rate-limits/multiple-limits.md) — per-second *and*
  per-day, or a rate *and* a concurrency ceiling

## Backends

Where the shared state lives. This is the most consequential decision in the
library and it is not a performance knob.

- [Choosing a backend](backends/README.md) — start here
- [Memory backend](backends/memory.md) — single process **only**
- [Redis backend](backends/redis.md) — anything larger, plus Valkey/DragonflyDB

## Integrations

Ready-made plugins are published one package per vendor from
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins). To write your
own, see [writing a plugin](writing-plugins.md).

## Project

- [Roadmap](roadmap.md) — what is planned, and why
- [Contributing](../CONTRIBUTING.md) — how to verify a change, and the bar for
  comments and docs
