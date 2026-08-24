# Redis backend

```bash
npm install @dianemo/core @dianemo/backend-redis ioredis
```

```ts
import { Redis } from "ioredis";
import RequestHandler from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";

const redis = new Redis(process.env.REDIS_URL);

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY,
  backend: redisBackend(redis),
});
```

## Why this one

Every process talks to the same Redis, so the token bucket, the request queue
and the freeze state are **one set of numbers rather than one set per process**.
A vendor that allows 100 requests a second sees 100 requests a second whether
you run one replica or twenty.

It is also a perfectly good choice for a single process. If you are unsure which
backend you need, use this one.

## Valkey, DragonflyDB and friends

Both are wire-compatible with Redis, so no separate adapter exists or is needed.
Construct their connection with `ioredis` and pass it to `redisBackend()`
unchanged.

## You own the connection

The backend duplicates your connection for its pub/sub listener, because a
connection in subscriber mode cannot serve any other command. `close()` closes
**only that duplicate**.

The connection you passed in is yours. Dianemo will not quit a connection it did
not create, since in most processes that connection is doing other work too.

**One backend per handler, one handler per process.** A backend owns a single
pub/sub duplicate, and `close()` quits it — so sharing one backend between two
handlers means either handler's `stop()` leaves the other receiving no
coordination messages at all. Give each handler its own `redisBackend()`.

## How atomicity is achieved

Every operation that spends from a shared budget has to be atomic against every
other process spending from it — between a read and a write from one process,
another will interleave. All such operations are therefore Lua scripts, executed
inside Redis rather than in the client.

Scripts are registered as ioredis custom commands, so calls send `EVALSHA`
rather than the full script body. Shipping the text every time sends kilobytes of
script per request across the scripts a single request touches, which becomes
megabytes a second at a few thousand requests a second. There is a one-time
`EVAL` fallback if Redis reports `NOSCRIPT` after a restart or a `SCRIPT FLUSH`.

## What it costs

The handler is **round-trip-bound, not Redis-bound**. In benchmarking, Redis sat
at 7.6% CPU with capacity for 312K commands/second against 63K consumed — the
limit was the number of sequential round-trips per request, not Redis itself.

That is why the uncontended fast path matters: it decides admission in a single
atomic script, taking a request from nine round-trips down to two, or one for a
`noLimit` client. See [benchmarks](../benchmarks.md).

## Operational notes

- **Redis must be reachable before `start()`.** The handler does not construct
  connections or read `REDIS_*` environment variables; you build the connection
  and hand it over.
- **Redis must not evict dianemo's keys.** Run it with
  `maxmemory-policy noeviction`, or give dianemo an instance that is not also a
  cache. An evicted token bucket is indistinguishable from one that was never
  written, and a bucket that was never written reads as a **full** budget — so
  the fleet resumes spending from the top of the interval without waiting for a
  refill. An evicted freeze reads as no freeze, and the fleet goes back to full
  rate into a vendor that has just rate limited you. Neither leaves a trace: no
  error, no log line, only the vendor's counters disagreeing with yours.

  **A TTL is not protection.** Nearly every key dianemo writes carries one — a
  token bucket expires a day after its last use, an instance registration in
  seconds — which makes them _candidates_ under `volatile-lru` and the other
  `volatile-*` policies rather than exempt from them. Only the instance and
  template registries are TTL-less. `noeviction` is the setting; there is no
  safe middle ground.

- **A shared Redis is fine for collisions, not for memory.** Keys are prefixed,
  so dianemo can live alongside a cache or a queue in the same instance and
  neither sees the other's keys. What the two cannot share is an eviction
  policy, and a cache is usually tuned to evict. If you must share one instance,
  make it `noeviction` and size it for the whole working set — a cache can still
  bound itself with TTLs, which expire under `noeviction` exactly as they do
  under any other policy.
- **Failover.** ioredis handles reconnection. Coordination state is rebuilt from
  what remains; a lost queue means in-flight requests are re-driven by their
  callers rather than silently dropped.
- **Replica clock skew does not affect coordination.** Every script that decides
  fleet-wide state reads Redis's own clock with `TIME`, so a freeze deadline, a
  token bucket's `lastUpdate` and a concurrency slot's age are all written and
  compared against one clock no matter which replica got there first. You do not
  need replica clocks in sync for the budget to hold.

  The one value that does come from the calling process is a request's **arrival
  timestamp**, which is approximate by design: skew between replicas can reorder
  requests _within_ one priority band and never across bands. See
  [Concepts](../concepts.md#cost-and-priority).
