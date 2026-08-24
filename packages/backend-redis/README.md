# @dianemo/backend-redis

Redis backend for [dianemo](https://github.com/vivo-us/dianemo) — the one to use whenever more than
one process shares an API budget.

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

## Why this rather than the memory backend

Every process talks to the same Redis, so the token bucket, the request queue and the freeze state
are **one set of numbers rather than one set per process**. A vendor that allows 100 requests a
second sees 100 requests a second whether you run one replica or twenty.

The memory backend in `@dianemo/core` gives each process its own private copy of every limit. That
is correct for a single-process program and silently wrong for anything else — four replicas would
send four times the agreed rate with no error from dianemo and no warning from the vendor. If your
process count is not exactly one, or might not stay exactly one, use this package.

It is also a fine choice for a single process. Being wrong in this direction costs one Redis
connection; being wrong in the other direction costs your API access.

## Valkey, DragonflyDB and friends

Both are wire-compatible with Redis, so no separate adapter exists or is needed — construct their
connection with `ioredis` and pass it to `redisBackend()` unchanged.

## You own the connection

The backend duplicates your connection for its pub/sub listener, because a connection in subscriber
mode cannot serve any other command. `close()` closes **only that duplicate**. The connection you
passed in is yours: dianemo will not quit a connection it did not create, since in most processes
that connection is doing other work too.

Keys are namespaced by the handler's `keyPrefix`, so several handlers can share one connection
without colliding.

## Redis must not evict dianemo's keys

Run it with `maxmemory-policy noeviction`, or give dianemo an instance that is not also a cache. An
evicted token bucket is indistinguishable from one that was never written, and a bucket that was
never written reads as a **full** budget — so the fleet resumes spending without waiting for a
refill, and an evicted freeze sends at full rate into a vendor that just rate limited you. Nothing
reports it.

TTLs are not protection: nearly every key dianemo writes carries one, which makes them candidates
under `volatile-lru` and its siblings rather than exempt. See the
[operational notes](https://github.com/vivo-us/dianemo/blob/master/docs/backends/redis.md#operational-notes).

## How atomicity is achieved

Every operation that spends from a shared budget has to be atomic against every other process
spending from it — between a read and a write from one process, another will interleave. All such
operations are therefore Lua scripts, executed inside Redis rather than in the client.

Scripts are registered as ioredis custom commands so calls send `EVALSHA` rather than the full
script body, falling back to `EVAL` once if Redis reports `NOSCRIPT` after a restart or a
`SCRIPT FLUSH`.

## Conformance

This backend is verified by the same suite as every other backend, in `test/conformance.test.ts` at
the repository root. Adding a backend means making that suite pass.

## Documentation

[Redis backend reference](https://github.com/vivo-us/dianemo/blob/master/docs/backends/redis.md) —
operational notes, clock-skew behaviour and what the coordination costs.
[Full documentation](https://github.com/vivo-us/dianemo#documentation).

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
