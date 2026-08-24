# Choosing a backend

A backend is where the shared state lives: the token buckets, the request queue,
the freeze state, distributed credentials and instance discovery.

**This is not a performance knob.** It decides whether your rate limit is real.

|                                 | [`memoryBackend()`](memory.md) | [`redisBackend(conn)`](redis.md) |
| ------------------------------- | ------------------------------ | -------------------------------- |
| Package                         | `@dianemo/core`                | `@dianemo/backend-redis`         |
| Dependencies                    | none                           | `ioredis`                        |
| Safe process count              | **exactly 1**                  | any                              |
| Budget shared between processes | no                             | yes                              |
| Survives a restart              | no                             | yes                              |
| OAuth tokens shared             | no                             | yes                              |
| `backend.distributed`           | `false`                        | `true`                           |

## The decision

Ask one question: **how many processes will run this handler?**

If the answer is "one, and it will stay one" — a CLI, a cron job, a desktop
app, a test suite, a single-instance service — the memory backend is correct and
costs you nothing.

For anything else, use Redis. And if you are unsure, use Redis: being wrong in
that direction costs one connection, while being wrong in the other direction
costs your API access.

## Why getting this wrong is quiet

The memory backend keeps the budget in local variables. Two processes each get a
full private copy of every limit, so a client configured for 100 requests a
second sends 200. Four processes send 400.

Nothing detects this. Dianemo cannot see the other processes, and the vendor
will not tell you — the first signal is usually a 429 storm, a throttled
account, or a suspended key. There is no log line to grep for, because from each
process's point of view everything is working perfectly.

Every backend declares which kind it is, so you can refuse to start:

```ts
if (process.env.REPLICAS !== "1" && !backend.distributed) {
  throw new Error(
    "Refusing to start: a memory backend cannot share a rate limit."
  );
}
```

## Both backends behave identically otherwise

Ordering, fairness, refill timing, freeze/thaw probing, cost weighting and
priority all behave the same whichever backend you choose. That is enforced
rather than asserted: `test/conformance.test.ts` is a single suite run against
every backend, and adding a backend means making it pass.

## Speed is the wrong reason to choose

The memory backend is 6–24× faster on raw coordination operations and about 8%
faster end to end, because a real request spends nearly all its time in HTTP
rather than in coordination. Once a rate limit actually binds, the two are
indistinguishable — the limiter sets the pace.

See [benchmarks](../benchmarks.md) for the numbers. Choosing memory to gain
throughput trades a correctness guarantee for a few percent you will only notice
if your upstream is faster than your network.

## Writing another backend

The contract is `DianemoBackend` in `packages/core/src/backend/types.ts`. It is
deliberately a set of **domain operations** (`acquireTokens`,
`tryAdmitImmediately`, `addRequest`) rather than storage primitives, because
every one of them must be atomic with respect to other spenders. Each backend
satisfies that in whatever way is natural for it — Lua scripts in Redis, or
simply being synchronous in memory.

Implement the interface, then run the conformance suite against it.
