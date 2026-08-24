# Memory backend

```bash
npm install @dianemo/core   # nothing else
```

```ts
import RequestHandler, { memoryBackend } from "@dianemo/core";

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY,
  backend: memoryBackend(),
});
```

## Single process only

This backend keeps the budget in local variables. **Every process that runs it
gets its own private copy of every rate limit.** Two processes configured for
100 requests a second will send 200; four will send 400.

Nothing reports this. Dianemo cannot see the other processes, and the vendor
will not warn you — see [choosing a backend](README.md#why-getting-this-wrong-is-quiet).

Use it when there is one process and there will stay one:

- a CLI or one-shot script,
- a cron job,
- a desktop application,
- a test suite,
- a genuinely single-instance service.

The moment a second process exists — a replica, a worker, a second pod, a
`pm2 -i 2`, an autoscaler that can reach 2 — switch to
[`@dianemo/backend-redis`](redis.md). That is not an optimisation; it is the
only thing that makes the limit real.

## Why it is so much simpler than the Redis backend

The Lua scripts on the Redis side exist because two processes can interleave
between a read and a write. Inside one Node process they cannot: a synchronous
function body runs to completion before anything else does.

So every operation here is a plain synchronous function, and its atomicity is a
property of the language rather than something to be arranged. A ~40-line Lua
script becomes about eight lines of arithmetic with no locking at all. That is
also why it is fast — nothing leaves the process.

The `atomic` option on `batch()` needs no special handling for the same reason:
nothing can interleave with a synchronous loop.

## What you give up besides distribution

- **Durability.** State lives in memory. A restart loses the queue, the token
  buckets and any cached OAuth tokens. Redis would have kept them.
- **Shared credentials.** OAuth tokens are per-process, so a refresh benefits
  only the process that performed it. Under a distributed backend one replica's
  refresh serves the whole fleet.
- **Cross-process broadcasts.** `awaitClient` will never receive a
  `templateClientAdded` broadcast from a peer. This is harmless in a single
  process — the client registers locally and the wait resolves immediately.

## Expiry

Keys expire lazily: a TTL is checked on read, and an unreferenced sweep runs on
an unref'd timer so keys nobody reads again cannot pin memory forever. The
sweep's timer is unref'd, so it never keeps a process alive on its own.

## Plugins

Plugins that cache state through `backend()` — a session cookie, a payment token
— still work. Their cache is simply process-local, which makes a caching plugin
repeat work per process rather than misbehave.
