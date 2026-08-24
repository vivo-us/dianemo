<p align="center">
  <img src="https://raw.githubusercontent.com/vivo-us/dianemo/master/assets/dianemo-banner.webp" alt="dianemo — distributed API rate limiter" width="720">
</p>

<p align="center">
  <strong>διανέμω</strong> — <em>to distribute, to apportion.</em><br>
  One API rate limit, shared correctly across every process that draws on it.
</p>

A host for outbound HTTP clients that share a rate limit across processes.

Most rate limiters assume one process. Run four replicas behind a load balancer and each
independently believes it owns the vendor's full budget, so you exhaust the limit at 4× the rate you
configured. Dianemo coordinates through a shared backend, so N replicas draw from one budget.

**Full documentation:** [github.com/vivo-us/dianemo](https://github.com/vivo-us/dianemo#documentation)

## What it does that a per-process limiter doesn't

- **Shared token buckets.** One budget per credential, refilled lazily and drawn down by every
  replica through the backend. Adding replicas doesn't multiply your request rate.
- **Cost-weighted priority queue.** A request declares a `cost` (how many tokens it consumes) and a
  `priority`, so a bulk backfill is admitted behind interactive traffic instead of starving it.
- **Freeze and thaw with single-flight probing.** When a vendor returns a hard limit, the client
  freezes. Recovery is probed by exactly one request across the whole fleet, rather than every
  replica retrying into a closed door.
- **Per-credential isolation.** One tenant hitting their ceiling doesn't stall anyone else's traffic
  through the same integration.
- **Leader election.** One process drains each client's queue; the rest enqueue and await.
- **Auth flows built in.** OAuth2 client-credentials with automatic refresh, static tokens and basic
  auth, with credentials supplied separately from client definitions.
- **OpenTelemetry spans** carrying `dianemo.cost`, `dianemo.priority` and `rate_limited` events when
  `@opentelemetry/api` has a tracer provider registered. A no-op otherwise.

## Install

Dianemo ships the coordination logic in `@dianemo/core` and the shared state in a **backend** you
choose. Which one you need is decided by a single question: _how many processes will run this?_

```bash
# One process, and it will stay one process.
npm install @dianemo/core

# More than one process — a replica set, a worker pool, a pod that autoscales.
npm install @dianemo/core @dianemo/backend-redis ioredis
```

ESM only, Node 20+. `ioredis` is a peer dependency of the Redis backend because you pass your own
connection in; two copies in one process would coordinate against different clients.

`@opentelemetry/api` is a direct dependency of core. It is itself dependency-free and no-ops unless
your application registers a tracer provider, so it costs nothing if you do not use tracing.

## Choosing a backend

This is the most consequential decision in the library, and it is not a performance tuning knob.

|                                 | `memoryBackend()` | `redisBackend(connection)` |
| ------------------------------- | ----------------- | -------------------------- |
| Package                         | `@dianemo/core`   | `@dianemo/backend-redis`   |
| Dependencies                    | none              | `ioredis`                  |
| Safe process count              | **exactly 1**     | any                        |
| Budget shared between processes | no                | yes                        |
| Survives a restart              | no                | yes                        |
| OAuth tokens shared             | no                | yes                        |

**The memory backend keeps the budget in local variables, so every process that runs it gets its own
private copy of every rate limit.** Two processes configured for 100 requests a second will send
200; four will send 400. Nothing in dianemo can detect this, and the vendor will not warn you — the
first signal is usually a 429 storm or a suspended key.

Use it when there is one process and there will stay one: a CLI, a cron job, a desktop app, a test
suite, a single-instance service. **The moment a second process exists — a replica, a worker, a
second pod, a `pm2 -i 2` — switch to a distributed backend.** That is not an optimisation; it is the
only thing that makes the limit real. If you are unsure, use Redis: being wrong in that direction
costs one connection, and being wrong in the other direction costs your API access.

Anything wire-compatible with Redis works unchanged — **Valkey** and **DragonflyDB** both speak the
same protocol, so `redisBackend()` takes their connections as-is.

[Full comparison](https://github.com/vivo-us/dianemo/blob/master/docs/backends/README.md)

## Quick start

```ts
import { Redis } from "ioredis";
import RequestHandler from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";
import fedex from "@dianemo/plugin-fedex";
import ups from "@dianemo/plugin-ups";

const redis = new Redis(process.env.REDIS_URL);

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY, // encrypts credentials at rest
  backend: redisBackend(redis),
  logger: console, // optional; silent by default
});

export const requests = handler.use(fedex, ups);

// Credentials arrive separately — from a vault, a database, or env.
await handler.addTemplateClient("fedex", {
  instanceId: "production",
  clientId: process.env.FEDEX_CLIENT_ID,
  clientSecret: process.env.FEDEX_CLIENT_SECRET,
  baseUrl: "https://apis.fedex.com",
});

// Fully typed from the plugins you passed to use().
await requests.fedex.cancelShipment("fedex:_:production", {
  accountNumber: { value: process.env.FEDEX_ACCOUNT_NUMBER },
  trackingNumber: "123456789012",
});
```

`handler.start()` is called automatically on the first request and is idempotent. It registers
SIGTERM/SIGINT/SIGHUP handlers so the queue drains on shutdown.

For a single-process program, swap two lines and drop the dependency entirely:

```ts
import RequestHandler, { memoryBackend } from "@dianemo/core";

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY,
  backend: memoryBackend(), // single process only — see "Choosing a backend"
});
```

Integrations are published one package per vendor from
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins), on their own release schedule — check
npm for the vendor you need rather than assuming it is there. Nothing waits on them:
`registerClientTemplate` plus `handleRequest` covers a vendor with no plugin, and
[writing your own](https://github.com/vivo-us/dianemo/blob/master/docs/writing-plugins.md) needs only
`@dianemo/core`.

## Concepts

| Term         | Meaning                                                                                    |
| ------------ | ------------------------------------------------------------------------------------------ |
| **Template** | The _shape_ of an integration: rate limit, auth flow, sub-clients. Carries no credentials. |
| **Client**   | A template plus one set of credentials, named `<template>:<orgId \| _>:<alias>`.           |
| **Plugin**   | A template plus the request functions that use it, packaged together.                      |
| **Grant**    | An independently rate-limited credential within a client.                                  |

Templates and credentials are deliberately separate. A template is code you ship; credentials are
data that arrives at runtime, encrypted into the backend and broadcast to every replica. That
separation is why a plugin never has to know whether its credentials came from a vault, a database,
or an environment variable.

[Sub-clients, grants, overrides, leader election and freeze/thaw](https://github.com/vivo-us/dianemo/blob/master/docs/concepts.md)

## Errors

Every error carries `statusCode` and `code` alongside `message`, so an HTTP layer can map them
without importing this package:

```ts
if (typeof err.statusCode === "number" && typeof err.code === "string") {
  res
    .status(err.statusCode)
    .json({ error: err.code, error_description: err.message });
}
```

Retries with backoff are automatic; what surfaces is what remained after them. `getOriginalStatus(err)`
and `getOriginalResponseData(err)` read the underlying vendor response off a `RequestError`, for
callers that want to turn a 404 into `null`.

[The full error reference](https://github.com/vivo-us/dianemo/blob/master/docs/concepts.md#errors)

## Logging

Pass anything with `debug`/`info`/`warn`/`error` accepting `(obj, msg)` or `(msg)`. A `pino`
instance satisfies this structurally. The default is silence: a library embedded in someone else's
process shouldn't claim their stdout uninvited.

## Performance

Roughly **6–8% overhead** over the same HTTP client at the same concurrency, measured against a
loopback upstream so the figure reflects coordination cost rather than vendor latency. Uncontended
requests skip the queue entirely, deciding admission in one atomic backend operation.

[Methodology, scenarios and the full results](https://github.com/vivo-us/dianemo/blob/master/docs/benchmarks.md)

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
