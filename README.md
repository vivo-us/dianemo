<p align="center">
  <img src="assets/dianemo-banner.webp" alt="dianemo — distributed API rate limiter" width="720">
</p>

<p align="center">
  <strong>διανέμω</strong> — <em>to distribute, to apportion.</em><br>
  One API rate limit, shared correctly across every process that draws on it.
</p>

A host for outbound HTTP clients that share a rate limit across processes.

Most rate limiters assume one process. Run four replicas behind a load balancer and each
independently believes it owns the vendor's full budget, so you exhaust the limit at 4× the rate you
configured. Dianemo coordinates through a shared backend, so N replicas draw from one budget.

## What it does that a per-process limiter doesn't

- **Shared token buckets.** One budget per credential, refilled lazily and drawn down by every
  replica through the backend. Adding replicas doesn't multiply your request rate.
- **Priority queue with cost-weighted spending.** A request declares a `priority`, so a bulk backfill
  at priority 1 is admitted behind interactive traffic at priority 5 instead of starving it, and a
  `cost` for how many tokens it consumes. Ordering is by priority, then retry count, then arrival —
  `cost` determines what a request spends, not where it sits in the queue.
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

```bash
# One process, and it will stay one process.
npm install @dianemo/core

# More than one process — a replica set, a worker pool, a pod that autoscales.
npm install @dianemo/core @dianemo/backend-redis ioredis
```

ESM only, Node 20+.

## Quick start

```ts
import { Redis } from "ioredis";
import RequestHandler from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";
import fedex from "@dianemo/plugin-fedex";

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY, // encrypts stored credentials
  backend: redisBackend(new Redis(process.env.REDIS_URL)),
  logger: console, // optional; silent by default
});

export const requests = handler.use(fedex);

// Credentials arrive separately — from a vault, a database, or env.
await handler.addTemplateClient("fedex", {
  instanceId: "production",
  clientId: process.env.FEDEX_CLIENT_ID,
  clientSecret: process.env.FEDEX_CLIENT_SECRET,
  baseUrl: "https://apis.fedex.com",
});

// Typed from exactly the plugins you passed to use().
await requests.fedex.cancelShipment("fedex:_:production", {
  trackingNumber: "123456789012",
});
```

`handler.start()` is called on the first request and is idempotent. It registers SIGTERM/SIGINT/SIGHUP
handlers so the queue drains on shutdown: `stop()` waits for queued and in-flight work to finish, up
to a 10-second grace period (`setDrainTimeout` changes it), then fails anything still waiting with a
`ClientUnavailableError` rather than leaving it to time out after the process meant to exit.

[Getting started](docs/getting-started.md) explains what each piece above is for.

## Choosing a backend

The one decision worth getting right before anything else, because it is not a performance knob:

```ts
import RequestHandler, { memoryBackend } from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";

new RequestHandler({ key, backend: memoryBackend() }); // single process ONLY
new RequestHandler({ key, backend: redisBackend(redis) }); // any number of processes
```

**`memoryBackend()` keeps the budget in local variables, so every process running it gets its own
private copy of every rate limit.** Two processes configured for 100 requests a second will send
200; four will send 400. Nothing in dianemo can detect this and the vendor will not warn you — the
first signal is usually a 429 storm or a suspended key. The moment a second process exists, switch
to a distributed backend.

If you are unsure, use Redis. Being wrong in that direction costs one connection; being wrong in the
other direction costs your API access.

See [choosing a backend](docs/backends/README.md) for the full comparison.

## Documentation

The manual lives in [`docs/`](docs/README.md).

- [Getting started](docs/getting-started.md) — a working setup, explained in the order the pieces matter
- [Concepts](docs/concepts.md) — sub-clients, grants, overrides, leader election, freeze/thaw
- [Choosing a rate limit](docs/rate-limits/README.md) — the four strategies, side by side
- [Several limits at once](docs/rate-limits/multiple-limits.md) — per-second *and* per-day, or a rate *and* a concurrency ceiling
- [Choosing a backend](docs/backends/README.md) — where shared state lives
- [Writing a plugin](docs/writing-plugins.md) — packaging an integration
- [Benchmarks](docs/benchmarks.md) — what each scenario measures, and the numbers
- [Roadmap](docs/roadmap.md) — what is planned, and why

## Packages

| Package                                            | What it is                                                                                     |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| [`@dianemo/core`](packages/core)                   | The handler, the plugin system, and the in-memory backend. Depends only on `axios` and `@opentelemetry/api`. |
| [`@dianemo/backend-redis`](packages/backend-redis) | Redis backend. Also covers Valkey and DragonflyDB, which are wire-compatible.                  |

Integrations are published separately, one package per vendor, from
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins) — FedEx, UPS, Shopify, Stripe, Amazon
SP-API and others, each carrying that vendor's rate-limit calibration and auth flow. They release on
their own schedule, so a particular vendor's package may not be on npm yet; the quick start above
shows the shape a plugin gives you rather than a guaranteed install.

## History

Dianemo has been running in production since 2023. It began as the outbound request layer inside a
private ERP monorepo and grew as the products built on it did — every rate-limit type in the library
exists because something needed it, not because the design felt incomplete without it.

Today it carries **over a million requests a day across 30+ integrations**, with all four rate-limit
types, sub-clients, grants, and freeze/thaw recovery in active use.

The git history here reflects none of that. This repository is the extracted and sanitized form of
that work: lifted out of the monorepo it grew up in, stripped of credentials and application
coupling, and rebuilt as a standalone library with a pluggable backend. The history starts at v1.0.0
because that is when it became something worth handing to someone else — not when it was written.

## Development

This is an npm-workspaces monorepo managed with Nx. Packages are versioned **independently**, so a
fix to the Redis backend does not bump core.

```
packages/core            @dianemo/core            interface + memory backend
packages/backend-redis   @dianemo/backend-redis   Redis/Valkey/DragonflyDB backend
```

```bash
npm install
npm run build          # nx run-many -t build
npm test               # vitest, whole workspace
npm run check          # typecheck every package against source
npm run lint
npm run format:fix
```

Releases use Nx version plans, the same idea as a changeset. Add one for any change to a published
package:

```bash
npm run version-plan          # writes .nx/version-plans/<name>.md
npm run release               # applies plans, bumps versions, writes CHANGELOGs
npm run release:publish       # publishes to npm
```

### Tests

`test/conformance.test.ts` is the important one: **a single suite run against every backend**. Two
backends that disagree about ordering, fairness or refill timing are two different products wearing
one name, so the token bucket, fast-path admission, concurrency slots, queue ordering, freeze/thaw
probing, locks and pub/sub are asserted identically against both. Adding a backend means making this
suite pass — nothing else is required, and nothing less is sufficient.

`test/handler.integration.test.ts` runs a whole handler end to end on each backend, asserting through
the backend interface rather than poking at storage. `test/lifecycle.integration.test.ts` covers the
real `start()`/`stop()` cycle against live Redis. Unit tests cover client naming, the error
hierarchy, `tryHandleRequest` wrapping, and plugin composition.

Type-level tests in `test/typeInference.test-d.ts` guard plugin inference. If `use()` ever degraded
to returning `any`, the runtime tests would still pass while every consumer silently lost type
safety, so that case is asserted explicitly.

The memory backend runs everywhere with no setup. Redis-backed cases skip unless `REDIS_URL` is set:

```bash
docker run -d -p 6399:6379 redis:7-alpine
REDIS_URL=redis://localhost:6399 npm test
```

## License

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
