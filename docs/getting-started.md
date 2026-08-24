# Getting started

The README gets you to a first request. This page explains what you just built,
in the order the pieces actually matter.

## Install

```bash
# More than one process — a replica set, a worker pool, a pod that autoscales
npm install @dianemo/core @dianemo/backend-redis ioredis

# One process, and it will stay one process
npm install @dianemo/core
```

ESM only, Node 20+.

## 1. The backend decides whether your limit is real

Do this first, because everything else assumes it.

```ts
import { Redis } from "ioredis";
import RequestHandler from "@dianemo/core";
import { redisBackend } from "@dianemo/backend-redis";

const redis = new Redis(process.env.REDIS_URL);

const handler = new RequestHandler({
  key: process.env.REQUEST_HANDLER_KEY, // 32 chars; encrypts credentials at rest
  backend: redisBackend(redis),
  logger: console, // optional; silent by default
});
```

`memoryBackend()` from `@dianemo/core` is the alternative, and it is correct
**only for a single process** — each process would otherwise enforce its own
private copy of every limit and quietly send N times the agreed rate. See
[choosing a backend](backends/README.md).

The connection is yours. Dianemo duplicates it for pub/sub and closes only that
duplicate, because in most processes that connection is doing other work too.

## 2. A template is a shape, not a credential

Declare the template's name and credential shape first. `ClientTemplates` is
empty until you augment it, so `registerClientTemplate("acme", …)` and
`addTemplateClient("acme", …)` do not compile without this block — that is what
makes a typo in a template name a compile error rather than a runtime
warn-and-skip.

```ts
import { buildClientName } from "@dianemo/core";
import type { OAuth2Credentials } from "@dianemo/core/client/types";

declare module "@dianemo/core" {
  interface ClientTemplates {
    acme: OAuth2Credentials;
  }
}
```

```ts
await handler.registerClientTemplate("acme", (creds) => [
  {
    name: buildClientName("acme", creds),
    rateLimit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 100,
      maxTokens: 100,
    },
    requestOptions: { defaults: { baseURL: creds.baseUrl } },
  },
]);
```

The template describes how the client behaves — its budget, its auth flow, its
base URL. It carries no secrets, which is what makes it publishable as a plugin.

Pick the rate limit from what the vendor publishes. If they say "600 per
minute", resist writing `interval: 60000, tokensToAdd: 600` — the average is
right and the tail is terrible. See
[`requestLimit`](rate-limits/request-limit.md) for why granularity matters more
than it looks.

## 3. Credentials arrive separately, at runtime

```ts
await handler.addTemplateClient("acme", {
  instanceId: "production",
  baseUrl: "https://api.acme.com",
  clientId: process.env.ACME_CLIENT_ID,
  clientSecret: process.env.ACME_CLIENT_SECRET,
});
```

This is the moment a _client_ exists. Credentials are encrypted with your `key`,
stored in the backend and broadcast to every replica — so one process adding a
credential makes it usable everywhere, without a redeploy.

Client names are `<template>:<organizationId | _>:<alias>`. The `_` is the
placeholder for "no organisation", which is the common case, and
`buildClientName` from `@dianemo/core` is what assembles one — it rejects a
segment containing `:` or whitespace, either of which would let two tenants
resolve to one client and one credential entry.

## 4. Making requests

```ts
const res = await handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.list",
  method: "GET",
  url: "/orders",
});
```

`handler.start()` is called automatically on the first request and is
idempotent. It registers SIGTERM/SIGINT/SIGHUP handlers so the queue drains on
shutdown.

`requestName` is not cosmetic — it appears in spans, logs and queue metadata.
Name the operation, not the URL.

### Cost and priority

```ts
await handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.export",
  method: "POST",
  url: "/orders/export",
  cost: 10, // this endpoint counts as ten calls
  priority: 1, // 0–10, higher first; keep bulk work behind interactive
});
```

Vendors rarely price every endpoint the same, and a backfill should not starve
the traffic a user is waiting on.

## 5. What happens under contention

Worth knowing before you debug it.

- **Uncontended requests skip the queue.** When nothing is waiting and the
  budget covers the request, one atomic operation admits it. The queue exists to
  order requests competing for a budget; when nothing is competing there is
  nothing to order.
- **The moment anything queues, new arrivals queue behind it.** The fast path
  cannot let a late request overtake an earlier one.
- **One process drains each client's queue.** The rest enqueue and await, so
  recovery is not a thundering herd.
- **A hard rate-limit response freezes the client**, and recovery is probed by
  exactly one request across the whole fleet rather than every replica retrying
  into a closed door.

## 6. Errors

```ts
import { RequestError, getOriginalStatus } from "@dianemo/core";

try {
  await handler.handleRequest({ ... });
} catch (err) {
  if (err instanceof RequestError) {
    console.error(err.code, getOriginalStatus(err));
  }
}
```

Every error carries `statusCode` and `code`, so a host that does not import
dianemo's classes can still duck-type them. Retries with backoff are automatic;
what surfaces is what remained after them.

## 7. Plugins, when you have more than one integration

```ts
import fedex from "@dianemo/plugin-fedex";
import ups from "@dianemo/plugin-ups";

export const requests = handler.use(fedex, ups);

await requests.fedex.cancelShipment("fedex:_:production", { ... });
```

A plugin bundles a template with its request functions. The namespace is
inferred from the plugins you pass — there is no module augmentation to write,
and a plugin you didn't pass doesn't appear on the result.

Ready-made plugins are published one package per vendor from
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins), on their own release
schedule — check npm for the vendor you need rather than assuming it is there. Steps
1–6 above never touch a plugin, so an integration without one is written exactly the
same way, and [writing a plugin](writing-plugins.md) packages it when you want to
share it.

## Production checklist

- [ ] Redis backend if there is any chance of a second process
- [ ] Redis runs `maxmemory-policy noeviction` — an evicted key reads as a full
      budget and the fleet overspends silently
      ([why](backends/redis.md#operational-notes))
- [ ] `REQUEST_HANDLER_KEY` is a real 32-character secret, not a literal
- [ ] Rate limits reflect published vendor limits, at the finest sensible interval
- [ ] `cost` set on endpoints the vendor prices higher
- [ ] Bulk work runs at a lower `priority` than interactive traffic
- [ ] A logger is passed, or you have accepted silence
- [ ] `@opentelemetry/api` has a tracer provider registered, if you want spans
- [ ] Shutdown lets `stop()` finish so queued work drains

## Where to go next

- [Concepts](concepts.md) — sub-clients, grants, overrides, leader election
- [Choosing a rate limit](rate-limits/README.md)
- [Choosing a backend](backends/README.md)
- [Writing a plugin](writing-plugins.md)
- [Benchmarks](benchmarks.md)
