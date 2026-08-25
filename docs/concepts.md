# Concepts

The vocabulary, and the pieces that do not belong to any single rate limit or
backend.

| Term           | Meaning                                                                                    |
| -------------- | ------------------------------------------------------------------------------------------ |
| **Template**   | The _shape_ of an integration: rate limit, auth flow, sub-clients. Carries no credentials. |
| **Client**     | A template plus one set of credentials, named `<template>:<orgId \| _>:<alias>`.           |
| **Sub-client** | A child of a client that shares its endpoint and auth but meters its own budget.           |
| **Grant**      | An independently rate-limited credential _within_ one client.                              |
| **Plugin**     | A template plus the request functions that use it, packaged together.                      |
| **Instance**   | One process running a handler.                                                             |
| **Controller** | The instance currently draining a given client's queue.                                    |

## Client names

```
<templateName>:<organizationId | "_">:<alias>[:<subClientPath>]
```

Position-based, so anything downstream can parse a name without knowing the
template. `_` in the second position means the integration is global rather than
scoped to an organisation.

```ts
import { buildClientName, parseClientName } from "@dianemo/core";

buildClientName("fedex", { instanceId: "production" });
// "fedex:_:production"

parseClientName("fedex:acme-co:production:rest");
// { templateName: "fedex", organizationId: "acme-co",
//   alias: "production", subClientPath: "rest" }
```

`parseClientName` returns `null` for anything with fewer than three segments —
`default`, the handler's no-template client, is the usual case.

Use `parseClientName` in anything that receives a client name from elsewhere: a
metrics listener, a dynamic rate-limit controller, an admin UI. Splitting on `:`
by hand works right up until a sub-client path appears.

## Sub-clients

One vendor, one set of credentials, several endpoint families that the vendor
meters **separately**. A REST API and a bulk-feed API under the same account
often have entirely different quotas.

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
    authentication: {/* ... */},
    subClients: [
      {
        name: "bulk",
        rateLimit: {
          type: "requestLimit",
          interval: 60_000,
          tokensToAdd: 10,
          maxTokens: 10,
        },
      },
      {
        name: "search",
        rateLimit: { type: "concurrencyLimit", maxConcurrency: 4 },
      },
    ],
  },
]);
```

A sub-client **inherits** the parent's endpoint configuration, authentication
and retry options, and **overrides** whatever it declares — most usefully its
own `rateLimit`. Address it by appending its name:

```ts
await handler.handleRequest({
  clientName: "acme:_:production:bulk",
  requestName: "acme.bulk.export",
  method: "POST",
  url: "/bulk/export",
});
```

Sub-clients nest. `acme:_:production:a:b` is `subClients[a].subClients[b]`.

### Sub-client or separate template?

Use a **sub-client** when the credentials and base configuration are genuinely
the same and only the budget differs. Use a **separate template** when the auth
flow or base URL differs — inheritance you have to override entirely is worse
than no inheritance.

Use [`sharedLimit`](rate-limits/shared-limit.md) for the opposite case: separate
credentials that must draw on _one_ budget. Sub-clients split one credential
into several budgets; `sharedLimit` merges several credentials into one.

## Grants

A grant is an independently rate-limited credential inside a single client —
typically one per tenant, where the vendor meters each tenant separately but you
want one client and one template.

Each grant gets its own token bucket and its own freeze state, so one tenant
exhausting their quota does not stall the others, and a 429 for one does not
pause the rest.

On a client with a budget — `requestLimit`, `concurrencyLimit` — grant-scoped
requests deliberately skip the uncontended fast path: that path checks a single
client-level bucket and freeze key, which would be the wrong pair for a grant.
They take the queued path, which reads the right ones.

`noLimit` is the exception, and it is one worth knowing about. It has no bucket,
so its fast path reads the right freeze key for a grant and keeps it — grant
traffic is admitted uncontended, which is the whole point of not configuring a
limit. What it cannot do is answer "is anything waiting?" per grant: there is one
queue per client, and the queue-depth check that stops a new arrival overtaking
work already waiting is necessarily client-wide. So while one tenant has a
backlog, another tenant's arrival takes the queued path behind it.

Isolation is of **budgets and freezes, not of arrival order**. One tenant's 429
never pauses another and one tenant's exhausted quota never spends another's, but
a tenant with a deep backlog does add queueing latency for its neighbours on a
`noLimit` client. Conceding the fast path for all grant traffic would remove that
coupling and cost every grant-scoped request the queued path forever — five extra
backend round trips each, measured — which is the opposite of what a client with
no limit is for.

```ts
await handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.list",
  method: "GET",
  url: "/orders",
  grantId: "tenant-42",
});
```

## Rate-limit overrides

Templates ship in code; the right limit is sometimes discovered in production.
Overrides let a specific `(template, instance)` pair deviate without a redeploy,
keyed by sub-client path:

```ts
await handler.addTemplateClient("acme", credentials, {
  rateLimitOverrides: {
    "": { type: "requestLimit", interval: 1000, tokensToAdd: 50, maxTokens: 50 },
    bulk: {
      type: "requestLimit",
      interval: 60_000,
      tokensToAdd: 5,
      maxTokens: 5,
    },
  },
});
```

`""` targets the parent; a named path targets that sub-client. A bare record of
paths is still accepted in that argument, as it was before this grew a wrapper.

**An override must keep the shape of the template default** — the same `type`,
or an array where the default is an [array](rate-limits/multiple-limits.md). The
merge swaps fields within the discriminant, so moving from `requestLimit` to
`concurrencyLimit`, or from one limit to several, changes what the template said
this client is. Either is skipped with a warning rather than applied.

This is the operator-side escape hatch and needs no permission from the template.
For a choice the plugin itself sanctions — a subscription plan, picked from a
list the plugin wrote — see
[letting callers pick a plan](writing-plugins.md#letting-callers-pick-a-plan).

## Instances, controllers and leader election

Every process running a handler registers itself as an **instance** and
heartbeats. For each client, one instance is elected **controller** and drains
that client's queue; the rest enqueue and await a `requestReady` broadcast.

This is why recovery is not a thundering herd, and why adding replicas does not
multiply the work done on the shared queue. When an instance stops or stops
heartbeating, its requests are reclaimed and roles are recomputed.

Under the [memory backend](backends/memory.md) there is exactly one instance by
definition, and it is always the controller.

## Freeze and thaw

When an upstream returns a hard rate-limit response, the client **freezes** —
its bucket is reset and admission stops until the freeze lapses.

Recovery is not "everyone retries at once". Exactly one request across the whole
fleet is nominated as the **thaw probe**. If it succeeds, the freeze clears; if
it fails, the freeze stands and the next request becomes the next probe. A
failed probe does not consume thaw progress.

With grants, probing is per grant: one probe per grant rather than one per
client, so recovering tenants do not wait behind each other.

### Clearing a freeze by hand

A freeze ends when a thaw probe succeeds, or when its deadline lapses and the
probe budget is spent. Nothing in the library ends one early, deliberately:
arming a freeze keeps the **longer** of the two deadlines, so that failures
arming concurrently cannot cut each other short. The same rule keeps a deadline
computed from a skewed clock or from an extreme `retryBackoffBaseTime`. Such a
deadline is bounded — both backends clamp it to 2^53-1 — but bounded far past
any horizon you care about. Waiting the key out is not an escape either: the
freeze key's TTL is sized from the deadline plus a minute, so it outlives the
freeze rather than expiring under it.

The escape hatch is the backend, not the client. Three methods on
`DianemoBackend` exist for this, and nothing in the library calls them:

- `clearFreezeState(key)` discards the freeze and its probe budget.
- `resetTokenBucket(key, maxTokens)` refills a bucket the freeze emptied.
- `clearConcurrency(key)` drops a slot ledger.

The keys they take are built from the handler's `keyPrefix` and the client's
name:

```
<keyPrefix>:requestHandler:<clientName>:freezeState
<keyPrefix>:requestHandler:<clientName>:rateLimit:<limitName>
<keyPrefix>:requestHandler:<clientName>:concurrency:<limitName>
```

A budget carries the name of the limit that owns it, so a client declaring
[several](rate-limits/multiple-limits.md) keeps them apart. A limit written in
the single form is named `default`.

`<keyPrefix>:` is absent if you did not configure one, and spaces in a client
name become underscores. A grant-isolated client freezes each grant separately,
so its keys carry the grant before the last segment —
`<keyPrefix>:requestHandler:<clientName>:grant:<grantId>:freezeState`, and the
same for the other two.

## Cost and priority

Both apply to every rate-limit type.

- **`cost`** — how much budget a request spends. Vendors rarely price every
  endpoint the same; a bulk export can legitimately count as ten calls. Under
  `concurrencyLimit`, cost occupies that many slots. Must be a positive number;
  fractional costs are supported and spend fractionally.
- **`priority`** — an integer 0–10, higher served first. A backfill at priority 0
  is admitted behind interactive traffic at priority 5 instead of starving it.

Both are validated per request: a cost that is zero, negative, `NaN` or infinite
is rejected, as is a priority outside 0–10. A negative cost would otherwise mint
budget rather than spend it.

Both feed one packed sorted-set score: priority first, then retry count, then
arrival time. A retried request sorts ahead of a fresh one at equal priority, so
work already part-done finishes rather than being crowded out.

This ordering is strict, not aging-based. A continuous supply of higher-priority
work can therefore starve lower-priority work. Within one client-level budget,
an expensive head that does not currently fit also waits ahead of cheaper work;
the scheduler preserves order rather than using spare capacity out of turn.
Grant-isolated clients are the exception: a blocked grant is skipped so it does
not stall unrelated tenants.

`cost` is not part of the score — it decides what a request spends, not where it
sits. Ordering within a priority band is by arrival to the millisecond, which
makes it approximate rather than strict FIFO: requests enqueued in the same
millisecond are ordered arbitrarily, and across replicas the timestamp comes from
whichever one received the request, so clock drift between them reorders the
queue. Priority ordering is unaffected — the bands are far wider than any
plausible drift.

## Cancellation

Pass an `AbortSignal` as `signal` and the request is cancellable at every stage:

```ts
const controller = new AbortController();
const res = handler.handleRequest({
  clientName: "acme:_:production",
  requestName: "acme.orders.list",
  method: "GET",
  url: "/orders",
  signal: controller.signal,
});
controller.abort();
```

Aborting before the request is sent spends **nothing** — no token, no concurrency
slot, no queue place — and a request already queued gives up its position rather
than holding it until the budget arrives. This matters most for the case it was
added for: a caller who has given up used to keep its place in the queue, be
admitted in turn, spend the budget, and only then fail inside axios, with real
traffic waiting behind it for a refill it did not need.

**Which error you get depends on when the abort lands.** Before dispatch it is
`RequestAbortedError` (499), this library's own. After dispatch the call belongs
to axios, which cancels it and throws its `CanceledError`. So a consumer branching
on `err.code === "request_aborted"` catches the give-up cases but not a signal
that fires while the response is in flight — check for both, or check the
`signal.aborted` you already hold.

`signal` must be a real `AbortSignal`. Axios types its own as `GenericAbortSignal`,
whose `addEventListener` is optional, but its Node adapter calls that method
unguarded — so a looser shape throws from inside axios _after_ this library has
admitted the request and spent its budget. That is rejected up front with a
`ConfigurationError` instead, on the same principle as a malformed `cost`: a bad
per-request value is refused where it enters, so it cannot resurface later as a
different and wrong diagnosis.

## Errors

Retries with backoff are automatic; what surfaces is what remained after them.
`httpStatusCodesToMute` downgrades expected-but-noisy statuses to debug logs
rather than errors — useful for a vendor that returns 500 for "not found".

Every error carries `statusCode` and `code` alongside `message`, so a host that
never imports dianemo's error classes can still map them:

```ts
if (typeof err.statusCode === "number" && typeof err.code === "string") {
  res
    .status(err.statusCode)
    .json({ error: err.code, error_description: err.message });
}
```

| Class                           | Status | When                                                                          |
| ------------------------------- | ------ | ----------------------------------------------------------------------------- |
| `ClientNotFoundError`           | 404    | A request named a client that isn't registered — usually a typo.              |
| `ClientUnavailableError`        | 503    | A client didn't arrive in time, or shutdown cancelled the wait.               |
| `ClientConflictError`           | 409    | Duplicate client registration.                                                |
| `RequestTimeoutError`           | 408    | Queued past `cleanupTimeout` without being admitted.                          |
| `RequestAbortedError`           | 499    | The caller's `signal` fired before the request was sent.                      |
| `RequestCostExceedsBudgetError` | 400    | A request's `cost` exceeds `maxTokens`, so it could never be satisfied.       |
| `NotOAuth2ClientError`          | 400    | Grant-token operations attempted on a client that is not OAuth2.              |
| `GrantRefreshTokenMissingError` | 400    | An OAuth2 grant was asked to refresh but has no stored refresh token.         |
| `NoResponseError`               | 500    | Every retry failed without a response.                                        |
| `ConfigurationError`            | 500    | Bad construction — duplicate plugin, template name containing `:`.            |
| `RequestError`                  | 500    | A failed outbound call, wrapping the original under `metadata.originalError`. |

`getOriginalStatus(err)` and `getOriginalResponseData(err)` read the underlying
vendor response off a `RequestError`, for callers that want to turn a 404 into
`null`.

Constructing an error does not log. `tryHandleRequest` logs, once, where the
failure is first observed — so a rethrow never produces a duplicate line.

**A hook that runs after a successful response cannot fail the request.** Once the
upstream has answered 2xx, the call has already had its effect at the vendor —
money moved, a shipment was booked — so a throw from `responseInterceptor` or from
`rateLimitChange` is logged and the response is still returned. Reporting a
success as a failure would invite the caller to retry something already done. Put
validation that must be able to reject a response in your own code around
`handleRequest`, not in these hooks. `requestInterceptor` is the opposite case: it
runs before the request is sent, so a throw there does prevent the call.

## Health checks and probes

Each client runs a periodic health check (`healthCheckIntervalMs`, default 10s)
that reclaims orphaned requests and nudges the queue.

`probeRequest` is separate: it describes a request an _external_ scheduler can
fire to test whether a sustained-downtime integration has recovered. Dianemo
stores the configuration and does not fire it itself.
