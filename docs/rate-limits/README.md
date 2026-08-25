# Choosing a rate limit

`rateLimit` is a list of limits — usually one, sometimes several. The choice
decides how the client behaves when requests compete, so match it to the limit
the vendor actually publishes rather than to whichever is easiest to configure.

| Type                                       | Use when the vendor limits            | Config                                 |
| ------------------------------------------ | ------------------------------------- | -------------------------------------- |
| [`noLimit`](no-limit.md)                   | nothing you need to enforce           | —                                      |
| [`requestLimit`](request-limit.md)         | requests per unit of time             | `interval`, `tokensToAdd`, `maxTokens` |
| [`concurrencyLimit`](concurrency-limit.md) | requests in flight at once            | `maxConcurrency`                       |
| [`sharedLimit`](shared-limit.md)           | one budget across several credentials | `clientName`                           |
| [several at once](multiple-limits.md)      | more than one of the above at once     | a `name` per limit                     |

## The quick decision

- The vendor says **"600 requests per minute"** → `requestLimit`. This is the
  common case by a wide margin.
- The vendor says **"no more than 5 concurrent connections"** → `concurrencyLimit`.
- You have several credentials that the vendor counts against **one** budget —
  sub-accounts, regional endpoints, a sandbox sharing production's quota →
  `sharedLimit` on the client that owns the budget.
- Internal service, or a vendor whose limit you genuinely do not need to
  respect → `noLimit`.
- The vendor says **"20 per second and 50,000 per day"**, or **"10 per second
  and at most 3 concurrent"** → [several](multiple-limits.md) of the above in one
  list. A request is sent only when every one of them admits it.

Rate limits are not either/or across a deployment: each client picks its own, so
one handler routinely runs all four at once.

## Cost and priority apply to all of them

Independent of type, each request may declare:

```ts
await handler.handleRequest({
  clientName: "carrier:_:production",
  requestName: "carrier.rates",
  method: "POST",
  url: "/rates",
  cost: 2, // how much budget this spends (default 1)
  priority: 1, // 0–10, higher is served first (default 1)
});
```

`cost` exists because vendors rarely price every endpoint the same — a bulk
export can legitimately count as ten calls. `priority` exists so a backfill can
be admitted behind interactive traffic instead of starving it.

Both feed one packed sorted-set score: priority first, then retry count, then
arrival time. A retried request sorts ahead of a fresh one at equal priority, so
work already part-done finishes rather than being crowded out.

Priority is strict: it does not age waiting work upward. Sustained
higher-priority traffic can starve lower-priority traffic, and a costly head on
one shared client-level budget is not bypassed by cheaper work. Grant-isolated
budgets are scanned independently so one tenant cannot cause that head-of-line
blocking for another.

## What every type shares

Whatever the strategy, all clients get the same surrounding machinery:

- **The uncontended fast path.** When nothing is queued and the budget covers
  the request, admission is decided by a single atomic operation and the queue
  is skipped entirely. See [backends](../backends/README.md).
- **Freeze and thaw.** A hard rate-limit response freezes the client. Recovery
  is probed by exactly one request across the whole fleet, rather than every
  replica retrying into a closed door.
- **Leader election.** One process drains each client's queue; the rest enqueue
  and await.
- **Per-grant isolation.** A client with multiple credentials can meter each one
  separately, so one tenant hitting their ceiling does not stall the others.

### `retry429s: false` disarms the freeze

The freeze is derived from the retry decision, so switching the retry off
switches the freeze off with it. `retry429s: false` hands a 429 straight to the
caller and **arms no freeze**, which means the rest of the fleet keeps sending
into the closed door. It is legal and it does not warn.

That is the only setting that disarms it. If you want the fleet-wide back-off,
leave `retry429s` on.

### A zero back-off does not disarm it

`retryBackoffBaseTime: 0` means "retry _this request_ with no local delay". It
says nothing about whether the rest of the fleet should keep hammering an
upstream that has just refused us, so it is not permitted to produce a
zero-length freeze — which the arming gate would read as no freeze at all.

The freeze duration is floored at **1000ms** for the types that derive it from
the configured back-off, `concurrencyLimit` and `noLimit`. `requestLimit` floors
its freeze at the refill interval instead, since a freeze empties the bucket and
is not over until the bucket can refill; a `sharedLimit` child floors it at its
parent's, and a client with [several limits](multiple-limits.md) at the shortest
of its own. The same floor catches a `retryBackoffBaseTime` that is negative or
`NaN` — a `rateLimitChange` reading a header the vendor omitted is the usual
source, and a negative would otherwise put `frozenUntil` in the past and leave
the client in permanent single-probe thaw.

So `retryBackoffBaseTime: 0` on a `noLimit` client gives you a request that
retries immediately and a fleet that still stands down for a second. The
retry wait and the freeze are separate numbers and are allowed to disagree.
