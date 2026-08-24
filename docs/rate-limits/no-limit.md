# `noLimit`

```ts
{
  type: "noLimit";
}
```

No budget, no queue, no coordination. Every request is admitted the moment it
arrives.

## When to use it

- Internal services you own, where the only real limit is capacity.
- A vendor with no published limit that you have no reason to self-throttle
  against.
- Local development against a mock.

## What it still gives you

`noLimit` is not "dianemo turned off". A `noLimit` client keeps:

- retries with backoff,
- auth flows, including OAuth2 refresh,
- OpenTelemetry spans and error wrapping,
- freeze/thaw **if the upstream returns a hard rate-limit response** — a vendor
  can still push back even when you were not throttling yourself.

So it remains worth routing through the handler rather than calling `axios`
directly: you get uniform error semantics and instrumentation, and switching to
a real limit later is a one-line config change rather than a rewrite.

## Cost

This is the cheapest client type, and deliberately so. With no budget there is
nothing for the queue to arbitrate, so routing through it would cost several
backend round-trips and buy nothing. Admission is one backend operation, which
answers both of the questions a `noLimit` client still has to ask: is anything
already queued, and is a freeze or a thaw probe standing.

Those two are the checks a `noLimit` client will not skip, and they are one
operation rather than two on purpose — asked separately, a peer can enqueue
between them, and the arrival that read "queue empty" then overtakes it. "No
configured limit" is not "ignore the limit the vendor just told us about", so a
frozen client still admits nothing, and a request nominated as the thaw probe is
handed back to the queue — that is the only path that can guarantee it is the
only one in flight.

A `noLimit` client does queue, which is why the first check exists: a hard
rate-limit response or an upstream failure freezes it, and everything that
arrives during the freeze waits. New arrivals keep waiting until that backlog has
drained, so nothing that arrived later is served earlier.

In the benchmark it runs at essentially the full upstream ceiling.

## Isolated grants share one queue

With `grantRateLimitBehavior: "isolated"`, each grant gets its own freeze state —
one tenant being rate limited does not freeze the others. The **queue** is still
one queue per client, and ordering is a property of that queue: while one grant's
backlog is draining, another grant's requests take the queued path rather than the
fast path, so they cannot be served ahead of it.

They are not blocked by it — the drain steps over frozen grants, so the other
tenant's work is admitted in turn — but it costs the queue path (about a third of
a millisecond and five extra Redis round-trips per request on the reference
measurement) for as long as some other grant has work waiting. Isolation is of
budgets and freezes, not of arrival order.

## Caveat

There is no back-pressure. If you fire ten thousand concurrent requests at a
`noLimit` client, ten thousand requests go out. Bound concurrency yourself, or
use [`concurrencyLimit`](concurrency-limit.md) if the point is to protect the
upstream rather than to obey a published quota.
