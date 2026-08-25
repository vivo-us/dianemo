# Several limits on one client

Many vendors publish more than one limit at once — 20 requests a second *and*
50,000 a day, or 10 a second *and* no more than 3 in flight. Declare `rateLimit`
as an array and a request is sent only when **every** entry can take it.

```ts
{
  name: "acme:_:production",
  rateLimit: [
    { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
    { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 50_000, maxTokens: 50_000 },
  ],
}
```

A single limit is still written exactly as before, without a name. The array is
only for clients that genuinely have more than one budget.

## Names

Every entry needs a `name`, matching `/^[a-z0-9_]{1,64}$/`, unique within the
client. The name is not decoration: it is the key the entry's budget is stored
under, `<client namespace>:rateLimit:per_day`. That is what makes reordering the
array safe, and what makes the balances legible when you go looking in Redis.

Two entries sharing a name would be one bucket metering two limits, so it is a
configuration error rather than a last-one-wins merge. So is an entry with no
name, an empty array, and a name with a colon, a space or a capital in it — all
three would otherwise resolve to a key that is ambiguous or needs escaping.

A single limit keeps the un-suffixed `<client namespace>:rateLimit` key it has
always used, so moving an existing client to the array form starts its budgets
fresh rather than inheriting the old balance under a new name.

## What may go in the array

Any of the four types, in any combination:

| Entry              | Contributes                                  |
| ------------------ | -------------------------------------------- |
| `requestLimit`     | a token bucket of its own                    |
| `concurrencyLimit` | a slot ledger of its own                     |
| `noLimit`          | nothing — legal, and useful as a placeholder |

[`sharedLimit`](shared-limit.md) is the exception: it may be the *only* limit a
client declares. See below.

Mixing a rate with a concurrency ceiling is the common second case:

```ts
rateLimit: [
  { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 10, maxTokens: 10 },
  { name: "in_flight", type: "concurrencyLimit", maxConcurrency: 3 },
]
```

## All or nothing

The property that matters is that a request either claims every budget or none
of them. Claiming them one at a time would leave the first spent whenever the
second declined — and nothing on the decline path hands a budget back, so those
tokens would be lost on every refusal, permanently, at whatever rate the tighter
limit refuses.

So the claim is one backend operation: a single Lua script on Redis, a single
synchronous pass on the memory backend. Both evaluate every budget before
committing to any.

This is why the multi-limit client needs a backend implementing
`acquireMultiLimit` and `releaseMultiLimit`. Both shipped backends do. A backend
written against an earlier version does not, and a client declaring several
limits refuses to start on it rather than silently metering against one.

## What the tightest limit decides

- **The cost ceiling.** `cost` is measured against the smallest ceiling in the
  array — the lowest `maxTokens` or `maxConcurrency`. A cost above it can never
  be admitted, so it is failed with `RequestCostExceedsBudgetError` rather than
  queued forever.
- **The throughput.** Whichever budget runs out first is the one that holds the
  client, which is the whole point.

## When it declines

A refusal reports the **longest** wait any budget asked for. Waking on the
per-second budget's next refill would only be refused by the per-day budget
again, so the wake-up is booked against the one that actually has to change.

A concurrency entry has no deadline of its own — a slot is freed by a completion,
which pokes the admission loop directly — so when concurrency is the only thing
blocking, the fallback is the slot TTL. That is the latest moment capacity is
guaranteed to exist, and it keeps a decline from booking a 1ms wake-up and
spinning.

## Freeze does not empty the buckets

A single [`requestLimit`](request-limit.md) client zeroes its bucket on a 429,
because the response proves the bucket's picture of the vendor was wrong.

A client with several budgets does not, and this is deliberate. The response does
not say *which* limit was breached, and zeroing a per-day bucket over a
per-second breach would stand the client down until tomorrow. The freeze window
is the stand-down instead: floored at the **shortest** refill interval in the
array — the soonest anything here can hand out a token at all — and lengthened by
each further 429, with recovery probed one request at a time as usual.

The shortest, not the longest, for the same reason: flooring a freeze at a
per-day interval would pause the fleet for a day over one per-second refusal.

## `sharedLimit` stands alone

A client that borrows another's budget declares `sharedLimit` and nothing else.
Combining it with limits of its own is a configuration error.

```ts
rateLimit: { type: "sharedLimit", clientName: "acme:_:production" }   // ✓
rateLimit: [{ name: "contract", type: "sharedLimit", clientName: "…" }] // ✓ same client

rateLimit: [
  { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 5, maxTokens: 5 },
  { name: "contract", type: "sharedLimit", clientName: "acme:_:production" },  // ✗ refused
]
```

The reason is the queue. A `sharedLimit` client is constructed with the owner's
name, so it has no queue of its own: its requests land in the owner's queue and
the **owner's controller** drains them, which is what gives one arbiter a view of
everything competing for that budget. `priority` therefore means something across
every client sharing it.

A client that also had budgets of its own could not adopt the owner's namespace —
its own bucket would land inside the owner's keyspace — so it would need a second
queue. Two queues on one balance is still *safe*, because the claim is atomic and
the budget is never oversold, but nothing arbitrates ordering between them:
priority stops carrying across, and a queue of cheap work can take each token as
it refills while a costly head next door never accumulates enough. Rather than
ship that, the combination is refused.

If you need both, model it as two clients: one that owns the local limit and one
that shares.

## The owner may declare several limits

Nothing about `sharedLimit` cares how many budgets the owner has. The child takes
the owner's queue as its own, so the owner's controller does the claiming — of all
its budgets, atomically, whatever they are called:

```ts
// owner
{ name: "acme:_:production", rateLimit: [
  { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
  { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 50_000, maxTokens: 50_000 },
]}

// borrower — spends both of the owner's budgets
{ name: "acme:_:sandbox", rateLimit: { type: "sharedLimit", clientName: "acme:_:production" } }
```

## Dynamic updates

`rateLimitChange` receives the whole array and returns a whole array. An update
that is not an array is ignored, because a single limit would mean a different
client class and a broadcast cannot change that — the rebuild path is what swaps
one client for another.

An update whose entries are unusable is refused and logged, keeping the last
workable configuration rather than wedging the client.

## Stats

`getClientStats` reports one entry per declared limit:

```ts
{
  type: "multiLimit",
  limits: [
    { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20, tokens: 17 },
    { name: "per_day", type: "requestLimit", interval: 86400000, tokensToAdd: 50000, maxTokens: 50000, tokens: 49873 },
  ],
}
```

A consumer switching on `rateLimit.type` gains a `"multiLimit"` case.
