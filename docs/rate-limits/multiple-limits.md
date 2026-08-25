# Several limits on one client

`rateLimit` is always a list, and a request is sent only when **every** limit in
it can take the request. Most clients have one; many vendors publish more than
one at once — 20 requests a second *and* 50,000 a day, or 10 a second *and* no
more than 3 in flight — and those go side by side in the same list.

```ts
{
  name: "acme:_:production",
  rateLimit: [
    { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
    { name: "per_day", type: "requestLimit", interval: 86_400_000, tokensToAdd: 50_000, maxTokens: 50_000 },
  ],
}
```

One limit is the same list with one entry, and needs no name:

```ts
rateLimit: [{ type: "requestLimit", interval: 1000, tokensToAdd: 100, maxTokens: 100 }]
```

## Names

A limit's `name` is the key its budget is stored under —
`<client namespace>:rateLimit:per_day` — which is what makes reordering the list
safe and what makes the balances legible when you go looking in Redis. It must
match `/^[a-z0-9_]{1,64}$/`: a colon, a space or a capital would resolve to a key
that is ambiguous or needs escaping.

**`name` may be omitted, where it is `default`.** A client with one limit
therefore never has to invent one, and its budget lives at
`<client namespace>:rateLimit:default`. Omit it twice and both limits take the
same name and would meter against one bucket, so that is a configuration error
rather than a last-one-wins merge — as is any other duplicate name, and an empty
list.

> **Upgrading from 1.x — do not roll this out incrementally.** Budgets moved.
> They used to live at the un-suffixed `<client namespace>:rateLimit`; they now
> carry the limit's name. Two consequences, and the second is the serious one.
>
> The old keys are orphaned, and a bucket that does not exist reads as **full**,
> so the first requests after the upgrade meet a fresh budget and can burst up to
> `maxTokens` before the new key starts metering.
>
> Worse, a **rolling deploy runs both key layouts at once**. Old replicas meter
> `<client>:rateLimit` while new ones meter `<client>:rateLimit:default`, and
> neither sees the other's balance. The queue path survives it — one controller
> per client drains the queue, whichever version it is on — but the uncontended
> fast path runs on every replica independently, so for the length of the
> rollout the fleet can send up to **twice** the agreed rate at the vendor.
>
> Stop the fleet, then start it on the new version. If you cannot, do the
> rollout inside a window where a doubled rate is acceptable, and expect the
> one-time burst above on top.

## What may go in the list

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

This is why a client declaring **more than one** budget needs a backend
implementing `acquireMultiLimit` and `releaseMultiLimit`. Both shipped backends
do. A backend written against an earlier version does not, and such a client
refuses to start on it rather than silently metering against one. A client with a
single budget is unaffected: it uses the single-key calls every backend has
always had.

## What the tightest limit decides

- **The cost ceiling.** `cost` is measured against the smallest ceiling in the
  list — the lowest `maxTokens` or `maxConcurrency`. A cost above it can never
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
list — the soonest anything here can hand out a token at all — and lengthened by
each further 429, with recovery probed one request at a time as usual.

The shortest, not the longest, for the same reason: flooring a freeze at a
per-day interval would pause the fleet for a day over one per-second refusal.

## `sharedLimit` stands alone

A client that borrows another's budget declares `sharedLimit` and nothing else.
Combining it with limits of its own is a configuration error.

```ts
rateLimit: [{ type: "sharedLimit", clientName: "acme:_:production" }]   // ✓
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
{ name: "acme:_:sandbox", rateLimit: [{ type: "sharedLimit", clientName: "acme:_:production" }] }
```

## Dynamic updates

`rateLimitChange` receives the client's limits and returns a replacement list.
Names may be omitted there exactly as they may in the original declaration. An
update naming a `sharedLimit` is ignored: that would be a different kind of
client, and a broadcast cannot change which kind a client is — the rebuild path
is what swaps one for another.

An update whose entries are unusable is refused and logged, keeping the last
workable configuration rather than wedging the client.

## Stats

`getClientStats().rateLimit` is a list, one entry per declared limit:

```ts
[
  { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20, tokens: 17 },
  { name: "per_day", type: "requestLimit", interval: 86400000, tokensToAdd: 50000, maxTokens: 50000, tokens: 49873 },
]
```

A client declaring one limit reports a list of one, named `default`. A consumer
that read `rateLimit.type` reads `rateLimit[0].type`, or looks the entry up by
name.
