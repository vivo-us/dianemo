# `concurrencyLimit`

Caps how many requests are **in flight at once**, rather than how many are sent
per unit of time.

```ts
rateLimit: [{ type: "concurrencyLimit", maxConcurrency: 5 }]
```

## When to use it

- The vendor says "no more than N concurrent connections" — some SOAP and
  legacy carrier APIs are specified this way.
- You are protecting something that degrades under parallelism rather than
  under volume: a database-backed endpoint, a report generator, a
  single-threaded service.
- You want back-pressure proportional to how slow the upstream currently is.
  A concurrency cap self-adjusts: if the vendor slows down, you send less.

Where a token bucket asks _how often_, this asks _how many at once_. If the
vendor publishes a per-second rate, use [`requestLimit`](request-limit.md) —
approximating one with the other requires assuming a latency that will not hold.

## How a slot works

Each admitted request holds a slot until it completes, then releases it. Slots
are cost-weighted: a request with `cost: 3` occupies three of them, so
`maxConcurrency: 10` admits ten cheap requests or three expensive ones.

Completion is the ordinary way a slot comes back, and there are two others. Both
exist so that a dead process cannot hold capacity forever, and both can therefore
release a slot whose request is in fact still open:

- **The TTL**, below.
- **Orphan reclamation.** The controller periodically clears queue _entries_ whose
  owning replica is no longer registered, so a dead replica's waiting work stops
  holding the queue open. It does **not** release the slots those requests held —
  only the TTL does that; see the recovery-time note below. The controller cannot
  distinguish "the owner is gone" from "the owner was declared gone", so a replica
  that is alive but has missed enough heartbeats to be pruned (roughly ten seconds
  of a stalled event loop) has its still-queued request dropped from under it, and
  a retry can then put the upstream briefly at `maxConcurrency + 1`. That is a
  deliberate trade:
  a transient breach when a replica stalls, in exchange for never stranding
  capacity when one dies.

Slots carry a TTL. A process that dies mid-request would otherwise hold its
slots forever, so an expired slot is reclaimed — the limiter degrades toward
letting work through rather than deadlocking.

Set that TTL with `requestOptions.concurrencySlotTtl` (default two minutes), and
set it **above the slowest response the upstream can legitimately produce**. It
is deliberately not `cleanupTimeout`: reclaiming a slot whose request is still in
flight hands it to someone else, and the effective cap becomes
`maxConcurrency × ceil(requestDuration / slotTtl)`. The two knobs want opposite
values — short admission timeouts, long slot TTLs — which is why they are
separate.

Three things about that TTL are easy to get wrong.

**A TTL that is too short hides under bursty load.** Expired slots are reaped
inside an admission attempt, not by a background sweep, so a burst that is
declined once and then waits for a release never reaps anything: the same
workload measured a peak of 2 against `maxConcurrency: 2` when fired as one
burst, and a peak of 6 when the arrivals were spread over the same window. Load
that arrives in bursts — which is how most people test — will not show you this
misconfiguration; sustained traffic will.

**Set the same value on every replica.** The TTL is read from the config of
whichever client is attempting admission, and it is applied to a ledger the whole
fleet shares, so the shortest value in the fleet decides when everyone's slots
expire. A rolling deploy that lowers `concurrencySlotTtl` therefore reaps live
slots held by requests the older replicas are still serving: measured at a peak
of 2 against a fleet cap of 1, for the length of the rollout. Change it in one
step across the fleet, and treat a mixed fleet as having the lowest value
configured anywhere in it.

**It is also your worst-case recovery time after a crash, which pushes the other
way.** Orphan reclamation does not release capacity at all — it prunes queue
metadata only. A replica killed while holding slots therefore strands _every_ one
of them, fast-path or queued, for the full `concurrencySlotTtl`, and nothing can
shorten it: the stale slots are reaped inside a later admission attempt, once the
lease lapses. Everything else on this page pushes the value up, above the slowest
legitimate response; this pushes it down, toward the longest capacity outage you
are willing to wear after a process dies. Pick it knowing you are trading between
those two, not just satisfying the first.

Releasing a dead owner's slots from the sweep instead was tried and withdrawn,
and the reason is the one that matters for tuning: a registration that has lapsed
does not prove the process is gone, only that it stopped heartbeating. A replica
that is alive but briefly unregistered — a long GC pause, a network blip — would
have its slots handed to someone else while its requests were still running at the
vendor, breaching the cap for real rather than merely holding capacity idle. The
lease keeps the guarantee one-directional: capacity may be reclaimed late, never
early. If that recovery time ever costs more than the safety is worth, the shape
of the change is to record an owner beside each slot (`HSET <key>:owners
<requestId> <ownerId>`) and reconcile the ledger directly — but it needs a way to
distinguish a dead owner from a stalled one before it is safe, and it is not a
one-line change: both claim paths, the release, the TTL reap and
`clearConcurrency` all touch the hash, in both backends.

Re-submitting the same `requestId` does not double-count: the holder is excluded
from its own occupancy check, so a retry cannot exhaust the limit by competing
with itself.

## Rebuilds

A client is rebuilt whenever its source data changes — a rate-limit override, a
credential rotation, a redeploy carrying an edited template. The slot ledger is
fleet-wide and a rebuild is a local event, so the rebuild is invisible to the
cap: requests still talking to the upstream keep their slots, the replacement
client meters against the same key, and nothing is admitted on top.

Requests already **queued** at that moment are handed to the replacement rather
than failed. They keep their place, and the replacement admits them in turn.

One consequence worth knowing before you rotate a credential: a request that was
already parked when the rotation landed resumes with the credential it was
admitted under, not the new one. Requests submitted after the rebuild use the new
one. (This affects a static `token` or `basic` credential, which is part of the
client definition; an `oauth2` client re-reads its tokens at dispatch and so
picks the rotation up.) The alternative was failing that request outright, which
is what used to happen and is worse for every ordinary rotation — but if you are
rotating **because the old secret was revoked**, expect the in-flight request to
come back as a 401 to its caller rather than to be quietly re-signed. Under
incident conditions that is the one caller you have to account for.

## Admission, not polling

This is worth knowing if you read the source or debug a stall.

Slots are claimed **during admission** — in the same decision that checks the
freeze state — rather than by a request waiting in a loop for one to free up. A
request that cannot get a slot simply is not admitted, the admission loop stays
free for others, and releasing a slot wakes it.

The waiting version looks equivalent and is not: only one request can wait at a
time, so it blocks every request behind it and the client admits roughly one
request per poll interval no matter how many slots are free.

The cap itself is the entire contract, so the conformance suite drains a backlog
of 120 requests through a 5-slot client while watching occupancy, and asserts
the peak never exceeds 5 and every slot is released afterwards. A limiter that
drains promptly but occasionally admits 6 is worse than useless, because the
breach is invisible until the vendor reacts.

## Interaction with freeze

A hard rate-limit response still freezes the client, and a frozen client admits
nothing regardless of free slots. Thaw probing works exactly as it does for the
other types.

The freeze duration comes from `retryBackoffBaseTime`, and this type does not
floor that value for the retry itself — so `retryBackoffBaseTime: 0` means the
failing request tries again with no local delay. It does **not** mean no freeze:
the freeze is floored at **1000ms**, because a zero-length one reads as no freeze
at all downstream and the rest of the fleet would keep sending into an upstream
that has just refused us. Occupancy is not the constraint being expressed here —
"how soon may _this_ request retry" and "how long should _everyone_ stand down"
are separate questions, and only the first is yours to set to zero.

A `retryBackoffBaseTime` that is negative or `NaN` hits the same floor. See
[choosing a rate limit](README.md) for the general rule and for the one setting
that does disarm the freeze.
