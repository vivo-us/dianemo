# Design notes

Implementation reasoning that spans more than one file — the things a new
maintainer needs once, and that are too long to sit usefully in a comment.

This is not the manual. [Concepts](concepts.md) explains the same system from a
consumer's point of view; if you are trying to *use* dianemo, read that instead.
Everything here is about why the internals are shaped the way they are, and each
section exists because getting it wrong has already cost someone a day.

## Clocks

Three different clocks appear in this codebase, and mixing them is the single
easiest way to introduce a bug that only shows up under load on someone else's
infrastructure.

**The backend's clock owns everything the fleet compares.** Freeze deadlines,
token-bucket `lastUpdate`, queue arrival timestamps — anything two replicas
reason about together comes from `backend.now()` (Redis `TIME`, or
`Date.now()` in the memory backend, which is the same clock by definition
because there is only one process).

The rule that follows: **when you compute a delay from a fleet-wide deadline,
both ends of the subtraction must come from the same clock.**
`scheduleDrainForFreeze` reads `frozenUntil` and `backend.now()` together for
exactly this reason. Using a local `Date.now()` against a backend-written
deadline makes the delay wrong by the host-to-Redis skew — and a backend running
ahead fires the timer *early*, whereupon the pass finds the client still frozen,
books nothing, and the queue sleeps until the health tick.

**Lua scripts that decide fleet-wide state take `TIME` and never an argument.**
`cleanupStaleFrozenGrants` decides which grants stop being marked frozen. A
caller-supplied timestamp there would let one skewed replica unmark a grant the
rest of the fleet still considers frozen. If such a script needs another `ARGV`,
add it after the existing ones and leave `TIME` alone.

**Process wall-clock is correct for one thing: a token's own lifetime.** The
`issuedAt` / `expiresAt` pair on a stored credential is written and read in the
same process frame, and the renewal margin is derived from the difference. Taking
one end from `backend.now()` would inject skew into a computed lifetime, and a
host lagging by more than the token's lifetime makes it negative.

**Arrival timestamps are approximate by construction**, and that is documented
for consumers in [Concepts](concepts.md#cost-and-priority): the timestamp comes
from whichever replica received the request, so drift between replicas reorders
requests *within* a priority band. Priority ordering is unaffected, because the
bands are far wider than any plausible drift — see below.

## Queue score bands

Priority, retry count and arrival are packed into one sorted-set score, lowest
served first, so that a single `ZRANGE` yields the next request. The layout lives
in `packages/core/src/backend/queueScore.ts` and is shared by both backends.

```
score = (MAX_PRIORITY - priority) * PRIORITY_BAND    PRIORITY_BAND = 1.01e14
      + (MAX_RETRIES  - retries)  * RETRY_BAND       RETRY_BAND    = 1e12
      + (timestamp - EPOCH_MS)                       EPOCH_MS      = 2026-01-01
```

Two arithmetic constraints, and both are tight enough that changing a constant
casually will break ordering rather than error:

- **Bands must not overlap.** Epoch-ms alone is ~1.76e12 without the offset,
  which is why the clock is offset at all: it keeps the arrival term below
  `RETRY_BAND` so a timestamp cannot bleed into the retry band.
- **The total must stay under 2^53**, where doubles stop representing
  consecutive integers. The current maximum is ~1.11e15 against a ceiling of
  ~9.007e15. A `1e15` priority band would exceed it and quantize the millisecond
  ordering away.

`calculateQueueScore` throws a `RangeError` on both violations rather than
silently mis-ordering, which is the only reason a mistake here is survivable.

**`QUEUE_RETRY_BAND` is an on-disk format.** Changing it reinterprets every
score already persisted in Redis. It must stay stable until a versioned queue
migration exists.

`cost` is deliberately not in the score. It decides what a request spends, not
where it sits.

## Numeric field parsing

Queue metadata is stored as strings — Redis hash fields on one side, a `Map` on
the other — and parsed back on both. **The Lua is the reference implementation
and the TypeScript matches it**, not the other way round.

That direction matters because `tonumber` and `Number` disagree on the empty
string: `tonumber("")` is `nil`, so `tonumber(x) or 1` yields the default, while
`Number("")` is `0`. On a `priority` field that difference is a silent demotion
to the bottom band in TypeScript and a default of 1 in Lua, on the same stored
byte. `parseStoredNumber` exists to make emptiness explicit and remove the whole
class rather than the one instance.

The rule applies to **every** read of a stored numeric field, not to an
enumerated list of them. An earlier version of this section listed the sites, and
the list went stale twice — once when the Lua moved into `lua/`, and once when
three inline copies of the freeze gate became one shared fragment. A list that
undercounts is worse than no list, because walking it feels like coverage. So:

- **In Lua**, a read is either `tonumber(x) or <default>` where a missing field
  means the default, or a bare `tonumber(x)` followed by an explicit `nil` test
  where absence means something else — no freeze state at all, or a bucket that
  has never been written and so counts as full. Both are correct; what is wrong is
  a bare `tonumber` whose `nil` then flows into arithmetic. Grep `tonumber(` in
  `packages/backend-redis/src/lua/` for the current set; there is no registry to
  keep in step.
- **A `nil` test is not enough where the value reaches `cjson.encode` or
  `string.format`.** `tonumber` uses `strtod`, so `"nan"` and `"inf"` both parse
  to numbers that pass a `nil` test, survive `math.min` and every comparison, and
  then fail at the point of serialisation — which failed the operation for a whole
  client rather than defaulting the one corrupt field. Use the `finiteNumber`
  fragment there. `parseStoredNumber` already reads a `NaN` as the default, so
  this is what makes the two sides agree.
- **In TypeScript**, the only implementation is `parseStoredNumber` /
  `parsePriority` / `parseFiniteStored` in
  `packages/core/src/backend/parseStored.ts`. Both backends go through it. A new
  read must use it rather than `Number`, `parseInt` or `parseFloat`, all three of
  which disagree with `tonumber` on some input. `parseFiniteStored` is the twin of
  the `finiteNumber` fragment, for a field that is compared and then written back
  — a freeze deadline, a probe budget.

`retries` and `timestamp` in `decodeRequest` and `toQueued` are the remaining
`parseInt` reads. Both backends read them identically, so the two agree with each
other; what they do not match is the Lua *score*, which uses `tonumber` — a stored
`"2.9"` reads as 2 and scores as 2.9. Harmless while nothing writes a fraction
there, and worth routing through `parseStoredNumber` if either ever becomes
non-integer.

The one exception, deliberate: `cost` is read with `parseFloat(...) || 1` in
`decodeRequest` and `toQueued`, which maps a stored `"0"` to 1. A request may not
legitimately cost 0 — `packages/core/src/request/index.ts` rejects anything below
`Number.EPSILON` — so 0 there is corruption and the default is the safer answer.
Both backends do this identically. `getQueueStats` does *not* share that reading:
it counts a stored `"0"` as 0 on both backends.

Two further rules inside that:

- **A NaN must never reach a score.** The memory backend's `sorted()` compares
  `a[1] - b[1] || lexical`, so a NaN score falls through to ordering by id: the
  priority is silently ignored and the comparator stops being a total order.
- **A read and a score treat `Infinity` differently, deliberately.** A score must
  accept whatever `tonumber` accepts, including `inf`, which both sides then
  clamp into the top band. A *read* is serialised into the `requestReady` payload,
  and `JSON.stringify(Infinity)` is `null`, so a corrupt `"Infinity"` would arrive
  at the owning replica as a null priority in a field typed `number`. Reads floor
  the non-finite to the default; scores keep it.

The priority default is 1, never 0, because 0 is the documented bottom of the
range and a request may legitimately ask for it. Mapping corruption onto 0 makes
it indistinguishable from an operator's deliberate choice.

## Credential keying and sub-clients

A sub-client inherits its parent's endpoint and authentication — that is what
makes "one credential, several budgets" work, and it is what
[Concepts](concepts.md#sub-clients) promises consumers. So **credentials are
filed under the root of the sub-client path, not under the full
`parent:child` name.**

Keying on the full name gives each sub-client its own credential entry, which
breaks the inheritance in both directions: a grant seeded on the parent is
invisible to the child, and seeding both stores two copies of one refresh token
that then replay each other's rotations.

Two mechanisms implement this, and they are easy to confuse:

- **`authOwnerName`** is set when a sub-client is merged onto its parent and
  carries the parent's own auth owner, so a nested path (`a:b:c`) resolves to the
  client that actually holds the credentials rather than to the template.
- **`sharedLimit` is the deliberate exception.** It is constructed with its
  *parent's* `name`, so its queue, bucket and freeze state resolve to the
  parent's keys — that is the whole point of the type. But it is a peer with its
  own credentials, so it keys those on its own registered name. Two clients
  filing tokens under one key means whichever refreshes first wins and the other
  attaches that token to its own requests, which the vendor will cheerfully
  accept.

The general rule: **budget state follows the parent; credentials follow whoever
owns them.** `usesGrantIsolation` reads from the budget owner for the same
reason, and it turns on whether the owner *accessor exists* rather than whether
it answered — `?? this.authData` cannot distinguish "this client owns its budget"
from "the owner has no auth data", and falling back to the borrower's own config
is exactly the disagreement the indirection removes.

## Freeze duration and retry wait are separate numbers

They used to be one, and code that assumes they still are will look correct.

- **`waitTime`** is how long *this request* sleeps before its next attempt.
- **The freeze duration** is how long *the whole fleet* stops spending.

They can legitimately disagree. `retryBackoffBaseTime: 0` is legal and means
"retry this request immediately" — it says nothing about whether the fleet should
keep hammering an upstream that just rejected us. Such a client has a zero
`waitTime` while its freeze may run the budget owner's entire refill interval.

Consequences worth knowing before touching either:

- **Gate freeze arming on the freeze duration, never on `waitTime`.** Keying it
  on the retry wait means a `retryBackoffBaseTime: 0` client can be rate limited
  repeatedly and never pause anyone sharing its budget.
- **`getFreezeBaseTime` is where the configured back-off becomes a duration**, so
  it is where zero, `NaN` and negatives are floored. A negative is the dangerous
  one: it is truthy, so the arming gate fires with `frozenUntil` already in the
  past, leaving the client in permanent single-probe thaw. `retryOptions` is never
  validated at construction, and `Number(process.env.BACKOFF)` with the variable
  unset is a realistic `NaN` source.
- **Subclasses that override must fold `super`'s result in**, never re-read
  `retryOptions`. `Math.max(interval, NaN)` is `NaN`, which serialises as `null`
  and reads as falsy at the arming gate — so a raw read arms no freeze at all.
- **`armFreeze` is monotone**: it keeps the longer of the two deadlines and never
  shortens one already standing. Everything that books a wake-up depends on this.
  A wake-up computed from a monotone deadline is either exact or early, and early
  self-corrects because the pass declines and re-books. A shrinking deadline would
  make it *late* with nothing behind it.

  It follows that no code path ends a freeze early. `clearFreezeState`,
  `resetTokenBucket` and `clearConcurrency` have no call sites in
  `packages/core/src` outside the backends themselves — they are on the interface
  for an operator, and are documented as such in
  [Concepts](concepts.md#clearing-a-freeze-by-hand). Keep them that way: a caller
  inside the library would be the shortening path this bullet exists to prevent.

## Claiming and then verifying leaves a residue

Admission excludes a request's own id from the occupancy sum, so that a resubmit
does not compete with itself. The consequence is that a claim for an id which has
just released always succeeds and silently re-creates its entry — and nothing is
left to release it, because the request is done, its queue entry is gone and its
`requestDone` is already consumed. The capacity sits claimed until its TTL,
shrinking the effective cap, and on a small cap taking it to zero.

`processRequests` re-reads the entry after claiming to catch this. **That narrows
the window; it does not close it.** A completion landing between the read and the
notification still strands the claim until the slot TTL, and orphan cleanup
cannot help because there is no entry left for it to see.

Closing it properly would mean claiming and confirming in one atomic operation,
which is impossible while the claim and the queue entry are separate keys written
by different callers. If you are tempted to "fix" the residue, that is the actual
prerequisite.

The related asymmetry: **a read that failed is not evidence the request is
gone.** Treating an unreadable entry as still queued keeps the claim, which is
the recoverable error — the request either runs, or times out and releases
through the ordinary abandonment path. Treating it as absent releases capacity
for a request that may still be live.

## A removed request cannot be re-added

`removeRequest` marks the id for as long as that request's own add could still be
outstanding, and `addRequest` refuses a marked id. That is what makes the ordering
contract on `addRequest` hold without trusting the transport to preserve it.

The contract is that the add commits before any `publish` issued after it is
delivered. Command order on one connection used to be enough, and it is not:
**ioredis retries a `NOSCRIPT` as a brand-new command**, attached in a promise
callback (`Script.js`), so it is queued a microtask later than anything the
application wrote in between. A plain `PUBLISH` issued behind an `addRequest`
therefore reaches Redis first whenever the script needs reloading — after an
operator `SCRIPT FLUSH`, or against a node that never loaded it. A full restart is
safe: the socket drops and ioredis re-`EVAL`s.

Without the mark, the abandonment's removal finds nothing to remove and the add
then creates an entry that is `pending`, carries a live `ownerId`, and has no
waiter. `cleanupOrphanedRequests` spares it precisely because its owner is alive,
and it still counts in `ZCARD` — so `queueEmptyGate` refuses the fast path for the
whole client, turning every request from two round-trips into nine, until the 24h
key TTL.

Three properties make the mark safe rather than a second hazard:

- **Ids are `crypto.randomUUID()` and never recycled**, so the only add a mark can
  refuse is one for the very request that was removed.
- **A retry keeps its entry** rather than removing and re-adding —
  `handleRequestDone` removes only on success or terminal failure — so a removal
  and a later add of one id never both happen in the ordinary flow.
- **It expires**, and it is sized from the consumer's `cleanupTimeout` rather than
  fixed. Raising `cleanupTimeout` lengthens the very window the mark exists to
  cover — how long a request's own enqueue can still be outstanding when another
  process removes it — so the mark is
  `max(REQUEST_TOMBSTONE_TTL_SECONDS, cleanupTimeout)`. The constant is the floor
  at 60s, not the value. The two used to drift apart, a fixed minute against a
  window the consumer configured. A permanent marker would also be correct, and
  would grow one key per request forever.

The race is narrow and the fix is not: the mark holds whichever of the two
actually arrives first, which is why the conformance tests assert the invariant
directly rather than trying to win the race.

## TTL arguments are validated before the write, not after

`normalizeTtlSeconds` in `packages/core/src/backend/ttl.ts` is the one reading of a
TTL, shared by both backends, and it is called **before** anything is written.

That ordering is the point. A Redis pipeline is not a transaction, so an `HSET`
followed by an out-of-range `EXPIRE` leaves the hash written with no expiry at all
— for a credential hash, an encrypted refresh token persisted forever. The memory
backend had the mirror-image bug: a `NaN` deadline made its `now >= expiresAt` test
false forever, so the key never expired either. Validating first removes both.

Consequences worth knowing:

- **A fraction rounds up**, because a TTL here is garbage collection rather than
  expiry enforcement — `expiresAt` decides whether a token may be used. Arriving a
  second late costs memory; arriving early discards a live credential.
- **Non-finite and out-of-range raise** rather than clamping, because both mean the
  caller computed something it did not intend. A caller with no bound passes no
  TTL.
- **Zero or negative still means "expire now"** on both backends, matching Redis
  `EXPIRE`. That is a real delete, so it is the one TTL that can lose data
  silently; `credentialTtlSeconds` floors well above it.
- **`credentialTtlSeconds` clamps its upper end too**, because
  `refresh_token_expires_in` is untrusted provider input checked only for being
  finite and positive, and a "never expires" sentinel exceeds what Redis accepts.

## The Redis backend requires one logical keyspace

Several scripts build metadata keys inside Lua from a prefix — they discover which
keys they need from a `ZRANGE` or `SMEMBERS` at runtime, so no `numberOfKeys` could
declare them — and two (`cleanupStaleFrozenGrants`, `getInstances`) take a key
prefix through `ARGV`. That breaks the `EVAL` key-declaration contract, which
matters for anything routing by key: Redis Cluster, a proxy, an active-active
deployment. **Dianemo supports a single logical keyspace only**, which is why no
Cluster adapter exists and `redisBackend` types its parameter as `Redis`.

## Absent state reads as permissive

Both backends read a missing key as the permissive answer. A token bucket with
no `tokens` field is full — `finiteNumber(..., maxTokens)` in
`lua/tokenBucket.ts`, `parseStoredNumber(..., config.maxTokens)` in the memory
backend — and absent freeze state is no freeze. That is the only sane reading
for a client's first request: nothing has been written yet, and the alternative
is that a new client stalls until something seeds its keys.

The cost is that **a key that was lost is indistinguishable from one that never
existed**. A bucket evicted mid-interval reads as a fresh full budget and the
fleet overspends; an evicted freeze resumes sending into a vendor that just
returned 429. Both are silent — there is no error to raise, because from the
script's point of view nothing went wrong.

The mitigation is operational rather than in code: Redis must run
`noeviction`, which is stated for users in
[the Redis backend's operational notes](backends/redis.md#operational-notes).
The alternative — reading absence as *unknown* and refusing admission until
state is proven — was rejected because it inverts the failure without removing
it. Every genuinely-first request and every new client would wedge alongside
every eviction, and nothing available at the point of the read distinguishes
the three. A limiter that fails closed on its own cold start is a worse trade
than one that depends on a documented Redis setting.

## An alive-instance set that omits the sweeper is not evidence

Orphan cleanup acts on the alive-instance set, and a set that has been lost is
indistinguishable from a fleet that died. Eviction under an `allkeys-*` policy, a
neighbour's `FLUSHDB`, and the window after a recovery before every replica has
re-registered all read the same way: "everyone is gone". Sweeping on that reads
every queued request as orphaned and deletes work whose owners are alive and
heartbeating — and the caller is told nothing, because it goes on waiting out its
admission budget and fails with `request_timeout` like any other slow request.

What makes the read checkable is that **every instance re-adds its own id on each
heartbeat**. A set that does not contain the sweeper's own id therefore cannot be
authoritative, whatever else it holds. Both backends refuse to sweep at all in
that case, and the Lua also refuses when its alive-id argument does not decode.
Skipping leaves a genuine orphan for one more health tick; sweeping on bad
evidence loses live work. The asymmetry decides it.

That is the opposite resolution to the section above, deliberately. An absent
token bucket is read permissively because a first request is the ordinary case
and failing closed would wedge every cold start. An absent alive set has no such
benign reading — nothing legitimately produces one that omits a live sweeper — so
the only thing it can mean is that the read is wrong.

One consequence, and it is the trade rather than a regression: **election
promotes a client to controller before its own registration lands**, so the
promotion sweep skips. A previously-crashed fleet's entries are reaped by the
health tick instead, up to `healthCheckIntervalMs` later — 10s by default. An
orphan lingering one tick is the price of never deleting live work, which is also
why the skip logs at debug rather than warn: every boot reaches it once with
nothing wrong.

## Two backends, one contract

`memoryBackend()` and `redisBackend()` implement the same interface and must be
semantically identical. `test/clientTypes/backendParity.test.ts` is the guard,
and a change to one backend's semantics is expected to touch the other.

Where they legitimately differ is *how* atomicity is achieved, not whether it
holds:

- Redis uses Lua scripts, because between a read and a write from process A,
  process B can and will interleave.
- The memory backend is synchronous. A function body runs to completion before
  anything else does, so atomicity is a property of the language rather than
  something to arrange. That is why there is no locking in it, and why it is
  faster.

Two things follow that are easy to get wrong:

- **Clamps must match on both sides.** A stats read that reports a token balance
  the acquire scripts would not actually hand out is a disagreement, even though
  neither side is "wrong" on its own. `maxTokens` can shrink underneath a stocked
  bucket — that is what `rateLimitChange` is for — so the clamp is unconditional
  rather than applied only after a refill.
- **`addRequest` must commit before any `publish` issued after it is delivered.**
  An abandonment announces itself on `requestDone` while the add may still be
  outstanding, and that announcement is what removes the entry. A removal
  delivered first finds nothing, the add then creates the entry, and it survives
  with a live `ownerId` that nobody awaits — spared by `cleanupOrphanedRequests`
  because its owner is alive, and pinning the queue non-empty, which disables the
  fast path for the whole client. Both shipped backends satisfy this
  *incidentally* — Redis orders commands on one connection, the memory backend
  commits without yielding — so a backend that batches, proxies or resolves the
  call across a yield has to arrange it explicitly.

One difference is deliberate and is not a parity break. `close()` releases what
the backend itself opened, and the two own different things: the memory
backend's store *is* the shared state, so closing discards it, while the Redis
backend owns only the pub/sub duplicate it created and must leave the caller's
connection and every key untouched. Both stay usable afterwards — `stop()`
closes the backend and `start()` re-subscribes, so a handler restart on one
backend is a supported pattern rather than a leak, and neither backend refuses a
`subscribe` after a `close`.
