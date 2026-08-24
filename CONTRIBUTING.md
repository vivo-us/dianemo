# Contributing

## Comments

This section exists because comment rot has a direction: comments accrete. Every pass over a
tricky function is tempted to add one, almost none are tempted to delete one, and a file that
nobody sets a bar for ends up at 30% commentary — where the signal that mattered is buried in
the restatement that didn't.

### The test

> A comment earns its place by carrying a fact that is **not in the code, not in the types, not
> in the test names, and not in `docs/`**.

Everything below follows from that one question. Ask it before writing a comment, and ask it
again before leaving one you didn't write.

### What passes

**An external system's behavior.** The highest-value comments in this repo are the ones
recording something a dependency or a runtime does that you would never guess from the call
site. These are unguessable, they are load-bearing, and losing one costs someone a day.

```ts
// `setTimeout` silently collapses a delay above 2^31-1 ms to 1ms, which turns a
// long refill wait into a hot spin. Waking early is harmless: the head re-declines.
```

Cite the source when there is one to cite — `RFC 6749 §6`, the name of the function that
misbehaves, the Lua builtin whose coercion surprised you. "Axios's Node adapter calls
`signal.addEventListener` unguarded" is checkable. "Axios is picky about signals" is not.

**An ordering or placement that is the correctness property.** When *where* a statement sits
matters more than what it does, say so in one clause, because the next person to tidy the
function is the audience:

```ts
// Cleared before the pass runs: callers that cannot proceed re-book on every pass,
// and a booking made while this timer is still set would be dropped.
```

**A deliberate choice where the obvious code is wrong.** Name the rejected alternative in a
clause, not a paragraph:

```ts
// `"rateLimit" in data`, not `??`: an explicit null or undefined must still throw,
// because the merge spread would overwrite a parent's limit with it.
```

**A pointer.** One line, and worth more than most paragraphs — especially between the two
backends, which must agree:

```ts
// Matches the clamp in the acquire scripts, so reported stats agree with what the
// bucket hands out. See the memory backend's twin.
```

### What fails

**Restating the next line.** If the code says `parseFloat`, `// Parse the value` adds nothing.

```ts
// Apply lazy calculation          ← delete
// Build updates object            ← delete
// Not frozen - can process        ← delete; the `if` says this
```

**How an earlier version was wrong.** This is the big one here, and it is the most tempting to
write, because you have just finished debugging it and the failure is vivid. Keep the **rule**
that came out of it; drop the story:

```ts
// ✗ Carrying the previous token's expiry over to its replacement made the hash TTL
//   shrink on every rotation and never grow, eventually deleting a refresh token
//   that was still valid — the precise outcome the TTL sizing exists to prevent.
//   Normalised ONCE and then used everywhere. Three separate readings of this field
//   disagreed about `0`. A provider sending `refresh_token_expires_in: 0` therefore
//   recorded the expiry as UNKNOWN while sizing the TTL from the access token alone,
//   so the grant was deleted an hour later with a live refresh token inside it.

// ✓ A refresh response may rotate `refresh_token` while omitting its expiry
//   (RFC 6749 §6); an unknown expiry is recorded as unknown rather than inherited.
//   Read once, because `0` reads differently through truthiness and `??`.
```

The deleted version is not lost. It is in the commit that introduced the fix, reachable by
`git log -S` and `git blame`, and — more usefully — in a test named for the behavior. We
already do this well:

```
it("sizes from a stored refresh token when the response omits one")
it("survives a cyclic body instead of throwing from inside the catch")
it("rejects an impossible cost up front instead of stalling the client")
```

A test name is a better home than a comment for one specific reason: **it fails when it stops
being true.** A comment describing a regression is unfalsifiable and drifts silently.

So: **if a comment narrates a bug, the fix is a test named for the behavior, not a longer
comment.** If no such test exists, that is the actual gap.

**Numbers from one debugging session.** `measured at ~900 acquire attempts per second` is an
artifact of one machine, one config, one afternoon. "Turns the drain into a hot spin" is the
durable claim.

**Anything already said above it.** A sentence in a method's JSDoc does not want repeating
inline eight lines later.

**Emphasis as a substitute for precision.** Comments here shout — `ONCE`, `BEFORE`, `MIS-NAMED`,
`is not`. Capitals stop meaning anything once a file is full of them. Put the load-bearing word
in a short sentence instead of an all-caps one.

### Length

Roughly: **a rationale that needs more than about four lines is telling you it belongs in
`docs/`.** Not always — a genuinely gnarly atomicity argument sometimes needs six — but treat
the fifth line as a prompt to ask where this really goes.

### Where it goes instead

| The reasoning is… | It goes in |
|---|---|
| Something a **user** must understand to configure or call this correctly | `docs/concepts.md`, or the relevant `docs/rate-limits/*` / `docs/backends/*` page |
| Deep implementation reasoning a **new maintainer** should read once, spanning several files | `docs/design-notes.md` |
| A behavior that must not regress | a test named for the behavior |
| Why *this* line is the way it is | a comment, kept short |
| What an exported symbol does and how to choose between options | JSDoc |

When reasoning moves to `docs/`, leave the pointer:

```ts
// Score layout and the band arithmetic: docs/design-notes.md#queue-score-bands
```

### JSDoc

On **exported** API, JSDoc is the manual and should be generous: what it does, the contract,
the failure modes, and which option to pick. It is what a consumer reads in their editor and it
is worth real prose. `logger.ts`, `plugin.ts`, `credentialTtl.ts` and `types.ts` are the model.

On **internal** members, JSDoc is only for a contract that callers or subclasses depend on — the
thing an override must preserve. Do not restate a signature the types already state:

```ts
/** @param name The client name. */   ← delete; `name: string` said that
```

### Two rules about the act of editing

**Replace a comment; never stack a new draft on the old one.** This produced the worst comment
in the repo's history — three consecutive drafts of one paragraph, 18 lines, all saying the same
thing, because three passes each appended rather than edited. If you are rewriting a comment,
delete the one that is there.

**Deleting a stale comment is a contribution.** You do not need a reason beyond "it does not
pass the test above." Do not preserve a comment out of deference to whoever wrote it.

### Section separators

One line, in the `// ---- name` form, and only in files long enough to need navigating
(`backend/types.ts`, `backend/memory.ts`):

```ts
// ---------------------------------------------------------------- store
```

Not the three-line boxed form. It costs three lines to do the same job, and a file that needs
six of them is usually asking to be split instead.

## Working on this repo

```bash
npm install
npm test              # vitest; redis-backed suites skip without a local redis
npm run check         # tsc, plus the type-level tests in type-tests/
npm run lint
npm run format        # prettier --check; `format:fix` to write
npm run sort-imports:check
npm run build
```

CI runs all of the above, plus the test suite against both a memory and a real Redis backend.
Both backends must agree — `test/clientTypes/backendParity.test.ts` is the guard, and a change
to one backend's semantics is expected to touch the other.

### Publishing

Releases go out with `NPM_CONFIG_OTP=<code> npm run release:publish`. The one-time password has to
arrive by environment: nx parses `--otp` with yargs, which reads the value as a number and drops a
leading zero, so roughly one code in ten reaches npm a digit short and is refused. Quoting does not
help — the coercion happens before the quotes matter.

### Tests read source, not the build

`vitest.config.ts` aliases `@dianemo/core` and `@dianemo/backend-redis` to their `src/index.ts`,
matching the `paths` the root `tsconfig.json` already gives tsc. Both resolvers therefore agree,
and `npm test` runs on a fresh clone before anything has been built.

The alias is load-bearing rather than a convenience. The Redis backend's `ops/` layer imports
`normalizeTtlSeconds`, `parseStoredNumber`, `calculateQueueScore` and
`REQUEST_TOMBSTONE_TTL_SECONDS` from `@dianemo/core` by name. Without it those resolve through the
`node_modules/@dianemo/core` symlink onto `packages/core/dist`, which a fresh clone has not built —
so every suite fails to collect — and a *stale* `dist` is worse, because the Redis half of a run
then reads compiled core while the memory half reads the source you just edited. The two disagree
and it reads as a parity bug in the backend you did not touch.

`npm run build` is still required before packing or publishing, since `main` and `exports` name
`dist`. It is not required to run the tests.
