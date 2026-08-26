---
name: typescript-conventions
description: TypeScript conventions for dianemo — declaration style, file size and section separators, the two-backend twin rule, error classes and their code strings, where types and helpers live, named signature types, and assigning awaits. Applies to .ts in packages/core and packages/backend-redis, in test/ and in type-tests/. Use when writing or refactoring TypeScript here — adding a backend operation, splitting a long file, tidying code, or reviewing a diff for style. Comments are not covered here — CONTRIBUTING.md owns those. For the checks before committing see pre-commit-checks.
---

# TypeScript conventions

House rules for a TypeScript file in this repo. Apply them to any file you create or
meaningfully change. Do not reformat untouched files as a side errand.

This is a **library with two packages**, and one relationship governs most of it:
`packages/core` holds the handler, the clients, the backend contract and the in-memory
backend; `packages/backend-redis` holds the Redis one. Both implement `DianemoBackend`
and **must stay semantically identical**.

| Kind                   | Where                                                            |
| ---------------------- | ---------------------------------------------------------------- |
| Public handler surface | `core/src/index.ts` — `RequestHandler`                           |
| Client                 | `core/src/client/index.ts`, `client/clientTypes/*.ts`            |
| Handler method         | `core/src/client/methods/*.ts` — long methods split off a class  |
| Backend contract       | `core/src/backend/types.ts` — `DianemoBackend`                   |
| Backend implementation | `core/src/backend/memory.ts`; `backend-redis/src/{index,ops}`    |
| Lua source             | `backend-redis/src/lua/*.ts` — Lua in template literals           |
| Utility                | `core/src/utils/*.ts`                                            |
| Types module           | any `types.ts`                                                   |

## The twin rule comes before every other rule here

A change to what a backend operation *means* is a change to both backends. Not "should
usually be" — `test/clientTypes/backendParity.test.ts` and `test/conformance.test.ts`
exist to fail when it isn't, and `docs/design-notes.md#two-backends-one-contract` is the
argument for why.

That extends into layout, which is why the large files are laid out the way they are.
`DianemoBackend` is section-separated in a fixed order:

```
store · locks · pub/sub · admission (fast path) · token bucket · concurrency ·
queue · freeze / thaw · instances · lifecycle
```

`backend/memory.ts` and `backend-redis/src/index.ts` both repeat that order in their own
separators, each with one local addition — `primitives` at the top of the memory backend,
`clock` in the Redis one. The Redis backend then splits the heavier operations out by the
same names again, one file per section, under `src/ops/` and `src/lua/`
(`tokenBucket`, `concurrency`, `queue`, `freeze`, `instances`).

**Put a new operation in the matching section of each.** The correspondence is what lets a
reader diff one backend against the other by eye, and it is worth more than any local
tidiness gained by breaking it.

## Declaration style

**`export function name()`, not `export const name = () =>`.** Function declarations
outnumber arrow consts roughly four to one in `packages/*/src`, and the exceptions are
mostly small factory or predicate values rather than a competing style. Class methods
are methods.

`import type` is enforced, not preferred: ESLint runs `consistent-type-imports` with
`fixStyle: "separate-type-imports"`, so a type-only import is its own `import type`
statement rather than an inline `type` specifier in a value import.

## File size

The measured shape of this repo is a lot of small files and four large ones:
`client/index.ts` (~1,500 lines), `backend/memory.ts` (~1,300), `core/src/index.ts`
(~970) and `client/methods/handleRequest.ts` (~660). Everything else is under ~500.

- **A new file targets under 300 lines.** If it is heading past that, it is usually a
  transport concern, a mapping concern and the orchestration over both.
- **The four large files are not an invitation to add a fifth.** They are large because
  each is one class implementing one interface, and `memory.ts` in particular is long
  precisely so that it reads section-for-section against its Redis twin. Splitting it
  would buy nothing and cost the correspondence above.
- **When you add to one of them, add to the right `// ----` section**, not to the end.

## Section separators

One line, in the `// ---- name` form, and only in files long enough to need navigating:

```ts
// ------------------------------------------------------------ token bucket
```

Not the three-line boxed form. `CONTRIBUTING.md` has the rationale; the point for you is
that a file needing six of them is usually asking to be split instead — with the backend
implementations as the deliberate exception.

## Errors

Everything thrown extends `RequestHandlerError`, which carries `statusCode` and `code`.

**The `code` string is public API.** Its JSDoc tells hosts to duck-type on `statusCode`
and `code` rather than import the classes, because the handler and the catch site may
resolve to different copies of the package. So renaming a code is a **breaking change**
and needs a `major` version plan — see `pull-requests`.

- Codes are `snake_case`: `client_not_found`, `request_cost_exceeds_budget`.
- A failure with its own class takes its code in that class's constructor.
  `ConfigurationError` and `ClientUnavailableError` take a `code` argument instead,
  because they cover a family of distinct causes.
- **One code per distinct failure**, never per function and never shared across two
  failures a caller would handle differently. A caller matching on the code cannot tell
  what failed otherwise, which is the entire reason the codes exist.
- `ConfigurationError` is for **programming errors** — bad config, a `:` in a template
  name, a duplicate plugin. Never a runtime condition.

Message prose is part of the error's value here: several messages name the offending
value and state the fix. Follow that.

## Assign an `await` before reaching into it

**Never `(await x).prop`, `(await x)[0]` or `(await x).length`.** The parenthesised form
buries the fact that the line does IO, throws away the name of what was fetched, and
leaves nothing to log or break on.

`packages/*/src` is at **zero** occurrences of this — the line is currently held, so any
you introduce is yours. The tests are not: about fifteen live in `test/`. Write new test
code the same way as source, but do not sweep the existing ones as a side errand.

A bare boolean predicate in a condition — `if (await client.hasOutstandingWork())` — is
accepted and used. The rule is about reaching *into* an awaited value, not about awaiting
in a condition at all.

## Name the types in a signature

A structural type written inline buries the signature. Declare it in the file's types
block, or in the area's `types.ts`, and refer to it by name so the function reads in one
line. Applies to return types and to any parameter whose type is more than a field or two.

Name them for what they are, not for the function they serve. Inferred return types need
no annotation — this is about types written out by hand.

## Where shared code lives

Each thing lives at the **lowest level that contains all of its users**:

| used by                                     | lives in                          |
| ------------------------------------------- | --------------------------------- |
| one module                                  | that module                       |
| several modules in one area                 | that area's `types.ts` / a util   |
| the handler and the clients                 | `core/src/utils/`                 |
| both backends                               | `core/src/backend/`, exported     |

The last row is load-bearing and easy to get wrong. The Redis backend's `ops/` layer
imports `normalizeTtlSeconds`, `parseStoredNumber`, `calculateQueueScore` and
`REQUEST_TOMBSTONE_TTL_SECONDS` **from `@dianemo/core` by name** — shared arithmetic lives
in core and is imported, rather than being reimplemented in the backend where it would
drift. If you find yourself writing a second copy of a calculation the memory backend
already does, export the first one instead.

Push things down as far as they go; promote only when a second area reaches for them.

## Extract only when it buys something

The size targets are not a mandate to shred. A 40-line function that reads straight
through beats four 10-line functions the reader has to reassemble.

Extract when:

- **Two or more genuine call sites.** Any size — and across the two backends, that is the
  rule above, not a judgment call.
- **The name replaces a comment.**
- **The caller would otherwise be doing two jobs.**

A single-use helper under about 15 lines is not pulling its weight.

## Imports

Ordered by `scripts/sort-imports.mjs`: descending line length, single-line statements
first, multi-line `{ … }` imports last. **The script is authoritative — do not hand-order.**
It walks `.ts`/`.tsx` only.

## Comments

**`CONTRIBUTING.md` owns this, and it is the longest section in it.** Read it rather than
working from memory; nothing from it is restated here on purpose, because a second copy of
the rules drifts from the first the moment either moves.

The one-line version, which is not a substitute for reading it: a comment earns its place
by carrying a fact that is not in the code, not in the types, not in the test names and
not in `docs/`. A rationale over about four lines belongs in `docs/` with a pointer left
behind; a comment narrating a bug should be a test named for the behavior.

`CONTRIBUTING.md` also has the table for **which** `docs/` page reasoning moves to —
`docs/concepts.md` and the `docs/rate-limits/*` and `docs/backends/*` pages for something a
user must know, `docs/design-notes.md` for maintainer reasoning spanning several files.

## Verifying a re-layout

Moving declarations is type-neutral, so anything that breaks is a real mistake. Run the
`pre-commit-checks` skill — it owns the command sequence and the order.
