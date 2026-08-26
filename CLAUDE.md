# dianemo

A host for outbound HTTP clients that share a rate limit across processes. `packages/core` holds
the handler, clients and the in-memory backend; `packages/backend-redis` holds the Redis one.
Both backends implement `DianemoBackend` and must stay semantically identical.

## Skills

Three skills in `.claude/skills/` carry the detail this file only summarises. Load the one
that fits rather than working from memory:

- **`typescript-conventions`** — declaration style, file size, the twin rule between the two
  backends, error classes and their `code` strings, where shared code lives.
- **`pre-commit-checks`** — the full command sequence, what each command does *not* cover,
  and the two ways a green local run differs from CI.
- **`pull-requests`** — title format, branch naming, the body shape this repo uses, and the
  version plan CI demands.

## Comments — read `CONTRIBUTING.md` before adding one

`CONTRIBUTING.md` has the full criteria with examples. The short version:

**A comment earns its place by carrying a fact that is not in the code, not in the types, not in
the test names, and not in `docs/`.**

Write a comment for: an external system's documented or observed behavior (`setTimeout`
collapsing above 2^31-1 ms, `RFC 6749 §6`, what axios's Node adapter does with a signal); an
ordering that is itself the correctness property; a deliberate choice where the obvious code
would be wrong, naming the rejected alternative in a clause; a pointer to `docs/` or to the
other backend's twin.

Do **not** write: what the next line does; how an earlier version was wrong; numbers measured
during one debugging session; anything already stated in the JSDoc above it or in `docs/`.

Specifically:

- **If a comment would narrate a bug, write a test named for the behavior instead.** A test
  fails when it stops being true; a comment about a past regression drifts silently. Match the
  existing style: `it("sizes from a stored refresh token when the response omits one")`.
- **A rationale longer than about four lines belongs in `docs/`** — `docs/concepts.md` if a user
  needs it, `docs/design-notes.md` if a maintainer does. Leave a one-line pointer in the code.
- **Replace comments, never stack them.** Do not append a new draft above an existing one. If
  you are rewriting a comment, delete the one that is there.
- **Deleting a stale comment is a contribution.** No justification needed beyond the test above.
- Generous JSDoc on exported API; on internals only a contract that subclasses or callers
  depend on.
- Section separators are the one-line `// ---- name` form, not three-line boxes.

## Verifying a change

```bash
npm test && npm run check && npm run lint && npm run format && npm run sort-imports:check
```

`npm run build` must also pass. Redis-backed suites skip without a local Redis; CI runs them
against a real one. Import order is enforced by `scripts/sort-imports.mjs`, so run
`npm run sort-imports` rather than hand-ordering imports.

Two things that line does not do: `npm test` omits `--typecheck`, so the type-level suite CI
runs never executes, and without `REDIS_URL` a green run is 112 tests short. Before pushing,
load `pre-commit-checks` — it owns the full sequence and the reasons for its order.

Prettier owns formatting — do not hand-wrap code to a column. Comment prose is not prettier's
business, so wrap that at 80 columns by hand to match the surrounding files.
