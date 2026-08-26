---
name: pre-commit-checks
description: The checks to run before committing or opening a PR in dianemo — format, sort imports, typecheck, lint, build, the test suite against both backends, and the version plan a published change needs. Covers what each command does not cover, and the two ways a local run differs from CI. Use before a git commit or push, when opening or updating a PR, and when asked whether a change is finished, ready, or safe to publish.
---

# Pre-commit checks

```bash
npm run format:fix && npm run sort-imports && \
npm run check && npm run lint && npm run build && npm test
```

That is the whole sequence when everything passes. The rest of this skill is the order's
reasoning, what each command silently leaves out, and the two steps above that are not
enough on their own.

Nothing here is enforced by a git hook. There is no husky or lint-staged, so a skipped
step is not caught before review — CI is the first thing that will tell you.

## 1. Format, then sort imports — in that order

```bash
npm run format:fix        # write
npm run format            # check only
npm run sort-imports      # write
npm run sort-imports:check
```

**The order is load-bearing.** `scripts/sort-imports.mjs` orders imports by descending
line length, so Prettier changing a line's width changes where that line sorts. Sort
first and you can be left with a file that fails `sort-imports:check` after formatting.

Prettier owns formatting — **do not hand-wrap code to a column**, and do not hand-write
how a union breaks. Run it and take what it gives you.

**Re-run `npm run format` immediately before committing**, not when you finish editing. A
later edit undoes the pass and `git add -A` commits it without complaint.

### What these two do not cover

| Not covered                                  | Consequence                                                    |
| -------------------------------------------- | -------------------------------------------------------------- |
| `type-tests/**` — outside the Prettier glob  | Format it by hand, in the style of the files around it         |
| **All Markdown** — `docs/`, READMEs, `CLAUDE.md` | Prettier never touches prose here. Wrap at 80 columns by hand |
| `bench/**` for the sorter — it walks `.ts` only | The `--dir bench` pass reports "All 0 file(s)"; `.mjs` imports are yours to order |

The Prettier glob is `{packages/*/src,test,bench,scripts}/**/*.{ts,mjs}` and the ESLint
target is `packages/*/src test` — so `type-tests/`, `bench/` and `scripts/` are linted by
nothing. Comment prose being hand-wrapped is a rule, not an oversight: see `CLAUDE.md`.

## 2. Typecheck

```bash
npm run check      # tsc -p tsconfig.json, then tsc -p type-tests/tsconfig.json
```

Two passes. The first covers `packages/*/src/**` and `test/**`; the second covers
`type-tests/**/*.test-d.ts`, which **nothing else runs** — Vitest's `typecheck.include` is
`test/**/*.test-d.ts`, so the `type-tests/` directory is reachable only through this
command.

The root `tsconfig.json` maps `@dianemo/core` and `@dianemo/backend-redis` to their
`src/index.ts` via `paths`, so a core change is typechecked against the Redis backend here
rather than after a publish. No build is needed first.

## 3. Lint

```bash
npm run lint
```

`consistent-type-imports` is an error, not a warning, so a type-only import must be its own
`import type` statement. `--fix` handles it.

## 4. Build

```bash
npm run build      # nx run-many -t build, both packages
```

Nx caches this. A run that reports `Cache 2/2 hit` in 120ms did not compile anything, which
is correct and not something to work around. `build` declares `dependsOn: ["^build"]`, so a
core change rebuilds the Redis backend after it.

**The build is not required to run the tests** — see below — but it is required before
packing or publishing, since `main` and `exports` name `dist`.

## 5. Test

```bash
npm test                                            # vitest run
npx vitest run --typecheck                          # what CI runs
REDIS_URL=redis://localhost:6379 npx vitest run --typecheck   # what CI runs, with Redis
```

Two things are missing from a bare `npm test`, and both are the kind that pass locally and
fail in CI.

**`npm test` does not run the type-level suite.** The `test` script is `vitest run` with no
`--typecheck`, so `test/typeInference.test-d.ts` never executes. Measured on master:
`npm test` collects 32 files / 613 tests, `npx vitest run --typecheck` collects 33 / 618.
Those five tests guard plugin namespace inference — if `use()` ever returns `any`, every
runtime test still passes while consumers silently lose type safety. **Run the `--typecheck`
form before pushing.**

**Redis-backed suites skip silently without `REDIS_URL`.** Eight files skip entirely —
`lifecycle.integration`, `luaGuards`, `redisClock`, `redisKeyTtl`, `redisOps`,
`clientTypes/backendFailure`, `replicas.failover`, `replicas.smoke` — and that is only 103
of it. The rest comes from suites that run against **every** backend and quietly lose their
Redis half, `test/conformance.test.ts` first among them. Measured on master with no Redis:
501 passed, **112 skipped**, reported as a green run.

The conformance suite is the one that proves the two backends agree. **A backend change
verified without Redis is not verified**, and the run will not tell you.

```bash
docker run --rm -p 6379:6379 redis:7-alpine    # CI uses the same image
```

Set `REDIS_URL` with no trailing path — the harnesses append a database index
(`${REDIS_URL}/${TEST_DB}`), so a trailing slash produces an unusable URL.

Expect **around 105 seconds**. `fileParallelism` is off deliberately: several suites assert
on timing and on occupancy at a shared upstream, and running four at once made them measure
the machine rather than the code. A slow run is not a hung one.

### Tests read source, not `dist`

`vitest.config.ts` aliases both packages to their `src/index.ts`, matching the `paths` the
root `tsconfig.json` gives tsc. So `npm test` works on a fresh clone before anything is
built, and — more importantly — a **stale `dist` cannot poison a run**. Without the alias
the Redis half of a run reads compiled core while the memory half reads your edits, the two
disagree, and it reads as a parity bug in the backend you did not touch.

## 6. A version plan, if a published package changed

```bash
npm run version-plan          # nx release plan — writes .nx/version-plans/<name>.md
npm run version-plan:check    # nx release plan:check
```

**Required whenever your change touches `@dianemo/core` or `@dianemo/backend-redis` in
anything other than Markdown.** Those are the only two projects in `nx.json`'s
`release.projects`, and `versionPlans.ignorePatternsForPlanCheck` is `**/*.md` — so a
docs-only PR needs no plan, and a change under `bench/`, `scripts/` or `.github/` needs
none either.

The plan file is frontmatter naming each package and its bump, then release-note prose:

```markdown
---
"@dianemo/core": major
"@dianemo/backend-redis": major
---

What changed, in the voice of a release note.

BREAKING: what a consumer must do, and what happens if they don't.
```

A renamed error `code` string, a moved key layout or a changed public signature is a
`major` — see `typescript-conventions` for why `code` counts. Ask Nx rather than grepping
paths: `plan:check` is the authority on whether a plan is required.

## 7. Check what you are actually committing

```bash
git status --short
git diff --staged
```

`git status --short` distinguishes staged (`M `) from unstaged (` M`); a plain `git diff`
hides staged work. Never commit `.env`. Do not sweep unrelated files into a feature
commit — a formatting pass over a file you did not otherwise touch belongs in its own
commit.

## 8. Self-review against the conventions

Load `typescript-conventions` and read the change against it, and read the comments in your
diff against **`CONTRIBUTING.md`**, which owns the comment rules in full. Read them rather
than working from memory — nothing from either is restated here on purpose, because a second
copy of the rules drifts from the first the moment either moves.

If you changed backend semantics, the specific question to ask is whether the **other**
backend needs the same change. `test/clientTypes/backendParity.test.ts` and
`test/conformance.test.ts` are the guard, and the second only runs with Redis.

## What CI re-runs

`.github/workflows/ci.yml` runs on every push to `master` and every PR. Four jobs, all on a
clean checkout with `npm ci`:

| job            | runs                                                                     |
| -------------- | ------------------------------------------------------------------------ |
| `static`       | `sort-imports:check`, `format`, `lint`, `check`, `build`                 |
| `test`         | `npx vitest run --typecheck` with a `redis:7-alpine` service             |
| `test-no-redis`| `npx vitest run --typecheck` with no Redis anywhere                      |
| `packaging`    | `build`, then packs `@dianemo/core` and installs the tarball into a bare project |

`.github/workflows/version-plan-check.yml` adds a fifth on PRs only:
`nx release plan:check --base=origin/master --head=HEAD`.

Three things CI does that you probably did not:

- **The `test` job asserts `REDIS_URL` is set** before running, on purpose: with it unset the
  job would silently duplicate `test-no-redis` and take every "runs on both backends" claim
  with it.
- **`test-no-redis` guards the promise that `@dianemo/core` is usable with no Redis in
  sight.** If you made core import something from the Redis package, this is what catches it.
- **`packaging` installs a packed tarball into a bare project**, asserts `ioredis` did not
  come with it, and typechecks a consumer file against the published `.d.ts`. A `file:`
  install symlinks the source tree and resolves the workspace's devDependencies, so it cannot
  detect a missing dependency — only the tarball can, which is how an unimportable published
  package slipped through once. Reproduce it locally with `npm pack` from
  `packages/core` and `npm install ./dianemo-core-*.tgz` in an empty directory.

CI only checks. It never runs `format:fix` or `sort-imports` in write mode, and nothing is
fixed for you.

## Publishing

```bash
NPM_CONFIG_OTP=<code> npm run release:publish
```

**The OTP has to arrive by environment.** Nx parses `--otp` with yargs, which reads the
value as a number and drops a leading zero, so roughly one code in ten reaches npm a digit
short and is refused. Quoting does not help — the coercion happens before the quotes matter.
