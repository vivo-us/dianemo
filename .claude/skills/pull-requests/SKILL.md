---
name: pull-requests
description: Opening and maintaining a pull request in dianemo — the conventional-commit title format, branch naming, the body shape this repo actually uses, the version plan CI demands, and keeping the body current as the branch changes. Use when opening, drafting, writing, or updating a PR or its description, when asked to put a change up for review, and after pushing further commits to a branch that already has a PR open.
---

# Pull requests

## Before anything

Run the `pre-commit-checks` skill first — including the two steps a green `npm test` does
not cover: the `--typecheck` form, and a run with a real Redis. Five CI jobs run on every
PR and they are the only enforcement there is, so a red pipeline on a fresh PR means a step
was skipped, not that CI found something clever.

**If your change touches either published package outside Markdown, write the version plan
before you open the PR.** `Version Plan Check` fails the PR otherwise.

## Branch and base

PRs target `master`. Branches are `<type>/<subject>` with the same type words as the commit
titles: `feat/multiple-rate-limits`, `fix/…`, `docs/…`, `chore/…`.

## Title

The title is a **conventional commit**, lowercase, matching this repo's history — `feat:`,
`fix:`, `docs:`, `refactor:`, `chore:`, with an optional scope and a `!` for a breaking
change:

```
feat!: several rate limits per client, template-declared plans, and one client shape
refactor!: normalise rate limits to one array shape and one client class
docs: correct how tests resolve core, and where the OTP comes from
```

Not Title Case, and not a mechanism description. Let the title carry the **subject** — what
a reader of the changelog needs — rather than the files touched.

```bash
gh pr create --base master --title "feat: …" --body-file <path>
```

Write the body to a file first; it is long and multi-line, and `--body` mangles it. Use the
session scratchpad, not the repo.

## The version plan

```bash
npm run version-plan          # writes .nx/version-plans/<name>.md
npm run version-plan:check
```

Nx generates each package's `CHANGELOG.md` from these files, so **the plan body is the
release note** — it is read by consumers, not reviewers. Write it as prose in the repo's
voice, and put the consumer's obligation under `BREAKING:`:

```markdown
---
"@dianemo/core": major
"@dianemo/backend-redis": major
---

`rateLimit` takes a list of limits, and a request is sent only when every one of them
admits it.

BREAKING: `rateLimit` no longer accepts a bare limit object — wrap it in a list. Budget
keys move to `<client>:rateLimit:<limitName>`, so a rolling deploy would run two key
layouts at once and can double the send rate; stop the fleet and restart it.
```

Only `@dianemo/core` and `@dianemo/backend-redis` require plans, and `**/*.md` is ignored —
a docs-only PR needs none. Ask `plan:check` rather than reasoning about paths.

**Anything that changes the shared key layout is a rolling-deploy hazard and must say so**,
in the plan and in the PR body. Two replicas metering different key names do not see each
other's balance, so the fleet can send at up to twice the agreed rate for the length of a
rollout. That is a data-plane incident, not a note.

## The body

**There is no PR template in this repo.** PR #1 is the working model, and its shape is
worth following because it is organised around what a reviewer has to decide:

### Opening paragraph

What this is and what standard it was held to. One or two sentences — "Two features, and
the simplification they made possible. Four commits, each verified against a real Redis
before the next was built on it." A reviewer should know what they are about to look at and
how much to trust it.

### One `##` section per theme

Not per file and not per commit. Each section says what changed and **why this way**, and
names the alternative that was rejected where there was one. This is the part a reviewer
actually works from, so put the reasoning that is not visible in the diff here:

> The property that matters is all-or-nothing. Claiming budgets in turn would leave the
> first spent whenever the second declined, and no decline path hands it back.

A table earns its place when a change has a measurable shape — line counts before and after,
classes removed, an operation's cost. Prose earns it the rest of the time.

### `## Breaking changes`

One bullet per break, each saying what a consumer must do. State the version each package
goes to. Repeat the rolling-deploy warning here if there is one; a reader who skips the
version plan must not miss it.

### `## Verification`

What you ran, and what you could not:

- The test totals, and **whether the Redis-gated suites actually ran**. "1023 tests, 34
  files, both backends against a real Redis — the 8 Redis-gated suites that normally skip
  all ran" is the sentence that matters here, because a green run without Redis is 112 tests
  short and silent about it.
- `check`, `lint`, `format`, `sort-imports:check` and `build` clean.
- **Any test assertion you changed, and why the behaviour under it is still right.** Changing
  a test to match new behaviour is normal; changing one to make it pass is not, and the
  difference is only visible if you say which you did.
- What you could not exercise. An unverified path stated plainly is useful; an unverified
  path presented as tested is the failure mode with teeth.

### Optional sections that pull their weight

- **`## Also fixed in passing`** — drive-by fixes, so a reviewer knows they are not part of
  the main thrust.
- **`## Left alone deliberately`** — the related-looking thing you did not touch, and why.
- **Convention changed** — a house rule this PR establishes or alters. If it belongs in
  `CLAUDE.md`, `CONTRIBUTING.md` or a skill, change it in this PR rather than promising to.

Delete a heading you have nothing for rather than writing "N/A".

## What reviewers here look for specifically

- **A backend change that touched only one backend.** The two must stay semantically
  identical. Say explicitly that you changed both, or why one needed no change —
  `test/conformance.test.ts` only proves it if you ran it with Redis.
- **A new or changed error `code` string.** It is public API that hosts duck-type on, so it
  is a breaking change; say so and make sure the plan is a `major`.
- **A key layout change.** As above.
- **A number with no source.** A timeout, a TTL, a band width or a retry ceiling should say
  where it came from and what happens either side of it.

## Keep the body current

**When you push further commits to a branch that already has a PR, update the body in the
same breath.** A stale body is worse than none — a reviewer who reads it and then finds a
change it does not mention stops trusting the whole thing.

```bash
gh pr view <number> --json body -q .body > <scratchpad>/pr.md
# edit, then:
gh pr edit <number> --body-file <scratchpad>/pr.md
```

Re-check on every push: a new section for what the new commits did, the breaking-change list
still complete, the verification counts still the ones you actually ran, and the version plan
still matching the size of the change. **If the scope grew, say so in the opening paragraph.**
If you cannot tell whether something merits a bullet, it does.

## Footer

End the body with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```
