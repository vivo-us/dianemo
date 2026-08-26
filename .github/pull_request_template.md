## Description

**REQUIRED** | What this changes and why, and what standard it was held to. A paragraph
or three — the gist, not a file list.

## Changes

**REQUIRED** | One section or bullet per theme, not per file and not per commit. Say what
changed and **why this way**, naming the alternative you rejected where there was one.
This is the part a reviewer works from, so put the reasoning the diff cannot show here.

## Version plan

**REQUIRED** | The bump each published package takes, and the plan file declaring it.
Write `None — no published package touched outside Markdown` if that is the case.
`npm run version-plan:check` is the authority; CI runs it too.

## Breaking changes

**OPTIONAL** | One bullet per break, saying what a consumer must do. Delete the heading if
there are none.

A renamed error `code`, a moved key layout or a changed public signature all belong here.
**A key layout change is a rolling-deploy hazard** — replicas metering different key names
cannot see each other's balance, so the fleet can send at up to twice the agreed rate for
the length of a rollout. Say so here as well as in the plan.

## Verification

**REQUIRED** | What you ran, and what you could not.

- Test totals, and **whether the Redis-gated suites actually ran**. Without `REDIS_URL` a
  green run is 112 tests short and silent about it, and `test/conformance.test.ts` — the
  suite proving the two backends agree — loses its Redis half rather than skipping visibly.
- Whether you ran the `--typecheck` form. `npm test` omits it, so the type-level suite CI
  runs does not execute locally.
- `check`, `lint`, `format`, `sort-imports:check` and `build`.
- **Any test assertion you changed, and why the behaviour under it is still right.**
- Anything you could not exercise. Stated plainly it is useful; presented as tested it is
  the failure mode with teeth.

## Additional notes

**OPTIONAL** | Anything else a reviewer needs that the diff cannot tell them — a drive-by
fix, a related-looking thing you left alone and why, a house rule this PR changes. Delete
the heading rather than writing "N/A".
