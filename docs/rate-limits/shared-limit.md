# `sharedLimit`

Points a client at another client's budget. Several sets of credentials, one
rate limit.

```ts
{ type: "sharedLimit", clientName: "carrier:_:production" }
```

## When to use it

The vendor counts several credentials against **one** quota:

- sub-accounts or child accounts under a parent contract,
- regional endpoints that share a global limit,
- a sandbox key metered against production's allowance,
- a second credential used only for a specific endpoint.

If each credential has its own independent quota, do **not** use this — give
each client its own [`requestLimit`](request-limit.md). If the credentials
belong to different tenants of the same client and each has its own quota, use
per-grant isolation instead.

## How it works

A `sharedLimit` client is constructed with the **parent's** name. Its queue,
token bucket, freeze state and pub/sub channels are all the parent's, so its
requests flow through the parent's queue and are metered by the parent's budget.
The only thing that differs is which credentials are attached.

Three consequences follow:

- **It is always a worker, never a controller.** The parent's controller drains
  the shared queue, so the child never runs the rate-limiting logic itself.
- **Credentials are not shared.** The token cache is keyed by the name the
  client was registered under, not by the parent's name, so each set of
  credentials refreshes and caches its own token. This is the point of the type
  — sharing that cache would mean one client silently sending the other's
  access token, which the vendor would accept.
- **You address it by its own registered name.** The sharing is a property of
  the registration, not of the call site: `handleRequest({ clientName })` looks
  the client up by the name you registered it under. Addressing the parent's
  name reaches the parent, with the parent's credentials.

## Freeze is shared too

If the upstream rate-limits one credential, the freeze applies to the shared
budget — which is usually right, because the vendor is counting them together.
If a 429 on one credential should _not_ pause the others, they do not actually
share a limit, and this is the wrong type.

One consequence worth stating: a child with `retry429s: false` will not arm the
freeze at all, so a 429 it receives does not pause its siblings. Turning off 429
retries on one member of a shared budget disables the shared back-off with it.

## `rateLimitChange` on a child rewrites the shared budget

A child owns no budget, so a `rateLimitChange` on a child applies to the budget
it draws on — the parent's. That is the only coherent target, but it means one
credential's response headers can redefine the quota every sibling is metered
against, last writer wins. Put `rateLimitChange` on the client that owns the
budget unless you specifically intend that.

## Configuration ordering

The parent must exist before a client can share its limit. In a plugin's
template builder, register the parent first: the child resolves the parent by
name at construction, and a name that does not resolve is a configuration error
rather than a silent fallback to unlimited.
