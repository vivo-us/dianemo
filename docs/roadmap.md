# Roadmap

What is planned, and why. No dates and no implementation detail — these are
directional, and the order below is roughly the order they are expected to land.

Nothing here is required for the library as it stands. Dianemo has run in
production since 2023 without any of it; these are the things that would make it
serve more people, or serve them in more places.

## Aggregate metrics

Dianemo emits OpenTelemetry traces today, which answer _what happened to this
request_. They do not answer _how close am I to the vendor's limit_, _how long is
work waiting_, or _how often are we freezing_ — the questions that decide whether
you trust a limiter enough to leave it alone.

Planned: fleet-wide aggregate metrics covering quota headroom, queue depth and
wait time, freeze frequency and admission rates, exported in a form existing
monitoring stacks can consume. Prometheus is the first target.

## Wrapping existing SDKs

Using dianemo with a vendor today means writing a plugin for it. That is the
right answer for an API with no official client, and wasteful for the many that
ship a good one. Stripe, AWS, Shopify and others have already solved types,
pagination, auth and error semantics, and reimplementing that is work that never
finishes — the SDK keeps moving and the reimplementation keeps lagging.

Planned: a way to place an existing SDK under dianemo's admission control, so its
calls draw on the same coordinated budget as everything else while keeping the
SDK's own interface and types.

## Multi-region coordination

Dianemo coordinates every process that shares a backend. A deployment spanning
regions does not want to share one — reaching across regions on every request
would cost more than the coordination is worth — but the vendor's quota is
usually global regardless of which region the request left from.

Planned: a coordinator above the regional deployments, so each region runs its
own instance at local speed while the global budget stays correct.
