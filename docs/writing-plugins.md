# Writing a plugin

A plugin packages one integration: how its client is shaped and authenticated,
plus the request functions that use it.

```ts
export const requests = handler.use(fedexPlugin, upsPlugin);
await requests.fedex.cancelShipment("fedex:_:production", { ... });
```

## The two halves

A plugin is deliberately split in two, and understanding why makes the rest
obvious.

**`registerTemplate`** describes the _shape_ of a client — its rate limit, auth
flow, base URL, sub-clients — and carries **no credentials**.

**`createRequests`** returns the functions that call the API.

Credentials arrive separately at runtime, via `handler.addTemplateClient()`.
That separation is why a plugin never has to know whether its credentials came
from a vault, a database, or an environment variable — and why a template can
be published to npm without anyone's secrets in it.

## Two ways to reach the handler

A request function needs a `tryHandleRequest` bound to the handler that owns it.
There are two ways to get one, and they differ only in how it reaches your
request functions.

**From the context**, using nothing but `@dianemo/core`. `createRequests`
receives the bound function and your requests close over it:

```ts
import { definePlugin } from "@dianemo/core";

export default definePlugin({
  name: "acme",
  registerTemplate: registerAcmeTemplate,
  createRequests: ({ tryHandleRequest }) => ({
    cancelShipment: (clientName: string, id: string) =>
      tryHandleRequest(
        { clientName, requestName: "acme.shipping.cancel", method: "PUT", url: `/ship/v1/shipments/cancel/${id}` },
        "ACM_0001",
        "Failed to cancel shipment"
      ),
  }),
});
```

This is self-contained and puts no constraint on the host. It suits a plugin
that lives in your own codebase, and a handful of request functions defined in
one file.

**From a module-scoped binding**, using
[`@dianemo/plugin-kit`](https://github.com/vivo-us/dianemo-plugins/tree/master/packages/plugin-kit).
Request functions import `tryHandleRequest` directly rather than receiving it,
which is what lets them live in separate files without threading a context
through every one:

```bash
npm install @dianemo/plugin-kit
```

`plugin-kit` ships from the plugins repo rather than this one and may not be on npm
yet. The `definePlugin` form above is in `@dianemo/core` and always available, so
reach for it if the install fails — the two differ only in how `tryHandleRequest`
arrives.

This is what the published
[dianemo-plugins](https://github.com/vivo-us/dianemo-plugins) catalogue uses,
because a vendor integration is routinely a few hundred request functions across
many files. It costs one handler per process — see
[the binding](#the-binding-and-its-one-rule) below.

The rest of this page uses the plugin-kit form. Everything about templates,
naming, rate limits and error codes applies equally to both.

## Minimal plugin

```ts
// src/index.ts
import { bindTryHandleRequest } from "@dianemo/plugin-kit";
import { definePlugin } from "@dianemo/core";
import { registerAcmeTemplate } from "./client.js";
import * as requests from "./requests/index.js";

export default definePlugin({
  name: "acme",
  registerTemplate: registerAcmeTemplate,
  createRequests: (ctx) => {
    bindTryHandleRequest(ctx);
    return requests;
  },
});

export { registerAcmeTemplate } from "./client.js";
export * from "./requests/index.js";
```

`name` is not decoration. It is the first segment of every client name and the
key credentials are filed under, so two plugins claiming one name would route
one's credentials to the other's builder. `use()` refuses that.

## The template

```ts
// src/client.ts
import RequestHandler, { buildClientName } from "@dianemo/core";
import type {
  CreateClientData,
  OAuth2Credentials,
} from "@dianemo/core/client/types";

declare module "@dianemo/core" {
  interface ClientTemplates {
    acme: OAuth2Credentials;
  }
}

export async function registerAcmeTemplate(handler: RequestHandler) {
  await handler.registerClientTemplate("acme", (creds): CreateClientData[] => [
    {
      name: buildClientName("acme", creds),
      rateLimit: {
        type: "requestLimit",
        interval: 1000,
        tokensToAdd: 100,
        maxTokens: 100,
      },
      requestOptions: { defaults: { baseURL: creds.baseUrl } },
      authentication: {
        type: "oauth2",
        clientId: creds.clientId,
        clientSecret: creds.clientSecret,
        refreshConfig: {
          url: `${creds.baseUrl}/oauth/token`,
          dataLocation: "urlEncodedForm",
          data: {
            grant_type: "client_credentials",
            client_id: "{{clientId}}",
            client_secret: "{{clientSecret}}",
          },
        },
      },
    },
  ]);
}
```

Three things worth noting:

- **The `declare module` block** is what gives `addTemplateClient("acme", …)`
  a typed credential shape at every call site. It is not optional polish:
  `ClientTemplates` is empty by design, so without it `keyof ClientTemplates` is
  `never` and neither `registerClientTemplate` nor `addTemplateClient` accepts
  the name at all.
- **`{{clientId}}` placeholders** are substituted from the client's credentials
  at refresh time. Write them as placeholders rather than interpolating the
  values, so the config object never holds a secret.
- **The builder returns an array.** One credential set may produce several
  clients — a vendor with separate rate limits per region, or a
  [`sharedLimit`](rate-limits/shared-limit.md) child alongside its parent.

Pick the rate limit from what the vendor publishes — see
[choosing a rate limit](rate-limits/README.md), and note that granularity
matters more than most people expect.

## Request functions

```ts
// src/requests/index.ts
import { tryHandleRequest } from "@dianemo/plugin-kit";

export const cancelShipment = async (
  clientName: string,
  id: string
): Promise<void> => {
  await tryHandleRequest(
    {
      clientName,
      requestName: "acme.shipping.cancel",
      method: "PUT",
      url: `/ship/v1/shipments/cancel/${id}`,
    },
    "ACM_0001",
    "Failed to cancel shipment"
  );
};
```

A request function's signature is **its own domain arguments** — a client name
and the data. No handler parameter. That is the whole point of the module-scoped
binding: `tryHandleRequest` is already bound to the owning handler, so a request
function stays callable without threading a handler through every layer.

`requestName` is dotted and namespaced by the plugin. It appears in spans, logs
and queue metadata, so make it describe the operation rather than the URL.

The last two arguments are an error code and a human message, used when the call
fails. A stable code per operation is worth the small effort — it is what makes
a failure greppable across services.

## The binding, and its one rule

`bindTryHandleRequest(ctx)` establishes a **module-scoped** binding shared by
every plugin in the process.

The cost is one handler per process. A second handler is refused loudly rather
than silently rerouting the first one's traffic — which would be a genuinely
hard bug to see, since requests would keep succeeding, against the wrong
rate-limit budget.

If you need two handlers, run them in separate processes.

## Plugin-owned state

`backend()` gives you the handler's backend for state your plugin needs to
cache — a session cookie one replica logs in for and the rest reuse, a payment
token with a server-side TTL:

```ts
import { backend, acquireLock, releaseLock } from "@dianemo/plugin-kit";

await backend().set(`acme:token:${clientName}`, token, ttlSeconds);
const cached = await backend().get(`acme:token:${clientName}`);
```

Namespace keys by plugin name — the backend is shared with the handler's own
bookkeeping and with every other plugin.

**How far that sharing reaches is the host's choice, not yours.** Under
[`@dianemo/backend-redis`](backends/redis.md) the cache is fleet-wide; under the
[memory backend](backends/memory.md) it is process-local. A plugin that assumes
sharing repeats work per process rather than breaking, which is the right
failure mode — but do not build correctness on top of it.

`acquireLock` / `releaseLock` cover work that should happen once rather than
once per replica, with the same caveat.

## Types come from inference

There is no module augmentation to write for the request namespace.
`handler.use(a, b)` infers the merged shape from the plugins you pass, so
`requests.acme.cancelShipment` is typed at the call site by virtue of existing.

The type-level test in `test/typeInference.test-d.ts` guards this: if `use()`
ever degraded to returning `any`, every runtime test would still pass while
every consumer silently lost type safety.

## Checklist

- [ ] `name` is unique and matches the client-name prefix
- [ ] `declare module` block types the credentials
- [ ] Secrets appear only as `{{placeholders}}` in refresh config
- [ ] Rate limit reflects what the vendor actually publishes
- [ ] `requestName` is dotted, namespaced, and describes the operation
- [ ] Every request function takes domain arguments only, no handler
- [ ] Backend keys are namespaced by plugin name
- [ ] A test composes the plugin onto a handler and checks the namespace
