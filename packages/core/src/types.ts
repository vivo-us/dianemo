import type { CreateClientData, RateLimitConfig } from "./client/types.js";
import type { DianemoBackend } from "./backend/types.js";
import type RequestHandler from "./index.js";
import type { Logger } from "./logger.js";

export type RequestHandlerStatus = "stopped" | "starting" | "started";

export type ClientTemplateBuilder<T = unknown> = (
  credentials: T,
  context: ClientTemplateContext
) => CreateClientData | CreateClientData[];

/**
 * What a template builder is told beyond the credentials.
 *
 * Every field is set only when the caller chose one of the rate-limit options
 * the template declared, and all are absent otherwise — including when a
 * template declares no options at all, which is how a builder that ignores this
 * argument keeps behaving exactly as it did.
 *
 * A builder does not have to place any of this. The chosen plan is applied to the
 * client tree by path after the builder returns, so a builder that only declares
 * `subClients: [{ name: "orders" }]` still gets the plan's `orders` limit. These
 * are here for a builder that wants to read the plan rather than receive it —
 * to decide whether a sub-client should exist at all, say.
 */
export interface ClientTemplateContext {
  /** The chosen plan's limit for the root client: its `""` path. */
  rateLimit?: RateLimitConfig;
  /**
   * The whole chosen plan, keyed by sub-client path the way
   * {@link RateLimitOverrides} is: `""` is the root, `"orders"` its sub-client of
   * that name, `"a:b"` a nested one.
   */
  rateLimits?: Record<string, RateLimitConfig>;
  /**
   * Which option was chosen. A plan usually changes more than a rate limit, so
   * this is here to branch on — sub-clients an entry-level plan does not get, a
   * different endpoint for enterprise.
   */
  rateLimitOption?: string;
}

/**
 * One plan a template offers.
 *
 * Either a limit for the root client, or a whole tree of them keyed by
 * sub-client path — which is what a vendor publishing a different limit per
 * endpoint needs, since a tier changes all of them at once.
 *
 * The two are told apart by shape: an array, or an object with a string `type`,
 * is a limit; anything else is read as a path map. A sub-client path is never a
 * bare `type` string, so the two cannot be confused.
 */
export type RateLimitPlan = RateLimitConfig | Record<string, RateLimitConfig>;

/**
 * How a template lets callers choose its rate limit.
 *
 * The limits are the template's own, named by keys the caller picks from: there
 * is no way for a caller to supply a limit the plugin did not write. A template
 * declaring no options accepts no choice, and `addTemplateClient` rejects one.
 *
 * ```ts
 * await handler.registerClientTemplate("acme", builder, {
 *   rateLimitOptions: {
 *     free: { type: "requestLimit", interval: 1000, tokensToAdd: 2, maxTokens: 2 },
 *     pro: {
 *       "": { type: "requestLimit", interval: 1000, tokensToAdd: 20, maxTokens: 20 },
 *       reports: [
 *         { name: "per_second", type: "requestLimit", interval: 1000, tokensToAdd: 2, maxTokens: 2 },
 *         { name: "per_day", type: "requestLimit", interval: 86400000, tokensToAdd: 500, maxTokens: 500 },
 *       ],
 *     },
 *   },
 *   defaultRateLimitOption: "free",
 * });
 * ```
 */
export interface ClientTemplateOptions {
  /**
   * Plans a caller may choose between, by key. Each is a limit for the root
   * client, or a {@link RateLimitPlan} covering its sub-clients too.
   */
  rateLimitOptions?: Record<string, RateLimitPlan>;
  /** Applied when a caller names none. Must be a key of `rateLimitOptions`. */
  defaultRateLimitOption?: string;
}

/** A registered template: its builder, and what it lets callers choose. */
export interface RegisteredTemplate {
  builder: ClientTemplateBuilder<unknown>;
  options: ClientTemplateOptions;
}

/**
 * Per-instance settings that travel with a template client's credentials.
 *
 * Stored beside them rather than inside, because they are not sensitive and a
 * replica that cannot decrypt the credentials can still read these.
 */
export interface TemplateClientOptions {
  /**
   * Which of the template's declared `rateLimitOptions` this instance is on —
   * a subscription plan, typically. Rejected if the template declared none, or
   * if it declared some and this is not one of them.
   */
  rateLimitOption?: string;
  /** Operator-side overrides, documented on {@link RateLimitOverrides}. */
  rateLimitOverrides?: RateLimitOverrides;
}

/**
 * Rate-limit overrides keyed by sub-client path: `""` is the parent, `"rest"` its
 * sub-client of that name, `"a:b"` a nested one. An override must keep the shape of
 * the template default — the same `type`, or an array where the default is an array
 * — because the merge swaps fields within the discriminant and changing the shape
 * would change the client class. See docs/concepts.md#rate-limit-overrides.
 *
 * This is the operator-side escape hatch and needs no permission from the template.
 * For a choice the template itself sanctions, see {@link ClientTemplateOptions}.
 */
export type RateLimitOverrides = Record<string, RateLimitConfig>;

/**
 * Registry of known client templates, so a typo at a call site is a compile error
 * rather than a runtime warn-and-skip. Augment it:
 *
 * ```ts
 * declare module "@dianemo/core" {
 *   interface ClientTemplates {
 *     fedex: OAuth2Credentials;
 *   }
 * }
 * ```
 */
// Empty by design; `object` here would accept any template name.
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface ClientTemplates {}

export type StartupHook = (handler: RequestHandler) => Promise<void> | void;

export interface RequestHandlerConstructorOptions {
  /** Encrypts every stored credential. Changing it orphans what is already stored. */
  key: string;
  /**
   * Where the shared state lives. Pick by how many processes will run this handler:
   * `redisBackend(connection)` for more than one, `memoryBackend()` only for exactly
   * one forever — it shares nothing, so two processes send twice the agreed rate and
   * nothing reports it. See docs/backends/README.md.
   *
   * You build the backend; the handler never constructs a connection of its own.
   */
  backend: DianemoBackend;
  /**
   * Where the handler writes its logs. Defaults to silence — a library
   * embedded in someone else's process should not claim their stdout.
   *
   * A `pino` instance satisfies this structurally.
   */
  logger?: Logger;
  /** Namespaces every backend key, so one Redis can host several handlers. */
  keyPrefix?: string;
  /**
   * Configures the built-in client, registered as `"default"` and reachable by
   * passing that as `clientName` — for requests that belong to no template.
   *
   * **Defaults to a `noLimit` client named `default`**
   */
  defaultClientOptions?: CreateClientData;
  /** Election weight for becoming a queue controller; higher wins. */
  priority?: number;
}

export interface RequestHandlerMetadata {
  id: string;
  status: RequestHandlerStatus;
  priority: number;
  registeredClients: string[];
  ownedClients: string[];
}
