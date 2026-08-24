import type { CreateClientData, RateLimitData } from "./client/types.js";
import type { DianemoBackend } from "./backend/types.js";
import type RequestHandler from "./index.js";
import type { Logger } from "./logger.js";

export type RequestHandlerStatus = "stopped" | "starting" | "started";

export type ClientTemplateBuilder<T = unknown> = (
  credentials: T
) => CreateClientData | CreateClientData[];

/**
 * Rate-limit overrides keyed by sub-client path: `""` is the parent, `"rest"` its
 * sub-client of that name, `"a:b"` a nested one. An override must keep the template
 * default's `type` — the merge swaps fields within the discriminant, so changing type
 * would change the client class. See docs/concepts.md#rate-limit-overrides.
 */
export type RateLimitOverrides = Record<string, RateLimitData>;

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
