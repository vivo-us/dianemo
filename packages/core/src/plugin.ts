import type { BoundTryHandleRequest } from "./tryHandleRequest.js";
import type { DianemoBackend } from "./backend/types.js";
import type RequestHandler from "./index.js";
import type { Logger } from "./logger.js";

/**
 * What a plugin is handed when the handler builds its request namespace.
 *
 * `tryHandleRequest` is already bound to the owning handler — a plugin closes
 * over it so its exported request functions never take a handler argument.
 */
export interface PluginContext {
  tryHandleRequest: BoundTryHandleRequest;
  logger: Logger;
  /**
   * For a plugin needing state of its own — a session cookie one replica logs in for
   * and the rest reuse. **Namespace your keys by plugin name**, and leave `close()`
   * to the handler.
   *
   * Under `memoryBackend()` nothing is actually shared, so treat sharing as an
   * optimisation: a plugin that assumes it should repeat work, not break.
   */
  backend: DianemoBackend;
  /**
   * Reach for `tryHandleRequest` first — a request function that captures the handler
   * cannot be composed into a differently-configured one. This is for the uncommon
   * plugin that needs client stats or must await registration before a first call.
   */
  handler: RequestHandler;
}

/**
 * One integration: rate-limit calibration and auth flow, plus the request functions
 * using them. `registerTemplate` describes the *shape* and carries no credentials —
 * those arrive via `addTemplateClient`, so a plugin never knows where they came from.
 */
export interface RequestHandlerPlugin<
  TName extends string = string,
  TRequests = unknown,
> {
  /** Template name, and the key credentials file under. Unique per handler. */
  name: TName;
  /**
   * Runs as a startup hook, before credentials are hydrated, so the builder exists by
   * the time any credential for it arrives.
   */
  registerTemplate(handler: RequestHandler): Promise<void> | void;
  /** Builds this plugin's request namespace against a bound context. */
  createRequests(ctx: PluginContext): TRequests;
}

/**
 * Pins `name` to a literal type so `handler.use(fedex)` yields
 * `{ fedex: { ...typed requests } }` without module augmentation. Without it
 * `name: "fedex"` widens to `string` and every namespace collapses onto one key.
 */
export function definePlugin<const TName extends string, TRequests>(
  plugin: RequestHandlerPlugin<TName, TRequests>
): RequestHandlerPlugin<TName, TRequests> {
  return plugin;
}

/** The object `use()` returns: each plugin's namespace under its own name. */
export type MergedNamespaces<P extends readonly RequestHandlerPlugin[]> = {
  [Plugin in P[number] as Plugin["name"]]: ReturnType<Plugin["createRequests"]>;
};
