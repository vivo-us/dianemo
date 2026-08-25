import { createTryHandleRequest } from "./tryHandleRequest.js";
import startSubscriptions from "./utils/startSubscriptions.js";
import updateClientRoles from "./utils/updateClientRoles.js";
import { generateClients } from "./utils/createClients.js";
import { assertNameSegment } from "./utils/clientName.js";
import type { DianemoBackend } from "./backend/types.js";
import type { RequestConfig } from "./request/types.js";
import { noopLogger, type Logger } from "./logger.js";
import type BaseClient from "./client/index.js";
import type { AxiosResponse } from "axios";
import EventEmitter from "node:events";
import crypto from "node:crypto";
import {
  loadTemplateClientsFromBackend,
  buildAndRegisterTemplateClients,
  destroyTemplateClients,
  encryptCredentials,
  decryptCredentials,
  templateClientNamesKey,
  assertTemplateOptions,
  normalizeTemplateClientOptions,
  serializeTemplateClientOptions,
  readTemplateClientOptions,
} from "./utils/templateClients.js";
import type {
  CreateClientData,
  ProbeRequestConfig,
  RateLimitConfig,
  SetGrantTokensData,
} from "./client/types.js";
import {
  ClientNotFoundError,
  ClientUnavailableError,
  ConfigurationError,
} from "./errors.js";
import {
  type MergedNamespaces,
  type PluginContext,
  type RequestHandlerPlugin,
} from "./plugin.js";
import type {
  ClientTemplateBuilder,
  ClientTemplateOptions,
  ClientTemplates,
  RateLimitOverrides,
  RegisteredTemplate,
  RequestHandlerConstructorOptions,
  RequestHandlerMetadata,
  RequestHandlerStatus,
  StartupHook,
  TemplateClientOptions,
} from "./types.js";

export type {
  SetGrantTokensData,
  OAuth2Credentials,
  TokenCredentials,
  BasicCredentials,
} from "./client/types.js";
export type {
  ClientTemplateBuilder,
  ClientTemplateContext,
  ClientTemplateOptions,
  ClientTemplates,
  StartupHook,
  RateLimitOverrides,
  RateLimitPlan,
  TemplateClientOptions,
} from "./types.js";

export {
  buildClientName,
  parseClientName,
  assertNameSegment,
  GLOBAL_ORGANIZATION_SEGMENT,
  type ParsedClientName,
} from "./utils/clientName.js";

export {
  RequestHandlerError,
  ClientNotFoundError,
  ClientConflictError,
  ConfigurationError,
  RequestTimeoutError,
  NoResponseError,
  ClientUnavailableError,
  NotOAuth2ClientError,
  GrantRefreshTokenMissingError,
  RequestCostExceedsBudgetError,
  RequestAbortedError,
  RequestError,
  getOriginalStatus,
  getOriginalResponseData,
  type RequestErrorMetadata,
} from "./errors.js";

export { noopLogger, type LogFn, type Logger } from "./logger.js";

export { default as MemoryBackend, memoryBackend } from "./backend/memory.js";
export {
  calculateQueueScore,
  MAX_QUEUE_PRIORITY,
  MAX_QUEUE_RETRIES,
  QUEUE_PRIORITY_BAND,
  QUEUE_RETRY_BAND,
  QUEUE_SCORE_EPOCH_MS,
} from "./backend/queueScore.js";
export { parsePriority, parseStoredNumber } from "./backend/parseStored.js";
export {
  normalizeTtlSeconds,
  REQUEST_TOMBSTONE_TTL_SECONDS,
} from "./backend/ttl.js";
export type {
  AcquireConcurrencyResult,
  AcquireMultiLimitResult,
  AcquireTokensResult,
  BatchOp,
  BatchOptions,
  ConcurrencyConfig,
  DianemoBackend,
  FreezeState,
  MessageHandler,
  MultiLimitSpec,
  QueueStats,
  QueuedRequest,
  TokenBucketConfig,
} from "./backend/types.js";

export {
  definePlugin,
  type MergedNamespaces,
  type PluginContext,
  type RequestHandlerPlugin,
} from "./plugin.js";

export {
  createTryHandleRequest,
  type BoundTryHandleRequest,
  type TryHandleRequestOptions,
} from "./tryHandleRequest.js";

export type { RequestConfig, RequestMetadata } from "./request/types.js";
export type {
  CreateClientData,
  RateLimitData,
  RateLimitConfig,
  NamedRateLimitData,
  ProbeRequestConfig,
  NoLimitClientOptions,
  RequestLimitClientOptions,
  ConcurrencyLimitClientOptions,
  SharedLimitClientOptions,
  RateLimitStats,
  SingleRateLimitStats,
  MultiRateLimitStats,
  NamedRateLimitStats,
} from "./client/types.js";
export type {
  RequestHandlerConstructorOptions,
  RequestHandlerMetadata,
  RequestHandlerStatus,
} from "./types.js";

export default class RequestHandler {
  protected id: string;
  protected priority: number;
  protected status: RequestHandlerStatus | "stopping";
  protected backend: DianemoBackend;
  protected namespace: string;
  protected logger: Logger;
  protected heartbeatTimeouts: Map<string, NodeJS.Timeout> = new Map();
  protected heartbeatInterval?: NodeJS.Timeout;
  protected emitter: NodeJS.EventEmitter = new EventEmitter();
  protected key: string;
  protected clients: Map<string, BaseClient> = new Map();
  protected defaultClient: CreateClientData;
  protected templates: Map<string, RegisteredTemplate> = new Map();
  protected templateClientMap: Map<string, Map<string, Set<string>>> =
    new Map();
  protected localTemplateClients: Set<string> = new Set();
  protected startupHooks: StartupHook[] = [];
  protected startPromise: Promise<void> | null = null;
  protected stopPromise: Promise<void> | null = null;
  protected shutdownHooksRegistered = false;
  /** Whether pub/sub handlers are installed, so a retried start cannot duplicate them. */
  public subscriptionsStarted = false;
  protected shutdownHandler?: () => void;
  private rolesRunning = false;
  private rolesRerun = false;
  private rolesRerunStartup = false;
  private rolesPromise: Promise<void> = Promise.resolve();
  /** Plugin names already registered via `use()`, for duplicate detection. */
  private pluginNames: Set<string> = new Set();

  constructor(data: RequestHandlerConstructorOptions) {
    if (!data.backend) {
      throw new ConfigurationError(
        "backend_required",
        "RequestHandler requires a `backend`. Use redisBackend(connection) for anything running more than one process, or memoryBackend() for a single process."
      );
    }
    // Every credential this handler stores is encrypted under this one string,
    // and the type alone does not stop it arriving empty at runtime — which is
    // what an unset environment variable produces. Caught here, because
    // otherwise it surfaces as a TypeError from inside node:crypto.
    if (typeof data.key !== "string" || data.key.length === 0) {
      throw new ConfigurationError(
        "key_required",
        "RequestHandler requires a non-empty `key`. It encrypts stored credentials, so use a high-entropy secret (32+ characters) from your secret store, not a literal."
      );
    }
    this.id = crypto.randomUUID();
    this.priority = data.priority || 1;
    this.status = "stopped";
    this.namespace = `${
      data.keyPrefix ? `${data.keyPrefix}:` : ""
    }requestHandler`;
    this.key = data.key;
    this.defaultClient = data.defaultClientOptions || {
      rateLimit: { type: "noLimit" },
      name: "default",
    };
    this.logger = data.logger ?? noopLogger;
    this.backend = data.backend;
  }

  /**
   * Registers plugins and returns their merged request namespaces. See
   * docs/writing-plugins.md.
   *
   * Synchronous and safe at module scope: template registration is queued as a
   * startup hook. Plugin names must be unique across every call.
   */
  public use<const P extends readonly RequestHandlerPlugin[]>(
    ...plugins: P
  ): MergedNamespaces<P> {
    const namespaces = {} as Record<string, unknown>;
    const tryHandleRequest = createTryHandleRequest(this, this.logger);

    for (const plugin of plugins) {
      // Template names are the first segment of a `:`-delimited client name and
      // the key credentials are filed under. Two plugins claiming one name would
      // silently route one's credentials to the other's builder, so refuse.
      if (this.pluginNames.has(plugin.name)) {
        throw new ConfigurationError(
          "duplicate_plugin",
          `Plugin "${plugin.name}" is already registered on this handler.`
        );
      }
      this.pluginNames.add(plugin.name);

      const ctx: PluginContext = {
        tryHandleRequest,
        logger: this.logger,
        backend: this.backend,
        handler: this,
      };
      namespaces[plugin.name] = plugin.createRequests(ctx);
      this.registerStartupHook(() => plugin.registerTemplate(this));
    }

    return namespaces as MergedNamespaces<P>;
  }

  /** Plugin names registered on this handler, in registration order. */
  public getRegisteredPlugins(): string[] {
    return Array.from(this.pluginNames);
  }

  public async start(): Promise<void> {
    if (this.stopPromise) await this.stopPromise;
    if (this.status === "started") return;
    if (this.startPromise) return this.startPromise;
    this.startPromise = this.doStart().catch(async (err) => {
      this.startPromise = null;
      // Back to "stopped", and tear down what did get set up. `handleRequest`
      // calls `start()` whenever the status is not "started", so a status left at
      // "starting" wedges the handler: every later request re-runs `doStart` and
      // adds another pub/sub listener to the same connection.
      this.status = "stopped";
      // Both of these belong to a start that did not finish, and only `doStop`
      // otherwise clears them — which this path never reaches. Leaving the flag set
      // makes the next `start()` skip subscribing and reach "started" deaf; leaving
      // the connection open keeps the event loop alive after the caller gives up.
      this.subscriptionsStarted = false;
      await this.backend.close().catch((closeError) => {
        this.logger.error(
          { error: closeError },
          "Failed to close the backend after a failed start"
        );
      });
      throw err;
    });
    return this.startPromise;
  }

  private async doStart(): Promise<void> {
    this.status = "starting";
    await startSubscriptions.bind(this)();

    // `generateClients`, not `createClient`: the latter builds exactly one
    // client and never looks at `subClients`, so sub-clients declared on the
    // default client were dropped with no warning and no error — a
    // configuration option that silently did nothing.
    await generateClients.bind(this)([this.defaultClient]);

    for (const hook of this.startupHooks) await hook(this);

    await loadTemplateClientsFromBackend.bind(this)();

    await this.scheduleClientRoles(true);
    this.status = "started";
    this.registerShutdownHooks();
    this.emitter.emit("instanceStarted");
    this.logger.info(`Started request handler instance with ID ${this.id}`);
  }

  /**
   * Runs during `start()`, after the backend is up but before credentials are
   * hydrated. Hooks run in registration order, so register a template-registration
   * hook before one that loads credentials for it. Throwing fails `start()`.
   */
  public registerStartupHook(hook: StartupHook): void {
    this.startupHooks.push(hook);
  }

  private registerShutdownHooks(): void {
    if (this.shutdownHooksRegistered) return;
    this.shutdownHooksRegistered = true;

    this.shutdownHandler = () => {
      this.stop().catch((err) => {
        this.logger.error(
          {
            error: err,
          },
          "Failed to stop request handler during shutdown"
        );
      });
    };

    process.once("SIGTERM", this.shutdownHandler);
    process.once("SIGINT", this.shutdownHandler);
    process.once("SIGHUP", this.shutdownHandler);
  }

  private unregisterShutdownHooks(): void {
    if (!this.shutdownHooksRegistered || !this.shutdownHandler) return;
    process.removeListener("SIGTERM", this.shutdownHandler);
    process.removeListener("SIGINT", this.shutdownHandler);
    process.removeListener("SIGHUP", this.shutdownHandler);
    this.shutdownHandler = undefined;
    this.shutdownHooksRegistered = false;
  }

  public async stop(): Promise<void> {
    if (
      this.status !== "started" &&
      this.status !== "starting" &&
      !this.stopPromise
    )
      return;
    if (this.stopPromise) return this.stopPromise;
    this.stopPromise = this.finishStartupThenStop().finally(() => {
      this.stopPromise = null;
    });
    return this.stopPromise;
  }

  private async finishStartupThenStop(): Promise<void> {
    if (this.status === "starting" && this.startPromise) {
      try {
        await this.startPromise;
      } catch {
        // start() restored the stopped state. Preserve its original rejection;
        // there is no running handler left for stop() to tear down.
        return;
      }
    }
    await this.doStop();
  }

  /**
   * Bounded because an orchestrator sends SIGKILL a few seconds after SIGTERM, so a
   * drain outliving that window helps nobody.
   */
  private drainTimeoutMs = 10_000;

  /** Overrides the shutdown drain budget. */
  public setDrainTimeout(ms: number): void {
    this.drainTimeoutMs = ms;
  }

  /**
   * Waits for each client's queue to empty, up to the drain budget. Ordering is
   * unchanged: the normal drain loop is what serves the backlog.
   */
  private async drainClients(): Promise<void> {
    const deadline = Date.now() + this.drainTimeoutMs;
    const clients = [...this.clients.values()];
    if (!clients.length) return;

    while (Date.now() < deadline) {
      let outstanding = 0;
      for (const client of clients) {
        try {
          // This replica's own work, not the queue's total. The queue key is
          // fleet-wide, and a worker cannot advance a peer's entry at all —
          // `processRequests` returns immediately for that role — so counting all of
          // it spends the whole drain budget on someone else's backlog.
          if (await client.hasOutstandingWork()) outstanding++;
        } catch {
          // A backend already going away cannot say what is left.
          return;
        }
      }
      if (outstanding === 0) return;
      // Nothing else pokes the loop once new requests stop arriving.
      for (const client of clients) {
        client.processRequests().catch(() => {});
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    this.logger.warn(
      `Shutdown drain did not finish within ${this.drainTimeoutMs}ms; failing what is left`
    );
  }

  /**
   * Stops admitting, then waits for what is already upstream to come back.
   *
   * A request outliving the drain budget still holds a concurrency slot, and the
   * call that releases it runs when the response arrives — so closing first left the
   * release with no connection to run on. Nothing then reclaims the slot except its
   * `concurrencySlotTtl`, which is measured from the claim, so a replacement replica
   * starts against a cap that is already occupied by a request that finished.
   *
   * Bounded by the same budget as the drain, because an orchestrator's SIGKILL is
   * the real deadline and an upstream that never answers must not hold the process.
   */
  private async awaitInFlightRequests(): Promise<void> {
    const clients = [...this.clients.values()];
    for (const client of clients) client.haltAdmission();
    if (!clients.length) return;

    const deadline = Date.now() + this.drainTimeoutMs;
    while (Date.now() < deadline) {
      const inFlight = clients.reduce(
        (total, client) => total + client.countInFlight(),
        0
      );
      if (inFlight === 0) return;
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    const remaining = clients.reduce(
      (total, client) => total + client.countInFlight(),
      0
    );
    this.logger.warn(
      `Shutdown closed with ${remaining} request(s) still upstream; their concurrency slots lapse on the slot TTL`
    );
  }

  private async doStop(): Promise<void> {
    if (this.status !== "started" && this.status !== "starting") return;
    this.status = "stopping";
    this.unregisterShutdownHooks();

    try {
      // Before tearing anything down, since the signal handlers exist for this.
      await this.drainClients();

      for (const timeout of this.heartbeatTimeouts.values()) {
        clearTimeout(timeout);
      }
      this.heartbeatTimeouts.clear();
      for (const client of this.clients.values()) {
        client.removeHealthCheckInterval();
        // Whatever did not drain in time fails with a reason, rather than an
        // opaque timeout long after the process meant to exit. These hold no slot —
        // they were never admitted — so failing them releases nothing.
        client.cancelWaitingRequests();
      }
      // After the waiters are failed and before anything is torn down: a request
      // already upstream releases its slot when the response lands, and that call
      // needs the backend still open.
      await this.awaitInFlightRequests();
      await this.backend.batch([
        { op: "srem", key: `${this.namespace}:instances`, member: this.id },
        { op: "del", key: `${this.namespace}:instance:${this.id}` },
        {
          op: "publish",
          channel: `${this.namespace}:instanceStopped`,
          message: this.id,
        },
      ]);
    } finally {
      // Reaching the terminal state is not optional even when a step above throws,
      // and this path is reachable in ordinary operation: deregistration fails
      // whenever the backend goes away first during a pod teardown, and `batch`
      // reports a failed pipeline command rather than swallowing it. A status left at
      // "stopping" wedges the handler — `stop()` becomes a no-op, `start()` returns
      // the resolved `startPromise` without re-running `doStart`, every request is
      // refused, and the heartbeat keeps the event loop alive so the process cannot
      // exit. The error still propagates; only the state is guaranteed.
      try {
        // Closes only what the backend opened for itself. A connection handed to
        // the backend belongs to the caller, who may still be using it — closing
        // that here would break unrelated work in the same process.
        await this.backend.close();
      } catch (error) {
        this.logger.error(
          { error },
          "Failed to close the backend during shutdown"
        );
      }
      // The connection that carried them is gone, so a later `start()` has to
      // install them again.
      this.subscriptionsStarted = false;
      // Drop the client instances too. Retaining them makes a later start()
      // treat them as unchanged and skip init(), leaving them with no
      // health-check interval — no orphan cleanup, no queue poke.
      this.clients.clear();
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = undefined;
      this.startPromise = null;
      this.status = "stopped";
    }
    this.logger.warn(`Stopped request handler instance with ID ${this.id}`);
  }

  /**
   * Key prefix for this handler, for an observer subscribing to handler-scoped
   * channels (`requestDone`, `freezeStateChanged`) or reading its keys.
   */
  public getNamespace(): string {
    return this.namespace;
  }

  /**
   * This replica's id. A persisting listener compares it against
   * `publisherInstanceId` so only one replica writes a fanned-out broadcast.
   */
  public getInstanceId(): string {
    return this.id;
  }

  /** Every template registered here; anything in this list is a valid `templateName`. */
  public getRegisteredTemplates(): string[] {
    return Array.from(this.templates.keys());
  }

  /**
   * The rate-limit options a template lets callers choose between, for building a
   * plan picker. Empty when the template declares none, which also means
   * `addTemplateClient` will reject a `rateLimitOption` for it.
   */
  public getRateLimitOptions<K extends keyof ClientTemplates>(
    templateName: K
  ): string[] {
    const declared = this.templates.get(templateName as string)?.options
      .rateLimitOptions;
    return declared ? Object.keys(declared) : [];
  }

  /** Loaded clients with their effective rate limit — template default plus overrides. */
  public getLoadedClients(): Array<{
    name: string;
    rateLimit: RateLimitConfig;
  }> {
    return Array.from(this.clients.values()).map((c) => ({
      name: c.getName(),
      rateLimit: c.getRateLimit(),
    }));
  }

  /** Names produced by the most recent build; empty if it never ran here. */
  public getTemplateClientNames(
    templateName: string,
    instanceId: string
  ): string[] {
    const set = this.templateClientMap.get(templateName)?.get(instanceId);
    return set ? Array.from(set) : [];
  }

  /** Stored for an external scheduler to fire; dianemo never fires it itself. */
  public getProbeConfig(clientName: string): ProbeRequestConfig | undefined {
    return this.clients.get(clientName)?.getProbeConfig();
  }

  public getMetadata(this: RequestHandler): RequestHandlerMetadata {
    const metadata: RequestHandlerMetadata = {
      id: this.id,
      // Preserve the public status union for source compatibility. A stopping
      // handler no longer accepts work and is externally equivalent to stopped.
      status: this.status === "stopping" ? "stopped" : this.status,
      priority: this.priority,
      registeredClients: Array.from(this.clients.keys()),
      ownedClients: [],
    };
    for (const c of this.clients.values()) {
      if (c.getRole() !== "controller") continue;
      metadata.ownedClients.push(c.getName());
    }
    return metadata;
  }

  public async handleRequest<TResponse = unknown, TRequestData = unknown>(
    config: RequestConfig<TRequestData>
  ): Promise<AxiosResponse<TResponse, TRequestData>> {
    if (this.status === "stopping" || this.stopPromise) {
      throw new ClientUnavailableError(
        "handler_stopping",
        "Request handler is stopping and cannot accept new requests."
      );
    }
    if (this.status !== "started") await this.start();
    const client = await this.waitForClient(config.clientName);
    return (await client.handleRequest(config as never)) as AxiosResponse<
      TResponse,
      TRequestData
    >;
  }

  /**
   * Briefly waits for a client whose credentials may still be in flight over
   * pub/sub, then throws `ClientNotFoundError` so a typo still surfaces loudly.
   */
  private async waitForClient(clientName: string): Promise<BaseClient> {
    const cached = this.clients.get(clientName);
    if (cached) return cached;

    const WAIT_FOR_CLIENT_TIMEOUT_MS = 5_000;
    const POLL_MS = 50;
    const deadline = Date.now() + WAIT_FOR_CLIENT_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, POLL_MS));
      const client = this.clients.get(clientName);
      if (client) return client;
    }
    throw new ClientNotFoundError(clientName);
  }

  /**
   * Resolves once `clientName` is registered locally, for a consumer whose first
   * call happens during its own boot.
   *
   * Warns at 30 s and throws a 503 at `timeoutMs` (default 5 min, overridable with
   * `DIANEMO_AWAIT_CLIENT_TIMEOUT_MS`), so a credential source that never comes up
   * fails a liveness probe rather than hanging. Deliberately more patient than the
   * per-request `waitForClient`, which stays impatient to surface typos.
   */
  public async awaitClient(
    clientName: string,
    opts: { signal?: AbortSignal; timeoutMs?: number } = {}
  ): Promise<void> {
    if (this.clients.has(clientName)) return;
    if (opts.signal?.aborted) {
      throw new ClientUnavailableError(
        "shutdown_aborted",
        "awaitClient was aborted"
      );
    }

    const channel = `clientRegistered:${clientName}`;
    const STALE_WAIT_WARN_MS = 30_000;
    const envTimeoutRaw = process.env.DIANEMO_AWAIT_CLIENT_TIMEOUT_MS;
    const envTimeout = envTimeoutRaw ? Number(envTimeoutRaw) : NaN;
    const timeoutMs =
      opts.timeoutMs ??
      (Number.isFinite(envTimeout) && envTimeout > 0 ? envTimeout : 300_000);

    await new Promise<void>((resolve, reject) => {
      const onRegistered = () => {
        cleanup();
        resolve();
      };
      const onAbort = () => {
        cleanup();
        reject(
          new ClientUnavailableError(
            "shutdown_aborted",
            "awaitClient was aborted"
          )
        );
      };
      const warnTimer = setTimeout(() => {
        this.logger.warn(
          { clientName },
          "Still awaiting client registration after 30s — has the credential source registered this client?"
        );
      }, STALE_WAIT_WARN_MS);
      const timeoutTimer = setTimeout(() => {
        cleanup();
        reject(
          new ClientUnavailableError(
            "client_unavailable",
            `Timed out after ${timeoutMs}ms awaiting client "${clientName}"`
          )
        );
      }, timeoutMs);
      const cleanup = () => {
        clearTimeout(warnTimer);
        clearTimeout(timeoutTimer);
        this.emitter.off(channel, onRegistered);
        opts.signal?.removeEventListener("abort", onAbort);
      };

      this.emitter.once(channel, onRegistered);
      opts.signal?.addEventListener("abort", onAbort);

      // Re-check after subscribing to close the TOCTOU window where the
      // client could have been registered between the cache check above
      // and the listener being installed.
      if (this.clients.has(clientName)) {
        cleanup();
        resolve();
      }
    });
  }

  /**
   * Registers a template builder. The name must be declared in `ClientTemplates`
   * via module augmentation, which is what types the credentials at the call site.
   *
   * Called after `start()`, it immediately builds any credentials already stored.
   */
  public async registerClientTemplate<K extends keyof ClientTemplates>(
    name: K,
    builder: ClientTemplateBuilder<ClientTemplates[K]>,
    options: ClientTemplateOptions = {}
  ): Promise<void> {
    // Template names occupy parts[0] of clientName, which is `:`-delimited
    // (`<template>:<orgId>:<alias>[:<sub>]`). A template containing `:`
    // would shift every subsequent segment and break parseClientName, so
    // reject loudly at registration time.
    if ((name as string).includes(":")) {
      throw new ConfigurationError(
        "invalid_template_name",
        `Template name "${String(name)}" must not contain ":"`
      );
    }
    assertTemplateOptions(name as string, options);
    this.templates.set(name as string, {
      builder: builder as ClientTemplateBuilder<unknown>,
      options,
    });

    if (this.status !== "started") return;

    const entries = await this.backend.smembers(`${this.namespace}:templates`);
    const prefix = `${name}::`;
    const myEntries = entries.filter((e) => e.startsWith(prefix));

    for (const entry of myEntries) {
      // Through the rebuild path rather than reading the blob here, so a client
      // built by a late registration gets the same stored plan and overrides one
      // built at startup does.
      await this.rebuildTemplateClient(
        name as string,
        entry.slice(prefix.length)
      );
    }

    if (myEntries.length > 0) {
      await this.scheduleClientRoles();
    }
  }

  /**
   * Coalesces role re-elections into one in-flight run plus at most one rerun, the
   * same lock+pending idiom as `processRequests`. One credential change triggers
   * every instance at once, and election is O(clients) plus a backend read.
   *
   * The returned promise resolves only after a run that began after this call, so a
   * caller still observes its own change.
   */
  public scheduleClientRoles(isStartup = false): Promise<void> {
    if (isStartup) this.rolesRerunStartup = true;
    if (this.rolesRunning) {
      this.rolesRerun = true;
      return this.rolesPromise;
    }
    this.rolesRunning = true;
    this.rolesPromise = (async () => {
      try {
        do {
          this.rolesRerun = false;
          const startup = this.rolesRerunStartup;
          this.rolesRerunStartup = false;
          await updateClientRoles.bind(this)(startup);
        } while (this.rolesRerun);
      } finally {
        this.rolesRunning = false;
      }
    })();
    return this.rolesPromise;
  }

  /**
   * Adds a template client, encrypting its credentials into the backend so peers and
   * restarts can rebuild it. Auto-starts the handler.
   *
   * For credentials one writer distributes to many readers; use
   * {@link addLocalTemplateClient} for per-replica ones.
   *
   * `options` carries this instance's `rateLimitOption` — one of the plans the
   * template declared — and any operator `rateLimitOverrides`. A bare overrides
   * record is still accepted in that position, as the argument used to be.
   */
  public async addTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    credentials: ClientTemplates[K] & { instanceId: string },
    options?: TemplateClientOptions | RateLimitOverrides
  ): Promise<void> {
    // status === "starting" means we're inside doStart already (e.g., called from a startup
    // hook); the backend is initialized at that point, so it's safe to proceed without auto-starting
    // — and awaiting start() from within itself would deadlock.
    this.assertTemplateMutationAllowed();
    if (this.status === "stopped") await this.start();
    await this.doAddTemplateClient(
      templateName,
      credentials,
      normalizeTemplateClientOptions(options)
    );
    await this.scheduleClientRoles();
  }

  /**
   * Builds a template client in-process, writing nothing to the backend and
   * broadcasting nothing.
   *
   * For credentials each replica already holds itself, injected via env: there is no
   * shared secret to distribute, and putting a per-replica identity on the shared bus
   * would leak it. Every replica calls this at startup and they converge without
   * pub/sub. Contrast {@link addTemplateClient}.
   */
  public async addLocalTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    credentials: ClientTemplates[K] & { instanceId: string },
    options?: TemplateClientOptions | RateLimitOverrides
  ): Promise<void> {
    this.assertTemplateMutationAllowed();
    if (this.status === "stopped") await this.start();
    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `addLocalTemplateClient called for unregistered template "${templateName}", skipping`
      );
      return;
    }
    const settings = normalizeTemplateClientOptions(options);
    this.assertRateLimitOptionAllowed(templateName, settings.rateLimitOption);
    const { instanceId } = credentials as { instanceId: string };
    this.localTemplateClients.add(`${templateName}::${instanceId}`);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      settings
    );
    await this.scheduleClientRoles();
  }

  /**
   * Refuses a plan the template does not offer.
   *
   * At the boundary rather than at build time: a key stored now is read back by
   * every replica and every restart, so accepting one the template never declared
   * would put a client on the template default while its record says otherwise.
   */
  private assertRateLimitOptionAllowed(
    templateName: string,
    rateLimitOption: string | undefined
  ): void {
    if (rateLimitOption === undefined) return;
    const declared = this.templates.get(templateName)?.options.rateLimitOptions;
    const available = declared ? Object.keys(declared) : [];
    if (available.includes(rateLimitOption)) return;
    throw new ConfigurationError(
      "unknown_rate_limit_option",
      available.length === 0
        ? `Template "${templateName}" declares no rateLimitOptions, so it accepts no rateLimitOption. A template chooses which limits callers may pick from; add them at registerClientTemplate.`
        : `Template "${templateName}" has no rateLimitOption "${rateLimitOption}". It offers: ${available
            .map((key) => `"${key}"`)
            .join(", ")}.`
    );
  }

  private async doAddTemplateClient(
    templateName: string,
    credentials: { instanceId: string },
    settings: TemplateClientOptions
  ): Promise<void> {
    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `addTemplateClient called for unregistered template "${templateName}", skipping`
      );
      return;
    }

    this.assertRateLimitOptionAllowed(templateName, settings.rateLimitOption);

    const { instanceId } = credentials;
    // At the boundary, so a bad identifier is rejected by the call that
    // introduced it rather than later, from inside client generation.
    assertNameSegment(instanceId, "instanceId");
    const organizationId = (credentials as { organizationId?: string })
      .organizationId;
    if (organizationId !== undefined && organizationId !== null) {
      assertNameSegment(organizationId, "organizationId");
    }
    const entry = `${templateName}::${instanceId}`;
    const encrypted = encryptCredentials(credentials, this.key);

    // MULTI/EXEC for atomicity — peers reading mid-write would otherwise
    // see template (new) but settings (stale-or-not-yet-deleted), and the
    // subsequent broadcast would race ahead of the cleanup. The window is
    // small but the cost of MULTI vs pipeline is negligible.
    const settingsJson = serializeTemplateClientOptions(settings);
    await this.backend.batch(
      [
        {
          op: "set",
          key: `${this.namespace}:template:${entry}`,
          value: encrypted,
        },
        { op: "sadd", key: `${this.namespace}:templates`, member: entry },
        settingsJson
          ? {
              op: "set" as const,
              key: `${this.namespace}:overrides:${entry}`,
              value: settingsJson,
            }
          : { op: "del" as const, key: `${this.namespace}:overrides:${entry}` },
      ],
      { atomic: true }
    );

    // Re-read from the backend so concurrent writers can't leave us with stale local
    // creds — last-write-wins from the backend is the source of truth on every replica.
    await this.rebuildTemplateClient(templateName, instanceId);

    await this.backend.publish(
      `${this.namespace}:templateClientAdded`,
      JSON.stringify({ templateName, instanceId })
    );
  }

  /**
   * Rebuilds from what the backend currently holds, rather than from what a caller
   * or a broadcast said — so cross-publisher pub/sub ordering cannot desync replicas.
   */
  public async rebuildTemplateClient(
    templateName: string,
    instanceId: string
  ): Promise<void> {
    const entry = `${templateName}::${instanceId}`;
    const encrypted = await this.backend.get(
      `${this.namespace}:template:${entry}`
    );
    if (!encrypted) return;
    const credentials = decryptCredentials(encrypted, this.key);
    const settings = await readTemplateClientOptions.bind(this)(entry);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      settings
    );
  }

  public async removeTemplateClient<K extends keyof ClientTemplates>(
    templateName: K,
    instanceId: string
  ): Promise<void> {
    this.assertTemplateMutationAllowed();
    if (this.status !== "started") await this.start();

    // Purges credentials, unlike the rebuild and peer-notification paths.
    await destroyTemplateClients.bind(this)(templateName, instanceId, true);

    const entry = `${templateName}::${instanceId}`;
    await this.backend.batch([
      { op: "del", key: `${this.namespace}:template:${entry}` },
      { op: "del", key: `${this.namespace}:overrides:${entry}` },
      { op: "srem", key: `${this.namespace}:templates`, member: entry },
      {
        op: "del",
        key: templateClientNamesKey(this.namespace, entry),
      },
    ]);

    await this.backend.publish(
      `${this.namespace}:templateClientRemoved`,
      JSON.stringify({ templateName, instanceId })
    );
  }

  private assertTemplateMutationAllowed(): void {
    if (this.status === "stopping" || this.stopPromise) {
      throw new ClientUnavailableError(
        "handler_stopping",
        "Request handler is stopping and cannot mutate template clients."
      );
    }
  }

  protected getClient(clientName: string): BaseClient {
    const client = this.clients.get(clientName);
    if (client) return client;
    throw new ClientNotFoundError(clientName);
  }

  public async getClientStats(clientName: string) {
    return this.getClient(clientName).getStats();
  }

  public async setGrantTokens(
    clientName: string,
    grantId: string,
    data: SetGrantTokensData
  ): Promise<void> {
    const client = this.getClient(clientName);
    await client.setGrantTokens(grantId, data);
  }
}
