import type { RequestDoneData, RequestMetadata } from "../request/types.js";
import { ClientUnavailableError, NotOAuth2ClientError } from "../errors.js";
import { REQUEST_TOMBSTONE_TTL_SECONDS } from "../backend/ttl.js";
import processRequests from "./methods/processRequests.js";
import type { QueuedRequest } from "../backend/types.js";
import { costCeilingFor } from "../utils/rateLimit.js";
import handleRequest from "./methods/handleRequest.js";
import { encrypt } from "../utils/encryption.js";
import type * as ClientTypes from "./types.js";
import type { Logger } from "../logger.js";
import type { AxiosInstance } from "axios";
import crypto from "node:crypto";
import axios from "axios";
import {
  credentialTtlSeconds,
  DEFAULT_REFRESH_TOKEN_TTL_SECONDS,
} from "../utils/credentialTtl.js";
import type {
  DianemoBackend,
  FreezeState,
  MultiLimitSpec,
} from "../backend/types.js";

/**
 * Shortest base duration for a client-wide freeze, used when the configured
 * back-off is zero. Long enough to be a real gate, short enough not to surprise
 * someone who asked for no back-off at all.
 */
const MIN_FREEZE_BASE_MS = 1000;

/**
 * Longest drain wake-up we will book. `setTimeout` clamps anything above
 * 2^31-1 ms to 1ms, which turns a very long wait into a spin, so a long wait is
 * split into several instead: the loop wakes, declines the head again, and books
 * the remainder.
 */
const MAX_DRAIN_DELAY_MS = 60_000;

interface PendingRequestDone {
  data: RequestDoneData;
  queueHandled: boolean;
  ownTypeHandled: boolean;
  freezeHandled: boolean;
  thawHandled: boolean;
}

abstract class BaseClient {
  protected id: string = crypto.randomUUID();
  protected instanceId: string;
  protected name: string;
  protected role: ClientTypes.ClientRole = "worker";
  protected http: AxiosInstance;
  protected backend: DianemoBackend;
  protected namespace: string;
  /**
   * Keyed by the name this client was *registered* under, which is not always
   * `name`: a `sharedLimit` client takes its parent's name so budget state resolves
   * to the parent's keys, but credentials are the one thing it must not share.
   */
  protected authNamespace: string;
  protected emitter: NodeJS.EventEmitter;
  protected logger: Logger;
  protected abstract rateLimit: ClientTypes.RateLimitConfig;
  protected metadata?: { [key: string]: unknown };
  protected requestOptions: ClientTypes.RequestOptions;
  protected authData?: ClientTypes.AuthCreateData;
  protected key: string;
  protected retryOptions: ClientTypes.RetryOptions;
  protected rateLimitChange?: ClientTypes.CreateClientData["rateLimitChange"];
  protected handlerNamespace: string;
  protected healthCheckIntervalMs: number;
  protected healthCheckInterval?: NodeJS.Timeout;
  protected httpStatusCodesToMute: number[];
  protected processingLock: boolean = false;
  protected processingPending: boolean = false;
  /** Pending post-freeze wake-ups, so shutdown can cancel them. */
  protected freezeTimers: Set<NodeJS.Timeout> = new Set();
  protected probeRequest?: ClientTypes.ProbeRequestConfig;
  protected sourceClientData: ClientTypes.CreateClientData;

  public handleRequest = handleRequest.bind(this);
  public processRequests = processRequests.bind(this);

  constructor(data: ClientTypes.ClientConstructorData, name: string) {
    this.emitter = data.emitter;
    this.http = axios.create(data.client.axiosOptions);
    this.logger = data.logger;
    this.backend = data.backend;
    this.name = name;
    this.instanceId = data.instanceId;
    this.handlerNamespace = data.handlerNamespace;
    this.namespace = `${data.handlerNamespace}:${this.name.replaceAll(
      / /g,
      "_"
    )}`;
    // Credentials file under the root of the sub-client path, because a
    // sub-client inherits its parent's authentication. `authOwnerName` resolves a
    // nested path to the client that actually holds them rather than to the
    // template. See docs/design-notes.md#credential-keying-and-sub-clients.
    const authOwner = data.client.authOwnerName ?? data.client.name;
    this.authNamespace = `${data.handlerNamespace}:${authOwner.replaceAll(
      / /g,
      "_"
    )}`;
    this.healthCheckIntervalMs = data.client.healthCheckIntervalMs || 10000;
    this.metadata = data.client.metadata;
    this.requestOptions = data.client.requestOptions || {};
    this.rateLimitChange = data.client.rateLimitChange;
    const { retryOptions } = data.client;
    this.httpStatusCodesToMute = data.client.httpStatusCodesToMute || [];
    this.retryOptions = {
      retryBackoffBaseTime: retryOptions?.retryBackoffBaseTime ?? 1000,
      retryBackoffMethod: retryOptions?.retryBackoffMethod ?? "exponential",
      retry429s: retryOptions?.retry429s ?? true,
      retry5xxs: retryOptions?.retry5xxs ?? true,
      maxRetries: retryOptions?.maxRetries ?? 3,
      retryStatusCodes: retryOptions?.retryStatusCodes ?? [],
      // `??` alone would pass a NaN through, and `retryOptions` is never validated at
      // construction — `Number(process.env.THAW_COUNT)` unset is a realistic source.
      // A NaN here reaches `armFreeze`, whose monotone merge compares false against it
      // and cancels the probe budget a 429 was owed.
      thawRequestCount: usableThawRequestCount(retryOptions?.thawRequestCount),
      retryHandler: retryOptions?.retryHandler,
    };
    this.authData = data.client.authentication;
    this.key = data.key;
    this.probeRequest = data.client.probeRequest;
    this.sourceClientData = data.client;
  }

  /**
   * What this client was built from, so `generateClients` can skip rebuilding
   * on a template re-broadcast whose credentials haven't changed. Without the
   * comparison, every re-broadcast destroys and recreates every client.
   *
   * Internal: the returned object carries `authentication` with the plaintext
   * client secret or token. Nothing outside client generation should read it.
   */
  public getSourceClientData(): ClientTypes.CreateClientData {
    return this.sourceClientData;
  }

  /**
   * The probe request an external scheduler can fire to test whether a
   * sustained-downtime integration has recovered. Dianemo stores this and never
   * fires it itself.
   */
  public getProbeConfig(): ClientTypes.ProbeRequestConfig | undefined {
    return this.probeRequest;
  }

  public abstract handleRateLimitUpdated(
    data: ClientTypes.RateLimitUpdatedData
  ): Promise<void> | void;
  protected abstract getRateLimitStats(): Promise<ClientTypes.RateLimitStats>;

  /**
   * Whether this request can skip the queue entirely.
   *
   * The queue orders requests competing for a budget; when nothing is
   * contending there is nothing to order, and enqueue + admission + two
   * broadcasts + removal are pure overhead. Subclasses that can decide
   * admission atomically override this. Returning false is always safe.
   */
  protected async tryAdmitImmediately(
    _request: RequestMetadata
  ): Promise<boolean> {
    return false;
  }
  /**
   * Gives back whatever `canProcessNextRequest` claimed, when the caller then
   * decides not to run the request after all.
   *
   * Admission and claiming are the same step for a concurrency client, so any
   * path that admits and then backs out strands a slot until its TTL expires.
   * No-op for the client types that claim nothing.
   */
  protected async releaseAdmission(_request: RequestMetadata): Promise<void> {}

  protected async refundUnusedAdmission(
    request: RequestMetadata
  ): Promise<void> {
    await this.releaseAdmission(request);
  }

  /** Returns capacity claimed for an attempt that will not be dispatched. */
  public async releaseUnusedAdmission(request: RequestMetadata): Promise<void> {
    await this.refundUnusedAdmission(request);
  }

  /**
   * Last check before a request takes a place in the queue.
   *
   * Registration-time validation cannot cover a dependency that disappears
   * later, and a request that queues into a budget nobody drains fails slowly
   * and uninformatively. Subclasses with such a dependency throw here instead.
   */
  protected async assertReadyToQueue(): Promise<void> {}

  /**
   * Whether admission claims capacity that must later be handed back.
   *
   * Only these clients need the drain loop to confirm a request is still queued
   * after claiming, so the rest do not pay the extra read on the hot path.
   */
  protected claimsOnAdmission(): boolean {
    return false;
  }

  /**
   * Claims whatever budget the request still needs, without ever blocking: the
   * admission loop holds `processingLock` across this call, so waiting here stalls
   * every other grant and every cheaper request behind it. Decline with a `waitTime`.
   */
  protected async tryAcquireTurn(
    _request: RequestMetadata
  ): Promise<ClientTypes.AcquireTurnResult> {
    return { acquired: true };
  }

  /**
   * Re-enters the admission loop once capacity is due.
   *
   * The loop stops when the head cannot proceed, so without this nothing would
   * restart it until the next health tick — capacity would arrive and sit idle.
   * Timers are kept per delay-target and cleared on destroy.
   */
  protected scheduleDrain(waitTimeMs: number): void {
    const delay = Math.min(
      MAX_DRAIN_DELAY_MS,
      Math.max(1, Math.ceil(waitTimeMs))
    );
    const dueAt = Date.now() + delay;
    // A later wake-up covers a sooner one only if it fires first, so keep the
    // earliest outstanding timer and drop the rest.
    if (this.drainTimer && this.drainDueAt <= dueAt) return;
    if (this.drainTimer) clearTimeout(this.drainTimer);
    this.drainDueAt = dueAt;
    this.drainTimer = setTimeout(() => {
      // Cleared before the pass runs, not after: callers that cannot proceed
      // re-book on every pass, and the guard above drops a booking made while a
      // timer is outstanding. Running the pass first would sleep with nothing
      // scheduled.
      this.drainTimer = undefined;
      this.processRequests().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | scheduled drain failed`
        );
      });
    }, delay);
    this.drainTimer.unref?.();
  }

  private drainTimer?: ReturnType<typeof setTimeout>;
  private drainDueAt = 0;

  protected clearDrainTimer(): void {
    if (this.drainTimer) clearTimeout(this.drainTimer);
    this.drainTimer = undefined;
    this.drainDueAt = 0;
  }

  protected abstract handleOwnTypeRequestDone(data: RequestDoneData): void;
  protected abstract handleFreezeOwnTypeRequests(
    grantId?: string
  ): Promise<void> | void;
  protected abstract getRetryBackoffBaseTime(isRateLimited?: boolean): number;

  /**
   * Base duration of a client-wide freeze: the configured back-off, sanitised.
   *
   * Zero, NaN and negatives all floor to `MIN_FREEZE_BASE_MS`. `retryOptions` is
   * never validated, and `retryBackoffBaseTime: 0` is legal — it means "retry this
   * request immediately", which says nothing about whether the fleet should keep
   * hammering an upstream that just rejected us. A negative is the dangerous one:
   * it is truthy, so the arming gate fires with `frozenUntil` already in the past,
   * leaving the client in permanent single-probe thaw.
   *
   * Subclasses override to raise the floor further and must fold this result in
   * rather than re-reading the raw field.
   */
  protected getFreezeBaseTime(): number {
    const base = this.retryOptions.retryBackoffBaseTime;
    return Number.isFinite(base) && base > 0 ? base : MIN_FREEZE_BASE_MS;
  }
  protected abstract handleDestroy(finalRemoval: boolean): Promise<void> | void;

  // ------------------------------------------------------- key helpers

  /** Grant-isolated clients meter each grant against its own bucket. */
  protected getRateLimitKey(grantId?: string): string {
    const baseKey = `${this.namespace}:rateLimit`;
    if (!grantId) return baseKey;
    if (!this.usesGrantIsolation()) return baseKey;
    return `${this.namespace}:grant:${grantId}:rateLimit`;
  }

  /**
   * Read from the client that owns the budget, not this one: a `sharedLimit` child's
   * `namespace` is its parent's but its `authData` is its own, so answering locally
   * would have the two resolve different keys and stop sharing a budget.
   */
  protected usesGrantIsolation(): boolean {
    // Which client to ask turns on whether the accessor exists, not on whether it
    // answered: `?? this.authData` cannot tell "this client owns its budget" from
    // "the owner has no auth data", and falling back to the borrower's own config
    // is the disagreement this indirection removes. An owner that declares no
    // `authentication` declares no isolation, which is a real answer.
    const auth = this.getBudgetOwnerAuthData
      ? this.getBudgetOwnerAuthData()
      : this.authData;
    return (
      auth?.type === "oauth2" && auth.grantRateLimitBehavior === "isolated"
    );
  }

  /** Set for clients whose budget lives on another client. */
  public getBudgetOwnerAuthData?: () => ClientTypes.AuthCreateData | undefined;
  /** Shared-limit clients delegate completion cleanup to the budget owner. */
  public getBudgetOwnerClient?: () => BaseClient | undefined;

  protected getQueueKey(): string {
    return `${this.namespace}:queue`;
  }

  protected getMetadataKeyPrefix(): string {
    return `${this.namespace}:request`;
  }

  public getRequestMetadataKey(requestId: string): string {
    return `${this.getMetadataKeyPrefix()}:${requestId}`;
  }

  /** Grant-isolated clients track slots per grant. */
  protected getConcurrencyKey(grantId?: string): string {
    const baseKey = `${this.namespace}:concurrency`;
    if (!grantId) return baseKey;
    if (!this.usesGrantIsolation()) return baseKey;
    return `${this.namespace}:grant:${grantId}:concurrency`;
  }

  /** Grant-isolated clients freeze each grant independently. */
  protected getFreezeStateKey(grantId?: string): string {
    const baseKey = `${this.namespace}:freezeState`;
    if (!grantId) return baseKey;
    if (!this.usesGrantIsolation()) return baseKey;
    return `${this.namespace}:grant:${grantId}:freezeState`;
  }

  // ------------------------------------------------- lifecycle methods

  public init() {
    this.startHealthCheckInterval();
  }

  /**
   * Updates the rate limit data and publishes to other instances.
   *
   * `source` identifies who triggered the change so subscribers can decide
   * whether to persist (a host may record every `dynamic` change) or just
   * update local state.
   */
  protected async updateRateLimit(
    data: ClientTypes.RateLimitConfig,
    source: ClientTypes.RateLimitUpdatedData["source"] = "operator"
  ) {
    const updatedData: ClientTypes.RateLimitUpdatedData = {
      clientName: this.name,
      rateLimit: data,
      source,
      publisherInstanceId: this.instanceId,
    };
    await this.backend.publish(
      `${this.handlerNamespace}:rateLimitUpdated`,
      JSON.stringify(updatedData)
    );
  }

  private startHealthCheckInterval() {
    if (this.healthCheckInterval) return;
    const tick = async () => {
      // Election is otherwise edge-triggered on instance started/updated/stopped,
      // and an ungraceful controller loss can miss every one of those edges at
      // once, leaving a client with no drainer. Idempotent, one read per tick.
      await this.reconcileRole?.();

      if (this.role === "controller") {
        await this.flushPendingRequestDone();
      } else {
        await this.forwardPendingRequestDone();
      }

      // Both cleanups recover state stranded by a crashed instance, so only the
      // controller runs them.
      if (this.role === "controller") {
        await this.cleanupOrphanedRequests();
        await this.cleanupStaleFrozenGrants();
      }
      this.processRequests().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | processRequests failed`
        );
      });
    };
    let running = false;
    this.healthCheckInterval = setInterval(() => {
      if (running) return;
      running = true;
      // Same reasoning as the heartbeat: an unawaited rejection from a
      // periodic tick must not terminate the consumer's process.
      void tick()
        .catch((error) => {
          this.logger.error(
            { error },
            `Client ${this.name} | health check failed`
          );
        })
        .finally(() => {
          running = false;
        });
    }, this.healthCheckIntervalMs);
  }

  /**
   * Removes queued requests whose owning instance is no longer registered.
   * Without this, an instance that dies mid-request leaves entries that pin the
   * queue above empty forever.
   */
  public async cleanupOrphanedRequests(): Promise<number> {
    if (this.role !== "controller") return 0;

    const aliveInstanceIds = await this.backend.smembers(
      `${this.handlerNamespace}:instances`
    );
    const aliveSet = new Set(aliveInstanceIds);

    // The backend refuses the same read, so this is only the round trip saved —
    // and the one place that can say why a sweep was skipped. Debug rather than
    // warn because election promotes a client before it registers the instance,
    // so every boot reaches this once with nothing wrong.
    if (!aliveSet.has(this.instanceId)) {
      this.logger.debug(
        `Client ${this.name} | Skipped orphan cleanup: the alive-instance set does not list this instance, so it cannot be read as authoritative`
      );
      return 0;
    }

    const cleaned = await this.backend.cleanupOrphanedRequests(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      aliveSet,
      this.instanceId
    );

    if (cleaned > 0) {
      this.logger.info(
        `Client ${this.name} | Cleaned up ${cleaned} orphaned requests`
      );
    }

    return cleaned;
  }

  /**
   * Releases grants left frozen by a crashed controller — in the frozen set,
   * past their freeze window, with no thaw probe in flight. Nothing else would
   * ever clear them, so the grant would stay unservable.
   */
  protected async cleanupStaleFrozenGrants(): Promise<void> {
    if (!this.usesGrantIsolation()) return;

    // The backend appends `<grantId>:freezeState` to this.
    const freezeStateKeyPrefix = `${this.namespace}:grant:`;

    const cleaned = await this.backend.cleanupStaleFrozenGrants(
      this.getFrozenGrantsKey(),
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      freezeStateKeyPrefix
    );

    if (cleaned > 0) {
      this.logger.debug(
        `Client ${this.name} | Cleaned up ${cleaned} stale frozen grants`
      );
    }
  }

  /**
   * Destroys the client and cleans up resources. Async because
   * `ConcurrencyLimitClient.handleDestroy` needs to release backend-tracked
   * concurrency slots before the new client takes over — without the await
   * the new client would see the old slots still held.
   */
  public async destroy(reason: ClientTypes.ClientDestroyReason): Promise<void> {
    await this.handleDestroy(reason === "finalRemoval");
    this.removeHealthCheckInterval();
    this.clearDrainTimer();
    // A rebuild is a hand-over, not a departure, so its parked requests stay
    // parked: their queue entries survive, the replacement client drains the same
    // fleet-wide queue, and `requestReady` reaches them through the handler-wide
    // emitter. Shutdown does not come through here — `doStop` cancels every
    // client's parked requests itself.
    if (reason !== "rebuild") this.cancelWaitingRequests();
    this.logger.debug(`Client ${this.name} | Destroyed (${reason})`);
  }

  /**
   * Deletes this client's stored OAuth2 credentials.
   *
   * Separate from `destroy`, which also runs on a rebuild and must not discard
   * tokens. This is for genuine removal.
   */
  public async purgeCredentials(): Promise<void> {
    const keys = [`${this.authNamespace}:oauth2`];
    for (const grantId of await this.backend.smembers(
      `${this.authNamespace}:grants`
    )) {
      keys.push(`${this.authNamespace}:oauth2:${grantId}`);
    }
    keys.push(`${this.authNamespace}:grants`);
    for (const key of keys) {
      await this.backend.del(key).catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | failed to delete credential key ${key}`
        );
      });
    }
  }

  /** Records a grant id so removal can find its credentials later. */
  public async trackGrantId(grantId: string): Promise<void> {
    if (!grantId) return;
    const key = `${this.authNamespace}:grants`;
    await this.backend.sadd(key, grantId);
    // The index outlives the hashes it points at, so without a TTL it is the one
    // credential key that only ever grows — and `purgeCredentials` walks it.
    // Refreshed on every write, so an active client's index never lapses.
    await this.backend
      .batch([
        {
          op: "expire",
          key,
          ttlSeconds: DEFAULT_REFRESH_TOKEN_TTL_SECONDS,
        },
      ])
      .catch(() => undefined);
  }

  /**
   * What the shutdown drain needs, which the queue alone cannot answer: it is
   * fleet-wide, so counting all of it waits on peers' entries, and counting only
   * this replica's misses a fast-path request that holds a slot but has no entry.
   */
  public async hasOutstandingWork(): Promise<boolean> {
    if (this.inFlightRequests > 0) return true;
    if (this.role === "controller" && this.pendingRequestDone.size > 0)
      return true;
    const all = await this.backend.getAllRequests(
      this.getQueueKey(),
      this.getMetadataKeyPrefix()
    );
    return all.some((request) => request.ownerId === this.instanceId);
  }

  /** Requests inside `handleRequest` on this replica right now. */
  protected inFlightRequests = 0;

  public trackRequestStarted(): void {
    this.inFlightRequests++;
  }

  public trackRequestFinished(): void {
    if (this.inFlightRequests > 0) this.inFlightRequests--;
  }

  /**
   * Whether admission has been stopped for shutdown. Read by `processRequests`.
   */
  public admissionHalted = false;

  /**
   * Stops admitting queued work, so nothing new is sent upstream.
   *
   * One-way: a halted client is discarded at the end of `stop()` rather than
   * resumed, and `start()` builds fresh instances.
   */
  public haltAdmission(): void {
    this.admissionHalted = true;
  }

  /** Requests this replica has sent upstream and is still awaiting. */
  public countInFlight(): number {
    return this.inFlightRequests;
  }

  /** Pending queue depth, for the shutdown drain. */
  public async getQueueStats(): Promise<{
    pending: number;
    inProgress: number;
  }> {
    const stats = await this.backend.getQueueStats(
      this.getQueueKey(),
      this.getMetadataKeyPrefix()
    );
    return { pending: stats.pending, inProgress: stats.inProgress };
  }

  /** Requests parked in `waitForRequestReady`, so shutdown can fail them. */
  private waitingRequests = new Map<string, (error: Error) => void>();
  private replacementClient?: BaseClient;
  private completionForwardTimer?: ReturnType<typeof setTimeout>;
  private completionForwardAttempts = 0;

  public registerWaitingRequest(
    requestId: string,
    cancel: (error: Error) => void
  ): void {
    this.waitingRequests.set(requestId, cancel);
  }

  public unregisterWaitingRequest(requestId: string): void {
    this.waitingRequests.delete(requestId);
  }

  /**
   * Fails one waiting request with a reason, if this replica is its owner.
   *
   * Used when the drain loop discovers a queued request can never be admitted.
   * Leaving it to time out told the caller the controller was slow, a minute
   * later, when the real answer — its cost no longer fits the budget — was known
   * immediately and is a different class of error.
   */
  public failWaitingRequest(requestId: string, error: Error): boolean {
    const cancel = this.waitingRequests.get(requestId);
    if (!cancel) return false;
    this.waitingRequests.delete(requestId);
    cancel(error);
    return true;
  }

  /**
   * Takes over the parked requests of the client this one replaces. They wake through
   * the handler-wide emitter unaided, but the registry that lets a client *reach*
   * them is per-instance — without it, neither `failWaitingRequest` nor shutdown
   * could give a caller an answer that was already known.
   */
  public adoptWaitingRequests(previous: BaseClient): void {
    for (const [requestId, cancel] of previous.waitingRequests) {
      this.waitingRequests.set(requestId, cancel);
    }
    previous.waitingRequests.clear();
    previous.replacementClient = this;
    for (const [completionId, data] of previous.pendingRequestDone) {
      this.pendingRequestDone.set(completionId, data);
    }
    previous.pendingRequestDone.clear();
    for (const completionId of previous.completedRequestIds) {
      this.completedRequestIds.add(completionId);
    }
    previous.completedRequestIds.clear();
  }

  /** Fails every parked request with a reason. Called on shutdown. */
  public cancelWaitingRequests(): void {
    const waiters = [...this.waitingRequests.values()];
    this.waitingRequests.clear();
    for (const cancel of waiters) {
      cancel(
        new ClientUnavailableError(
          "handler_stopped",
          `Request handler stopped before client ${this.name} could admit this request.`
        )
      );
    }
  }

  public removeHealthCheckInterval() {
    // A freeze can outlast the handler that scheduled it, and firing after
    // shutdown means touching a closed backend.
    for (const timer of this.freezeTimers) clearTimeout(timer);
    this.freezeTimers.clear();
    if (this.completionForwardTimer) {
      clearTimeout(this.completionForwardTimer);
      this.completionForwardTimer = undefined;
    }
    if (!this.healthCheckInterval) return;
    clearInterval(this.healthCheckInterval);
    this.healthCheckInterval = undefined;
  }

  /**
   * The name this client was registered under.
   *
   * Not `this.name`, which for a `sharedLimit` client is its parent's — that is
   * how it resolves to the parent's queue and bucket, but it made the handler's
   * own inventory report two clients under one name and the child's real name
   * nowhere.
   */
  public getName() {
    return this.sourceClientData.name;
  }

  public getRole() {
    return this.role;
  }

  public getRateLimit(): ClientTypes.RateLimitConfig {
    return this.rateLimit;
  }

  /** The auth config this client was built with, for a child resolving its budget owner's. */
  public getAuthData(): ClientTypes.AuthCreateData | undefined {
    return this.authData;
  }

  /**
   * Updates the client role and triggers request processing if controller.
   * When becoming controller, immediately runs cleanup to handle any
   * orphaned requests or stale frozen grants from crashed instances.
   */
  public async updateRole(role: ClientTypes.ClientRole) {
    if (role === this.role) return;
    const wasWorker = this.role === "worker";
    this.role = role;
    if (wasWorker && role === "controller") {
      await this.flushPendingRequestDone();
      await this.cleanupOrphanedRequests();
      await this.cleanupStaleFrozenGrants();
    }

    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed`
      );
    });
  }

  // ------------------------------------------ request queue operations

  /** Returns whether the entry was created, or already existed. */
  public async addRequestToQueue(request: QueuedRequest): Promise<boolean> {
    const created = await this.backend.addRequest(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      request
    );
    await this.notifyRequestAdded();
    return created;
  }

  /**
   * Tells whichever replica is draining that there is work to look at.
   *
   * Separate from `addRequestToQueue` because a retry becomes claimable through
   * a status update rather than an add, and it needs the same wake-up.
   */
  public async notifyRequestAdded(): Promise<void> {
    // Only a remote controller needs waking: the local call below covers this
    // replica, and a worker receiving the broadcast returns immediately, so on a
    // single-replica deployment the publish is a round trip that achieves nothing.
    if (this.role !== "controller") {
      await this.backend.publish(
        `${this.handlerNamespace}:requestAdded`,
        JSON.stringify({ clientName: this.name })
      );
    }

    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed`
      );
    });
  }

  /**
   * Header names this client is known to put credentials in, lowercased.
   *
   * Three configuration surfaces let a consumer name their own credential
   * header — `customHeaderName`, `axiosOptions.headers` and
   * `requestOptions.defaults.headers` — so all three are treated as sensitive.
   */
  protected getSensitiveHeaderNames(): ReadonlySet<string> {
    if (this.sensitiveHeaderNames) return this.sensitiveHeaderNames;
    const names = new Set<string>();
    const custom = this.authData?.customHeaderName?.toLowerCase();
    if (custom) names.add(custom);
    const configured = [
      this.sourceClientData.axiosOptions?.headers,
      this.sourceClientData.requestOptions?.defaults?.headers,
    ];
    for (const headers of configured) {
      if (!headers || typeof headers !== "object") continue;
      for (const name of Object.keys(headers)) names.add(name.toLowerCase());
    }
    this.sensitiveHeaderNames = names;
    return names;
  }
  private sensitiveHeaderNames?: ReadonlySet<string>;

  /** Public view of {@link getCostCeiling}, for the drain loop. */
  public getCostCeilingForQueue(): number | undefined {
    return this.getCostCeiling();
  }

  /**
   * The largest `cost` this client could ever admit, or undefined when it has no
   * ceiling. A `sharedLimit` client answers for its parent, whose budget it
   * spends; `noLimit` has none, and several limits are bounded by the tightest.
   */
  protected getCostCeiling(): number | undefined {
    return costCeilingFor(this.rateLimit, () => this.getParentRateLimit?.());
  }

  /**
   * Resolves the rate limit this client draws on, for a `sharedLimit` child.
   * Injected by `createClients`, which is where the client registry lives.
   */
  public getParentRateLimit?: () => ClientTypes.RateLimitConfig | undefined;

  /**
   * The budgets a request must claim here, in the form the backend takes them.
   * Empty for a client with nothing to meter against.
   */
  public getLimitSpecs(
    _grantId: string | undefined,
    _slotId: string
  ): MultiLimitSpec[] {
    return [];
  }

  /**
   * Member id a request's capacity is recorded under. Each attempt claims and
   * releases its own, so a retry cannot release the slot its predecessor holds.
   */
  protected getSlotId(
    request: Pick<RequestMetadata, "requestId" | "retries">
  ): string {
    return request.retries === 0
      ? request.requestId
      : `${request.requestId}:retry:${request.retries}`;
  }

  /**
   * Re-runs role election. Injected by the handler, which owns the instance
   * registry; called from the health tick so election is periodic rather than
   * only edge-triggered.
   */
  public reconcileRole?: () => Promise<void>;

  /**
   * Claims the next pending request, marking it in-progress. `skipGrantIds`
   * passes over frozen grants so one stalled tenant does not block the queue.
   */
  protected async getNextRequest(
    skipGrantIds?: string[],
    skipRequestIds?: string[]
  ): Promise<QueuedRequest | null> {
    return this.backend.getNextRequest(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      skipGrantIds,
      skipRequestIds
    );
  }

  protected getFrozenGrantsKey(): string {
    return `${this.namespace}:frozenGrants`;
  }

  /** Empty unless this client isolates its grants — nothing else freezes per grant. */
  protected async getFrozenGrantIds(): Promise<string[]> {
    if (!this.usesGrantIsolation()) return [];

    return this.backend.smembers(this.getFrozenGrantsKey());
  }

  protected async addFrozenGrantId(grantId: string): Promise<void> {
    if (!grantId) return;
    // Gated to match every reader: `getFrozenGrantIds` and
    // `cleanupStaleFrozenGrants` both return early when the client does not
    // isolate, so a mark written here would be read by nothing and swept by
    // nothing. Such a client freezes client-wide anyway.
    if (!this.usesGrantIsolation()) return;
    await this.backend.sadd(this.getFrozenGrantsKey(), grantId);
  }

  protected async removeFrozenGrantId(grantId: string): Promise<void> {
    if (!grantId) return;
    await this.backend.srem(this.getFrozenGrantsKey(), grantId);
  }

  protected async updateRequestInQueue(
    requestId: string,
    updates: Partial<QueuedRequest>
  ): Promise<void> {
    await this.backend.updateRequest(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      requestId,
      updates
    );
  }

  public async getRequestFromQueue(
    requestId: string
  ): Promise<QueuedRequest | null> {
    return this.backend.getRequest(this.getMetadataKeyPrefix(), requestId);
  }

  public async removeRequestFromQueue(
    requestId: string
  ): Promise<{ wasThawRequest: boolean }> {
    return this.backend.removeRequest(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      requestId,
      // The mark has to outlast the window in which this request's own add can
      // still be outstanding, and that window is the admission budget.
      Math.max(
        REQUEST_TOMBSTONE_TTL_SECONDS,
        Math.ceil((this.requestOptions.cleanupTimeout || 60000) / 1000)
      )
    );
  }

  // -------------------------------------------- request event handlers

  /** Wakes the worker awaiting this request's turn. */
  public handleRequestReady(request: RequestMetadata) {
    this.emitter.emit(`requestReady:${request.requestId}`, request);
  }

  /** Completions awaiting cleanup after a transient backend failure. */
  private pendingRequestDone = new Map<string, PendingRequestDone>();
  /** Makes concurrent health/subscription/direct flushes single-flight. */
  private completionFlushes = new Map<string, Promise<void>>();
  /** Prevents pub/sub echoes from applying cleanup twice. */
  private completedRequestIds = new Set<string>();

  /**
   * Records a completion on every replica that observes it. Only the current
   * controller applies cleanup; workers retain the record across election gaps.
   */
  public async finalizeOwnedRequest(data: RequestDoneData): Promise<void> {
    if (this.replacementClient) {
      await this.replacementClient.finalizeOwnedRequest(data);
      return;
    }
    if (this.getBudgetOwnerClient) {
      const budgetOwner = this.getBudgetOwnerClient();
      if (budgetOwner && budgetOwner !== this) {
        await budgetOwner.finalizeOwnedRequest(data);
        return;
      }
      if (!budgetOwner) {
        this.storePendingRequestDone(data);
        this.scheduleCompletionForward();
        return;
      }
    }
    const completionId = `${data.requestId}:${data.retries}:${data.responseStatus}`;
    if (this.completedRequestIds.has(completionId)) return;
    this.storePendingRequestDone(data);
    if (this.role === "controller") {
      await this.flushPendingRequestDone(completionId);
    }
  }

  private storePendingRequestDone(data: RequestDoneData): void {
    const completionId = `${data.requestId}:${data.retries}:${data.responseStatus}`;
    if (this.completedRequestIds.has(completionId)) return;
    if (!this.pendingRequestDone.has(completionId)) {
      this.pendingRequestDone.set(completionId, {
        data,
        queueHandled: false,
        ownTypeHandled: false,
        freezeHandled: false,
        thawHandled: false,
      });
      // New work gets its own retry budget: the cap in
      // `scheduleCompletionForward` is meant to bound one burst's spin while a
      // budget owner is missing, not the client's whole lifetime.
      this.completionForwardAttempts = 0;
    }
  }

  private scheduleCompletionForward(): void {
    if (this.completionForwardTimer || this.completionForwardAttempts >= 50)
      return;
    this.completionForwardAttempts++;
    this.completionForwardTimer = setTimeout(() => {
      this.completionForwardTimer = undefined;
      void this.forwardPendingRequestDone().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | could not forward completion to rebuilt budget owner`
        );
      });
    }, 100);
  }

  private async forwardPendingRequestDone(): Promise<void> {
    if (this.getBudgetOwnerClient) {
      const owner = this.getBudgetOwnerClient();
      if (owner) {
        for (const pending of this.pendingRequestDone.values()) {
          await owner.finalizeOwnedRequest(pending.data);
        }
        this.pendingRequestDone.clear();
        this.completionForwardAttempts = 0;
      } else if (this.pendingRequestDone.size > 0) {
        this.scheduleCompletionForward();
      }
      return;
    }

    for (const [completionId, pending] of this.pendingRequestDone) {
      if (pending.data.ownerId === this.instanceId) {
        await this.backend.publish(
          `${this.handlerNamespace}:requestDone`,
          JSON.stringify(pending.data)
        );
      }
      // Role reconciliation above established that another replica is the
      // controller, which now owns the retained copy.
      this.pendingRequestDone.delete(completionId);
    }
  }

  private async flushPendingRequestDone(
    onlyCompletionId?: string
  ): Promise<void> {
    const entries = onlyCompletionId
      ? ([
          [onlyCompletionId, this.pendingRequestDone.get(onlyCompletionId)],
        ] as Array<[string, PendingRequestDone | undefined]>)
      : [...this.pendingRequestDone.entries()];
    for (const [completionId, pending] of entries) {
      if (!pending) continue;
      const existing = this.completionFlushes.get(completionId);
      if (existing) {
        await existing;
        continue;
      }
      const flush = (async () => {
        try {
          await this.handleRequestDone(pending);
          this.pendingRequestDone.delete(completionId);
          this.completedRequestIds.add(completionId);
          if (this.completedRequestIds.size > 10_000) {
            const oldest = this.completedRequestIds.values().next().value;
            if (oldest) this.completedRequestIds.delete(oldest);
          }
        } catch (error) {
          this.logger.error(
            { error },
            `Client ${this.name} | completion cleanup for ${completionId} will be retried by the health check`
          );
        }
      })();
      this.completionFlushes.set(completionId, flush);
      await flush.finally(() => this.completionFlushes.delete(completionId));
    }
  }

  private async handleRequestDone(pending: PendingRequestDone) {
    const { data } = pending;
    const isRetry = data.responseStatus === "retry";

    // A retry keeps its entry at pending status; success or failure removes it.
    // The removal reports whether this was the thaw probe, saving a read.
    let isThawRequest = data.isThawRequest;
    if (!pending.queueHandled && !isRetry && !data.fastPath) {
      const removed = await this.removeRequestFromQueue(data.requestId);
      isThawRequest ||= removed.wasThawRequest;
      data.isThawRequest = isThawRequest;
      pending.queueHandled = true;
    } else if (!pending.queueHandled && isRetry && data.grantId) {
      // A retrying probe keeps its queue entry, so neither the removal above nor a
      // freeze timer below releases the grant; without this it waits for
      // `cleanupStaleFrozenGrants` on the next health tick.
      //
      // Read from the entry, not from `data.isThawRequest`: nomination is written
      // to the entry by `tryStartThawRequest` on this side, and the owning
      // replica's `Request` keeps the `false` it was constructed with. Ordered
      // after the freeze test so it only costs a round trip when it can matter.
      if (!data.freezeClient || !freezeDuration(data)) {
        const entry = await this.getRequestFromQueue(data.requestId);
        if (entry?.isThawRequest) await this.removeFrozenGrantId(data.grantId);
      }
      pending.queueHandled = true;
    } else if (data.fastPath || !data.grantId) {
      pending.queueHandled = true;
    }

    // `freezeClient`, not merely a non-zero duration: a retry the consumer asked
    // for through `retryStatusCodes` or `retryHandler` says nothing about the other
    // requests, and freezing on it would zero the whole token bucket.
    //
    // Gated on the freeze duration rather than `waitTime`, because the two are
    // separate numbers and can disagree — a legal `retryBackoffBaseTime: 0` has a
    // zero wait while its freeze may run a whole refill interval.
    const freezeFor = freezeDuration(data);
    if (!pending.freezeHandled && data.freezeClient && freezeFor) {
      // Wake against the freeze in force, not the one this completion asked for:
      // `armFreeze` keeps the longer of the two, and waking early declines without
      // re-booking. `extraMs` rides back on the write, costing no round trip.
      const { extraMs } = await this.handleFreezeRequests(data);
      const waitFor = freezeFor + extraMs;
      // Unfreezing the grant before processing is what lets the next thaw probe
      // through. This timer routinely outlives the handler that scheduled it, so
      // the rejection guard keeps an unheld promise from surfacing in the host
      // process, and the retained handle lets shutdown cancel a pending freeze.
      const timer = setTimeout(() => {
        this.freezeTimers.delete(timer);
        void (async () => {
          if (data.grantId) await this.removeFrozenGrantId(data.grantId);
          await this.processRequests();
        })().catch((error) => {
          this.logger.error(
            { error },
            `Client ${this.name} | post-freeze processing failed`
          );
        });
      }, waitFor);
      this.freezeTimers.add(timer);
      pending.freezeHandled = true;
    } else if (!(data.freezeClient && freezeFor)) {
      pending.freezeHandled = true;
    }

    // Type-specific cleanup, e.g. releasing a concurrency slot; retries release too
    // and re-acquire next attempt. Ordered before the re-drain below because
    // `acquireConcurrency` excludes a request's own id from the in-flight sum, so a
    // re-drain running first could re-acquire a slot this release then deletes.
    try {
      if (!pending.ownTypeHandled) {
        await this.handleOwnTypeRequestDone(data);
        pending.ownTypeHandled = true;
      }
    } catch (error) {
      this.logger.error(
        { error },
        `Client ${this.name} | request-done cleanup failed`
      );
      // The request owner retains this completion and retries it on its health
      // tick. Swallowing the failure here would mark cleanup complete and leave
      // a concurrency slot stranded until its TTL.
      throw error;
    }

    // Nothing else wakes the drain loop for a retry that did not freeze the
    // client — a freeze schedules its own wake-up. Coalesces through
    // `processingLock`. Uses the same freeze condition as the arming above, so
    // the two cannot disagree about whether a freeze happened.
    if (isRetry && !(data.freezeClient && freezeDuration(data))) {
      this.processRequests().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | processRequests failed`
        );
      });
    }

    if (isThawRequest && !pending.thawHandled) {
      await this.backend.updateThawProgress(
        this.getFreezeStateKey(data.grantId),
        data.responseStatus === "success",
        `${data.requestId}:${data.retries}:${data.responseStatus}`
      );

      // Clearing the frozen mark releases the single-probe claim, whether the
      // probe succeeded or not. `processRequests` re-adds it when it nominates
      // the next probe; a failed probe consumes no thaw progress.
      if (data.grantId) await this.removeFrozenGrantId(data.grantId);
      pending.thawHandled = true;

      this.processRequests().catch((error) => {
        this.logger.error(
          { error },
          `Client ${this.name} | processRequests failed`
        );
      });
    }

    this.processRequests().catch((error) => {
      this.logger.error(
        { error },
        `Client ${this.name} | processRequests failed after completion`
      );
    });
  }

  /**
   * Arms the freeze and reports how much longer than this completion asked for
   * the freeze now runs, so the caller can wake against the real deadline.
   */
  private async handleFreezeRequests(
    data: RequestDoneData
  ): Promise<{ state: FreezeState; extraMs: number }> {
    const grantInfo = data.grantId ? ` (grant: ${data.grantId})` : "";
    const freezeFor = freezeDuration(data);
    this.logger.debug(`Freezing requests${grantInfo} for ${freezeFor}ms...`);

    const requestedUntil = (await this.backend.now()) + freezeFor;
    const thawCount = data.isRateLimited
      ? this.retryOptions.thawRequestCount
      : 0;

    // `armFreeze`, not `setFreezeState`: several failures may be arming
    // concurrently, and a plain write let whichever landed last set the deadline —
    // so a first-failure 1x arm could cut a third-retry 3x arm short, and every
    // replica read the shortened deadline. It also keeps a 5xx's zero probe
    // budget from cancelling the probes a 429 is still owed.
    const state = await this.backend.armFreeze(
      this.getFreezeStateKey(data.grantId),
      requestedUntil,
      thawCount
    );
    const frozenUntil = state.frozenUntil;

    // Arm first so no request or abort refund can enter between emptying a
    // token bucket and publishing the freeze. Admission consults this key, and
    // shipped backends also refuse refunds while it is live.
    await this.handleFreezeOwnTypeRequests(data.grantId);

    if (data.grantId) await this.addFrozenGrantId(data.grantId);

    // 429s are normal API behavior, not downtime. Treat upstream failures
    // (5xx / ECONNRESET / ETIMEDOUT / ECONNABORTED) as the downtime signal —
    // those are the cases where the integration is genuinely unable to serve.
    if (!data.isRateLimited) {
      await this.publishFreezeStateChanged({
        state: "frozen",
        grantId: data.grantId,
        frozenUntil,
        reason: "UPSTREAM_UNAVAILABLE",
      });
    }

    // Positive only when another arm's deadline won.
    return { state, extraMs: Math.max(0, frozenUntil - requestedUntil) };
  }

  /**
   * Announces a freeze so a host can open a downtime window. There is deliberately no
   * "recovered" signal — the freeze deadline reflects back-off, not evidence the
   * integration is serving — so observers watch for a successful `requestDone`.
   * Errors are swallowed: this only reports on the lifecycle, it is not part of it.
   */
  protected async publishFreezeStateChanged(payload: {
    state: "frozen";
    grantId?: string;
    frozenUntil?: number;
    reason: "UPSTREAM_UNAVAILABLE";
  }): Promise<void> {
    try {
      await this.backend.publish(
        `${this.handlerNamespace}:freezeStateChanged`,
        JSON.stringify({
          clientName: this.name,
          ...payload,
        })
      );
    } catch (err) {
      this.logger.warn(
        {
          clientName: this.name,
          state: payload.state,
          error: err instanceof Error ? err.message : String(err),
        },
        "Failed to publish freezeStateChanged"
      );
    }
  }

  // ---------------------------------------------- freeze state helpers

  /**
   * Whether the next queued request may proceed now. With an isolated grant,
   * the freeze state consulted is that grant's own.
   *
   * Takes the whole request because subclasses decide on more than the grant:
   * a concurrency-limited client claims its slot here, so that a request which
   * cannot proceed leaves the admission loop free instead of blocking it.
   */
  protected async canProcessNextRequest(request: RequestMetadata): Promise<{
    canProcess: boolean;
    isThawRequest: boolean;
    /**
     * When the freeze this refusal is due to lapses, on the backend's clock.
     * Passed up so the drain loop can book its own wake-up for it; the backend
     * reads it in the same command that decides `canProcess`, so it is free.
     */
    frozenUntil?: number;
  }> {
    return this.backend.canProcessRequest(
      this.getFreezeStateKey(request.grantId)
    );
  }

  /**
   * Books the drain loop's return for when a freeze lapses. Both ends of the
   * subtraction are the backend's clock, which is what makes the delay right under
   * skew, and `armFreeze` is monotone, so a booking is exact or early and early
   * self-corrects. Called on every decline for that reason.
   *
   * Clock ownership: docs/design-notes.md#clocks.
   */
  protected async scheduleDrainForFreeze(
    frozenUntil?: number,
    grantId?: string
  ): Promise<void> {
    // The caller has it whenever the refusal came from `canProcessNextRequest`.
    // The `ClientFrozenError` path does not — that throw comes from inside
    // admission — so it pays one read, on the freeze path only.
    let deadline = frozenUntil;
    if (deadline === undefined) {
      const state = await this.backend
        .canProcessRequest(this.getFreezeStateKey(grantId))
        .catch(() => undefined);
      // Only a freeze that still refuses carries a wake-up. `frozenUntil` is
      // reported whenever freeze state exists at all, so a client whose freeze has
      // lapsed with probes open reports a deadline in the past — and a refusal there
      // came from something else, an occupied concurrency slot, which its release
      // wakes. Booking on that past deadline instead made every pass re-decline and
      // re-book against `scheduleDrain`'s 1ms floor.
      if (!state || state.canProcess) return;
      deadline = state.frozenUntil;
    }
    if (deadline === undefined) return;

    const now = await this.backend.now().catch(() => undefined);
    if (now === undefined) return;
    this.scheduleDrain(deadline - now);
  }

  /**
   * Whether the client is frozen right now. Unlike `canProcessNextRequest` this
   * claims nothing, so it is safe to call repeatedly from inside a wait loop.
   */
  /** Public view of {@link getFreezeStateKey}, for a client spending this budget. */
  public getFreezeStateKeyFor(grantId?: string): string {
    return this.getFreezeStateKey(grantId);
  }

  /** Public view of {@link isFrozen}, for a client spending this one's budget. */
  public async isBudgetFrozen(grantId?: string): Promise<boolean> {
    return this.isFrozen(grantId);
  }

  protected async isFrozen(grantId?: string): Promise<boolean> {
    const { canProcess } = await this.backend.canProcessRequest(
      this.getFreezeStateKey(grantId)
    );
    return !canProcess;
  }

  protected async hasThawRequestInProgress(grantId?: string): Promise<boolean> {
    if (!grantId) return false;
    return this.backend.hasThawRequestInProgress(
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      grantId
    );
  }

  /**
   * Claims this request as the grant's single thaw probe, returning "exists" if
   * another already holds the claim.
   *
   * Marking the grant frozen, checking for an existing probe and staking the
   * claim have to be one atomic step, or two controllers racing each other
   * would both probe an API that just rate-limited us.
   */
  protected async tryStartThawRequest(
    grantId: string | undefined,
    requestId: string
  ): Promise<"started" | "exists"> {
    return this.backend.tryStartThawRequest(
      this.getFrozenGrantsKey(),
      this.getQueueKey(),
      this.getMetadataKeyPrefix(),
      requestId,
      grantId ?? ""
    );
  }

  // -------------------------------------------------------- statistics

  public async getStats(): Promise<ClientTypes.ClientStatistics> {
    const [rateLimit, _queueStats, freezeState, allRequests] =
      await Promise.all([
        this.getRateLimitStats(),
        this.backend.getQueueStats(
          this.getQueueKey(),
          this.getMetadataKeyPrefix()
        ),
        this.backend.getFreezeState(this.getFreezeStateKey()),
        this.backend.getAllRequests(
          this.getQueueKey(),
          this.getMetadataKeyPrefix()
        ),
      ]);

    // The backend's clock, not this process's. `frozenUntil` is written from
    // it and every other comparison in the codebase is made against it, so a
    // process running fast would report a live freeze as lapsed.
    const now = await this.backend.now();
    const isFrozen = freezeState ? now < freezeState.frozenUntil : false;
    const isThawing = freezeState
      ? freezeState.thawRequestCount > 0 && !isFrozen
      : false;

    const requestsInQueue: ClientTypes.ClientRequestsStatistics = {
      count: 0,
      cost: 0,
      requests: [],
    };
    const requestsInProgress: ClientTypes.ClientRequestsStatistics = {
      count: 0,
      cost: 0,
      requests: [],
    };

    for (const request of allRequests) {
      const metadata: RequestMetadata = {
        requestId: request.requestId,
        clientName: request.clientName,
        requestName: request.requestName,
        status: request.status === "pending" ? "inQueue" : "inProgress",
        priority: request.priority,
        cost: request.cost,
        retries: request.retries,
        timestamp: request.timestamp,
        grantId: request.grantId,
        isThawRequest: request.isThawRequest,
        ownerId: request.ownerId,
      };

      if (request.status === "pending") {
        requestsInQueue.count++;
        requestsInQueue.cost += request.cost;
        requestsInQueue.requests.push(metadata);
      } else {
        requestsInProgress.count++;
        requestsInProgress.cost += request.cost;
        requestsInProgress.requests.push(metadata);
      }
    }

    return {
      // The registered name, matching `getName()`. A `sharedLimit` child's
      // `name` is its parent's, so reporting that filed the child's stats under
      // the parent and left the child's own name absent from the inventory.
      clientName: this.getName(),
      isFrozen,
      isThawing,
      thawRequestCount: freezeState?.thawRequestCount ?? 0,
      rateLimit,
      requestsInQueue,
      requestsInProgress,
    };
  }

  /**
   * Stores a grant's OAuth2 tokens, for a host seeding them after a user
   * completes authorization.
   */
  public async setGrantTokens(
    grantId: string,
    data: ClientTypes.SetGrantTokensData
  ): Promise<void> {
    if (!this.authData || this.authData.type !== "oauth2") {
      throw new NotOAuth2ClientError(this.name);
    }
    const key = `${this.authNamespace}:oauth2:${grantId}`;
    // One TTL covers the whole hash, and the hash holds the refresh token, so
    // it must be sized from the refresh token's lifetime. Sizing it from the
    // access token would drop both an hour after issue, leaving the grant
    // recoverable only by re-running authorization.
    const now = Date.now();
    const ttlSeconds = credentialTtlSeconds(
      (data.expiresAt - now) / 1000,
      true,
      data.refreshTokenExpiresAt
        ? (data.refreshTokenExpiresAt - now) / 1000
        : undefined
    );
    await this.backend.hset(
      key,
      {
        accessToken: encrypt(data.accessToken, this.key),
        expiresAt: data.expiresAt,
        // Process wall-clock, sharing the clock with the `expiresAt` it is
        // subtracted from — deliberately not `backend.now()`, which would inject
        // host-to-Redis skew into the computed lifetime. Lets the renewal margin
        // scale to how long the token was actually issued for, instead of
        // discarding a token shorter-lived than the flat margin on first use.
        issuedAt: now,
        tokenType: data.tokenType || "Bearer",
        refreshToken: encrypt(data.refreshToken, this.key),
        refreshTokenExpiresAt: data.refreshTokenExpiresAt,
      },
      ttlSeconds
    );
    // Nothing else knows which grants exist, and removal has to enumerate them.
    await this.trackGrantId(grantId);

    this.logger.debug(
      `Set grant tokens for client ${this.name}, grant ${grantId}`
    );
  }
}

/**
 * How long a freeze should last for this completion.
 *
 * Falls back to `waitTime` for a payload from a replica that predates
 * `freezeTime`, so a rolling deploy cannot lose the freeze.
 */
function freezeDuration(data: RequestDoneData): number {
  return data.freezeTime ?? data.waitTime;
}

/**
 * Probe budget for a freeze, sanitised the way `getFreezeBaseTime` sanitises a
 * duration — falling back rather than throwing, because `retryOptions` is
 * documented as unvalidated and a bad value here costs precision, not liveness.
 *
 * A probe budget is a count, and each backend stores it as one — so a fraction
 * surviving this far means a different number of probes per backend for a single
 * configuration. Truncated toward zero rather than floored, because that is what
 * both do to it: Lua's `string.format('%d', …)` renders `-2.9` as `-2`, and the
 * memory backend's twin is `Math.trunc`. The guard above makes the two identical
 * here today; matching keeps them identical if it ever loosens.
 */
function usableThawRequestCount(configured: number | undefined): number {
  if (configured === undefined) return 3;
  if (!Number.isFinite(configured) || configured < 0) return 3;
  return Math.trunc(configured);
}

export default BaseClient;
