import type { RequestConfig, RequestMetadata } from "../request/types.js";
import type { AxiosResponse, CreateAxiosDefaults } from "axios";
import type { DianemoBackend } from "../backend/types.js";
import type { Logger } from "../logger.js";
import type EventEmitter from "events";

export type ClientRole = "controller" | "worker";

/**
 * Why a client is being torn down. Three cases rather than a boolean, because a
 * rebuild and a removal both answer "no" to "may I clear fleet-wide state?" yet want
 * opposite treatment for the requests parked here.
 *
 * - `rebuild` — a replacement for this name follows, and inherits the parked requests.
 * - `removal` — going away, credentials kept.
 * - `finalRemoval` — going away for good, so fleet-wide state may be cleared.
 */
export type ClientDestroyReason = "rebuild" | "removal" | "finalRemoval";

/**
 * Outcome of a non-blocking attempt to claim the budget a request needs.
 *
 * `waitTime` is how long until the attempt is worth repeating, so the admission
 * loop can schedule its own return instead of sleeping on the spot.
 */
export interface AcquireTurnResult {
  acquired: boolean;
  waitTime?: number;
  /** Set when no amount of waiting will help, so the caller fails the request. */
  error?: string;
}

export interface BaseCredentialsData {
  instanceId: string;
  baseUrl: string;
  isSandbox?: boolean;
  /**
   * Organization scope for multi-tenant deployments. `null` (or undefined)
   * means "no org" — internal-service rows and single-tenant integrations
   * resolve to a literal `_` segment in the resulting clientName so the
   * format `<template>:<orgId>:<alias>[:<sub>...]` always has the same
   * shape and can be parsed positionally.
   */
  organizationId?: string | null;
}

export interface OAuth2Credentials extends BaseCredentialsData {
  clientId: string;
  clientSecret: string;
}

export interface TokenCredentials extends BaseCredentialsData {
  token: string;
}

export interface BasicCredentials extends BaseCredentialsData {
  username: string;
  password: string;
}

export type RateLimitChange<
  R extends RateLimitConfig = RateLimitConfig,
  N extends RateLimitConfig = R,
> = (
  oldRateLimit: R,
  response: AxiosResponse
) => Promise<N | undefined> | N | undefined;

export interface ClientConstructorData {
  client: CreateClientData;
  backend: DianemoBackend;
  handlerNamespace: string;
  logger: Logger;
  key: string;
  emitter: EventEmitter;
  /** Owns every request this replica originates. */
  instanceId: string;
}

/**
 * A request an external scheduler fires to detect recovery from sustained downtime.
 * It goes through the normal pipeline — auth, rate limit, retries — so make it a
 * cheap read. Dianemo stores this and never fires it itself.
 */
export interface ProbeRequestConfig {
  /** The request to fire. `clientName` is filled in by the scheduler. */
  config: Omit<RequestConfig, "clientName">;
  /** Cadence between probe attempts while the integration is considered down. */
  intervalMs: number;
}

export interface CreateClientData<R extends RateLimitConfig = RateLimitConfig> {
  name: string;
  /**
   * Which client's credential entry this one uses. Set internally when a
   * sub-client is merged onto its parent; a sub-client shares the parent's
   * authentication, so it must not file its own tokens.
   *
   * Not for callers to set — `generateClients` derives it.
   */
  authOwnerName?: string;
  /**
   * The Rate Limit info for the Client. Defaults to `noLimit`.
   *
   * An array declares several limits that must **all** admit a request before it
   * is sent — a per-second and a per-day cap, say. Every entry needs a `name`.
   * See docs/rate-limits/multiple-limits.md.
   */
  rateLimit?: R;
  /**
   * Computes a dynamic budget update. Shared clients receive their shared-limit
   * descriptor but update the concrete budget owned by their parent.
   */
  rateLimitChange?: RateLimitChange<
    R,
    [RateLimitConfig] extends [R]
      ? RateLimitConfig
      : R extends readonly NamedRateLimitData[]
        ? NamedRateLimitData[]
        : R extends SharedLimitClientOptions
          ? Exclude<RateLimitData, SharedLimitClientOptions>
          : R
  >;
  requestOptions?: RequestOptions;
  retryOptions?: Partial<RetryOptions>;
  /** Any HTTP status code included in this array will result in a debug log rather than an error log */
  httpStatusCodesToMute?: number[];
  /**
   * Reclaims orphaned requests, re-runs election and nudges the queue.
   *
   * **Default: 10000 ms (10 seconds)**
   */
  healthCheckIntervalMs?: number;
  /** Carried through untouched; dianemo never reads it. */
  metadata?: { [key: string]: unknown };
  axiosOptions?: CreateAxiosDefaults;
  authentication?: AuthCreateData;
  /** Inherit this client's endpoint, auth and retry options; override the rest. */
  subClients?: CreateClientData[];
  probeRequest?: ProbeRequestConfig;
}

export interface RateLimitUpdatedData {
  clientName: string;
  rateLimit: RateLimitConfig;
  /**
   * `init` at construction, `operator` for an admin update, `dynamic` from a
   * `rateLimitChange` callback. A listener that persists changes filters on this.
   */
  source: "init" | "operator" | "dynamic";
  /**
   * Instance id of the request handler that published this event. Used by
   * persistence-side listeners so only
   * one replica writes to the DB — every replica receives the broadcast,
   * so without this filter we'd take N writes per dynamic event.
   */
  publisherInstanceId: string;
}

export type RateLimitData =
  | RequestLimitClientOptions
  | ConcurrencyLimitClientOptions
  | NoLimitClientOptions
  | SharedLimitClientOptions;

/**
 * One entry of a multi-limit array: any rate limit, plus the `name` its budget
 * is keyed under. Names must match `/^[a-z0-9_]{1,64}$/` and be unique within
 * the client, because they become backend key segments.
 */
export type NamedRateLimitData = RateLimitData & { name: string };

/**
 * What a client declares as its `rateLimit`: one limit, or several that must all
 * admit before a request is sent. See docs/rate-limits/multiple-limits.md.
 */
export type RateLimitConfig = RateLimitData | NamedRateLimitData[];

export interface NoLimitClientOptions {
  type: "noLimit";
}

export interface RequestLimitClientOptions {
  type: "requestLimit";
  /** Milliseconds between refills. */
  interval: number;
  /** Tokens added per `interval`. */
  tokensToAdd: number;
  /** Bucket ceiling, and so the largest `cost` this client can ever admit. */
  maxTokens: number;
}

export interface ConcurrencyLimitClientOptions {
  type: "concurrencyLimit";
  /** Slots in flight at once, and so the largest admissible `cost`. */
  maxConcurrency: number;
}

export interface SharedLimitClientOptions {
  type: "sharedLimit";
  /** The name of the client to share a rate limit with */
  clientName: string;
}

export type SingleRateLimitStats =
  | RequestLimitClientStats
  | ConcurrencyLimitClientOptions
  | NoLimitClientOptions
  | SharedLimitClientOptions;

export type RateLimitStats = SingleRateLimitStats | MultiRateLimitStats;

export type NamedRateLimitStats = SingleRateLimitStats & { name: string };

/** What a client with several limits reports: one entry per declared limit. */
export interface MultiRateLimitStats {
  type: "multiLimit";
  limits: NamedRateLimitStats[];
}

export interface RequestLimitClientStats extends RequestLimitClientOptions {
  tokens: number;
}

export interface RequestOptions {
  /**
   * When cleaning up requests, how long until the request is counted as timed-out.
   *
   * Default: 60000 milliseconds (1 minute)
   */
  cleanupTimeout?: number;
  /**
   * Crash recovery only. Must exceed the slowest response the upstream can
   * legitimately produce, or a slot is reaped while its request is still in flight and
   * the effective cap becomes a multiple of `maxConcurrency`.
   *
   * Default: 120000 milliseconds (2 minutes)
   */
  concurrencySlotTtl?: number;
  /** Metadata to carry with the request. It is up to the user to validate the metadata */
  metadata?: Record<string, unknown>;
  /**
   * Default values to set for each request
   */
  defaults?: RequestDefaults;
  /** Runs before any global request interceptor. */
  requestInterceptor?: RequestInterceptor;
  /** Runs before any global response interceptor. */
  responseInterceptor?: ResponseInterceptor;
}

export interface RetryOptions {
  /** Default: 3. */
  maxRetries: number;
  /**
   * The base number of ms to wait before retrying a server error (5xx) request
   *
   * **Default value: 1000 ms**
   */
  retryBackoffBaseTime: number;
  /** Default: `"exponential"`. */
  retryBackoffMethod: BackoffType;
  /** Default: true. Setting it false also disarms the fleet-wide freeze. */
  retry429s: boolean;
  /** Default: true. */
  retry5xxs: boolean;
  retryHandler?: RetryHandler;
  /** Retried without freezing the fleet, unlike a 429 or 5xx. */
  retryStatusCodes: number[];
  /**
   * The number of requests in a row must come back with a 2xx status to start sending requests at full speed again after a rate limit has been breached
   *
   * **Default value: 3**
   */
  thawRequestCount: number;
}

export interface RequestDefaults {
  headers?: Record<string, string>;
  baseURL?: string;
  params?: Record<string, string>;
}

export type BackoffType = "exponential" | "linear";

/**
 * Decides whether a failed request is retried, for statuses dianemo does not
 * classify itself. A retry granted here does not freeze the fleet.
 */
export type RetryHandler = (error: unknown) => Promise<boolean> | boolean;

/**
 * Last chance to change a request before it is sent. **Must return a config
 * object** — a void return drops the request's configuration.
 */
export type RequestInterceptor = (
  config: RequestConfig
) => Promise<RequestConfig> | RequestConfig;

/** Last chance to reshape a response before the caller sees it. */
export type ResponseInterceptor = (
  config: RequestConfig,
  response: AxiosResponse
) => Promise<void> | void;

export interface ClientStatistics {
  clientName: string;
  isFrozen: boolean;
  isThawing: boolean;
  thawRequestCount: number;
  rateLimit: RateLimitStats;
  requestsInQueue: ClientRequestsStatistics;
  requestsInProgress: ClientRequestsStatistics;
}

export interface ClientRequestsStatistics {
  count: number;
  cost: number;
  requests: RequestMetadata[];
}

export interface ClientTokensUpdatedData {
  clientId: string;
  clientName: string;
  tokens: number;
}

export type AuthCreateData = AuthDataOAuth2 | AuthDataToken | AuthDataBasic;

/**
 * Determines how grant children handle rate limiting:
 * - "shared" (default): All grants share the parent's rate limit bucket
 * - "isolated": Each grant gets its own rate limit bucket using parent's rate limit rules
 */
export type GrantRateLimitBehavior = "shared" | "isolated";

export interface AuthDataOAuth2 extends CustomHeader {
  type: "oauth2";
  clientId: string;
  clientSecret: string;
  grantRefreshConfig?: OAuthRefreshConfig;
  refreshConfig: OAuthRefreshConfig;
  metadata?: { [key: string]: unknown };
  grantRateLimitBehavior?: GrantRateLimitBehavior;
}

export interface OAuthRefreshConfig {
  url: string;
  /**
   * Where to put the authentication data in the request:
   *
   * - `jsonBody`: The data will be sent in the body of the request as JSON
   * - `urlQuery`: The data will be sent in the URL as query parameters
   * - `urlEncodedForm`: The data will be sent in the body of the request as a URL encoded form data
   */
  dataLocation: "jsonBody" | "urlQuery" | "urlEncodedForm";
  /**
   * The data to send in the request
   *
   * Some values are available via keywords so you don't have to hardcode them
   * - `{{clientId}}`: The clientId from the authentication data
   * - `{{clientSecret}}`: The clientSecret from the authentication data
   * - `{{refreshToken}}`: The refreshToken from the authentication data
   */
  data: Record<string, string>;
  /**
   * Any custom headers to include in the request
   *
   * NOTE: If using Basic Auth, use the useBasicAuth property instead
   */
  customHeaders?: { [key: string]: string };
  /** Sends `clientId:clientSecret`, base64-encoded, as an `Authorization: Basic` header instead of in `data`. */
  useBasicAuth?: boolean;
  /**
   * Reshapes the refresh request for a provider needing per-call data (a nonce, a
   * signature). The result stays local: it is never written back onto shared
   * `authData`, so a non-idempotent interceptor cannot accumulate across refreshes.
   */
  requestInterceptor?: (
    config: OAuthRefreshConfig
  ) => Promise<OAuthRefreshConfig> | OAuthRefreshConfig;
  /** Normalises a non-standard token response into the OAuth2 shape dianemo expects. */
  responseInterceptor?: (
    res: AxiosResponse
  ) =>
    | Promise<OAuthResponse | OAuthGrantTypeResponse>
    | OAuthResponse
    | OAuthGrantTypeResponse;
}

export interface AuthDataToken extends CustomHeader {
  type: "token";
  token: string;
  encodeBase64?: boolean;
}

export interface AuthDataBasic extends CustomHeader {
  type: "basic";
  username: string;
  password: string;
}

export interface OAuthResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
  refresh_token?: string;
  refresh_token_expires_in?: number;
}

export interface OAuthGrantTypeResponse extends OAuthResponse {
  refresh_token: string;
}

/** Seeds a grant's OAuth2 tokens from a store dianemo did not obtain them from. */
export interface SetGrantTokensData {
  accessToken: string;
  /** When the access token expires (Unix timestamp in milliseconds) */
  expiresAt: number;
  /** Default: `"Bearer"`. */
  tokenType?: string;
  refreshToken: string;
  /** When the refresh token expires (Unix timestamp in milliseconds) */
  refreshTokenExpiresAt: number;
}

export interface CustomHeader {
  /** Sends the token here instead of in `Authorization`. */
  customHeaderName?: string;
  /** Scheme in front of the token. Default: `"Basic"` for basic auth, else `"Bearer"`. */
  customPrefix?: string;
  /** Sends the bare token with no scheme prefix. Default: false. */
  excludePrefix?: boolean;
}
