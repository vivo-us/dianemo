import type { AxiosRequestConfig, Method } from "axios";

export type RequestStatus = "inQueue" | "inProgress";

export interface RequestConfig<
  TRequestData = unknown,
> extends AxiosRequestConfig<TRequestData> {
  clientName: "default" | string;
  method: Method;
  /** Logical name for this request used for metrics labeling (e.g. "netSuite.records.salesOrder.get"). */
  requestName: string;
  /** Selects which grant's credentials and budget this request uses. */
  grantId?: string;
  /**
   * Integer 0-10, higher served first. Strict, not aging-based, so a continuous
   * supply of higher-priority work can starve lower.
   *
   * **Default value: 1**
   */
  priority?: number;
  metadata?: Record<string, unknown>;
  /**
   * Budget this request spends, for vendors that do not price every endpoint the
   * same. Fractional costs spend fractionally, and cost decides what a request
   * spends rather than where it sits in the queue.
   *
   * **Default value: 1**
   */
  cost?: number;
  /**
   * Set by the `Request` constructor before any `requestInterceptor` runs. An
   * interceptor needing a correlation id should read this rather than mint one: it is
   * already unique per outbound call, including retries.
   */
  requestId?: string;
  /**
   * Cancels the request at every stage: aborted before dispatch it fails with
   * `RequestAbortedError` having spent no budget, and after dispatch axios cancels
   * the call. See docs/concepts.md#cancellation.
   *
   * Narrower than axios's `GenericAbortSignal`, which makes `addEventListener`
   * optional — its Node adapter calls it unguarded, so the looser shape throws from
   * inside axios after this client has already spent on the request.
   */
  signal?: AbortSignal;
}

export interface RequestMetadata {
  requestId: string;
  status: RequestStatus;
  priority: number;
  timestamp: number;
  clientName: string;
  cost: number;
  /**
   * `cost` as the call site actually set it, undefined when it did not — which is how
   * the metrics path tells a cost-budgeted endpoint from a count-budgeted one. The
   * rate limiter uses the post-default `cost` above.
   */
  explicitCost?: number;
  requestName: string;
  retries: number;
  grantId?: string;
  /** The grant's single nominated recovery probe. */
  isThawRequest: boolean;
  /** Admitted without queueing; no queue entry exists for it. */
  fastPath?: boolean;
  /** Whoever originated it. Orphan cleanup spares an entry whose owner is alive. */
  ownerId: string;
}

export interface RequestRetryData {
  retry: boolean;
  message: string;
  isRateLimited: boolean;
  waitTime: number;
  /**
   * Whether this failure warrants freezing the whole client rather than backing
   * off just this request. True for 429s and upstream failures, which apply to
   * everyone; false for a retry the consumer asked for by status or handler.
   */
  freezeClient: boolean;
  /**
   * How long the client-wide freeze should last, when `freezeClient` is set. Separate
   * from `waitTime`, which paces only this request: the two can legitimately disagree.
   * See docs/design-notes.md#freeze-duration-and-retry-wait-are-separate-numbers.
   */
  freezeTime?: number;
}

export interface RequestDoneData extends RequestMetadata {
  responseStatus: "success" | "failure" | "retry";
  waitTime: number;
  /** How long the client-wide freeze should last. See `RequestRetryData`. */
  freezeTime?: number;
  isRateLimited: boolean;
  /** Whether the controller should freeze the client. See `RequestRetryData`. */
  freezeClient: boolean;
  /** Wall-clock time when the request lifecycle terminated, set by getRequestDoneData. */
  completedAt: number;
  /** HTTP status code observed (response or error.response). Absent for network errors. */
  httpStatus?: number;
}
