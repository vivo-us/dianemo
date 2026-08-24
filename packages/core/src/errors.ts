/**
 * Base class for everything this library throws. Carries `statusCode` and `code` so
 * a host can map errors structurally without importing these types — duck-type on
 * those two fields when the handler and the catch site may resolve to different
 * copies of the package. See docs/concepts.md#errors.
 */
export class RequestHandlerError extends Error {
  constructor(
    public readonly statusCode: number,
    public readonly code: string,
    message: string
  ) {
    super(message);
    this.name = new.target.name;
  }
}

/** A request named a client that is not registered on this instance. */
export class ClientNotFoundError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      404,
      "client_not_found",
      `Client with name ${clientName} does not exist.`
    );
  }
}

/** A client with this name is already registered. */
export class ClientConflictError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      409,
      "client_conflict",
      `Client with name ${clientName} already exists.`
    );
  }
}

/**
 * The handler was constructed or driven with unusable configuration — a
 * template name containing `:`, a duplicate plugin registration, a missing
 * backend. Always a programming error, never a runtime condition.
 */
export class ConfigurationError extends RequestHandlerError {
  constructor(code: string, message: string) {
    super(500, code, message);
  }
}

/** A queued request was never admitted by the controller before its deadline. */
export class RequestTimeoutError extends RequestHandlerError {
  constructor(requestId: string, timeoutMs: number) {
    super(
      408,
      "request_timeout",
      `Request ${requestId} timed out waiting for controller after ${timeoutMs}ms`
    );
  }
}

/** Every retry attempt failed without producing a response. */
export class NoResponseError extends RequestHandlerError {
  constructor() {
    super(500, "no_response", "No response received after maximum retries.");
  }
}

/**
 * A client did not become available in time, or the wait was cancelled by
 * shutdown. Distinct from `ClientNotFoundError`: this is a transient
 * availability problem, not a misconfigured name.
 */
export class ClientUnavailableError extends RequestHandlerError {
  constructor(code: string, message: string) {
    super(503, code, message);
  }
}

/** Grant-token operations were attempted on a client that is not OAuth2. */
export class NotOAuth2ClientError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      400,
      "client_not_oauth2",
      `Client ${clientName} does not support OAuth2 grant tokens.`
    );
  }
}

/** An OAuth2 grant was asked to refresh but has no stored refresh token. */
export class GrantRefreshTokenMissingError extends RequestHandlerError {
  constructor(grantId: string) {
    super(
      400,
      "grant_refresh_token_missing",
      `No refresh token stored for grant "${grantId}"`
    );
  }
}

/**
 * A request declared a cost larger than the client's entire token budget, so
 * no amount of waiting could ever admit it.
 */
export class RequestCostExceedsBudgetError extends RequestHandlerError {
  constructor(clientName: string, cost: number, maxTokens: number) {
    super(
      400,
      "request_cost_exceeds_budget",
      `Request cost (${cost}) exceeds the maximum tokens (${maxTokens}) for client ${clientName}`
    );
  }
}

/**
 * The caller's `AbortSignal` fired while the request was queued.
 *
 * Raised instead of letting the request wait for capacity it will never use:
 * being admitted first would spend a token or a concurrency slot on a request
 * that axios then rejects anyway.
 */
export class RequestAbortedError extends RequestHandlerError {
  constructor(requestId: string) {
    super(
      499,
      "request_aborted",
      `Request ${requestId} was aborted before it was sent`
    );
  }
}

/**
 * Internal control-flow signal: the client froze while this request waited for
 * its turn. Never reaches the caller — `processRequests` catches it and returns
 * the request to the queue.
 */
export class ClientFrozenError extends RequestHandlerError {
  constructor(clientName: string) {
    super(
      503,
      "client_frozen",
      `Client ${clientName} froze while the request was waiting for its turn.`
    );
  }
}

export interface RequestErrorMetadata {
  context?: string;
  errors?: unknown[];
  [key: string]: unknown;
}

/**
 * A failed outbound call, wrapped with a stable error code and the original
 * error's salient fields under `metadata.originalError`.
 *
 * Thrown by `tryHandleRequest`, which is also what logs it — constructing one
 * has no side effects, so it is safe to build in a test or rethrow without
 * emitting a duplicate log line.
 */
export class RequestError extends Error {
  public readonly errorId: string;
  public readonly code: string;
  public readonly statusCode: number;
  public readonly metadata?: RequestErrorMetadata;

  constructor(
    code: string,
    message: string,
    options?: {
      statusCode?: number;
      metadata?: RequestErrorMetadata;
      cause?: Error;
      /** Injectable for deterministic tests; defaults to a random UUID. */
      errorId?: string;
    }
  ) {
    super(message);
    this.name = "RequestError";
    this.code = code;
    this.errorId = options?.errorId ?? crypto.randomUUID();
    this.statusCode = options?.statusCode ?? 500;
    this.metadata = options?.metadata;
    if (options?.cause) {
      // Non-enumerable, as `new Error(msg, { cause })` produces. Plain
      // assignment would put the cause in `JSON.stringify(err)`, and for an
      // Axios cause that reaches `toJSON()` and its headers. Still readable as
      // `err.cause`.
      Object.defineProperty(this, "cause", {
        value: options.cause,
        writable: true,
        enumerable: false,
        configurable: true,
      });
    }
  }
}

/**
 * The upstream status behind a `RequestError`, for a caller mapping one to a
 * domain result (404 → null). Undefined for a network-level failure, which has none.
 */
export function getOriginalStatus(err: unknown): number | undefined {
  if (!(err instanceof RequestError)) return undefined;
  const original = err.metadata?.originalError;
  if (!original || typeof original !== "object") return undefined;
  const status = (original as { status?: unknown }).status;
  return typeof status === "number" ? status : undefined;
}

/**
 * Extract the response body of the underlying Axios error attached to a
 * `RequestError`. Returns `undefined` when no response was captured.
 *
 * Useful for callers that need to read structured error payloads — e.g.,
 * a 410 Gone response carrying a `mergedIntoId` pointer.
 */
export function getOriginalResponseData(err: unknown): unknown {
  if (!(err instanceof RequestError)) return undefined;
  const original = err.metadata?.originalError;
  if (!original || typeof original !== "object") return undefined;
  return (original as { data?: unknown }).data;
}
