import type { RequestConfig } from "./request/types.js";
import type { RequestErrorMetadata } from "./errors.js";
import { redactBody } from "./utils/redact.js";
import type RequestHandler from "./index.js";
import type { AxiosResponse } from "axios";
import { RequestError } from "./errors.js";
import type { Logger } from "./logger.js";
import { AxiosError } from "axios";

export interface TryHandleRequestOptions {
  context?: string;
  metadata?: Record<string, unknown>;
}

/**
 * Executes a request through the handler it was bound to, wrapping any failure
 * in a `RequestError` carrying a stable code.
 *
 * The handler is captured by the closure rather than passed per call. That is
 * what lets a plugin's request functions keep signatures like
 * `cancelShipment(clientName, data)` — no handler threading, so call sites are
 * unaffected by how the handler was composed.
 */
export type BoundTryHandleRequest = <T = unknown, D = unknown>(
  config: RequestConfig<D>,
  code: string,
  message: string,
  options?: TryHandleRequestOptions
) => Promise<AxiosResponse<T, D>>;

/**
 * Builds the `tryHandleRequest` bound to a specific handler. Called once per
 * plugin during `use()`; plugins receive the result via their context.
 *
 * Logging happens here rather than in `RequestError`'s constructor so that
 * building or rethrowing an error emits nothing — one failed call produces
 * exactly one log line, at the point the failure is first observed.
 */
export function createTryHandleRequest(
  handler: RequestHandler,
  logger: Logger
): BoundTryHandleRequest {
  return async function tryHandleRequest<T = unknown, D = unknown>(
    config: RequestConfig<D>,
    code: string,
    message: string,
    options?: TryHandleRequestOptions
  ): Promise<AxiosResponse<T, D>> {
    try {
      return await handler.handleRequest<T, D>(config);
    } catch (error) {
      // Already wrapped deeper in the call stack — rethrow untouched so the
      // error code always comes from the innermost call site that has the
      // most specific context, and so it is logged only once.
      if (error instanceof RequestError) throw error;

      const metadata: RequestErrorMetadata = {
        ...options?.metadata,
        client: config.clientName,
        context:
          options?.context ??
          (error instanceof Error ? error.message : undefined),
        originalError: formatError(error),
      };

      const wrapped = new RequestError(code, message, {
        metadata,
        cause: error instanceof Error ? error : undefined,
      });

      logger.error(
        {
          code: wrapped.code,
          errorId: wrapped.errorId,
          metadata: wrapped.metadata,
        },
        `Error ${wrapped.errorId}: Code: ${wrapped.code}: ${wrapped.message}${
          metadata.context ? `\n - Context: ${metadata.context}` : ""
        }`
      );

      throw wrapped;
    }
  };
}

function formatError(error: unknown): Record<string, unknown> {
  if (error instanceof AxiosError) {
    return {
      message: error.message,
      code: error.code,
      status: error.response?.status,
      statusText: error.response?.statusText,
      data: redactBody(error.response?.data),
    };
  }
  if (error instanceof Error) {
    return { message: error.message, stack: error.stack };
  }
  return { error };
}
