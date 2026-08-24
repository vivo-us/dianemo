import { createTryHandleRequest } from "../packages/core/src/tryHandleRequest.js";
import { noopLogger, type Logger } from "../packages/core/src/logger.js";
import type RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it, vi } from "vitest";
import { AxiosError, AxiosHeaders } from "axios";
import {
  RequestError,
  getOriginalStatus,
} from "../packages/core/src/errors.js";

const config = {
  clientName: "fedex:_:production",
  requestName: "fedex.shipping.cancel",
  method: "PUT" as const,
  url: "/ship/v1/shipments/cancel/123",
};

const handlerThatThrows = (err: unknown) =>
  ({
    handleRequest: vi.fn().mockRejectedValue(err),
  }) as unknown as RequestHandler;

const recordingLogger = () => {
  const calls: unknown[][] = [];
  const logger: Logger = {
    ...noopLogger,
    error: (...args: unknown[]) => {
      calls.push(args);
    },
  };
  return { logger, calls };
};

describe("createTryHandleRequest", () => {
  it("passes the response through on success", async () => {
    const response = { status: 200, data: { ok: true } };
    const handler = {
      handleRequest: vi.fn().mockResolvedValue(response),
    } as unknown as RequestHandler;

    const tryHandleRequest = createTryHandleRequest(handler, noopLogger);
    await expect(
      tryHandleRequest<{ ok: boolean }>(config, "FDX_0001", "boom")
    ).resolves.toBe(response);
    expect(handler.handleRequest).toHaveBeenCalledWith(config);
  });

  it("wraps a non-RequestError failure with the supplied code", async () => {
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(new Error("socket hang up")),
      noopLogger
    );

    const err = await tryHandleRequest(config, "FDX_0001", "Failed to cancel")
      .then(() => null)
      .catch((e: unknown) => e);

    expect(err).toBeInstanceOf(RequestError);
    const requestError = err as RequestError;
    expect(requestError.code).toBe("FDX_0001");
    expect(requestError.message).toBe("Failed to cancel");
    expect(requestError.metadata?.client).toBe("fedex:_:production");
    // The original message becomes the context when none was supplied.
    expect(requestError.metadata?.context).toBe("socket hang up");
  });

  it("captures the Axios status so callers can branch on it", async () => {
    const axiosError = new AxiosError(
      "Request failed with status code 404",
      "ERR_BAD_REQUEST"
    );
    axiosError.response = {
      status: 404,
      statusText: "Not Found",
      data: { detail: "no such shipment" },
      headers: new AxiosHeaders(),
      config: { headers: new AxiosHeaders() },
    };

    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(axiosError),
      noopLogger
    );

    const err = await tryHandleRequest(config, "FDX_0001", "boom").catch(
      (e: unknown) => e
    );

    expect(getOriginalStatus(err)).toBe(404);
    expect((err as RequestError).metadata?.originalError).toMatchObject({
      status: 404,
      statusText: "Not Found",
      code: "ERR_BAD_REQUEST",
      data: { detail: "no such shipment" },
    });
  });

  it("rethrows an existing RequestError untouched", async () => {
    // The innermost call site has the most specific code; re-wrapping would
    // bury it behind a generic outer one.
    const inner = new RequestError("FDX_9999", "specific failure");
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(inner),
      noopLogger
    );

    const err = await tryHandleRequest(config, "FDX_0001", "generic").catch(
      (e: unknown) => e
    );
    expect(err).toBe(inner);
  });

  it("logs a wrapped failure exactly once", async () => {
    const { logger, calls } = recordingLogger();
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(new Error("nope")),
      logger
    );

    await tryHandleRequest(config, "FDX_0001", "boom").catch(() => {});
    expect(calls).toHaveLength(1);
    expect(calls[0][0]).toMatchObject({ code: "FDX_0001" });
  });

  it("does not log a rethrown RequestError again", async () => {
    const { logger, calls } = recordingLogger();
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(new RequestError("FDX_9999", "already logged")),
      logger
    );

    await tryHandleRequest(config, "FDX_0001", "boom").catch(() => {});
    expect(calls).toHaveLength(0);
  });

  it("merges caller metadata and prefers an explicit context", async () => {
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows(new Error("underlying")),
      noopLogger
    );

    const err = (await tryHandleRequest(config, "FDX_0001", "boom", {
      context: "cancelling a shipment for order 42",
      metadata: { orderId: "42" },
    }).catch((e: unknown) => e)) as RequestError;

    expect(err.metadata?.orderId).toBe("42");
    expect(err.metadata?.context).toBe("cancelling a shipment for order 42");
  });

  it("handles a thrown non-Error value", async () => {
    const tryHandleRequest = createTryHandleRequest(
      handlerThatThrows("just a string"),
      noopLogger
    );

    const err = (await tryHandleRequest(config, "FDX_0001", "boom").catch(
      (e: unknown) => e
    )) as RequestError;

    expect(err).toBeInstanceOf(RequestError);
    expect(err.metadata?.originalError).toEqual({ error: "just a string" });
    expect(err.cause).toBeUndefined();
  });
});
