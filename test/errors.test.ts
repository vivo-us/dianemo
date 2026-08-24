import { describe, expect, it } from "vitest";
import {
  ClientConflictError,
  ClientNotFoundError,
  ClientUnavailableError,
  ConfigurationError,
  NoResponseError,
  NotOAuth2ClientError,
  RequestError,
  RequestHandlerError,
  RequestTimeoutError,
  getOriginalResponseData,
  getOriginalStatus,
} from "../packages/core/src/errors.js";

describe("RequestHandlerError subclasses", () => {
  it.each([
    [new ClientNotFoundError("fedex:_:production"), 404, "client_not_found"],
    [new ClientConflictError("fedex:_:production"), 409, "client_conflict"],
    [
      new ConfigurationError("duplicate_plugin", "boom"),
      500,
      "duplicate_plugin",
    ],
    [new RequestTimeoutError("req-1", 60000), 408, "request_timeout"],
    [new NoResponseError(), 500, "no_response"],
    [
      new ClientUnavailableError("shutdown_aborted", "x"),
      503,
      "shutdown_aborted",
    ],
    [new NotOAuth2ClientError("ecb:_:live"), 400, "client_not_oauth2"],
  ])("%s carries its status and code", (err, statusCode, code) => {
    expect(err).toBeInstanceOf(RequestHandlerError);
    expect(err).toBeInstanceOf(Error);
    expect(err.statusCode).toBe(statusCode);
    expect(err.code).toBe(code);
  });

  it("names itself after the concrete subclass", () => {
    // A host error handler logging `err.name` should see the specific class,
    // not the base — this is what makes a stack trace readable.
    expect(new ClientNotFoundError("x").name).toBe("ClientNotFoundError");
    expect(new NoResponseError().name).toBe("NoResponseError");
  });

  it("includes the offending client name in the message", () => {
    expect(new ClientNotFoundError("fedex:_:production").message).toContain(
      "fedex:_:production"
    );
  });
});

describe("RequestError", () => {
  it("defaults to a 500 and generates an error id", () => {
    const err = new RequestError("FDX_0001", "Failed to cancel shipment");
    expect(err.statusCode).toBe(500);
    expect(err.code).toBe("FDX_0001");
    expect(err.name).toBe("RequestError");
    expect(err.errorId).toMatch(/^[0-9a-f-]{36}$/);
  });

  it("accepts an explicit error id so tests can assert on log output", () => {
    const err = new RequestError("FDX_0001", "boom", { errorId: "fixed-id" });
    expect(err.errorId).toBe("fixed-id");
  });

  it("preserves the cause chain", () => {
    const cause = new Error("socket hang up");
    const err = new RequestError("FDX_0001", "boom", { cause });
    expect(err.cause).toBe(cause);
  });

  it("does not log on construction", () => {
    // Constructing an error is not an event. tryHandleRequest owns the log,
    // so a rethrow or a test fixture cannot emit a phantom line.
    const spy = { called: false };
    const original = console.error;
    console.error = () => {
      spy.called = true;
    };
    try {
      new RequestError("FDX_0001", "boom");
    } finally {
      console.error = original;
    }
    expect(spy.called).toBe(false);
  });
});

describe("getOriginalStatus", () => {
  it("reads the status captured from an Axios failure", () => {
    const err = new RequestError("FS_0001", "not found", {
      metadata: { originalError: { status: 404, data: { detail: "gone" } } },
    });
    expect(getOriginalStatus(err)).toBe(404);
  });

  it("returns undefined when no status was captured", () => {
    const err = new RequestError("FS_0001", "network down", {
      metadata: { originalError: { message: "ECONNREFUSED" } },
    });
    expect(getOriginalStatus(err)).toBeUndefined();
  });

  it("returns undefined for anything that is not a RequestError", () => {
    expect(getOriginalStatus(new Error("plain"))).toBeUndefined();
    expect(getOriginalStatus(undefined)).toBeUndefined();
    expect(
      getOriginalStatus({ metadata: { originalError: { status: 404 } } })
    ).toBeUndefined();
  });
});

describe("getOriginalResponseData", () => {
  it("returns the captured response body", () => {
    const err = new RequestError("OMS_0001", "gone", {
      metadata: {
        originalError: { status: 410, data: { mergedIntoId: "abc" } },
      },
    });
    expect(getOriginalResponseData(err)).toEqual({ mergedIntoId: "abc" });
  });

  it("returns undefined when no response was captured", () => {
    expect(
      getOriginalResponseData(new RequestError("X", "y", { metadata: {} }))
    ).toBeUndefined();
  });
});
