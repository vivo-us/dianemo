import type { DianemoBackend } from "../packages/core/src/backend/types.js";
import { memoryBackend } from "../packages/core/src/backend/memory.js";
import { ConfigurationError } from "../packages/core/src/errors.js";
import { definePlugin } from "../packages/core/src/plugin.js";
import RequestHandler from "../packages/core/src/index.js";
import { describe, expect, it, vi } from "vitest";

/**
 * `use()` and the constructor only store the backend reference, so an unstarted
 * memory backend is enough to exercise plugin composition here. Anything that
 * calls `start()` belongs in an integration test.
 */
const stubBackend = () => memoryBackend();

const newHandler = () =>
  new RequestHandler({ key: "0".repeat(32), backend: stubBackend() });

const fedex = definePlugin({
  name: "fedex",
  registerTemplate: vi.fn(async () => {}),
  createRequests: ({ tryHandleRequest }) => ({
    cancelShipment: (clientName: string, id: string) =>
      tryHandleRequest(
        {
          clientName,
          requestName: "fedex.shipping.cancel",
          method: "PUT",
          url: `/ship/v1/shipments/cancel/${id}`,
        },
        "FDX_0001",
        "Failed to cancel shipment"
      ),
  }),
});

const ups = definePlugin({
  name: "ups",
  registerTemplate: async () => {},
  createRequests: () => ({ track: (n: string) => n }),
});

describe("use()", () => {
  it("returns each plugin's namespace under its own name", () => {
    const requests = newHandler().use(fedex, ups);

    expect(Object.keys(requests).sort()).toEqual(["fedex", "ups"]);
    expect(typeof requests.fedex.cancelShipment).toBe("function");
    expect(typeof requests.ups.track).toBe("function");
  });

  it("records the plugins it registered", () => {
    const handler = newHandler();
    handler.use(fedex, ups);
    expect(handler.getRegisteredPlugins()).toEqual(["fedex", "ups"]);
  });

  it("accumulates across separate calls", () => {
    const handler = newHandler();
    const a = handler.use(fedex);
    const b = handler.use(ups);
    expect(Object.keys(a)).toEqual(["fedex"]);
    expect(Object.keys(b)).toEqual(["ups"]);
    expect(handler.getRegisteredPlugins()).toEqual(["fedex", "ups"]);
  });

  it("rejects two plugins claiming the same name in one call", () => {
    const impostor = definePlugin({
      name: "fedex",
      registerTemplate: async () => {},
      createRequests: () => ({}),
    });
    expect(() => newHandler().use(fedex, impostor)).toThrow(ConfigurationError);
  });

  it("rejects a duplicate name across separate calls", () => {
    const handler = newHandler();
    handler.use(fedex);
    expect(() => handler.use(fedex)).toThrow(/already registered/);
  });

  it("does not register templates eagerly", () => {
    // Templates must register during start(), after the backend is up but before
    // credentials are hydrated — not at use() time, which happens at module
    // scope before anything is connected.
    const spy = vi.fn(async () => {});
    const plugin = definePlugin({
      name: "probe",
      registerTemplate: spy,
      createRequests: () => ({}),
    });
    newHandler().use(plugin);
    expect(spy).not.toHaveBeenCalled();
  });

  it("gives request functions no handler parameter", () => {
    // The call-site contract: a request function's signature is its own
    // domain arguments. If the handler ever leaks in, 400+ call sites break.
    const requests = newHandler().use(fedex);
    expect(requests.fedex.cancelShipment.length).toBe(2);
  });
});

describe("constructor", () => {
  it("requires a backend", () => {
    expect(
      () =>
        new RequestHandler({
          key: "k",
        } as unknown as { key: string; backend: DianemoBackend })
    ).toThrow(ConfigurationError);
  });

  it("constructs with no environment variables set", () => {
    // The whole point of severing the host's utils: importing and constructing
    // the handler must not depend on LOG_LEVEL, SERVICE_NAME, or REDIS_URL.
    const saved = { ...process.env };
    delete process.env.LOG_LEVEL;
    delete process.env.SERVICE_NAME;
    delete process.env.REDIS_URL;
    try {
      expect(() => newHandler()).not.toThrow();
    } finally {
      process.env = saved;
    }
  });

  it("defaults to a silent logger", () => {
    const spy = vi.spyOn(console, "log");
    newHandler().use(fedex);
    expect(spy).not.toHaveBeenCalled();
    spy.mockRestore();
  });

  it("namespaces keys by the configured prefix", () => {
    const prefixed = new RequestHandler({
      key: "k",
      backend: stubBackend(),
      keyPrefix: "test",
    });
    expect(prefixed.getNamespace()).toBe("test:requestHandler");
    expect(newHandler().getNamespace()).toBe("requestHandler");
  });

  it("gives each instance a distinct id", () => {
    expect(newHandler().getInstanceId()).not.toBe(newHandler().getInstanceId());
  });
});
