import { expectTypeOf, it } from "vitest";
import RequestHandler, {
  definePlugin,
  memoryBackend,
  type CreateClientData,
  type RequestConfig,
  type RequestLimitClientOptions,
  type RateLimitData,
  type SharedLimitClientOptions,
  type DianemoBackend,
} from "@dianemo/core";

const handler = new RequestHandler({ key: "k", backend: memoryBackend() });

declare const legacyBackend: Omit<
  DianemoBackend,
  "refundTokens" | "acquireQueuedConcurrency"
>;
const compatibleBackend: DianemoBackend = legacyBackend;
void compatibleBackend;

const fedex = definePlugin({
  name: "fedex",
  registerTemplate: async () => {},
  createRequests: () => ({
    cancelShipment: (n: string) => Promise.resolve(n.length),
  }),
});

it("correlates rateLimitChange with the configured client kind", () => {
  const client = {
    name: "limited",
    rateLimit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 1,
      maxTokens: 5,
    },
    rateLimitChange: (old: RequestLimitClientOptions) => ({
      ...old,
      maxTokens: old.maxTokens + 1,
    }),
  } satisfies CreateClientData<RequestLimitClientOptions>;
  expectTypeOf(client.rateLimitChange).parameter(0).toHaveProperty("maxTokens");

  const invalid = {
    name: "bad",
    rateLimit: {
      type: "requestLimit",
      interval: 1000,
      tokensToAdd: 1,
      maxTokens: 5,
    },
    // @ts-expect-error a requestLimit client cannot become concurrencyLimit
    rateLimitChange: () => ({ type: "concurrencyLimit", maxConcurrency: 2 }),
  } satisfies CreateClientData<RequestLimitClientOptions>;
  void invalid;
});

it("keeps dynamic client configs and documented shared updates compatible", () => {
  const dynamicLimit: RateLimitData = {
    type: "concurrencyLimit",
    maxConcurrency: 2,
  };
  const dynamic: CreateClientData = {
    name: "dynamic",
    rateLimit: dynamicLimit,
  };
  expectTypeOf(dynamic).toMatchTypeOf<CreateClientData>();

  interface ExtendedClient extends CreateClientData {
    custom?: string;
  }
  const extended: ExtendedClient = { name: "extended", custom: "yes" };
  expectTypeOf(extended.custom).toEqualTypeOf<string | undefined>();

  const shared = {
    name: "child",
    rateLimit: { type: "sharedLimit", clientName: "parent" },
    rateLimitChange: (_old: SharedLimitClientOptions) => ({
      type: "requestLimit" as const,
      interval: 1000,
      tokensToAdd: 1,
      maxTokens: 2,
    }),
  } satisfies CreateClientData<SharedLimitClientOptions>;
  expectTypeOf(shared.rateLimitChange).returns.toHaveProperty("maxTokens");
});

it("keeps response payloads unknown unless the caller supplies a type", () => {
  const direct = handler.handleRequest({
    clientName: "default",
    requestName: "typed",
    method: "GET",
  });
  expectTypeOf(direct).resolves.toHaveProperty("data").toBeUnknown();

  const typed = handler.handleRequest<{ id: string }>({
    clientName: "default",
    requestName: "typed",
    method: "GET",
  });
  expectTypeOf(typed).resolves.toHaveProperty("data").toEqualTypeOf<{
    id: string;
  }>();
});

it("checks authentication discriminants and request interceptor contracts", () => {
  const tokenClient = {
    name: "token-client",
    authentication: { type: "token", token: "secret" },
  } satisfies CreateClientData;
  expectTypeOf(tokenClient.authentication.token).toBeString();

  const invalidAuth = {
    name: "bad-auth",
    authentication: {
      type: "token",
      // @ts-expect-error token authentication does not accept basic credentials
      username: "user",
      password: "password",
    },
  } satisfies CreateClientData;
  void invalidAuth;

  const interceptor = (config: RequestConfig): RequestConfig => config;
  const configured = {
    name: "intercepted",
    requestOptions: { requestInterceptor: interceptor },
  } satisfies CreateClientData;
  expectTypeOf(
    configured.requestOptions.requestInterceptor
  ).returns.toMatchTypeOf<RequestConfig>();
});

const ups = definePlugin({
  name: "ups",
  registerTemplate: async () => {},
  createRequests: () => ({ track: () => "1Z" }),
});

it("infers each plugin's namespace under its own key", () => {
  const requests = handler.use(fedex, ups);

  expectTypeOf(requests).toHaveProperty("fedex");
  expectTypeOf(requests).toHaveProperty("ups");
  expectTypeOf(requests.fedex.cancelShipment).returns.resolves.toBeNumber();
  expectTypeOf(requests.ups.track).returns.toBeString();
  // An unregistered plugin must not appear on the namespace.
  expectTypeOf(requests).not.toHaveProperty("shopify");
});
