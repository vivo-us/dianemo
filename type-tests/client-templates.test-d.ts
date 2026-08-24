import RequestHandler, {
  memoryBackend,
  type OAuth2Credentials,
  type TokenCredentials,
} from "@dianemo/core";

declare module "@dianemo/core" {
  interface ClientTemplates {
    oauth: OAuth2Credentials;
    token: TokenCredentials;
  }
}

const handler = new RequestHandler({ key: "k", backend: memoryBackend() });

await handler.registerClientTemplate("oauth", (credentials) => ({
  name: credentials.instanceId,
}));
await handler.addTemplateClient("oauth", {
  instanceId: "a",
  baseUrl: "https://example.test",
  clientId: "id",
  clientSecret: "secret",
});

await handler.addTemplateClient("oauth", {
  instanceId: "a",
  baseUrl: "https://example.test",
  // @ts-expect-error token credentials cannot be paired with the oauth name
  token: "wrong-kind",
});

const name: "oauth" | "token" = "oauth" as "oauth" | "token";
const credentials: (OAuth2Credentials | TokenCredentials) & {
  instanceId: string;
} = {} as never;
// Dynamic dispatch remains source-compatible; consumers that need correlation
// should preserve the name/credentials pair as a discriminated tuple.
await handler.addTemplateClient(name, credentials);

async function addGeneric<
  K extends keyof import("@dianemo/core").ClientTemplates,
>(
  key: K,
  value: import("@dianemo/core").ClientTemplates[K] & { instanceId: string }
) {
  await handler.addTemplateClient(key, value);
}
void addGeneric;
