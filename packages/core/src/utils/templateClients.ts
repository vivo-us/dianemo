import { collectClientNames, generateClients } from "./createClients.js";
import { assertUsableRateLimits } from "./rateLimit.js";
import { encrypt, decrypt } from "./encryption.js";
import { isMultiRateLimit } from "./rateLimit.js";
import { ConfigurationError } from "../errors.js";
import type RequestHandler from "../index.js";
import type {
  CreateClientData,
  RateLimitConfig,
  RateLimitData,
} from "../client/types.js";
import type {
  ClientTemplateContext,
  ClientTemplateOptions,
  RateLimitOverrides,
  RateLimitPlan,
  TemplateClientOptions,
} from "../types.js";

/**
 * Serializes concurrent rebuilds of one `(templateName, instanceId)`. A local writer
 * races its own `templateClientAdded` broadcast, and two interleaved rebuilds both
 * pass the destroy phase, so the second `createClient` throws and both write the same
 * `templateClientMap` entry. Keyed by handler, so two in one process stay independent.
 */
const buildLocks = new Map<string, Promise<void>>();

/**
 * Entries already present in `templateClientMap` (i.e. built earlier in the
 * same `start()` by a startup hook calling `addTemplateClient`) are skipped,
 * so we don't tear them down and re-create them — `resetClient` doesn't
 * destroy(), so a rebuild leaks the old client's healthCheckInterval.
 */
export async function loadTemplateClientsFromBackend(
  this: RequestHandler
): Promise<void> {
  const entries = await this.backend.smembers(`${this.namespace}:templates`);
  const seenInBackend = new Set<string>();

  for (const entry of entries) {
    const separatorIdx = entry.indexOf("::");
    if (separatorIdx === -1) continue;
    const templateName = entry.slice(0, separatorIdx);
    const instanceId = entry.slice(separatorIdx + 2);
    seenInBackend.add(`${templateName}::${instanceId}`);

    if (!this.templates.has(templateName)) {
      this.logger.warn(
        `Template "${templateName}" is in the backend but not registered on this instance — skipping`
      );
      continue;
    }
    if (this.templateClientMap.get(templateName)?.has(instanceId)) continue;

    const encrypted = await this.backend.get(
      `${this.namespace}:template:${entry}`
    );
    if (!encrypted) continue;

    // One undecryptable entry used to reject `start()` outright, which also
    // skipped every later template in this loop. Ciphertext written under a
    // previous handler key is an expected, recoverable state — the OAuth token
    // path already treats it that way — so it is logged and stepped over.
    let credentials: unknown;
    try {
      credentials = decryptCredentials(encrypted, this.key);
    } catch {
      this.logger.error(
        `Stored credentials for ${entry} could not be decrypted with the current key; skipping this client. Re-register it to store credentials under the new key.`
      );
      continue;
    }

    const settings = await readTemplateClientOptions.bind(this)(entry);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      settings
    );
  }

  for (const [templateName, instances] of this.templateClientMap) {
    for (const instanceId of Array.from(instances.keys())) {
      const key = `${templateName}::${instanceId}`;
      if (seenInBackend.has(key)) continue;
      if (this.localTemplateClients.has(key)) continue;
      await destroyTemplateClients.bind(this)(templateName, instanceId);
    }
  }
}

/**
 * Sister to the encrypted credentials key — these aren't sensitive, so they are
 * stored as plain JSON and a replica that cannot decrypt the credentials can
 * still read them. Missing key, or unparseable JSON, means template defaults.
 */
export async function readTemplateClientOptions(
  this: RequestHandler,
  entry: string
): Promise<TemplateClientOptions | undefined> {
  const raw = await this.backend.get(`${this.namespace}:overrides:${entry}`);
  if (!raw) return undefined;
  try {
    return decodeTemplateClientOptions(JSON.parse(raw));
  } catch {
    this.logger.warn(`Malformed template options JSON for ${entry}; ignoring`);
    return undefined;
  }
}

/**
 * Reads back what this key holds, in either of the two shapes it has had.
 *
 * Before rate-limit options existed the value was the overrides record itself,
 * and those entries are still in every backend that predates this — so an object
 * naming neither of the current fields is read as that record rather than as an
 * empty settings object, which would silently drop an operator's overrides on
 * the first restart after an upgrade.
 */
function decodeTemplateClientOptions(parsed: unknown): TemplateClientOptions {
  if (!parsed || typeof parsed !== "object") return {};
  const value = parsed as Record<string, unknown>;
  if ("rateLimitOption" in value || "rateLimitOverrides" in value) {
    return value as TemplateClientOptions;
  }
  return { rateLimitOverrides: value as RateLimitOverrides };
}

/**
 * Accepts either shape at the call site too, so the argument that used to be a
 * bare overrides record still is one.
 */
export function normalizeTemplateClientOptions(
  options?: TemplateClientOptions | RateLimitOverrides
): TemplateClientOptions {
  if (!options) return {};
  return decodeTemplateClientOptions(options);
}

/** The JSON to store, or `undefined` when there is nothing worth a key. */
export function serializeTemplateClientOptions(
  settings: TemplateClientOptions
): string | undefined {
  const overrides = settings.rateLimitOverrides;
  const hasOverrides = !!overrides && Object.keys(overrides).length > 0;
  if (!hasOverrides && settings.rateLimitOption === undefined) return undefined;
  return JSON.stringify({
    ...(settings.rateLimitOption === undefined
      ? {}
      : { rateLimitOption: settings.rateLimitOption }),
    ...(hasOverrides ? { rateLimitOverrides: overrides } : {}),
  });
}

/**
 * Rejects a template whose declared options could not be built from.
 *
 * At registration, because these are the plugin author's own constants: a plan
 * with an unusable budget should fail the deploy that introduced it, not the
 * first tenant who picks it.
 */
export function assertTemplateOptions(
  templateName: string,
  options: ClientTemplateOptions
): void {
  const declared = options.rateLimitOptions;
  if (declared) {
    for (const [key, plan] of Object.entries(declared)) {
      for (const [path, rateLimit] of Object.entries(planPaths(plan))) {
        assertUsableRateLimits(
          rateLimit,
          `${templateName} (option "${key}"${path === "" ? "" : `, path "${path}"`})`
        );
      }
    }
  }
  const fallback = options.defaultRateLimitOption;
  if (fallback === undefined) return;
  if (declared && fallback in declared) return;
  throw new ConfigurationError(
    "unknown_rate_limit_option",
    `Template "${templateName}" names "${fallback}" as its defaultRateLimitOption, which is not one of its rateLimitOptions${
      declared ? `: ${Object.keys(declared).join(", ")}` : " (it declares none)"
    }.`
  );
}

/**
 * Serialized per `(templateName, instanceId)` so concurrent triggers can't
 * interleave their destroy/create phases (see `buildLocks` doc above).
 */
export async function buildAndRegisterTemplateClients(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  settings?: TemplateClientOptions
): Promise<void> {
  const lockKey = `${templateName}::${instanceId}`;
  const existing = buildLocks.get(lockKey);
  const run = (async () => {
    if (existing) {
      try {
        await existing;
      } catch {
        // Prior attempt failed — we still want to try our own rebuild.
      }
    }
    await doBuildAndRegister.call(
      this,
      templateName,
      instanceId,
      credentials,
      settings
    );
  })();
  buildLocks.set(lockKey, run);
  try {
    await run;
  } finally {
    // Only clear if this is still the latest run for this key.
    if (buildLocks.get(lockKey) === run) buildLocks.delete(lockKey);
  }
}

async function doBuildAndRegister(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  credentials: unknown,
  settings?: TemplateClientOptions
): Promise<void> {
  const template = this.templates.get(templateName);
  if (!template) {
    this.logger.warn(
      `No builder registered for template "${templateName}", skipping`
    );
    return;
  }

  const context = resolveTemplateContext.bind(this)(
    templateName,
    template.options,
    settings
  );
  const result = template.builder(credentials, context);
  const clientDataList: CreateClientData[] = Array.isArray(result)
    ? result
    : [result];

  // The plan first, then the overrides on top: the plan is what the template
  // says this tenant is entitled to, and an override is the operator overruling
  // it. The other order would let a plan quietly undo an operator's decision.
  if (context.rateLimits) {
    applyPlan.bind(this)(templateName, clientDataList, context.rateLimits);
  }

  const overrides = settings?.rateLimitOverrides;
  // A shape mismatch skips the override — changing shape would switch the
  // underlying client class.
  if (overrides && Object.keys(overrides).length > 0) {
    for (const root of clientDataList) {
      applyOverrides.bind(this)(root, overrides, "");
    }
  }

  // The names are third-party builder output, and `doAddTemplateClient` validates
  // only the segments it was given, not that the builder used them. A name that does
  // not vary by instance collapses every tenant onto one client, which
  // `generateClients` destroys and recreates per tenant rather than reporting a
  // conflict — so each tenant's requests go out under whichever authenticated first.
  //
  // Only the instance segment is required: a template may legitimately build several
  // differently-named root clients.
  for (const client of clientDataList) {
    const segments = client.name.split(":");
    if (!segments.includes(instanceId)) {
      throw new ConfigurationError(
        "template_client_name_mismatch",
        `Template "${templateName}" built a client named "${client.name}" for instance "${instanceId}", which does not appear in the name. Client names must identify their instance — use buildClientName() — or two tenants resolve to one client and one credential entry.`
      );
    }
  }

  const newClientNames = collectClientNames(clientDataList);

  const prevNames =
    this.templateClientMap.get(templateName)?.get(instanceId) ??
    new Set<string>();
  for (const prevName of prevNames) {
    if (newClientNames.has(prevName)) continue;
    const stale = this.clients.get(prevName);
    if (stale) {
      // A name the template stopped producing: nothing replaces it, so its parked
      // requests are failed rather than handed on.
      await stale.destroy("removal");
      this.clients.delete(prevName);
    }
  }

  if (!this.templateClientMap.has(templateName)) {
    this.templateClientMap.set(templateName, new Map());
  }
  this.templateClientMap.get(templateName)!.set(instanceId, newClientNames);

  // Recorded so a replica that never built these clients can still find their
  // credential keys when the instance is removed.
  const namesKey = templateClientNamesKey(
    this.namespace,
    `${templateName}::${instanceId}`
  );
  await this.backend.del(namesKey).catch(() => {});
  for (const name of newClientNames) {
    await this.backend.sadd(namesKey, name).catch(() => {});
  }

  await generateClients.bind(this)(clientDataList);
}

/**
 * Resolves the plan a template client is on into what its builder is handed.
 *
 * An option this template no longer declares is dropped rather than refused: the
 * key was written when it was valid, and a plugin that renames a plan should not
 * make every client already on it unbuildable on the next restart. The warning
 * is what says the client is now on the template default.
 */
function resolveTemplateContext(
  this: RequestHandler,
  templateName: string,
  options: ClientTemplateOptions,
  settings?: TemplateClientOptions
): ClientTemplateContext {
  const chosen = settings?.rateLimitOption ?? options.defaultRateLimitOption;
  if (chosen === undefined) return {};
  const plan = options.rateLimitOptions?.[chosen];
  if (plan === undefined) {
    this.logger.warn(
      `Template "${templateName}" no longer declares a rateLimitOption "${chosen}"; building on the template default instead`
    );
    return {};
  }
  const rateLimits = planPaths(plan);
  return { rateLimit: rateLimits[""], rateLimits, rateLimitOption: chosen };
}

/**
 * A plan as the path map everything downstream works in.
 *
 * A plan may name one limit for the root client or a limit per sub-client path,
 * and the two are told apart by shape: an array, or an object carrying a string
 * `type`, is a limit. A path map's values are limits and its keys are sub-client
 * paths, so its own `type` — if a sub-client is somehow called that — holds an
 * object rather than a string, which is why the discriminant is the value's type
 * and not merely the key's presence.
 */
export function planPaths(
  plan: RateLimitPlan
): Record<string, RateLimitConfig> {
  if (Array.isArray(plan)) return { "": plan };
  if (typeof (plan as { type?: unknown }).type === "string") {
    return { "": plan as RateLimitConfig };
  }
  return plan as Record<string, RateLimitConfig>;
}

/**
 * Places a chosen plan's limits onto the clients the builder produced.
 *
 * Unlike an override, a plan is not held to the shape the builder declared: both
 * are the template author's own, the plan was validated at registration, and a
 * plan silently skipped for disagreeing with a default it is meant to replace
 * would be far harder to explain than one that simply applies.
 *
 * A path matching no client is a plugin bug — a renamed sub-client, most likely —
 * so it is reported rather than dropped, since the symptom otherwise is one
 * endpoint quietly running unlimited.
 */
function applyPlan(
  this: RequestHandler,
  templateName: string,
  clients: CreateClientData[],
  rateLimits: Record<string, RateLimitConfig>
): void {
  const applied = new Set<string>();
  const walk = (client: CreateClientData, path: string): void => {
    const limit = rateLimits[path];
    if (limit !== undefined) {
      client.rateLimit = limit;
      applied.add(path);
    }
    for (const sub of client.subClients ?? []) {
      walk(sub, path === "" ? sub.name : `${path}:${sub.name}`);
    }
  };
  for (const root of clients) walk(root, "");

  for (const path of Object.keys(rateLimits)) {
    if (applied.has(path)) continue;
    this.logger.warn(
      `Template "${templateName}" has a rate-limit option covering path "${path}", which no client it built matches; that limit was not applied`
    );
  }
}

function applyOverrides(
  this: RequestHandler,
  client: CreateClientData,
  overrides: RateLimitOverrides,
  path: string
): void {
  const override = overrides[path];
  if (override !== undefined) {
    const existing: RateLimitConfig = client.rateLimit ?? { type: "noLimit" };
    const mismatch = describeShapeMismatch(existing, override);
    if (mismatch) {
      this.logger.warn(
        `Rate-limit override for path "${path}" ${mismatch} — skipping override`
      );
    } else {
      client.rateLimit = override;
    }
  }
  if (client.subClients) {
    for (const sub of client.subClients) {
      const subPath = path === "" ? sub.name : `${path}:${sub.name}`;
      applyOverrides.bind(this)(sub, overrides, subPath);
    }
  }
}

/**
 * Why an override may not replace a template default, or `undefined` when it may.
 *
 * An override swaps fields within the shape the template declared; it does not
 * change which client class is built. One limit may only be replaced by one of
 * the same type, and several only by several — a single-to-array swap would turn
 * a `requestLimit` client into a multi-limit one behind the template's back.
 */
function describeShapeMismatch(
  existing: RateLimitConfig,
  override: RateLimitConfig
): string | undefined {
  const existingIsMulti = isMultiRateLimit(existing);
  if (existingIsMulti !== isMultiRateLimit(override)) {
    return existingIsMulti
      ? "declares one rate limit but the template default declares several"
      : "declares several rate limits but the template default declares one";
  }
  if (existingIsMulti) return undefined;
  const overrideType = (override as RateLimitData).type;
  const existingType = (existing as RateLimitData).type;
  if (overrideType === existingType) return undefined;
  return `has type "${overrideType}" but the template default is "${existingType}"`;
}

export async function destroyTemplateClients(
  this: RequestHandler,
  templateName: string,
  instanceId: string,
  /**
   * Whether to delete the clients' stored credentials too. False for a rebuild,
   * which recreates the same clients and would otherwise force a needless
   * re-authentication; true for a genuine removal.
   */
  purgeCredentials = false
): Promise<void> {
  const names =
    this.templateClientMap.get(templateName)?.get(instanceId) ??
    new Set<string>();
  for (const name of names) {
    const client = this.clients.get(name);
    if (client) {
      if (purgeCredentials) await client.purgeCredentials();
      // `purgeCredentials` marks a genuine removal rather than a rebuild, which
      // is also what decides whether fleet-wide state may be cleared. Either way
      // this client is going away with no replacement, so its parked requests are
      // failed with a reason instead of waiting out `cleanupTimeout`.
      await client.destroy(purgeCredentials ? "finalRemoval" : "removal");
      this.clients.delete(name);
    }
  }

  // A replica that never built these clients has nothing to enumerate, but the
  // caller is about to delete the template blob and the registry entry — after
  // which no purge can ever find the credentials again, and they sit in the
  // backend for their full TTL. Deriving the keys from the name means a removal
  // handled by a read-only replica still deletes what the tenant asked us to.
  if (purgeCredentials && names.size === 0) {
    await purgeCredentialsByName
      .bind(this)(templateName, instanceId)
      .catch((error: unknown) => {
        this.logger.error(
          { error },
          `Failed to purge credentials for ${templateName}::${instanceId} without a local client`
        );
      });
  }
  this.templateClientMap.get(templateName)?.delete(instanceId);
  this.localTemplateClients.delete(`${templateName}::${instanceId}`);
}

/** Where the client names for a template instance are recorded, for any replica to read. */
export function templateClientNamesKey(
  namespace: string,
  entry: string
): string {
  return `${namespace}:templateClients:${entry}`;
}

/**
 * Deletes credential keys for a template instance using the client names
 * recorded at registration.
 *
 * Needed because the names cannot be reconstructed from `templateName` and
 * `instanceId` alone — the organization segment sits between them — and the
 * replica handling a removal may never have built these clients. Without this
 * the credentials outlive the removal by their full TTL, unreachable, because
 * the registry entry that would have named them is deleted in the same call.
 */
async function purgeCredentialsByName(
  this: RequestHandler,
  templateName: string,
  instanceId: string
): Promise<void> {
  const entry = `${templateName}::${instanceId}`;
  const names = await this.backend.smembers(
    templateClientNamesKey(this.namespace, entry)
  );
  if (names.length === 0) {
    this.logger.warn(
      `No recorded client names for ${entry}; stored credentials may remain until their TTL expires`
    );
    return;
  }

  for (const name of names) {
    const authNamespace = `${this.namespace}:${name.replaceAll(/ /g, "_")}`;
    const keys = [`${authNamespace}:oauth2`];
    for (const grantId of await this.backend.smembers(
      `${authNamespace}:grants`
    )) {
      keys.push(`${authNamespace}:oauth2:${grantId}`);
    }
    keys.push(`${authNamespace}:grants`);
    for (const key of keys) {
      await this.backend.del(key);
    }
  }
}

export function encryptCredentials(credentials: unknown, key: string): string {
  return encrypt(JSON.stringify(credentials), key);
}

export function decryptCredentials(encrypted: string, key: string): unknown {
  return JSON.parse(decrypt(encrypted, key));
}
