import type { CreateClientData, RateLimitData } from "../client/types.js";
import { collectClientNames, generateClients } from "./createClients.js";
import type { RateLimitOverrides } from "../types.js";
import { encrypt, decrypt } from "./encryption.js";
import { ConfigurationError } from "../errors.js";
import type RequestHandler from "../index.js";

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

    const overrides = await readOverrides.bind(this)(entry);
    await buildAndRegisterTemplateClients.bind(this)(
      templateName,
      instanceId,
      credentials,
      overrides
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
 * Sister to the encrypted credentials key — overrides aren't sensitive so
 * we store them as plain JSON. Missing key (or unparseable JSON) means
 * "no overrides", template defaults apply.
 */
async function readOverrides(
  this: RequestHandler,
  entry: string
): Promise<RateLimitOverrides | undefined> {
  const raw = await this.backend.get(`${this.namespace}:overrides:${entry}`);
  if (!raw) return undefined;
  try {
    return JSON.parse(raw) as RateLimitOverrides;
  } catch {
    this.logger.warn(`Malformed overrides JSON for ${entry}; ignoring`);
    return undefined;
  }
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
  overrides?: RateLimitOverrides
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
      overrides
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
  overrides?: RateLimitOverrides
): Promise<void> {
  const builder = this.templates.get(templateName);
  if (!builder) {
    this.logger.warn(
      `No builder registered for template "${templateName}", skipping`
    );
    return;
  }

  const result = builder(credentials);
  const clientDataList: CreateClientData[] = Array.isArray(result)
    ? result
    : [result];

  // type mismatch skips the override — changing type would switch the underlying client class.
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

function applyOverrides(
  this: RequestHandler,
  client: CreateClientData,
  overrides: RateLimitOverrides,
  path: string
): void {
  const override = overrides[path];
  if (override !== undefined) {
    const existing: RateLimitData = client.rateLimit ?? { type: "noLimit" };
    if (override.type !== existing.type) {
      this.logger.warn(
        `Rate-limit override for path "${path}" has type "${override.type}" but template default is "${existing.type}" — skipping override`
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
