import ConcurrencyLimitClient from "../client/clientTypes/concurrencyLimitClient.js";
import RequestLimitClient from "../client/clientTypes/requestLimitClient.js";
import SharedLimitClient from "../client/clientTypes/sharedLimitClient.js";
import type { CreateClientData, RateLimitData } from "../client/types.js";
import { ClientConflictError, ConfigurationError } from "../errors.js";
import NoLimitClient from "../client/clientTypes/noLimitClient.js";
import type BaseClient from "../client/index.js";
import type RequestHandler from "../index.js";

/**
 * Recursively collects all client names that a list of CreateClientData will produce,
 * expanding sub-clients using the same parent:child naming as mergeChildParentClients.
 */
export function collectClientNames(
  clients: CreateClientData[],
  parentPrefix?: string
): Set<string> {
  const names = new Set<string>();
  for (const client of clients) {
    const name = parentPrefix ? `${parentPrefix}:${client.name}` : client.name;
    names.add(name);
    if (client.subClients) {
      for (const subName of collectClientNames(client.subClients, name)) {
        names.add(subName);
      }
    }
  }
  return names;
}

/**
 * Builds clients, merging parent data into sub-clients. An existing client whose
 * source config is deep-equal is kept rather than recreated, so a credential
 * re-broadcast with unchanged values is a quiet no-op rather than a fleet-wide flood
 * of destroy/create cycles.
 */
export async function generateClients(
  this: RequestHandler,
  clients: CreateClientData[],
  parent?: CreateClientData
) {
  for (const declared of clients) {
    const client = parent
      ? mergeChildParentClients.bind(this)(declared, parent)
      : declared;
    const existing = this.clients.get(client.name);
    if (!existing || !clientDataEqual(existing.getSourceClientData(), client)) {
      const previous = await resetClient.bind(this)(client.name);
      await createClient.bind(this)(client);
      // The replacement takes over the requests the old client had parked, so it
      // can still fail them with a reason if the rebuilt budget cannot serve them.
      if (previous)
        this.clients.get(client.name)?.adoptWaitingRequests(previous);
    }
    // Recursed from what was declared, not from the merged copy: the merge drops
    // `subClients` so a child does not inherit its siblings, so reading them back
    // off the merged object stops the recursion at depth one — while
    // `collectClientNames` keeps going, reporting clients that were never built.
    if (declared.subClients) {
      await generateClients.bind(this)(declared.subClients, client);
    }
  }
}

/**
 * Excludes `subClients`, which `generateClients` compares independently after the
 * parent merge. Any two functions compare equal: a template builder mints fresh
 * closures every call, and a change to their code needs a restart anyway.
 */
function clientDataEqual(a: CreateClientData, b: CreateClientData): boolean {
  const { subClients: _aSub, ...aRest } = a;
  const { subClients: _bSub, ...bRest } = b;
  return deepEqualIgnoringFunctionIdentity(aRest, bRest);
}

function deepEqualIgnoringFunctionIdentity(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === "function" && typeof b === "function") return true;
  if (a === null || b === null || a === undefined || b === undefined) {
    return a === b;
  }
  if (typeof a !== typeof b) return false;
  if (typeof a !== "object") return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    const arrA = a as unknown[];
    const arrB = b as unknown[];
    if (arrA.length !== arrB.length) return false;
    return arrA.every((v, i) => deepEqualIgnoringFunctionIdentity(v, arrB[i]));
  }
  const objA = a as Record<string, unknown>;
  const objB = b as Record<string, unknown>;
  const aKeys = Object.keys(objA);
  const bKeys = Object.keys(objB);
  if (aKeys.length !== bKeys.length) return false;
  for (const k of aKeys) {
    if (!Object.prototype.hasOwnProperty.call(objB, k)) return false;
    if (!deepEqualIgnoringFunctionIdentity(objA[k], objB[k])) return false;
  }
  return true;
}

function mergeChildParentClients(
  child: CreateClientData,
  parent: CreateClientData
): CreateClientData {
  const merged = {
    ...deepCloneWithFunctions(parent),
    ...child,
    name: `${parent.name}:${child.name}`,
    // Credentials belong to the client at the top of the sub-client chain, since
    // a sub-client inherits its parent's authentication. Inheriting the parent's
    // own owner rather than using `parent.name` keeps that true at any nesting
    // depth. A `sharedLimit` child is not a sub-client and never reaches here.
    authOwnerName: parent.authOwnerName ?? parent.name,
    metadata: {
      ...(parent.metadata ? deepCloneWithFunctions(parent.metadata) : {}),
      ...child.metadata,
    },
    axiosOptions: {
      ...(parent.axiosOptions
        ? deepCloneWithFunctions(parent.axiosOptions)
        : {}),
      ...child.axiosOptions,
    },
    requestOptions: {
      ...(parent.requestOptions
        ? deepCloneWithFunctions(parent.requestOptions)
        : {}),
      ...child.requestOptions,
      defaults: {
        ...(parent.requestOptions?.defaults
          ? deepCloneWithFunctions(parent.requestOptions.defaults)
          : {}),
        ...child.requestOptions?.defaults,
      },
    },
    retryOptions: {
      ...(parent.retryOptions
        ? deepCloneWithFunctions(parent.retryOptions)
        : {}),
      ...child.retryOptions,
    },
  } as unknown as CreateClientData;
  delete merged.subClients;
  return merged;
}

function deepCloneWithFunctions<T>(obj: T): T {
  if (obj === null || typeof obj !== "object") return obj;

  if (Array.isArray(obj)) {
    return obj.map((item) => deepCloneWithFunctions(item)) as T;
  }

  const cloned = {} as T;
  for (const key in obj) {
    if (Object.prototype.hasOwnProperty.call(obj, key)) {
      const value = obj[key];
      if (typeof value === "function") cloned[key] = value;
      else if (value !== null && typeof value === "object") {
        cloned[key] = deepCloneWithFunctions(value);
      } else cloned[key] = value;
    }
  }
  return cloned;
}

/**
 * Resets the client with the given name so a new client can take over.
 * Destroys the previous client first so its health-check interval and any
 * pending in-memory state (e.g. a concurrency client's backend-tracked slots)
 * are released — without this, every rebuild (template re-registration,
 * hot reload) leaks state that the new client can't see.
 */
export async function resetClient(
  this: RequestHandler,
  clientName: string
): Promise<BaseClient | undefined> {
  const client = this.clients.get(clientName);
  if (!client) return undefined;
  await client.updateRole("worker");
  // "rebuild", not a removal: a replacement for this name is created next, and it
  // inherits both the fleet-wide slot ledger and this client's parked requests.
  await client.destroy("rebuild");
  this.clients.delete(clientName);
  // Returned so the replacement can adopt what this one was still holding.
  return client;
}

export async function createClient(
  this: RequestHandler,
  data: CreateClientData
) {
  const existing = this.clients.get(data.name);
  if (existing) {
    throw new ClientConflictError(data.name);
  }
  const baseData = {
    client: data,
    backend: this.backend,
    handlerNamespace: this.namespace,
    logger: this.logger,
    key: this.key,
    emitter: this.emitter,
    instanceId: this.id,
  };
  // Lets the health tick re-run election; see its call site.
  const attachReconcile = <T extends { reconcileRole?: () => Promise<void> }>(
    client: T
  ): T => {
    client.reconcileRole = async () => {
      await this.scheduleClientRoles();
    };
    return client;
  };
  // `"rateLimit" in data`, not `??`: only an absent property is the documented
  // `noLimit` default. A property that is present and null or undefined throws,
  // because `rateLimit: null` is what `JSON.parse` of a backend override or a
  // nullable database column produces, and an explicit `undefined` on a sub-client
  // is worse still — the merge spread overwrites the parent's limit with it, so
  // `rateLimit: row.limit ?? undefined` would drop an inherited cap rather than
  // inherit it. Normalising either with `??` silently removes a cap.
  const declared = "rateLimit" in data ? data.rateLimit : { type: "noLimit" };
  if (declared === null || declared === undefined) {
    throw new ConfigurationError(
      "unknown_rate_limit_type",
      `Client "${data.name}" declares rateLimit as ${JSON.stringify(
        declared ?? null
      )}. Omit the property entirely for the noLimit default, or name a type: requestLimit, concurrencyLimit, sharedLimit, noLimit.`
    );
  }
  const rateLimit = declared as RateLimitData;

  switch (rateLimit.type) {
    case "requestLimit": {
      const rlClient = new RequestLimitClient(baseData, rateLimit);
      this.clients.set(data.name, attachReconcile(rlClient));
      await rlClient.init();
      break;
    }
    case "concurrencyLimit": {
      const clClient = new ConcurrencyLimitClient(baseData, rateLimit);
      this.clients.set(data.name, attachReconcile(clClient));
      await clClient.init();
      break;
    }
    case "sharedLimit": {
      // A child whose parent does not exist is forced to `worker` by the
      // election and so is never drained by anyone: it accepts requests, sends
      // none, and grows its queue by one permanent entry per request.
      const parentName = rateLimit.clientName;
      const parent = this.clients.get(parentName);
      if (!parent) {
        throw new ConfigurationError(
          "shared_limit_parent_not_found",
          `Client "${data.name}" shares the rate limit of "${parentName}", which is not registered. Register the parent client first — a sharedLimit client cannot draw on a budget that does not exist.`
        );
      }
      if (parent.getRateLimit().type === "sharedLimit") {
        throw new ConfigurationError(
          "shared_limit_parent_is_shared",
          `Client "${data.name}" shares the rate limit of "${parentName}", which is itself a sharedLimit client. Point it at the client that owns the budget.`
        );
      }
      const slClient = new SharedLimitClient(baseData, rateLimit);
      // So the child can reject an oversized cost against its parent's ceiling.
      slClient.getParentRateLimit = () =>
        this.clients.get(parentName)?.getRateLimit();
      // Grant isolation splits the budget keys, and the budget is the parent's.
      // Answering from the child's own auth config would have a differently
      // authenticated child resolve the un-isolated key while the parent
      // resolved an isolated one, so grant traffic would stop being shared.
      slClient.getBudgetOwnerAuthData = () =>
        this.clients.get(parentName)?.getAuthData();
      slClient.getBudgetOwnerClient = () => this.clients.get(parentName);
      this.clients.set(data.name, attachReconcile(slClient));
      await slClient.init();
      break;
    }
    case "noLimit": {
      const nlClient = new NoLimitClient(baseData, rateLimit);
      this.clients.set(data.name, attachReconcile(nlClient));
      await nlClient.init();
      break;
    }
    default: {
      // A NAMED type we do not recognise. Silently falling back to noLimit
      // removes the cap: TypeScript covers hand-written config, but overrides
      // come back from the backend as untyped JSON and template builders are
      // routinely fed database rows, so a typo reaches here at runtime with the
      // limit intact in the caller's mind and absent in fact.
      throw new ConfigurationError(
        "unknown_rate_limit_type",
        `Client "${data.name}" has an unrecognised rateLimit.type ${JSON.stringify(
          (rateLimit as { type?: unknown }).type
        )}. Expected one of: requestLimit, concurrencyLimit, sharedLimit, noLimit.`
      );
    }
  }

  this.emitter.emit(`clientRegistered:${data.name}`);
}
