import type { QueuedRequest } from "@dianemo/core";
import { parsePriority } from "@dianemo/core";

/**
 * The one place a queue entry crosses between a `QueuedRequest` and the Redis
 * hash that stores it.
 *
 * Twin of the memory backend's `toQueued`/`addRequest` pair: the two backends
 * must decode one stored hash to the same object, which the conformance suite
 * asserts against both.
 */

/**
 * Rebuilds the field map from Lua's `HGETALL`, which arrives as a flat
 * `[key1, val1, key2, val2, ...]` array rather than a map.
 *
 * Null for an odd-length or empty array: a truncated reply would otherwise
 * decode to an entry with a field silently holding the next field's name.
 */
export function decodeFlatHash(arr: unknown): Record<string, string> | null {
  if (!Array.isArray(arr) || arr.length === 0 || arr.length % 2 !== 0) {
    return null;
  }
  const metadata: Record<string, string> = {};
  for (let i = 0; i < arr.length; i += 2) {
    metadata[arr[i] as string] = arr[i + 1] as string;
  }
  return metadata;
}

/**
 * Null unless all four of `requestId`, `clientName`, `requestName` and `status`
 * are present, which is how a caller tells a live entry from a remnant whose
 * metadata has partly expired under it.
 */
export function decodeRequest(
  metadata: Record<string, string> | null
): QueuedRequest | null {
  if (
    !metadata ||
    !metadata.requestId ||
    !metadata.clientName ||
    !metadata.requestName ||
    !metadata.status
  ) {
    return null;
  }

  return {
    requestId: metadata.requestId,
    clientName: metadata.clientName,
    requestName: metadata.requestName,
    status: metadata.status as "pending" | "inProgress",
    priority: parsePriority(metadata.priority),
    cost: parseFloat(metadata.cost) || 1,
    retries: parseInt(metadata.retries, 10) || 0,
    timestamp: parseInt(metadata.timestamp, 10) || 0,
    grantId: metadata.grantId || undefined,
    isThawRequest: metadata.isThawRequest === "true",
    ownerId: metadata.ownerId || "",
  };
}

/** Flattens to the `HSET` argument list the addRequest script unpacks. */
export function encodeRequestFields(request: QueuedRequest): string[] {
  const fields: Record<string, string> = {
    requestId: request.requestId,
    clientName: request.clientName,
    requestName: request.requestName,
    status: request.status,
    priority: request.priority.toString(),
    cost: request.cost.toString(),
    retries: request.retries.toString(),
    timestamp: request.timestamp.toString(),
    grantId: request.grantId || "",
    isThawRequest: request.isThawRequest ? "true" : "false",
    ownerId: request.ownerId,
  };
  const flat: string[] = [];
  for (const [k, v] of Object.entries(fields)) flat.push(k, v);
  return flat;
}
