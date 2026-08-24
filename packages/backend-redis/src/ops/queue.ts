import type { QueueStats, QueuedRequest } from "@dianemo/core";
import type { Ctx } from "../context.js";
import {
  decodeFlatHash,
  decodeRequest,
  encodeRequestFields,
} from "../requestCodec.js";
import {
  calculateQueueScore,
  normalizeTtlSeconds,
  REQUEST_TOMBSTONE_TTL_SECONDS,
} from "@dianemo/core";

/**
 * Marks an id as removed, so a later add for it is refused rather than creating an
 * entry nobody awaits. Declared as a key rather than concatenated inside the script,
 * so the scripts still name every key they touch.
 */
function tombstoneKey(metadataKeyPrefix: string, requestId: string): string {
  return `${metadataKeyPrefix}:${requestId}:removed`;
}

/** Request-queue operations. Twin of the memory backend's queue section. */
export function queueOps(ctx: Ctx) {
  return {
    /**
     * Returns false when the id is already queued, so a retry is safe to re-add.
     * `ttl` defaults to 24 hours.
     */
    async addRequest(
      queueKey: string,
      metadataKeyPrefix: string,
      request: QueuedRequest,
      ttl: number = 86400
    ): Promise<boolean> {
      // Packs (priority, retries, arrival) into one sorted-set score, lowest
      // first. The band layout and why it cannot grow is in
      // core/backend/queueScore.ts, which the queue re-score Lua mirrors.
      const score = calculateQueueScore(
        request.priority,
        request.retries,
        request.timestamp
      );
      const metadataKey = `${metadataKeyPrefix}:${request.requestId}`;

      const flat = encodeRequestFields(request);
      // Normalised here rather than left to the script, which ZADDs and HSETs
      // before its EXPIRE: a TTL Redis rejects would leave an entry nobody
      // awaits, with no expiry, pinning the queue non-empty.
      const ttlSeconds = normalizeTtlSeconds(ttl);

      const added = await ctx.run(
        "dianemoAddRequest",
        queueKey,
        metadataKey,
        tombstoneKey(metadataKeyPrefix, request.requestId),
        request.requestId,
        score,
        ttlSeconds,
        ...flat
      );
      return added === 1;
    },

    async getQueueLength(queueKey: string): Promise<number> {
      return ctx.redis.zcard(queueKey);
    },

    async getNextRequest(
      queueKey: string,
      metadataKeyPrefix: string,
      skipGrantIds?: string[],
      skipRequestIds?: string[]
    ): Promise<QueuedRequest | null> {
      const result = await ctx.run(
        "dianemoGetNextRequest",
        queueKey,
        metadataKeyPrefix,
        JSON.stringify(skipGrantIds || []),
        JSON.stringify(skipRequestIds || [])
      );

      if (!result) return null;

      return decodeRequest(decodeFlatHash(JSON.parse(result as string)));
    },

    /**
     * Null when the entry is gone — which is how the drain loop tells a completed
     * request from one still waiting.
     */
    async getRequest(
      metadataKeyPrefix: string,
      requestId: string
    ): Promise<QueuedRequest | null> {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      return decodeRequest(await ctx.redis.hgetall(metadataKey));
    },

    /**
     * A change to `priority` or `retries` re-scores the sorted-set entry in the same
     * atomic step, so the queue order can never disagree with the metadata.
     */
    async updateRequest(
      queueKey: string,
      metadataKeyPrefix: string,
      requestId: string,
      updates: Partial<QueuedRequest>
    ): Promise<void> {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;

      const luaUpdates: Record<string, string | number | boolean> = {};
      if (updates.status !== undefined) luaUpdates.status = updates.status;
      if (updates.retries !== undefined) luaUpdates.retries = updates.retries;
      if (updates.priority !== undefined) {
        luaUpdates.priority = updates.priority;
      }
      if (updates.isThawRequest !== undefined) {
        luaUpdates.isThawRequest = updates.isThawRequest;
      }

      if (Object.keys(luaUpdates).length === 0) return;

      await ctx.run(
        "dianemoUpdateRequest",
        queueKey,
        metadataKey,
        JSON.stringify(luaUpdates)
      );
    },

    /**
     * Removes a request from the queue and reports whether it was the thaw probe.
     *
     * The flag is returned rather than read separately because the caller needs
     * it at exactly the moment the entry is being deleted — two round-trips for
     * one logical step, on the hottest path there is.
     */
    async removeRequest(
      queueKey: string,
      metadataKeyPrefix: string,
      requestId: string,
      tombstoneTtlSeconds: number = REQUEST_TOMBSTONE_TTL_SECONDS
    ): Promise<{ wasThawRequest: boolean }> {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const result = await ctx.run(
        "dianemoRemoveRequest",
        queueKey,
        metadataKey,
        tombstoneKey(metadataKeyPrefix, requestId),
        requestId,
        tombstoneTtlSeconds
      );
      return { wasThawRequest: result === "true" };
    },

    async getQueueStats(
      queueKey: string,
      metadataKeyPrefix: string
    ): Promise<QueueStats> {
      const result = await ctx.run(
        "dianemoGetQueueStats",
        queueKey,
        metadataKeyPrefix
      );

      return JSON.parse(result as string) as QueueStats;
    },

    async getAllRequests(
      queueKey: string,
      metadataKeyPrefix: string
    ): Promise<QueuedRequest[]> {
      const result = await ctx.run(
        "dianemoGetAllRequests",
        queueKey,
        metadataKeyPrefix
      );

      const rawRequests = JSON.parse(result as string) as string[][];
      const requests: QueuedRequest[] = [];

      for (const arr of rawRequests) {
        const request = decodeRequest(decodeFlatHash(arr));
        if (request) requests.push(request);
      }

      return requests;
    },

    /**
     * Drops queue entries whose metadata has expired, or whose owning instance is not
     * in the alive set. Keyed on ownership rather than an arbitrary age, so a slow
     * request is never mistaken for an abandoned one.
     *
     * One Lua round trip rather than O(2N) calls.
     */
    async cleanupOrphanedRequests(
      queueKey: string,
      metadataKeyPrefix: string,
      aliveInstanceIds: Set<string>,
      currentInstanceId: string
    ): Promise<number> {
      const result = await ctx.run(
        "dianemoCleanupOrphanedRequests",
        queueKey,
        metadataKeyPrefix,
        JSON.stringify([...aliveInstanceIds]),
        currentInstanceId
      );

      return result as number;
    },
  };
}
