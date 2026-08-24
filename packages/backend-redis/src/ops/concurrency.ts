import { normalizeTtlSeconds } from "@dianemo/core";
import type { Ctx } from "../context.js";
import type {
  AcquireConcurrencyResult,
  ConcurrencyConfig,
} from "@dianemo/core";

/** Concurrency operations. Twin of the memory backend's concurrency section. */
export function concurrencyOps(ctx: Ctx) {
  return {
    async tryAdmitConcurrency(
      queueKey: string,
      concurrencyKey: string,
      freezeKey: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig,
      ttl = 86400
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which claims the slot
      // before its EXPIRE: a TTL Redis rejects would raise with the slot held
      // and the caller holding no request to release it by.
      const ttlSeconds = normalizeTtlSeconds(ttl);
      const result = await ctx.run(
        "dianemoTryAdmitConcurrency",
        queueKey,
        concurrencyKey,
        freezeKey,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        ttlSeconds
      );
      return result === 1;
    },

    async acquireConcurrency(
      key: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig
    ): Promise<AcquireConcurrencyResult> {
      const result = await ctx.run(
        "dianemoAcquireConcurrency",
        key,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        86400 // TTL for the key
      );

      return JSON.parse(result as string) as AcquireConcurrencyResult;
    },

    async acquireQueuedConcurrency(
      key: string,
      metadataKey: string,
      cost: number,
      requestId: string,
      config: ConcurrencyConfig
    ): Promise<AcquireConcurrencyResult> {
      const result = await ctx.run(
        "dianemoAcquireQueuedConcurrency",
        key,
        metadataKey,
        cost,
        config.maxConcurrency,
        requestId,
        config.requestTtl,
        86400
      );
      return JSON.parse(result as string) as AcquireConcurrencyResult;
    },

    /**
     * Idempotent: releasing a slot that is already gone is a no-op, which is what
     * makes an abandoned attempt safe to release twice.
     */
    async releaseConcurrency(key: string, requestId: string): Promise<void> {
      await ctx.run("dianemoReleaseConcurrency", key, requestId);
    },

    async getConcurrencyState(
      key: string,
      requestTtl: number
    ): Promise<{ currentConcurrency: number; activeRequests: string[] }> {
      // Refused here so both backends raise the same class for the same input.
      // The script already fails on a non-finite cutoff, but as an ioredis
      // ReplyError from `ZRANGEBYSCORE key '(nan' '+inf'`. See the memory twin.
      if (!Number.isFinite(requestTtl)) {
        throw new RangeError(
          `requestTtl must be a finite number, received ${String(requestTtl)}`
        );
      }
      const result = await ctx.run(
        "dianemoGetConcurrencyState",
        key,
        requestTtl
      );

      return JSON.parse(result as string) as {
        currentConcurrency: number;
        activeRequests: string[];
      };
    },

    /**
     * Never call this from one replica while peers may still be running: it drops
     * their slots too, and the cap stops holding until they finish.
     */
    async clearConcurrency(key: string): Promise<void> {
      const costKey = `${key}:costs`;
      await Promise.all([ctx.redis.del(key), ctx.redis.del(costKey)]);
    },
  };
}
