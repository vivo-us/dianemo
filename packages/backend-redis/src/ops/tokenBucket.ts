import type { AcquireTokensResult, TokenBucketConfig } from "@dianemo/core";
import { normalizeTtlSeconds, parseStoredNumber } from "@dianemo/core";
import type { Ctx } from "../context.js";

/** Token-bucket operations. Twin of the memory backend's token-bucket section. */
export function tokenBucketOps(ctx: Ctx) {
  return {
    /**
     * Attempts to admit a request without queueing. See `tryAdmitImmediately`
     * in `lua/tokenBucket.ts` for the conditions.
     */
    async tryAdmitImmediately(
      queueKey: string,
      bucketKey: string,
      freezeKey: string,
      cost: number,
      config: TokenBucketConfig,
      ttl = 86400
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which spends the tokens
      // before its EXPIRE: a TTL Redis rejects would raise with the budget
      // already gone and nothing on this path to refund it.
      const ttlSeconds = normalizeTtlSeconds(ttl);
      const result = await ctx.run(
        "dianemoTryAdmitImmediately",
        queueKey,
        bucketKey,
        freezeKey,
        cost,
        config.maxTokens,
        config.tokensToAdd,
        config.interval,
        ttlSeconds
      );
      return result === 1;
    },

    async acquireTokens(
      key: string,
      cost: number,
      config: TokenBucketConfig
    ): Promise<AcquireTokensResult> {
      const result = await ctx.run(
        "dianemoTokenBucket",
        key,
        cost,
        config.maxTokens,
        config.tokensToAdd,
        config.interval,
        86400 // TTL for the key
      );

      return JSON.parse(result as string) as AcquireTokensResult;
    },

    async getTokenBucketState(
      key: string,
      config: TokenBucketConfig
    ): Promise<{ tokens: number; lastUpdate: number }> {
      // Redis's clock, not this process's. `lastUpdate` was written by whichever
      // replica last refilled the bucket, and every acquire script measures
      // elapsed time against redis TIME — so a reader on a local clock running
      // ahead reports refill the bucket will not actually grant, and one running
      // behind hides refill it will.
      //
      // One MULTI, not concurrent reads: Redis runs a transaction to completion
      // before any other client's command, and separate reads let an
      // `acquireTokens` land between two of them.
      const results = await ctx.redis
        .multi()
        .time()
        .hmget(key, "tokens", "lastUpdate")
        .exec();
      const failed = results?.find(([error]) => error);
      if (failed?.[0]) throw failed[0];
      if (!results) {
        throw new Error(`token bucket read for "${key}" was discarded`);
      }
      const [seconds, micros] = results[0][1] as [string, string];
      const now = Number(seconds) * 1000 + Math.floor(Number(micros) / 1000);
      const [tokens, lastUpdate] = results[1][1] as (string | null)[];

      // parseStoredNumber, not parseFloat: it is the one reading of a stored number
      // both backends share, and it keeps a fractional balance that truncation would
      // disagree with the memory twin about.
      let currentTokens = parseStoredNumber(
        tokens ?? undefined,
        config.maxTokens
      );
      let currentLastUpdate = parseStoredNumber(lastUpdate ?? undefined, now);

      const elapsed = now - currentLastUpdate;
      if (elapsed > 0 && config.interval > 0) {
        const intervalsElapsed = Math.floor(elapsed / config.interval);
        if (intervalsElapsed > 0) {
          currentTokens = Math.min(
            currentTokens + intervalsElapsed * config.tokensToAdd,
            config.maxTokens
          );
          // Advanced with the credit, as the acquire scripts and the memory twin
          // both do. Reporting the un-advanced value left a caller deriving the
          // progress already made toward the next refill a whole interval out.
          currentLastUpdate += intervalsElapsed * config.interval;
        }
      }
      // Matches the clamp in the acquire scripts, so reported stats agree with
      // what the bucket will actually hand out after a budget reduction.
      currentTokens = Math.min(currentTokens, config.maxTokens);

      return { tokens: currentTokens, lastUpdate: currentLastUpdate };
    },

    async resetTokenBucket(key: string, maxTokens: number): Promise<void> {
      // Expiry set here too, so a bucket reset and then never acquired against
      // does not outlive every other write to it. The memory twin sizes the same
      // 86400 through `writeBucket`.
      const pipeline = ctx.redis.pipeline();
      pipeline.hset(key, "tokens", maxTokens, "lastUpdate", await ctx.now());
      pipeline.expire(key, 86400);
      const results = await pipeline.exec();
      const failed = results?.find(([error]) => error);
      if (failed?.[0]) throw failed[0];
    },

    /**
     * Returns budget claimed for a request that was never dispatched.
     *
     * `freezeKey` is optional to the caller but not to the script: a custom command
     * is registered with a fixed key count, so an absent one is passed as an empty
     * string and the script skips the freeze check.
     */
    async refundTokens(
      key: string,
      cost: number,
      maxTokens: number,
      freezeKey?: string
    ): Promise<void> {
      await ctx.run(
        "dianemoRefundTokens",
        key,
        freezeKey ?? "",
        cost,
        maxTokens
      );
    },
  };
}
