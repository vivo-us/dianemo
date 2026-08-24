import type { FreezeState } from "@dianemo/core";
import type { Ctx } from "../context.js";

/**
 * One script, so the read, the merge, the write and the TTL cannot be split.
 *
 * This was three round trips with an `await now()` in the middle, and "take the
 * larger" cannot be layered on a separate read at all: two arms would both read
 * the old value and the loser would still win the write.
 */
async function writeFreezeState(
  ctx: Ctx,
  key: string,
  frozenUntil: number,
  thawRequestCount: number,
  mode: "set" | "max"
): Promise<FreezeState> {
  const result = (await ctx.run(
    "dianemoWriteFreezeState",
    key,
    frozenUntil,
    thawRequestCount,
    mode
  )) as [number, number];
  return { frozenUntil: result[0], thawRequestCount: result[1] };
}

async function readFreezeState(
  ctx: Ctx,
  key: string
): Promise<{
  frozenUntil: number;
  thawRequestCount: number;
  inFreezeWindow: boolean;
} | null> {
  const result = (await ctx.run("dianemoReadFreezeState", key)) as
    [number, number, number] | null;
  if (!result) return null;
  return {
    frozenUntil: result[0],
    thawRequestCount: result[1],
    inFreezeWindow: result[2] === 1,
  };
}

/** Freeze and thaw operations. Twin of the memory backend's freeze/thaw section. */
export function freezeOps(ctx: Ctx) {
  return {
    async tryAdmitNoLimit(
      queueKey: string,
      freezeKey: string
    ): Promise<boolean> {
      const result = await ctx.run(
        "dianemoTryAdmitNoLimit",
        queueKey,
        freezeKey
      );
      return result === 1;
    },

    /** Set the freeze state for a client. */
    async setFreezeState(
      key: string,
      frozenUntil: number,
      thawRequestCount: number
    ): Promise<void> {
      await writeFreezeState(ctx, key, frozenUntil, thawRequestCount, "set");
    },

    /**
     * Arms a freeze without ever shortening one already standing, and returns the state
     * now in force — which may not be what was asked for, so schedule wake-ups against
     * the return value.
     *
     * Both fields take the larger value. `frozenUntil` because the back-off multiplier
     * comes from the failing request's own retry count, so a later-landing shallow
     * failure must not overwrite a deeper one's deadline; it cannot ratchet, since each
     * arm is computed from a fresh `now`. `thawRequestCount` because probe budget is an
     * obligation, not a reading — a 5xx arm carries 0 and would cancel a 429's
     * outstanding probes, turning single-flight recovery into a stampede.
     */
    async armFreeze(
      key: string,
      frozenUntil: number,
      thawRequestCount: number
    ): Promise<FreezeState> {
      return writeFreezeState(ctx, key, frozenUntil, thawRequestCount, "max");
    },

    async getFreezeState(key: string): Promise<FreezeState | null> {
      const state = await readFreezeState(ctx, key);
      if (!state) return null;
      return {
        frozenUntil: state.frozenUntil,
        thawRequestCount: state.thawRequestCount,
      };
    },

    /**
     * Records the outcome of a thaw probe.
     *
     * One script rather than read-then-write, so two probes completing together
     * cannot both decrement from the same starting count.
     */
    async updateThawProgress(
      key: string,
      success: boolean,
      completionId?: string
    ): Promise<FreezeState | null> {
      const raw = (await ctx.run(
        "dianemoUpdateThawProgress",
        key,
        `${key}:thawCompletions`,
        completionId ?? "",
        success ? "1" : "0"
      )) as string | null;
      if (!raw) return null;
      const parsed = JSON.parse(raw) as FreezeState | { cleared: true };
      if ("cleared" in parsed) return null;
      return parsed;
    },

    /** Clear the freeze state (client is no longer frozen). */
    async clearFreezeState(key: string): Promise<void> {
      await ctx.redis.del(key);
    },

    /**
     * Check if a client is currently frozen (either in freeze period or thawing).
     */
    async isFrozen(key: string): Promise<boolean> {
      // A non-null read already means "inside the freeze window, or still owing a
      // probe" — the script returns nil for anything else.
      return (await readFreezeState(ctx, key)) !== null;
    },

    /**
     * Check if client can process the next request (not frozen, or ready for thaw test).
     * Note: Caller should also check hasThawRequestInProgress() to ensure only one
     * thaw request is in flight at a time.
     */
    async canProcessRequest(key: string): Promise<{
      canProcess: boolean;
      isThawRequest: boolean;
      frozenUntil?: number;
    }> {
      const state = await readFreezeState(ctx, key);

      if (!state) return { canProcess: true, isThawRequest: false };

      // `frozenUntil` rides back with the refusal: the caller's next move is to book
      // a wake-up, and this clock's deadline is the only trustworthy one.
      if (state.inFreezeWindow) {
        return {
          canProcess: false,
          isThawRequest: false,
          frozenUntil: state.frozenUntil,
        };
      }

      return {
        canProcess: true,
        isThawRequest: true,
        frozenUntil: state.frozenUntil,
      };
    },

    async hasThawRequestInProgress(
      queueKey: string,
      metadataKeyPrefix: string,
      grantId: string
    ): Promise<boolean> {
      const result = await ctx.run(
        "dianemoHasThawRequestInProgress",
        queueKey,
        metadataKeyPrefix,
        grantId
      );

      return result === 1;
    },

    /**
     * Claims `requestId` as the grant's single thaw probe, atomically, so two
     * controllers cannot both probe an API that just rate-limited us.
     *
     * Returns "exists" when another probe already holds the claim.
     */
    async tryStartThawRequest(
      frozenGrantsKey: string,
      queueKey: string,
      metadataKeyPrefix: string,
      requestId: string,
      grantId: string
    ): Promise<"started" | "exists"> {
      const metadataKey = `${metadataKeyPrefix}:${requestId}`;
      const result = await ctx.run(
        "dianemoTryStartThawRequest",
        frozenGrantsKey,
        queueKey,
        metadataKeyPrefix,
        metadataKey,
        grantId,
        requestId
      );

      return result as "started" | "exists";
    },

    async cleanupStaleFrozenGrants(
      frozenGrantsKey: string,
      queueKey: string,
      metadataKeyPrefix: string,
      freezeStateKeyPrefix: string
    ): Promise<number> {
      const result = await ctx.run(
        "dianemoCleanupStaleFrozenGrants",
        frozenGrantsKey,
        queueKey,
        metadataKeyPrefix,
        freezeStateKeyPrefix
      );

      return result as number;
    },
  };
}
