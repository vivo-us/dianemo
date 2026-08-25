import type { AcquireMultiLimitResult, MultiLimitSpec } from "@dianemo/core";
import { normalizeTtlSeconds } from "@dianemo/core";
import type { Ctx } from "../context.js";

/**
 * Several budgets claimed as one operation. Twin of the memory backend's
 * several-budgets section.
 */
export function multiLimitOps(ctx: Ctx) {
  return {
    async acquireMultiLimit(
      specs: MultiLimitSpec[],
      cost: number
    ): Promise<AcquireMultiLimitResult> {
      const { keys, payload } = encodeSpecs(specs);
      const result = await ctx.runVariadic(
        "dianemoAcquireMultiLimit",
        keys,
        cost,
        JSON.stringify(payload),
        86400
      );
      return JSON.parse(result as string) as AcquireMultiLimitResult;
    },

    async releaseMultiLimit(
      specs: MultiLimitSpec[],
      cost: number
    ): Promise<void> {
      if (specs.length === 0) return;
      const { keys, payload } = encodeSpecs(specs, { includeFreezeKeys: true });
      await ctx.runVariadic(
        "dianemoReleaseMultiLimit",
        keys,
        cost,
        JSON.stringify(payload)
      );
    },

    async tryAdmitMultiLimit(
      queueKey: string,
      freezeKeys: string[],
      specs: MultiLimitSpec[],
      cost: number,
      ttl = 86400
    ): Promise<boolean> {
      // Normalised here rather than left to the script, which spends the budgets
      // before its EXPIRE: a TTL Redis rejects would raise with them already
      // claimed and nothing on this path to hand them back.
      const ttlSeconds = normalizeTtlSeconds(ttl);
      const index = new KeyIndex([queueKey]);
      const freezeIndices = freezeKeys.map((key) => index.of(key));
      const encoded = encodeSpecs(specs, { index });
      const result = await ctx.runVariadic(
        "dianemoTryAdmitMultiLimit",
        index.keys,
        cost,
        JSON.stringify({
          specs: encoded.payload,
          freezeKeys: freezeIndices,
        }),
        ttlSeconds
      );
      return result === 1;
    },
  };
}

/**
 * Assigns each distinct key one 1-based `KEYS` position.
 *
 * Deduplicating matters beyond size: the same key appearing twice in `KEYS` is
 * what a cross-slot check in Redis Cluster would count twice, and the scripts
 * address budgets by index, so one position per key keeps the two views of a
 * budget identical.
 */
class KeyIndex {
  readonly keys: string[];
  private positions = new Map<string, number>();

  constructor(initial: string[] = []) {
    this.keys = [];
    for (const key of initial) this.of(key);
  }

  of(key: string): number {
    const existing = this.positions.get(key);
    if (existing !== undefined) return existing;
    this.keys.push(key);
    const position = this.keys.length;
    this.positions.set(key, position);
    return position;
  }
}

/** One JSON object per budget, addressing its key by `KEYS` position. */
type SpecPayload = {
  kind: MultiLimitSpec["kind"];
  k: number;
  fk?: number;
  maxTokens?: number;
  tokensToAdd?: number;
  interval?: number;
  maxConcurrency?: number;
  requestTtl?: number;
  slotId?: string;
};

function encodeSpecs(
  specs: MultiLimitSpec[],
  options: { index?: KeyIndex; includeFreezeKeys?: boolean } = {}
): { keys: string[]; payload: SpecPayload[] } {
  const index = options.index ?? new KeyIndex();
  const payload = specs.map((spec): SpecPayload => {
    const base = { kind: spec.kind, k: index.of(spec.key) };
    // Only the release path consults them, so the admission scripts are not
    // handed keys they would never read — a key in `KEYS` is a key Redis Cluster
    // requires to hash to this slot.
    const fk =
      options.includeFreezeKeys && spec.freezeKey
        ? { fk: index.of(spec.freezeKey) }
        : {};
    if (spec.kind === "tokenBucket") {
      return {
        ...base,
        ...fk,
        maxTokens: spec.config.maxTokens,
        tokensToAdd: spec.config.tokensToAdd,
        interval: spec.config.interval,
      };
    }
    return {
      ...base,
      ...fk,
      maxConcurrency: spec.config.maxConcurrency,
      requestTtl: spec.config.requestTtl,
      slotId: spec.slotId,
    };
  });
  return { keys: index.keys, payload };
}
