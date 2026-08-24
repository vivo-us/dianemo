import type { Ctx } from "../context.js";

/**
 * Instance-coordination operations. Twin of the memory backend's instance
 * section.
 */
export function instanceOps(ctx: Ctx) {
  return {
    async getInstances(
      instanceSetKey: string,
      instanceKeyPrefix: string,
      currentInstanceId: string
    ): Promise<Array<{ id: string; data: string }>> {
      const result = await ctx.run(
        "dianemoGetInstances",
        instanceSetKey,
        instanceKeyPrefix,
        currentInstanceId
      );

      return JSON.parse(result as string) as Array<{
        id: string;
        data: string;
      }>;
    },
  };
}
