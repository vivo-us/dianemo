import { fireMany, harnesses, startUpstream, withHandler } from "./harness.js";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

/**
 * `sharedLimit` — several credentials, one budget.
 *
 * The promise is that the child draws on the *parent's* budget rather than one
 * of its own. That is easy to appear to satisfy and easy to get wrong: a child
 * that silently got its own bucket would pass any test that only checked
 * requests succeed, while doubling what the vendor sees.
 */
describe.each(harnesses(7))("sharedLimit — $name", (harness) => {
  let upstream: Awaited<ReturnType<typeof startUpstream>>;

  beforeAll(async () => {
    upstream = await startUpstream(0);
  });
  afterAll(async () => {
    await upstream.close();
  });

  const PARENT = "parent:_:a";
  const PARENT_BUCKET = "ct:requestHandler:parent:_:a:rateLimit";
  const CHILD_BUCKET = "ct:requestHandler:child:_:a:rateLimit";

  const run = (
    parentLimit: Record<string, unknown>,
    fn: Parameters<typeof withHandler>[3]
  ) =>
    withHandler(
      harness,
      upstream.baseURL,
      [
        // Order matters: the child resolves its parent by name at construction.
        { name: "parent", rateLimit: { type: "requestLimit", ...parentLimit } },
        {
          name: "child",
          rateLimit: { type: "sharedLimit", clientName: PARENT },
        },
      ],
      fn
    );

  const bucketConfig = (interval: number, tokens: number) => ({
    maxTokens: tokens,
    tokensToAdd: tokens,
    interval,
  });

  it("serves requests addressed by the parent's name", async () => {
    // Addressing the shared client by its parent's name is the whole point of
    // the type, and the part that surprises people.
    await run(
      { interval: 1000, tokensToAdd: 100, maxTokens: 100 },
      async (handler) => {
        const results = await fireMany(handler, PARENT, 20);
        expect(results.every((r) => r.status === 200)).toBe(true);
      }
    );
  }, 30_000);

  it("spends the parent's bucket", async () => {
    await run(
      { interval: 60_000, tokensToAdd: 50, maxTokens: 50 },
      async (handler, backend) => {
        await fireMany(handler, PARENT, 10);
        const parent = await backend.getTokenBucketState(
          PARENT_BUCKET,
          bucketConfig(60_000, 50)
        );
        expect(parent.tokens).toBe(40);
      }
    );
  }, 30_000);

  it("keeps no bucket of its own", async () => {
    // The failure this guards against: a child that quietly got its own budget
    // would still serve every request, while the vendor saw twice the rate.
    await run(
      { interval: 60_000, tokensToAdd: 50, maxTokens: 50 },
      async (handler, backend) => {
        await fireMany(handler, PARENT, 10);
        const child = await backend.hgetall(CHILD_BUCKET);
        expect(child).toEqual({});
      }
    );
  }, 30_000);

  it("shares one budget rather than one each", async () => {
    // Twelve tokens total. If parent and child each had twelve, this finishes
    // immediately; sharing forces a refill wait.
    await run(
      { interval: 1500, tokensToAdd: 12, maxTokens: 12 },
      async (handler) => {
        const started = Date.now();
        await fireMany(handler, PARENT, 24);
        expect(Date.now() - started).toBeGreaterThan(1_000);
      }
    );
  }, 30_000);

  it("reports the shared configuration in stats", async () => {
    await run(
      { interval: 1000, tokensToAdd: 100, maxTokens: 100 },
      async (handler) => {
        const stats = await handler.getClientStats("child:_:a");
        expect(stats.rateLimit).toMatchObject({
          type: "sharedLimit",
          clientName: PARENT,
        });
      }
    );
  }, 30_000);
});
