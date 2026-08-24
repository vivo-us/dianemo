import { createServer } from "node:http";

/**
 * Upstream under test control.
 *
 * By default it answers instantly, which isolates coordination cost — but no
 * real API does that, and at zero latency the handler's overhead looks far more
 * significant than it is in practice. `setProfile` makes it behave like a real
 * vendor: a latency distribution, and a share of 429s and 5xxs.
 */
export async function startUpstream() {
  let profile = { p50: 0, p99: 0, rate429: 0, rate500: 0 };
  let served = 0;
  const wait = (delay) => new Promise((r) => setTimeout(r, delay));

  const server = createServer(async (_req, res) => {
    served++;
    const { p50, p99, rate429, rate500 } = profile;
    if (p50 > 0) {
      // Log-normal-ish: most requests near p50, a thin tail out to p99.
      const roll = Math.random();
      const delay =
        roll > 0.99 ? p99 : roll > 0.9 ? p50 + (p99 - p50) * 0.25 : p50;
      await wait(delay * (0.75 + Math.random() * 0.5));
    }
    const r = Math.random();
    if (r < rate429) {
      res.writeHead(429, {
        "Retry-After": "1",
        "Content-Type": "application/json",
      });
      return res.end('{"error":"rate_limited"}');
    }
    if (r < rate429 + rate500) {
      res.writeHead(500, { "Content-Type": "application/json" });
      return res.end('{"error":"server_error"}');
    }
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
  server.keepAliveTimeout = 60_000;
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address();

  return {
    server,
    baseURL: `http://127.0.0.1:${port}`,
    setProfile: (p) => {
      profile = { p50: 0, p99: 0, rate429: 0, rate500: 0, ...p };
    },
    servedCount: () => served,
  };
}
