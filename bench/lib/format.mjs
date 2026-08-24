/** Shared number formatting and percentile maths. */

export const ms = (n) => `${n.toFixed(1)} ms`;
export const rps = (count, seconds) => `${Math.round(count / seconds)} req/s`;

export function stats(samples) {
  const s = [...samples].sort((a, b) => a - b);
  const at = (q) => s[Math.min(s.length - 1, Math.floor(s.length * q))];
  const mean = s.length ? s.reduce((a, b) => a + b, 0) / s.length : 0;
  return {
    p50: at(0.5),
    p95: at(0.95),
    p99: at(0.99),
    max: s[s.length - 1],
    mean,
  };
}

export function median(xs) {
  const v = [...xs].sort((a, b) => a - b);
  if (!v.length) return 0;
  return v.length % 2
    ? v[(v.length - 1) / 2]
    : (v[v.length / 2 - 1] + v[v.length / 2]) / 2;
}
