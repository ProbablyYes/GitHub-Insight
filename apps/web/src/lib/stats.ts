// Lightweight statistical helpers for client-side analytics.
// Intended for sample sizes <= a few thousand; no external deps.

function mean(xs: number[]): number {
  if (xs.length === 0) return 0;
  let s = 0;
  for (const x of xs) s += x;
  return s / xs.length;
}

function variance(xs: number[], avg?: number): number {
  if (xs.length < 2) return 0;
  const m = avg ?? mean(xs);
  let s = 0;
  for (const x of xs) s += (x - m) * (x - m);
  return s / (xs.length - 1);
}

/** Welch's unequal-variance t-test. Returns the t statistic and Welch–Satterthwaite df. */
export function welchT(a: number[], b: number[]): { t: number; df: number } {
  const na = a.length;
  const nb = b.length;
  if (na < 2 || nb < 2) return { t: 0, df: 0 };
  const ma = mean(a);
  const mb = mean(b);
  const va = variance(a, ma);
  const vb = variance(b, mb);
  const se = Math.sqrt(va / na + vb / nb);
  if (se === 0 || !isFinite(se)) return { t: 0, df: 0 };
  const t = (ma - mb) / se;
  const num = (va / na + vb / nb) ** 2;
  const den = (va * va) / (na * na * (na - 1)) + (vb * vb) / (nb * nb * (nb - 1));
  const df = den > 0 ? num / den : 0;
  return { t, df };
}

/** Cohen's d with pooled SD (Hedges-style pooled variance). */
export function cohenD(a: number[], b: number[]): number {
  const na = a.length;
  const nb = b.length;
  if (na < 2 || nb < 2) return 0;
  const ma = mean(a);
  const mb = mean(b);
  const va = variance(a, ma);
  const vb = variance(b, mb);
  const pooled = Math.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2));
  if (pooled === 0 || !isFinite(pooled)) return 0;
  return (ma - mb) / pooled;
}

// Deterministic cheap PRNG — Mulberry32 seeded from sample sizes so bootstrap
// results are stable across renders without external libraries.
function mulberry32(seed: number) {
  let t = seed >>> 0;
  return () => {
    t = (t + 0x6d2b79f5) >>> 0;
    let r = t;
    r = Math.imul(r ^ (r >>> 15), r | 1);
    r ^= r + Math.imul(r ^ (r >>> 7), r | 61);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

/** Percentile-method bootstrap CI for Cohen's d. B defaults to 200 for speed. */
export function bootstrapCIofCohenD(
  a: number[],
  b: number[],
  B = 200,
  alpha = 0.05,
): [number, number] {
  if (a.length < 3 || b.length < 3) return [0, 0];
  const rnd = mulberry32(a.length * 1000 + b.length);
  const ds: number[] = [];
  for (let i = 0; i < B; i++) {
    const sa: number[] = new Array(a.length);
    const sb: number[] = new Array(b.length);
    for (let j = 0; j < a.length; j++) sa[j] = a[Math.floor(rnd() * a.length)]!;
    for (let j = 0; j < b.length; j++) sb[j] = b[Math.floor(rnd() * b.length)]!;
    ds.push(cohenD(sa, sb));
  }
  ds.sort((x, y) => x - y);
  const lo = ds[Math.max(0, Math.floor((alpha / 2) * B))] ?? 0;
  const hi = ds[Math.min(B - 1, Math.floor((1 - alpha / 2) * B))] ?? 0;
  return [lo, hi];
}

/** OLS slope of ys against x = 0..n-1. Returns 0 when n < 2. */
export function olsSlope(ys: number[]): number {
  const n = ys.length;
  if (n < 2) return 0;
  const xMean = (n - 1) / 2;
  const yMean = mean(ys);
  let num = 0;
  let den = 0;
  for (let i = 0; i < n; i++) {
    const dx = i - xMean;
    num += dx * (ys[i]! - yMean);
    den += dx * dx;
  }
  if (den === 0) return 0;
  return num / den;
}

/** Scale an array to [0, 1] using min–max; constant arrays become zeros. */
export function minMaxNormalize(xs: number[]): number[] {
  if (xs.length === 0) return [];
  let lo = Infinity;
  let hi = -Infinity;
  for (const x of xs) {
    if (!isFinite(x)) continue;
    if (x < lo) lo = x;
    if (x > hi) hi = x;
  }
  if (!isFinite(lo) || !isFinite(hi) || hi === lo) {
    return xs.map(() => 0);
  }
  const span = hi - lo;
  return xs.map((x) => (isFinite(x) ? (x - lo) / span : 0));
}
