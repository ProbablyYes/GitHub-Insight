// Authenticity composite: fuse 4 orthogonal "suspicion" signals into a single
// interpretable 0..1 score and tag the dominant signal per repo.
//
// Signals (higher = more suspicious):
//   alertZ        — offline anomaly z-score magnitude (volume spike).
//   dnaOutlierZ   — Mahalanobis-style z-distance from hot-cohort DNA mean.
//   watcherBot    — fraction of a repo's watchers classified as bots.
//   communityBot  — fraction of bots in the similarity community the repo sits in.
//
// Approach: per-signal min–max normalise across the input batch, average the
// four normalised values (equal weights — simpler to explain than PCA), then
// pick argmax over the normalised vector as the dominant suspicion driver.

import { minMaxNormalize } from "@/lib/stats";

import type { AuthenticityInputRow } from "@/lib/dashboard";

export type AuthenticitySignalKey =
  | "alertZ"
  | "dnaOutlierZ"
  | "watcherBot"
  | "communityBot";

export const SIGNAL_LABEL: Record<AuthenticitySignalKey, string> = {
  alertZ: "volume anomaly",
  dnaOutlierZ: "DNA outlier",
  watcherBot: "watcher bots",
  communityBot: "community bots",
};

export type AuthenticityRow = {
  repoName: string;
  watchers: number;
  raw: Record<AuthenticitySignalKey, number>;
  normalized: Record<AuthenticitySignalKey, number>;
  composite: number;
  dominantSignal: AuthenticitySignalKey;
};

const SIGNAL_KEYS: AuthenticitySignalKey[] = [
  "alertZ",
  "dnaOutlierZ",
  "watcherBot",
  "communityBot",
];

export function computeAuthenticity(
  rows: AuthenticityInputRow[],
): AuthenticityRow[] {
  if (rows.length === 0) return [];

  // Per-signal min-max normalisation across the batch.
  const normalized: Record<AuthenticitySignalKey, number[]> = {
    alertZ: minMaxNormalize(rows.map((r) => r.alertZ)),
    dnaOutlierZ: minMaxNormalize(rows.map((r) => r.dnaOutlierZ)),
    watcherBot: minMaxNormalize(rows.map((r) => r.watcherBot)),
    communityBot: minMaxNormalize(rows.map((r) => r.communityBot)),
  };

  const out: AuthenticityRow[] = [];
  for (let i = 0; i < rows.length; i++) {
    const row = rows[i]!;
    const nObj: Record<AuthenticitySignalKey, number> = {
      alertZ: normalized.alertZ[i] ?? 0,
      dnaOutlierZ: normalized.dnaOutlierZ[i] ?? 0,
      watcherBot: normalized.watcherBot[i] ?? 0,
      communityBot: normalized.communityBot[i] ?? 0,
    };
    const composite =
      (nObj.alertZ + nObj.dnaOutlierZ + nObj.watcherBot + nObj.communityBot) /
      SIGNAL_KEYS.length;
    let dominant: AuthenticitySignalKey = "alertZ";
    let best = -1;
    for (const key of SIGNAL_KEYS) {
      if (nObj[key] > best) {
        best = nObj[key];
        dominant = key;
      }
    }
    out.push({
      repoName: row.repoName,
      watchers: row.watchers,
      raw: {
        alertZ: row.alertZ,
        dnaOutlierZ: row.dnaOutlierZ,
        watcherBot: row.watcherBot,
        communityBot: row.communityBot,
      },
      normalized: nObj,
      composite,
      dominantSignal: dominant,
    });
  }
  out.sort((a, b) => b.composite - a.composite);
  return out;
}
