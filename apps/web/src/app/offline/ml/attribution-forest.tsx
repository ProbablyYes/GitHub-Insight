"use client";

import { useMemo } from "react";

export type AttributionRow = {
  featureName: string;
  cohortScope: "all" | "humans_only" | "bots_only" | string;
  cohenD: number;
  cohensDLow: number;
  cohensDHigh: number;
  tStat: number;
  meanHot: number;
  meanCold: number;
  nHot: number;
  nCold: number;
  direction: string;
};

const SCOPE_ORDER = ["all", "humans_only", "bots_only"] as const;
const SCOPE_LABEL: Record<string, string> = {
  all: "all",
  humans_only: "humans",
  bots_only: "bots",
};
const SCOPE_COLOR: Record<string, string> = {
  all: "var(--accent-positive)",
  humans_only: "var(--accent-info)",
  bots_only: "var(--accent-change)",
};

/**
 * Classic forest plot: per feature we draw a horizontal error bar spanning
 * the Cohen's-d 95% bootstrap CI, with the point estimate as a dot. Having
 * three cohort scopes stacked per feature doubles as a robustness check —
 * if the "all" bar and the "humans_only" bar agree, the effect isn't just
 * driven by bot-heavy automation.
 *
 * Cohen's d thresholds (Cohen 1988):
 *   |d| < 0.2  negligible
 *   0.2 - 0.5  small
 *   0.5 - 0.8  medium
 *   >= 0.8     large
 */
export function AttributionForest({
  rows,
  maxFeatures = 10,
}: {
  rows: AttributionRow[];
  maxFeatures?: number;
}) {
  const { features, xMin, xMax } = useMemo(() => {
    const byFeature = new Map<string, AttributionRow[]>();
    for (const r of rows) {
      if (!byFeature.has(r.featureName)) byFeature.set(r.featureName, []);
      byFeature.get(r.featureName)!.push(r);
    }
    // rank features by |cohen_d| of the "all" scope, fallback to max
    const ranked = [...byFeature.entries()]
      .map(([name, list]) => {
        const all = list.find((x) => x.cohortScope === "all");
        const score = all ? Math.abs(all.cohenD) : Math.max(...list.map((x) => Math.abs(x.cohenD)));
        return { name, list, score };
      })
      .sort((a, b) => b.score - a.score)
      .slice(0, maxFeatures);

    let mn = 0;
    let mx = 0;
    for (const f of ranked) {
      for (const r of f.list) {
        mn = Math.min(mn, r.cohensDLow);
        mx = Math.max(mx, r.cohensDHigh);
      }
    }
    const pad = Math.max(0.2, (mx - mn) * 0.08);
    return { features: ranked, xMin: mn - pad, xMax: mx + pad };
  }, [rows, maxFeatures]);

  if (!features.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No attribution rows.</div>;
  }

  const W = 620;
  const ROW_H = 26;
  const LABEL_W = 130;
  const AXIS_H = 18;
  const PLOT_W = W - LABEL_W - 16;

  const xScale = (d: number) => LABEL_W + ((d - xMin) / (xMax - xMin)) * PLOT_W;

  const refLines = [-0.8, -0.5, -0.2, 0, 0.2, 0.5, 0.8].filter((v) => v > xMin && v < xMax);

  const H = features.length * ROW_H * 3 + AXIS_H + 20;

  return (
    <div style={{ overflowX: "auto" }}>
      <svg width={W} height={H} style={{ fontSize: 10, fontFamily: "var(--font-mono, monospace)" }}>
        {/* x-axis band */}
        <g transform={`translate(0, ${H - AXIS_H})`}>
          <line x1={LABEL_W} y1={0} x2={W - 4} y2={0} stroke="var(--muted)" strokeWidth={1} />
          {refLines.map((v) => (
            <g key={v}>
              <line
                x1={xScale(v)}
                x2={xScale(v)}
                y1={-H + AXIS_H + 10}
                y2={0}
                stroke={v === 0 ? "var(--muted)" : "var(--pixel-border)"}
                strokeWidth={v === 0 ? 1 : 0.5}
                strokeDasharray={v === 0 ? "" : "2 3"}
              />
              <text x={xScale(v)} y={12} fill={v === 0 ? "var(--muted)" : "var(--pixel-border)"} textAnchor="middle" fontSize={9}>
                {v === 0 ? "0" : v.toFixed(1)}
              </text>
            </g>
          ))}
          <text x={W - 4} y={12} fill="var(--muted)" textAnchor="end" fontSize={9}>
            Cohen&apos;s d
          </text>
        </g>

        {/* Cohen threshold band labels at top */}
        <g transform="translate(0, 12)">
          {["small (0.2)", "medium (0.5)", "large (0.8)"].map((lbl, i) => {
            const dval = [0.2, 0.5, 0.8][i]!;
            if (dval <= xMin || dval >= xMax) return null;
            return (
              <text key={lbl} x={xScale(dval)} y={0} fill="var(--muted)" textAnchor="middle" fontSize={9}>
                {lbl}
              </text>
            );
          })}
        </g>

        {/* feature rows */}
        {features.map((f, i) => {
          const baseY = 20 + i * ROW_H * 3;
          return (
            <g key={f.name}>
              <text
                x={LABEL_W - 6}
                y={baseY + ROW_H * 1.2}
                textAnchor="end"
                fill="var(--fg)"
                fontSize={10}
              >
                {f.name}
              </text>
              {SCOPE_ORDER.map((scope, scopeIdx) => {
                const row = f.list.find((r) => r.cohortScope === scope);
                if (!row) return null;
                const y = baseY + scopeIdx * (ROW_H - 2) + 8;
                const x0 = xScale(row.cohensDLow);
                const x1 = xScale(row.cohensDHigh);
                const cx = xScale(row.cohenD);
                const crossZero = row.cohensDLow <= 0 && row.cohensDHigh >= 0;
                return (
                  <g key={`${f.name}-${scope}`} opacity={crossZero ? 0.45 : 1}>
                    {/* CI bar */}
                    <line
                      x1={x0}
                      x2={x1}
                      y1={y}
                      y2={y}
                      stroke={SCOPE_COLOR[scope]}
                      strokeWidth={2}
                    />
                    <line x1={x0} x2={x0} y1={y - 4} y2={y + 4} stroke={SCOPE_COLOR[scope]} strokeWidth={2} />
                    <line x1={x1} x2={x1} y1={y - 4} y2={y + 4} stroke={SCOPE_COLOR[scope]} strokeWidth={2} />
                    <circle cx={cx} cy={y} r={3} fill={SCOPE_COLOR[scope]} stroke="var(--bg)" strokeWidth={1} />
                    {/* scope tag + d value */}
                    <text x={x1 + 4} y={y + 3} fill={SCOPE_COLOR[scope]} fontSize={9}>
                      {SCOPE_LABEL[scope]} d={row.cohenD.toFixed(2)}
                    </text>
                  </g>
                );
              })}
              {/* separator */}
              {i < features.length - 1 ? (
                <line
                  x1={4}
                  x2={W - 4}
                  y1={baseY + ROW_H * 3 - 2}
                  y2={baseY + ROW_H * 3 - 2}
                  stroke="var(--pixel-border)"
                  strokeDasharray="1 3"
                />
              ) : null}
            </g>
          );
        })}
      </svg>
      <div style={{ display: "flex", gap: 14, marginTop: 4, fontSize: 10, color: "var(--muted)" }}>
        {SCOPE_ORDER.map((s) => (
          <span key={s}>
            <span style={{ color: SCOPE_COLOR[s], fontWeight: 700 }}>■</span> {SCOPE_LABEL[s]}
          </span>
        ))}
        <span style={{ marginLeft: "auto", opacity: 0.8 }}>
          faded = 95% CI crosses 0 (not statistically distinguishable)
        </span>
      </div>
    </div>
  );
}
