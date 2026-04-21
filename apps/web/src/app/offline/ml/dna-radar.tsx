"use client";

import { useMemo } from "react";

import type { RepoDnaPoint } from "@/lib/dashboard";

/** Eight dimensions chosen to stay on comparable [0, 1] scales. */
const DIMENSIONS: { key: keyof RepoDnaPoint; label: string }[] = [
  { key: "watchShare", label: "watch" },
  { key: "forkShare", label: "fork" },
  { key: "prShare", label: "pr" },
  { key: "pushShare", label: "push" },
  { key: "issuesShare", label: "issue" },
  { key: "botRatio", label: "bot" },
  { key: "nightRatio", label: "night" },
  { key: "activeDaysRatio", label: "active-d" },
];

/**
 * A minimal 8-axis radar for comparing the mean hot-cohort DNA vs the mean
 * cold-cohort DNA. We plot cohort means, not individual repos, so a single
 * outlier can't skew the shape. The actual repos are listed below the chart
 * for context.
 */
export function DnaRadar({
  rows,
  height = 320,
}: {
  rows: RepoDnaPoint[];
  height?: number;
}) {
  const { hotMean, coldMean, hotRepos, coldRepos } = useMemo(() => {
    const hot = rows.filter((r) => r.cohortGroup === "hot");
    const cold = rows.filter((r) => r.cohortGroup === "cold");
    const mean = (list: RepoDnaPoint[]) =>
      DIMENSIONS.reduce<Record<string, number>>((acc, dim) => {
        acc[dim.key as string] = list.length
          ? list.reduce((a, r) => a + (Number(r[dim.key]) || 0), 0) / list.length
          : 0;
        return acc;
      }, {});
    return {
      hotMean: mean(hot),
      coldMean: mean(cold),
      hotRepos: hot,
      coldRepos: cold,
    };
  }, [rows]);

  if (!rows.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No repo-DNA data.</div>;
  }

  const SIZE = height;
  const cx = SIZE / 2;
  const cy = SIZE / 2;
  const r = SIZE * 0.38;
  const n = DIMENSIONS.length;

  const axisPoint = (i: number, frac: number) => {
    const angle = (i / n) * 2 * Math.PI - Math.PI / 2;
    return [cx + Math.cos(angle) * r * frac, cy + Math.sin(angle) * r * frac];
  };

  const path = (means: Record<string, number>) =>
    DIMENSIONS.map((dim, i) => {
      const v = Math.min(1, Math.max(0, Number(means[dim.key as string]) || 0));
      const [x, y] = axisPoint(i, v);
      return `${i === 0 ? "M" : "L"}${x.toFixed(1)},${y.toFixed(1)}`;
    }).join(" ") + " Z";

  return (
    <div style={{ display: "grid", gridTemplateColumns: `${SIZE}px 1fr`, gap: 16, alignItems: "start" }}>
      <svg width={SIZE} height={SIZE} style={{ fontSize: 10 }}>
        {/* rings */}
        {[0.25, 0.5, 0.75, 1].map((frac) => (
          <circle
            key={frac}
            cx={cx}
            cy={cy}
            r={r * frac}
            fill="none"
            stroke="var(--pixel-border)"
            strokeDasharray={frac === 1 ? "" : "2 3"}
            strokeWidth={0.7}
          />
        ))}
        {/* axes + labels */}
        {DIMENSIONS.map((dim, i) => {
          const [x, y] = axisPoint(i, 1);
          const [lx, ly] = axisPoint(i, 1.12);
          return (
            <g key={dim.label}>
              <line x1={cx} y1={cy} x2={x} y2={y} stroke="var(--pixel-border)" strokeWidth={0.5} />
              <text
                x={lx}
                y={ly}
                fill="var(--muted-strong)"
                fontSize={10}
                textAnchor={Math.abs(lx - cx) < 6 ? "middle" : lx < cx ? "end" : "start"}
                dominantBaseline={ly < cy ? "alphabetic" : "hanging"}
              >
                {dim.label}
              </text>
            </g>
          );
        })}
        {/* cold polygon */}
        <path d={path(coldMean)} fill="var(--accent-info)" fillOpacity={0.18} stroke="var(--accent-info)" strokeWidth={1.5} />
        {/* hot polygon */}
        <path d={path(hotMean)} fill="var(--accent-magenta)" fillOpacity={0.22} stroke="var(--accent-magenta)" strokeWidth={1.5} />
        {/* legend */}
        <g transform={`translate(6, 6)`}>
          <rect x={0} y={0} width={10} height={10} fill="var(--accent-magenta)" opacity={0.85} />
          <text x={14} y={9} fill="var(--accent-magenta)" fontSize={10}>hot</text>
          <rect x={0} y={14} width={10} height={10} fill="var(--accent-info)" opacity={0.85} />
          <text x={14} y={23} fill="var(--accent-info)" fontSize={10}>cold</text>
        </g>
      </svg>
      <div style={{ fontSize: 11, color: "var(--muted-strong)", lineHeight: 1.5 }}>
        <div style={{ color: "var(--accent-magenta)", fontWeight: 700, marginBottom: 2 }}>
          hot · {hotRepos.length} top repos
        </div>
        <div style={{ marginBottom: 10, wordBreak: "break-word" }}>
          {hotRepos.map((r, i) => (
            <span key={r.repoName}>
              {i > 0 ? ", " : ""}
              <span style={{ color: "var(--fg)" }}>{r.repoName}</span>
            </span>
          ))}
        </div>
        <div style={{ color: "var(--accent-info)", fontWeight: 700, marginBottom: 2 }}>
          cold · {coldRepos.length} bottom repos
        </div>
        <div style={{ marginBottom: 10, wordBreak: "break-word" }}>
          {coldRepos.map((r, i) => (
            <span key={r.repoName}>
              {i > 0 ? ", " : ""}
              <span style={{ color: "var(--fg)" }}>{r.repoName}</span>
            </span>
          ))}
        </div>
        <div style={{ fontSize: 10, color: "var(--muted)", lineHeight: 1.45, marginTop: 8 }}>
          Rings at 25% / 50% / 75% / 100%. Every axis is a [0,1] share except{" "}
          <em>active-d</em> (fraction of 30 days active). A bigger polygon on an axis = the
          cohort&apos;s <em>mean</em> is higher on that feature.
        </div>
      </div>
    </div>
  );
}
