"use client";

import { useMemo } from "react";
import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
} from "recharts";

export type ArchetypePoint = {
  repoName: string;
  archetype: string;
  pc1: number;
  pc2: number;
  starsLog: number;
  authorRatio: number;
  retention: number;
  issueRatio: number;
};

export const ARCHETYPE_COLORS: Record<string, string> = {
  star_hungry_showcase: "var(--accent-magenta)",
  community_driven: "var(--accent-info)",
  pr_heavy_collab: "var(--accent-positive)",
  steady_core: "var(--accent-purple)",
  burst_factory: "var(--accent-change)",
  misc: "var(--muted)",
};

export const ARCHETYPE_LABEL: Record<string, string> = {
  star_hungry_showcase: "Star-hungry Showcase",
  community_driven: "Community Driven",
  pr_heavy_collab: "PR-heavy Collab",
  steady_core: "Steady Core",
  burst_factory: "Burst Factory",
  misc: "Misc",
};

export const ARCHETYPE_DESCRIPTION: Record<string, string> = {
  star_hungry_showcase:
    "Watch-dominated repos (watch_share >= 0.6, pr_push_ratio < 0.5) -- hype gets stars but little actual code activity.",
  community_driven:
    "Watcher-heavy with moderate contribution -- live projects where stargazers actually contribute too.",
  pr_heavy_collab:
    "PR/push-dominant (pr_push_ratio >= 2) -- small but active engineering teams collaborating daily.",
  steady_core:
    "Balanced activity across watch/PR/push, low bot ratio -- the 'quiet workhorses' of the ecosystem.",
  burst_factory:
    "High event volume in < 5 active days -- one-shot migrations, release dumps, CI explosions.",
  misc: "No single feature dominates -- outliers that don't cleanly fit any archetype.",
};

export function ArchetypeScatter({
  points,
  height = 340,
}: {
  points: ArchetypePoint[];
  height?: number;
}) {
  const grouped = useMemo(() => {
    const m = new Map<string, ArchetypePoint[]>();
    for (const p of points) {
      const k = p.archetype || "mixed";
      if (!m.has(k)) m.set(k, []);
      m.get(k)!.push(p);
    }
    return [...m.entries()].sort((a, b) => b[1].length - a[1].length);
  }, [points]);

  if (!points.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No archetype data.</div>;
  }

  return (
    <div>
      <ResponsiveContainer width="100%" height={height}>
        <ScatterChart margin={{ top: 12, right: 20, bottom: 20, left: 12 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="var(--pixel-border)" />
          <XAxis
            type="number"
            dataKey="pc1"
            stroke="var(--muted)"
            tick={{ fontSize: 10 }}
            label={{ value: "PC1 (popularity —dev activity)", position: "insideBottom", offset: -8, fontSize: 10, fill: "var(--muted)" }}
          />
          <YAxis
            type="number"
            dataKey="pc2"
            stroke="var(--muted)"
            tick={{ fontSize: 10 }}
            label={{ value: "PC2 (niche —support load)", angle: -90, position: "insideLeft", offset: 4, fontSize: 10, fill: "var(--muted)" }}
          />
          <ZAxis type="number" dataKey="starsLog" range={[24, 260]} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            contentStyle={{
              background: "var(--surface-elevated)",
              border: "1px solid var(--pixel-border)",
              fontSize: 11,
            }}
            labelFormatter={() => ""}
            content={({ active, payload }) => {
              if (!active || !payload?.length) return null;
              const d = payload[0].payload as ArchetypePoint;
              return (
                <div
                  style={{
                    background: "var(--surface-elevated)",
                    border: "1px solid var(--pixel-border)",
                    fontSize: 11,
                    padding: 8,
                    minWidth: 200,
                  }}
                >
                  <div style={{ color: ARCHETYPE_COLORS[d.archetype] ?? "var(--fg)", fontWeight: 700, marginBottom: 4 }}>
                    {ARCHETYPE_LABEL[d.archetype] ?? d.archetype}
                  </div>
                  <div style={{ color: "var(--fg)" }}>{d.repoName}</div>
                  <div style={{ color: "var(--muted)", marginTop: 4 }}>
                    stars: {Math.round(Math.expm1(d.starsLog))} · retention: {d.retention.toFixed(2)}
                  </div>
                  <div style={{ color: "var(--muted)" }}>
                    author-ratio: {d.authorRatio.toFixed(2)} · issue-ratio: {d.issueRatio.toFixed(2)}
                  </div>
                </div>
              );
            }}
          />
          {grouped.map(([arch, pts]) => (
            <Scatter
              key={arch}
              name={ARCHETYPE_LABEL[arch] ?? arch}
              data={pts}
              fill={ARCHETYPE_COLORS[arch] ?? "var(--fg)"}
              fillOpacity={0.8}
            />
          ))}
        </ScatterChart>
      </ResponsiveContainer>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 10, marginTop: 8, fontSize: 10 }}>
        {grouped.map(([arch, pts]) => (
          <span key={arch} style={{ color: ARCHETYPE_COLORS[arch] ?? "var(--fg)" }}>
            —{ARCHETYPE_LABEL[arch] ?? arch} ({pts.length})
          </span>
        ))}
      </div>
    </div>
  );
}

