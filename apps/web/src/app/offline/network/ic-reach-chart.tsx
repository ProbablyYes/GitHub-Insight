"use client";

import {
  CartesianGrid,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type IcReachRow = {
  k: number;
  strategy: string;
  expectedReach: number;
  reachStddev: number;
  simRuns: number;
};

const STRATEGY_LABEL: Record<string, string> = {
  greedy: "CELF-Greedy (IC-optimal)",
  top_degree: "Top-Degree",
  top_pagerank: "Top-PageRank",
  top_coreness: "Top-Coreness",
  random: "Random",
};

const STRATEGY_COLOR: Record<string, string> = {
  greedy: "var(--accent-success, #4caf50)",
  top_degree: "var(--accent-info, #42a5f5)",
  top_pagerank: "var(--accent-warning, #ffb300)",
  top_coreness: "var(--accent, #e91e63)",
  random: "var(--muted, #999)",
};

const STRATEGY_ORDER = [
  "greedy",
  "top_degree",
  "top_pagerank",
  "top_coreness",
  "random",
];

export function IcReachChart({
  rows,
  height = 320,
}: {
  rows: IcReachRow[];
  height?: number;
}) {
  if (rows.length === 0) {
    return (
      <div style={{ fontSize: 11, color: "var(--muted)" }}>
        (no IC reach data yet – run <code>network_ic.py</code>)
      </div>
    );
  }

  // pivot: {k: number, greedy: reach, top_degree: reach, ...}
  const ks = [...new Set(rows.map((r) => r.k))].sort((a, b) => a - b);
  const strategies = [...new Set(rows.map((r) => r.strategy))].sort(
    (a, b) => STRATEGY_ORDER.indexOf(a) - STRATEGY_ORDER.indexOf(b),
  );
  const byKey: Record<string, IcReachRow> = {};
  for (const r of rows) byKey[`${r.strategy}|${r.k}`] = r;

  const data = ks.map((k) => {
    const point: Record<string, number> = { k };
    for (const s of strategies) {
      const r = byKey[`${s}|${k}`];
      if (r) point[s] = r.expectedReach;
    }
    return point;
  });

  return (
    <div style={{ width: "100%", height }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 12, right: 20, left: 0, bottom: 8 }}>
          <CartesianGrid stroke="var(--border)" strokeDasharray="2 2" />
          <XAxis
            dataKey="k"
            type="number"
            domain={[1, ks[ks.length - 1]]}
            tick={{ fontSize: 10, fill: "var(--muted)" }}
            label={{ value: "#seeds (k)", fontSize: 10, fill: "var(--muted)", position: "insideBottom", dy: 8 }}
          />
          <YAxis
            tick={{ fontSize: 10, fill: "var(--muted)" }}
            label={{ value: "expected reach", angle: -90, fontSize: 10, fill: "var(--muted)", position: "insideLeft", dx: 10 }}
          />
          <Tooltip
            formatter={(value, name) => {
              const v = Number(value);
              const key = String(name);
              return [
                Number.isFinite(v) ? v.toFixed(1) : String(value),
                STRATEGY_LABEL[key] ?? key,
              ] as [string, string];
            }}
            labelFormatter={(l) => `k = ${l}`}
            contentStyle={{ fontSize: 11, background: "var(--bg)" }}
          />
          <Legend
            wrapperStyle={{ fontSize: 10 }}
            formatter={(value) => STRATEGY_LABEL[value] ?? value}
          />
          {strategies.map((s) => (
            <Line
              key={s}
              type="monotone"
              dataKey={s}
              stroke={STRATEGY_COLOR[s] ?? "var(--fg)"}
              strokeWidth={s === "greedy" ? 2.5 : 1.5}
              strokeDasharray={s === "random" ? "4 3" : undefined}
              dot={{ r: s === "greedy" ? 3.5 : 2.5 }}
              activeDot={{ r: 5 }}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
