"use client";

import { useMemo, useState } from "react";
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

import type { RepoRankHistoryPoint } from "@/lib/dashboard";

type Props = {
  rows: RepoRankHistoryPoint[];
};

// 8 contrasty pixel-compatible colours.
const PALETTE = [
  "#33ccff",
  "#ff66cc",
  "#7cc57c",
  "#ffcc00",
  "#ff6b6b",
  "#b38cff",
  "#33ff99",
  "#ffa64d",
];

type Mode = "rank" | "score";

function shortRepo(name: string, max = 22): string {
  return name.length <= max ? name : `${name.slice(0, max - 1)}…`;
}

export function RankPathChart({ rows }: Props) {
  const [mode, setMode] = useState<Mode>("rank");

  const repos = useMemo(() => {
    const seen = new Set<string>();
    const order: string[] = [];
    for (const r of rows) {
      if (!r.repoName) continue;
      if (!seen.has(r.repoName)) {
        seen.add(r.repoName);
        order.push(r.repoName);
      }
    }
    // Order by the rank on the latest day so the legend reads top→bottom.
    if (rows.length === 0) return order;
    const latestDate = rows[rows.length - 1]!.metricDate;
    const latestMap = new Map<string, number>();
    for (const r of rows) {
      if (r.metricDate === latestDate) latestMap.set(r.repoName, r.rankNo);
    }
    return [...order].sort(
      (a, b) => (latestMap.get(a) ?? 9999) - (latestMap.get(b) ?? 9999),
    );
  }, [rows]);

  const data = useMemo(() => {
    const byDate = new Map<string, Record<string, number | string>>();
    for (const r of rows) {
      if (!byDate.has(r.metricDate)) {
        byDate.set(r.metricDate, { metricDate: r.metricDate });
      }
      const o = byDate.get(r.metricDate)!;
      o[r.repoName] = mode === "rank" ? r.rankNo : r.rankScore;
    }
    return [...byDate.values()].sort((a, b) =>
      (a.metricDate as string).localeCompare(b.metricDate as string),
    );
  }, [rows, mode]);

  if (rows.length === 0) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center", padding: 24 }}>
        No rank-history data yet. Run the Spark job.
      </p>
    );
  }

  const hasMultipleDays = new Set(rows.map((r) => r.metricDate)).size >= 2;

  return (
    <div>
      <div style={{ display: "flex", gap: 6, marginBottom: 8 }}>
        <button
          className={`nes-btn ${mode === "rank" ? "is-primary" : ""}`}
          type="button"
          onClick={() => setMode("rank")}
          style={{ fontSize: 10, padding: "4px 8px" }}
        >
          rank_no
        </button>
        <button
          className={`nes-btn ${mode === "score" ? "is-primary" : ""}`}
          type="button"
          onClick={() => setMode("score")}
          style={{ fontSize: 10, padding: "4px 8px" }}
        >
          rank_score
        </button>
        <span style={{ color: "var(--muted)", fontSize: 10, alignSelf: "center" }}>
          {hasMultipleDays
            ? `${repos.length} repos × ${new Set(rows.map((r) => r.metricDate)).size} days`
            : "single day snapshot"}
        </span>
      </div>
      <ResponsiveContainer width="100%" height={320}>
        <LineChart data={data} margin={{ top: 8, right: 24, bottom: 8, left: 8 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#30304a" />
          <XAxis
            dataKey="metricDate"
            tick={{ fontSize: 10 }}
            stroke="var(--muted-strong)"
            tickFormatter={(v: string) => v?.slice(5) || v}
          />
          <YAxis
            tick={{ fontSize: 10 }}
            stroke="var(--muted-strong)"
            reversed={mode === "rank"}
            allowDecimals={mode === "score"}
            label={{
              value: mode === "rank" ? "rank_no (lower=better)" : "rank_score",
              angle: -90,
              position: "insideLeft",
              fill: "var(--muted)",
              fontSize: 10,
            }}
          />
          <Tooltip
            contentStyle={{
              background: "#0a0a18",
              border: "1px solid var(--divider)",
              fontSize: 11,
            }}
            labelStyle={{ color: "var(--fg)" }}
            formatter={(value, name) => {
              const v = Number(value);
              return [
                mode === "rank" ? `#${v}` : v.toFixed(4),
                shortRepo(String(name ?? ""), 30),
              ];
            }}
          />
          <Legend
            verticalAlign="bottom"
            wrapperStyle={{ fontSize: 10 }}
            formatter={(value: string) => shortRepo(value, 26)}
          />
          {repos.map((repo, i) => (
            <Line
              key={`line-${i}-${repo}`}
              type="monotone"
              dataKey={repo}
              stroke={PALETTE[i % PALETTE.length]}
              strokeWidth={2}
              dot={{ r: 2 }}
              activeDot={{ r: 4 }}
              connectNulls
              isAnimationActive={false}
            />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
