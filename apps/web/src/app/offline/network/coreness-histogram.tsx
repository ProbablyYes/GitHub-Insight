"use client";

import { useMemo, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type CorenessBucket = {
  coreness: number;
  count: number;
};

export function CorenessHistogram({
  buckets,
  color = "var(--accent-info)",
  median,
  height = 220,
  includeZero = false,
}: {
  buckets: CorenessBucket[];
  color?: string;
  median?: number;
  height?: number;
  /** if false (default), drop k=0 (isolated nodes) from the chart so the tail
   *  is actually readable — we surface the isolated count as a caption instead. */
  includeZero?: boolean;
}) {
  const [scale, setScale] = useState<"linear" | "log">("linear");

  const zeroCount = useMemo(
    () => buckets.find((b) => b.coreness === 0)?.count ?? 0,
    [buckets],
  );
  const data = useMemo(
    () => buckets.filter((b) => includeZero || b.coreness > 0),
    [buckets, includeZero],
  );
  const totalShown = data.reduce((a, b) => a + b.count, 0);
  const maxBar = Math.max(1, ...data.map((b) => b.count));

  // log scale domain — recharts doesn't do log axes for 0 values gracefully,
  // so we manually compute ticks and feed scale="log"
  const maxK = data.reduce((m, b) => Math.max(m, b.coreness), 0);

  if (!buckets.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No coreness data.</div>;
  }
  if (!data.length) {
    return (
      <div style={{ color: "var(--muted)", fontSize: 11 }}>
        All nodes are isolated (k = 0). Graph has no edges at this threshold.
      </div>
    );
  }

  return (
    <div>
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          fontSize: 10,
          color: "var(--muted)",
          marginBottom: 4,
        }}
      >
        <span>
          {totalShown.toLocaleString()} nodes with k &ge; 1
          {zeroCount > 0 && !includeZero ? (
            <>
              {" · "}
              <span style={{ color: "var(--muted-strong)" }}>
                {zeroCount.toLocaleString()} isolated (k = 0, hidden)
              </span>
            </>
          ) : null}
        </span>
        <div style={{ display: "inline-flex", gap: 4 }}>
          {(["linear", "log"] as const).map((s) => (
            <button
              key={s}
              type="button"
              onClick={() => setScale(s)}
              className="nes-btn"
              style={{
                padding: "2px 8px",
                fontSize: 9,
                color: scale === s ? "var(--bg)" : "var(--muted-strong)",
                background: scale === s ? color : "transparent",
                borderColor: color,
                cursor: "pointer",
              }}
            >
              {s === "linear" ? "linear" : "log"}
            </button>
          ))}
        </div>
      </div>

      <ResponsiveContainer width="100%" height={height}>
        <BarChart data={data} margin={{ top: 10, right: 16, bottom: 18, left: 4 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="var(--pixel-border)" />
          <XAxis
            dataKey="coreness"
            stroke="var(--muted)"
            tick={{ fontSize: 10 }}
            label={{ value: "k (coreness)", position: "insideBottom", offset: -8, fontSize: 10, fill: "var(--muted)" }}
          />
          <YAxis
            stroke="var(--muted)"
            tick={{ fontSize: 10 }}
            allowDecimals={false}
            scale={scale === "log" ? "log" : "linear"}
            domain={scale === "log" ? [1, "auto"] : [0, "auto"]}
            allowDataOverflow={scale === "log"}
            label={{ value: scale === "log" ? "nodes (log)" : "nodes", angle: -90, position: "insideLeft", fontSize: 10, fill: "var(--muted)" }}
          />
          <Tooltip
            contentStyle={{
              background: "var(--surface-elevated)",
              border: "1px solid var(--pixel-border)",
              fontSize: 11,
            }}
            formatter={(v, _name, payload) => {
              const n = Number(v);
              const share = totalShown > 0 ? ((n / totalShown) * 100).toFixed(1) : "0";
              return [`${n.toLocaleString()} (${share}%)`, "nodes"];
            }}
            labelFormatter={(l) => `k = ${l}${l === maxK ? " (inner core)" : ""}`}
          />
          {typeof median === "number" && median > 0 ? (
            <ReferenceLine
              x={median}
              stroke="var(--accent-change)"
              strokeDasharray="4 2"
              label={{ value: `median k=${median}`, fill: "var(--accent-change)", fontSize: 10 }}
            />
          ) : null}
          <Bar dataKey="count" fill={color}>
            {data.map((b) => (
              <Cell
                key={`c-${b.coreness}`}
                fill={b.coreness === maxK ? "var(--accent-magenta)" : color}
              />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>

      <div style={{ marginTop: 6, fontSize: 10, color: "var(--muted)", lineHeight: 1.4 }}>
        <strong style={{ color: "var(--muted-strong)" }}>How to read:</strong> each bar
        counts how many nodes sit in a k-core of exactly that depth. Higher k = a denser,
        more tightly-wound neighbourhood. The magenta bar marks the <em>inner core</em>
        (highest k observed) — these are the structurally most-central nodes. k = 0 (isolated
        in this graph) is hidden by default; toggle <code>log</code> to see the long tail.
      </div>
    </div>
  );
}
