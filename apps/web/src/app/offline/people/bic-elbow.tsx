"use client";

import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import type { ActorPersonaBicPoint } from "@/lib/dashboard";

type Props = {
  rows: ActorPersonaBicPoint[];
};

export function BicElbowChart({ rows }: Props) {
  if (rows.length === 0) {
    return <p style={{ color: "var(--muted)", textAlign: "center" }}>No BIC sweep yet.</p>;
  }
  const sorted = [...rows].sort((a, b) => a.k - b.k);
  const minBic = Math.min(...sorted.map((r) => r.bic));
  const selected = sorted.find((r) => r.isSelected);
  return (
    <div>
      <div style={{ width: "100%", height: 220 }}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={sorted} margin={{ top: 10, right: 20, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.06)" />
            <XAxis
              dataKey="k"
              tick={{ fill: "#9aa", fontSize: 12, fontFamily: '"Zpix", monospace' }}
              axisLine={{ stroke: "#333" }}
              tickLine={false}
            />
            <YAxis
              tick={{ fill: "#9aa", fontSize: 11, fontFamily: '"Zpix", monospace' }}
              axisLine={{ stroke: "#333" }}
              tickLine={false}
              domain={[minBic * 0.98, "auto"]}
            />
            <Tooltip
              contentStyle={{
                borderRadius: 0,
                border: "2px solid #333",
                background: "#111",
                color: "#d4d4d4",
                fontFamily: '"Zpix", monospace',
                fontSize: 12,
              }}
              formatter={(value, name) => {
                const v = typeof value === "number" ? value : Number(value ?? 0);
                const n = typeof name === "string" ? name : String(name ?? "");
                return [v.toFixed(0), n];
              }}
              labelFormatter={(k) => `k = ${k}`}
            />
            <Line
              type="monotone"
              dataKey="bic"
              name="BIC"
              stroke="#a07bff"
              strokeWidth={2}
              dot={{ r: 3, fill: "#a07bff" }}
            />
            {selected ? (
              <ReferenceDot
                x={selected.k}
                y={selected.bic}
                r={8}
                fill="#33ff57"
                stroke="#0a0a0a"
                strokeWidth={2}
                label={{
                  value: `k*=${selected.k}`,
                  position: "top",
                  fill: "#33ff57",
                  fontSize: 11,
                  fontFamily: '"Zpix", monospace',
                }}
              />
            ) : null}
          </LineChart>
        </ResponsiveContainer>
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "repeat(auto-fit, minmax(110px, 1fr))",
          gap: 4,
          marginTop: 12,
          fontSize: 11,
          color: "var(--muted-strong)",
        }}
      >
        {sorted.map((r) => (
          <div
            key={r.k}
            style={{
              border: "1px solid rgba(255,255,255,0.06)",
              padding: "4px 6px",
              background: r.isSelected ? "rgba(51,255,87,0.1)" : undefined,
            }}
          >
            <div style={{ color: r.isSelected ? "var(--accent-positive)" : "var(--fg)" }}>
              k={r.k} {r.isSelected ? "★" : ""}
            </div>
            <div style={{ fontSize: 10 }}>BIC {r.bic.toFixed(0)}</div>
            <div style={{ fontSize: 10, color: "var(--muted)" }}>logL {r.logLikelihood.toFixed(0)}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
