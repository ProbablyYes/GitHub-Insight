"use client";

import {
  CartesianGrid,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
  ZAxis,
  ResponsiveContainer,
} from "recharts";

import type { RepoAssociationRulePoint } from "@/lib/dashboard";

type Props = {
  rules: RepoAssociationRulePoint[];
};

export function RulesParetoScatter({ rules }: Props) {
  if (rules.length === 0) {
    return <p style={{ color: "var(--muted)", textAlign: "center" }}>No rules.</p>;
  }
  const frontier = rules
    .filter((r) => r.isFrontier === 1)
    .map((r) => ({
      support: r.support,
      confidence: r.confidence,
      lift: r.lift,
      label: `${r.antecedent} ⇒ ${r.consequent}`,
    }));
  const rest = rules
    .filter((r) => r.isFrontier !== 1)
    .map((r) => ({
      support: r.support,
      confidence: r.confidence,
      lift: r.lift,
      label: `${r.antecedent} ⇒ ${r.consequent}`,
    }));

  return (
    <div>
      <div style={{ width: "100%", height: 300 }}>
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 10, right: 20, left: 10, bottom: 20 }}>
            <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.06)" />
            <XAxis
              type="number"
              dataKey="support"
              name="support"
              tick={{ fill: "#9aa", fontSize: 11, fontFamily: '"Zpix", monospace' }}
              axisLine={{ stroke: "#333" }}
              tickLine={false}
              tickFormatter={(v) => `${(Number(v) * 100).toFixed(1)}%`}
              label={{
                value: "support",
                position: "insideBottom",
                offset: -8,
                fill: "#9aa",
                fontSize: 11,
                fontFamily: '"Zpix", monospace',
              }}
            />
            <YAxis
              type="number"
              dataKey="confidence"
              name="confidence"
              tick={{ fill: "#9aa", fontSize: 11, fontFamily: '"Zpix", monospace' }}
              axisLine={{ stroke: "#333" }}
              tickLine={false}
              tickFormatter={(v) => `${(Number(v) * 100).toFixed(0)}%`}
              label={{
                value: "confidence",
                angle: -90,
                position: "insideLeft",
                offset: 10,
                fill: "#9aa",
                fontSize: 11,
                fontFamily: '"Zpix", monospace',
              }}
            />
            <ZAxis type="number" dataKey="lift" range={[30, 320]} name="lift" />
            <Tooltip
              contentStyle={{
                borderRadius: 0,
                border: "2px solid #333",
                background: "#111",
                color: "#d4d4d4",
                fontFamily: '"Zpix", monospace',
                fontSize: 11,
              }}
              formatter={(value, name) => {
                const v = typeof value === "number" ? value : Number(value ?? 0);
                const n = typeof name === "string" ? name : String(name ?? "");
                if (n === "support" || n === "confidence") {
                  return [`${(v * 100).toFixed(2)}%`, n];
                }
                if (n === "lift") return [`${v.toFixed(2)}×`, n];
                return [String(value), n];
              }}
              labelFormatter={(_, payload) => {
                const datum = payload && payload[0] ? (payload[0].payload as { label?: string }) : null;
                return datum?.label ?? "";
              }}
            />
            <Scatter name="dominated" data={rest} fill="#555" shape="circle" />
            <Scatter name="Pareto frontier" data={frontier} fill="#ff66cc" shape="diamond" />
          </ScatterChart>
        </ResponsiveContainer>
      </div>
      <div style={{ display: "flex", gap: 16, marginTop: 8, fontSize: 11, color: "var(--muted-strong)" }}>
        <span>
          <span
            style={{
              display: "inline-block",
              width: 10,
              height: 10,
              background: "#ff66cc",
              marginRight: 4,
              verticalAlign: "middle",
            }}
          />
          Pareto frontier ({frontier.length})
        </span>
        <span>
          <span
            style={{
              display: "inline-block",
              width: 10,
              height: 10,
              background: "#555",
              marginRight: 4,
              verticalAlign: "middle",
            }}
          />
          dominated ({rest.length})
        </span>
        <span>Dot size ∝ lift</span>
      </div>
    </div>
  );
}
