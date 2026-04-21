"use client";

import {
  CartesianGrid,
  ComposedChart,
  Dot,
  Legend,
  Line,
  ReferenceDot,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

export type BicPoint = {
  k: number;
  bic: number;
  logLikelihood: number;
  nParams: number;
  isSelected: number;
};

/**
 * Plots the Bayesian Information Criterion (BIC) for a range of candidate k
 * values used when picking the GMM model. We over-plot log-likelihood on a
 * secondary axis so the reader can see both: likelihood rises forever with k,
 * but BIC — log-likelihood penalised by n_params · log(n_samples) — picks the
 * smallest value that isn't just memorising the training set.
 *
 *   BIC = k·ln(n) − 2·ln(L)
 *
 * Lower BIC = better. The dip in the curve IS the story.
 */
export function BicSweepChart({
  data,
  height = 260,
}: {
  data: BicPoint[];
  height?: number;
}) {
  if (!data.length) return <div style={{ color: "var(--muted)", fontSize: 11 }}>No BIC data.</div>;

  const selected = data.find((d) => d.isSelected === 1) ?? data.reduce((a, b) => (a.bic < b.bic ? a : b));
  const minBic = Math.min(...data.map((d) => d.bic));
  const maxBic = Math.max(...data.map((d) => d.bic));
  const padBic = Math.max(1, (maxBic - minBic) * 0.1);

  return (
    <ResponsiveContainer width="100%" height={height}>
      <ComposedChart data={data} margin={{ top: 10, right: 30, bottom: 14, left: 4 }}>
        <CartesianGrid strokeDasharray="2 4" stroke="var(--pixel-border)" />
        <XAxis
          dataKey="k"
          stroke="var(--muted)"
          tick={{ fontSize: 10 }}
          label={{ value: "k (number of clusters)", position: "insideBottom", offset: -4, fontSize: 10, fill: "var(--muted)" }}
        />
        <YAxis
          yAxisId="bic"
          orientation="left"
          stroke="var(--accent-magenta)"
          tick={{ fontSize: 10, fill: "var(--accent-magenta)" }}
          domain={[minBic - padBic, maxBic + padBic]}
          tickFormatter={(v) => (Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toFixed(0))}
          label={{ value: "BIC (lower = better)", angle: -90, position: "insideLeft", fontSize: 10, fill: "var(--accent-magenta)" }}
        />
        <YAxis
          yAxisId="ll"
          orientation="right"
          stroke="var(--accent-info)"
          tick={{ fontSize: 10, fill: "var(--accent-info)" }}
          tickFormatter={(v) => (Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(0)}k` : v.toFixed(0))}
          label={{ value: "log-likelihood", angle: 90, position: "insideRight", fontSize: 10, fill: "var(--accent-info)" }}
        />
        <Tooltip
          contentStyle={{
            background: "var(--surface-elevated)",
            border: "1px solid var(--pixel-border)",
            fontSize: 11,
          }}
          labelFormatter={(l) => `k = ${l}`}
          formatter={(v, n) => {
            const num = Number(v);
            const pretty = Math.abs(num) >= 1000 ? `${(num / 1000).toFixed(1)}k` : num.toFixed(1);
            return [pretty, String(n)];
          }}
        />
        <Legend wrapperStyle={{ fontSize: 10 }} />
        <Line
          yAxisId="bic"
          type="monotone"
          dataKey="bic"
          name="BIC"
          stroke="var(--accent-magenta)"
          strokeWidth={2}
          dot={(props) => {
            const { cx, cy, payload } = props as { cx: number; cy: number; payload: BicPoint };
            const isSel = payload.isSelected === 1;
            return (
              <Dot
                key={`bic-${payload.k}`}
                cx={cx}
                cy={cy}
                r={isSel ? 6 : 3}
                fill={isSel ? "var(--accent-positive)" : "var(--accent-magenta)"}
                stroke="var(--bg)"
                strokeWidth={isSel ? 2 : 0}
              />
            );
          }}
        />
        <Line
          yAxisId="ll"
          type="monotone"
          dataKey="logLikelihood"
          name="log-likelihood"
          stroke="var(--accent-info)"
          strokeWidth={1}
          strokeDasharray="3 3"
          dot={false}
        />
        <ReferenceDot
          yAxisId="bic"
          x={selected.k}
          y={selected.bic}
          r={0}
          label={{
            value: `selected k=${selected.k}`,
            position: "top",
            fontSize: 10,
            fill: "var(--accent-positive)",
          }}
        />
      </ComposedChart>
    </ResponsiveContainer>
  );
}
