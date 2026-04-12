"use client";

import { Bar, BarChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis } from "recharts";

type Point = { label: string; value: number };

export function MetricBarChart({
  data,
  color = "#33ccff",
}: {
  data: Point[];
  color?: string;
}) {
  return (
    <div style={{ width: "100%", height: 280 }}>
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} margin={{ top: 8, right: 12, left: -8, bottom: 0 }}>
          <CartesianGrid strokeDasharray="2 4" stroke="rgba(255,255,255,0.06)" />
          <XAxis
            dataKey="label"
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
            minTickGap={30}
          />
          <YAxis
            tick={{ fill: "#666", fontSize: 12, fontFamily: '"Zpix", monospace' }}
            axisLine={{ stroke: "#333" }}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              borderRadius: 0,
              border: "2px solid #333",
              background: "#111",
              color: "#d4d4d4",
              fontFamily: '"Zpix", monospace',
              fontSize: 13,
            }}
          />
          <Bar dataKey="value" fill={color} radius={0} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
