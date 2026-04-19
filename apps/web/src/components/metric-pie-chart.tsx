"use client";

import { Cell, Pie, PieChart, ResponsiveContainer, Tooltip } from "recharts";

type Point = { label: string; value: number };

const COLORS = ["#33ff57", "#33ccff", "#ffcc00", "#ff3333"];

export function MetricPieChart({
  data,
  onSliceClick,
}: {
  data: Point[];
  onSliceClick?: (label: string) => void;
}) {
  return (
    <div style={{ width: "100%", height: 280, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
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
          <Pie
            data={data}
            dataKey="value"
            nameKey="label"
            innerRadius={55}
            outerRadius={90}
            paddingAngle={2}
            stroke="#0a0a0a"
            strokeWidth={2}
            onClick={(payload) => {
              const p = payload as unknown as {
                name?: string;
                label?: string;
                payload?: { label?: string };
              };
              const label = p?.name ?? p?.payload?.label ?? p?.label;
              if (typeof label === "string" && label.length > 0) {
                onSliceClick?.(label);
              }
            }}
          >
            {data.map((entry, index) => (
              <Cell key={entry.label} fill={COLORS[index % COLORS.length]} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}
