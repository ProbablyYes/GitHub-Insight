"use client";

import {
  CartesianGrid,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { useEntityDrawer } from "@/components/entity";

type Point = {
  repoName: string;
  clusterId: number;
  pcaX: number;
  pcaY: number;
  healthScore: number;
  rankNo: number;
};

const COLORS = ["#33ff57", "#33ccff", "#ffcc00", "#ff3333"];
const DIM = "#333";

export function ClusterScatter({
  data,
  focusCluster,
}: {
  data: Point[];
  focusCluster?: number | null;
}) {
  const { open } = useEntityDrawer();
  const grouped = new Map<number, Point[]>();
  for (const p of data) {
    if (!grouped.has(p.clusterId)) grouped.set(p.clusterId, []);
    grouped.get(p.clusterId)!.push(p);
  }

  const series = Array.from(grouped.entries()).sort((a, b) => a[0] - b[0]);

  return (
    <div style={{ width: "100%", height: 360, minWidth: 0 }}>
      <ResponsiveContainer width="100%" height="100%">
        <ScatterChart margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
          <CartesianGrid stroke="#223" strokeDasharray="3 3" />
          <XAxis type="number" dataKey="pcaX" name="pcaX" tick={{ fill: "#9aa" }} />
          <YAxis type="number" dataKey="pcaY" name="pcaY" tick={{ fill: "#9aa" }} />
          <Tooltip
            cursor={{ strokeDasharray: "3 3" }}
            contentStyle={{
              borderRadius: 0,
              border: "2px solid #333",
              background: "#111",
              color: "#d4d4d4",
              fontFamily: '"Zpix", monospace',
              fontSize: 12,
            }}
            formatter={(_, __, item) => {
              const p = (item as { payload?: Point } | undefined)?.payload;
              if (!p) return ["", ""];
              return [`cluster ${p.clusterId} · health ${p.healthScore.toFixed(3)}`, shortRepo(p.repoName)];
            }}
          />
          {series.map(([clusterId, points], idx) => {
            const isDim =
              focusCluster != null && focusCluster !== clusterId;
            return (
              <Scatter
                key={clusterId}
                name={`cluster ${clusterId}`}
                data={points}
                fill={isDim ? DIM : COLORS[idx % COLORS.length]}
                fillOpacity={isDim ? 0.25 : 1}
                onClick={(payload: unknown) => {
                  const repo = (payload as { repoName?: string } | undefined)?.repoName;
                  if (repo) open({ type: "repo", id: repo });
                }}
                cursor="pointer"
              />
            );
          })}
        </ScatterChart>
      </ResponsiveContainer>
    </div>
  );
}

function shortRepo(repoName: string) {
  return repoName.length <= 28 ? repoName : `${repoName.slice(0, 25)}...`;
}
