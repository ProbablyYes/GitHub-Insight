"use client";

import { useMemo } from "react";

export type WeeklyPoint = {
  weekIdx: number;
  weekStart: string;
  weekEnd: string;
  repoName: string;
  communityId: string;
  communitySize: number;
};

export type LineagePoint = {
  weekIdx: number;
  communityId: string;
  prevCommunityId: string;
  eventType: string;
  membersCount: number;
  overlapJaccard: number;
};

const EVENT_COLOR: Record<string, string> = {
  continue: "var(--accent-positive)",
  merge_or_split: "var(--accent-change)",
  birth: "var(--accent-info)",
  death: "var(--accent-danger)",
  reform: "var(--accent-magenta)",
};

type Node = {
  id: string;
  week: number;
  size: number;
  x: number;
  y: number;
  h: number;
};

type Flow = {
  from: string;
  to: string;
  weight: number;
  eventType: string;
};

export function CommunitySankey({
  weekly,
  lineage,
  width = 900,
  height = 420,
  minSize = 2,
}: {
  weekly: WeeklyPoint[];
  lineage: LineagePoint[];
  width?: number;
  height?: number;
  minSize?: number;
}) {
  const { nodes, flows, weeks } = useMemo(() => {
    const sizeByWeekComm = new Map<string, number>();
    const weekSet = new Set<number>();
    for (const r of weekly) {
      weekSet.add(r.weekIdx);
      const key = `${r.weekIdx}:${r.communityId}`;
      sizeByWeekComm.set(key, (sizeByWeekComm.get(key) ?? 0) + 1);
    }
    const weeks = [...weekSet].sort((a, b) => a - b);
    const byWeek: Record<number, { id: string; size: number }[]> = {};
    for (const [key, size] of sizeByWeekComm.entries()) {
      if (size < minSize) continue;
      const [wStr, cid] = key.split(":");
      const w = Number(wStr);
      (byWeek[w] ||= []).push({ id: cid!, size });
    }
    for (const w of weeks) {
      if (!byWeek[w]) continue;
      byWeek[w]!.sort((a, b) => b.size - a.size);
    }

    const padX = 60;
    const padY = 20;
    const colW = weeks.length > 1 ? (width - padX * 2) / (weeks.length - 1) : 0;
    const nodes: Record<string, Node> = {};
    for (const w of weeks) {
      const list = byWeek[w] ?? [];
      const totalSize = list.reduce((a, b) => a + b.size, 0) || 1;
      const gap = 4;
      const maxH = height - padY * 2 - gap * Math.max(list.length - 1, 0);
      let y = padY;
      for (const { id, size } of list) {
        const h = Math.max(6, (maxH * size) / totalSize);
        nodes[`${w}:${id}`] = {
          id,
          week: w,
          size,
          x: padX + colW * (weeks.indexOf(w)),
          y,
          h,
        };
        y += h + gap;
      }
    }

    const flows: Flow[] = [];
    for (const l of lineage) {
      if (l.eventType === "birth" || l.eventType === "death") continue;
      if (!l.prevCommunityId || !l.communityId) continue;
      const from = `${l.weekIdx - 1}:${l.prevCommunityId}`;
      const to = `${l.weekIdx}:${l.communityId}`;
      if (!nodes[from] || !nodes[to]) continue;
      flows.push({
        from,
        to,
        weight: Math.max(l.overlapJaccard * l.membersCount, 1),
        eventType: l.eventType,
      });
    }

    return { nodes, flows, weeks };
  }, [weekly, lineage, width, height, minSize]);

  if (!weeks.length) {
    return <div style={{ color: "var(--muted)", fontSize: 11 }}>No temporal lineage data.</div>;
  }

  const maxW = Math.max(...flows.map((f) => f.weight), 1);

  return (
    <div style={{ width: "100%", overflowX: "auto" }}>
      <svg viewBox={`0 0 ${width} ${height}`} style={{ width: "100%", height, background: "#0a0a0f", border: "2px solid #222" }}>
        {weeks.map((w, i) => (
          <text
            key={`h${w}`}
            x={60 + (weeks.length > 1 ? ((width - 120) / (weeks.length - 1)) * i : 0)}
            y={14}
            fill="#d4d4d4"
            fontSize={10}
            fontFamily="Zpix, monospace"
            textAnchor="middle"
          >
            week {w}
          </text>
        ))}

        <g>
          {flows.map((f, i) => {
            const a = nodes[f.from]!;
            const b = nodes[f.to]!;
            const opacity = 0.15 + 0.55 * (f.weight / maxW);
            const cx = (a.x + b.x) / 2;
            const path = `M ${a.x + 8} ${a.y + a.h / 2} C ${cx} ${a.y + a.h / 2}, ${cx} ${b.y + b.h / 2}, ${b.x - 8} ${b.y + b.h / 2}`;
            const w = 1 + 8 * (f.weight / maxW);
            return (
              <path
                key={i}
                d={path}
                fill="none"
                stroke={EVENT_COLOR[f.eventType] ?? "#888"}
                strokeOpacity={opacity}
                strokeWidth={w}
              />
            );
          })}
        </g>

        <g>
          {Object.entries(nodes).map(([k, n]) => (
            <g key={k}>
              <rect
                x={n.x}
                y={n.y}
                width={8}
                height={n.h}
                fill="#33ccff"
                stroke="#0a0a0f"
                strokeWidth={0.5}
              />
              <title>{`${n.id.slice(0, 10)}… · week ${n.week} · ${n.size} repos`}</title>
              {n.h >= 14 ? (
                <text x={n.x + 12} y={n.y + n.h / 2 + 3} fill="#8bd17c" fontSize={9} fontFamily="Zpix, monospace">
                  {n.size}
                </text>
              ) : null}
            </g>
          ))}
        </g>

        <g transform={`translate(${width - 160}, ${height - 70})`}>
          {["continue", "merge_or_split", "reform"].map((ev, i) => (
            <g key={ev} transform={`translate(0, ${i * 14})`}>
              <rect width={8} height={8} fill={EVENT_COLOR[ev]} />
              <text x={12} y={8} fill="#d4d4d4" fontSize={10} fontFamily="Zpix, monospace">
                {ev}
              </text>
            </g>
          ))}
        </g>
      </svg>
    </div>
  );
}
