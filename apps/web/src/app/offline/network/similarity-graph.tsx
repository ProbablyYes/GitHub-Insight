"use client";

import { useMemo } from "react";

type Edge = {
  src: string;
  dst: string;
  jaccard: number;
  sharedActors: number;
};

type NodeInfo = {
  id: string;
  community?: string;
  rankNo?: number;
};

type Props = {
  edges: Edge[];
  nodeInfo: Record<string, NodeInfo>;
  width?: number;
  height?: number;
};

const PALETTE = [
  "#33ff57",
  "#33ccff",
  "#ffcc00",
  "#ff66cc",
  "#ff6633",
  "#a07bff",
  "#8bd17c",
  "#c6d1ff",
  "#4ea5ff",
  "#ff9f4a",
  "#88dd66",
  "#d17cff",
];

function shortRepo(name: string): string {
  return name.length <= 22 ? name : `${name.slice(0, 19)}...`;
}

function hashString(value: string): number {
  let h = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    h ^= value.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

export function SimilarityGraph({ edges, nodeInfo, width = 900, height = 520 }: Props) {
  const { nodes, communityColor } = useMemo(() => {
    const nodeSet = new Set<string>();
    for (const edge of edges) {
      nodeSet.add(edge.src);
      nodeSet.add(edge.dst);
    }

    const communityIds = Array.from(
      new Set(
        Array.from(nodeSet)
          .map((id) => nodeInfo[id]?.community)
          .filter((c): c is string => Boolean(c))
      )
    );
    const communityColor: Record<string, string> = {};
    communityIds.forEach((id, idx) => {
      communityColor[id] = PALETTE[idx % PALETTE.length];
    });

    const nodeList = Array.from(nodeSet).sort();
    const cx = width / 2;
    const cy = height / 2;

    const grouped = new Map<string, string[]>();
    for (const id of nodeList) {
      const community = nodeInfo[id]?.community ?? "_";
      if (!grouped.has(community)) grouped.set(community, []);
      grouped.get(community)!.push(id);
    }

    const communities = Array.from(grouped.entries());
    const positions: Record<string, { x: number; y: number; color: string; community: string }> = {};
    const baseR = Math.min(width, height) / 2 - 30;

    communities.forEach(([cid, members], ci) => {
      const angle = (2 * Math.PI * ci) / Math.max(communities.length, 1);
      const groupCx = cx + Math.cos(angle) * baseR * 0.55;
      const groupCy = cy + Math.sin(angle) * baseR * 0.55;
      const innerR = 20 + Math.min(members.length * 4, baseR * 0.35);
      members.forEach((id, mi) => {
        const theta = (2 * Math.PI * mi) / Math.max(members.length, 1);
        const jitter = ((hashString(id) % 100) / 100 - 0.5) * 6;
        const x = groupCx + Math.cos(theta) * innerR + jitter;
        const y = groupCy + Math.sin(theta) * innerR + jitter;
        positions[id] = {
          x,
          y,
          color: communityColor[cid] ?? "#888",
          community: cid,
        };
      });
    });

    return {
      nodes: nodeList.map((id) => ({
        id,
        ...positions[id],
        rankNo: nodeInfo[id]?.rankNo,
      })),
      communityColor,
    };
  }, [edges, nodeInfo, width, height]);

  const maxJaccard = Math.max(...edges.map((e) => e.jaccard), 0.0001);

  return (
    <div style={{ width: "100%", overflow: "hidden" }}>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        style={{ width: "100%", height, background: "#0a0a0f", border: "2px solid #222" }}
      >
        <g>
          {edges.map((edge) => {
            const a = nodes.find((n) => n.id === edge.src);
            const b = nodes.find((n) => n.id === edge.dst);
            if (!a || !b) return null;
            const opacity = 0.15 + 0.75 * (edge.jaccard / maxJaccard);
            const strokeColor = a.color === b.color ? a.color : "#556";
            return (
              <line
                key={`${edge.src}->${edge.dst}`}
                x1={a.x}
                y1={a.y}
                x2={b.x}
                y2={b.y}
                stroke={strokeColor}
                strokeOpacity={opacity}
                strokeWidth={0.6 + 1.5 * (edge.jaccard / maxJaccard)}
              />
            );
          })}
        </g>
        <g>
          {nodes.map((node) => {
            const communitySuffix = node.community ? ` · community ${node.community}` : "";
            const rankSuffix =
              node.rankNo && node.rankNo < 999999 ? ` · rank #${node.rankNo}` : "";
            const titleText = `${node.id}${communitySuffix}${rankSuffix}`;
            return (
              <g key={node.id}>
                <circle cx={node.x} cy={node.y} r={4} fill={node.color} stroke="#000" strokeWidth={0.5} />
                <title>{titleText}</title>
              </g>
            );
          })}
        </g>
        <g>
          {Object.entries(communityColor)
            .slice(0, 8)
            .map(([cid, color], idx) => (
              <g key={cid} transform={`translate(12, ${12 + idx * 16})`}>
                <rect width={10} height={10} fill={color} />
                <text x={14} y={9} fill="#d4d4d4" fontSize={10} fontFamily="Zpix, monospace">
                  community {shortRepo(cid)}
                </text>
              </g>
            ))}
        </g>
      </svg>
    </div>
  );
}
