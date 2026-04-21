"use client";

import { useEffect, useState } from "react";

type Insight = {
  insightText: string;
  insightType: string;
};

type Props = {
  insights: Insight[];
};

export function InsightsTicker({ insights }: Props) {
  const [idx, setIdx] = useState(0);
  const safe = insights.filter((it) => it.insightText && it.insightText.trim().length > 0);

  useEffect(() => {
    if (safe.length <= 1) return;
    const timer = setInterval(() => {
      setIdx((i) => (i + 1) % safe.length);
    }, 5000);
    return () => clearInterval(timer);
  }, [safe.length]);

  if (safe.length === 0) return null;

  const current = safe[idx];

  return (
    <div
      className="nes-container is-dark"
      style={{
        marginBottom: "var(--pixel-space-5)",
        padding: "var(--pixel-space-3) var(--pixel-space-4)",
        display: "flex",
        alignItems: "center",
        gap: "var(--pixel-space-3)",
        minHeight: 36,
      }}
    >
      <span
        style={{
          color: "var(--accent-change)",
          fontSize: "var(--fs-caption)",
          letterSpacing: 1,
          whiteSpace: "nowrap",
        }}
      >
        ▸ INSIGHT
      </span>
      <span style={{ color: "var(--muted-strong)", fontSize: "var(--fs-caption)", whiteSpace: "nowrap" }}>
        [{current?.insightType ?? "info"}]
      </span>
      <span
        key={idx}
        style={{
          color: "var(--fg)",
          fontSize: "var(--fs-body)",
          lineHeight: "var(--lh-tight)",
          flex: 1,
          minWidth: 0,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}
        className="animate-float"
      >
        {current?.insightText}
      </span>
      {safe.length > 1 ? (
        <span style={{ color: "var(--muted)", fontSize: "var(--fs-caption)", whiteSpace: "nowrap" }}>
          {idx + 1}/{safe.length}
        </span>
      ) : null}
    </div>
  );
}
