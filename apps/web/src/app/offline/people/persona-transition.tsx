"use client";

import type { ActorPersonaTransitionPoint } from "@/lib/dashboard";

type Props = {
  rows: ActorPersonaTransitionPoint[];
};

export function PersonaTransitionMatrix({ rows }: Props) {
  // Build label order: stable (self) diagonal first, then by frequency
  const earlyLabels = Array.from(new Set(rows.map((r) => r.personaEarly)));
  const lateLabels = Array.from(new Set(rows.map((r) => r.personaLate)));
  const labelSet = new Set([...earlyLabels, ...lateLabels]);
  const labels = Array.from(labelSet).sort();

  // Build lookup
  const cell = new Map<string, ActorPersonaTransitionPoint>();
  rows.forEach((r) => cell.set(`${r.personaEarly}→${r.personaLate}`, r));

  // Compute a stability score per row (diagonal / row_total) for ordering
  const rowStability = new Map<string, number>();
  for (const e of labels) {
    const diag = cell.get(`${e}→${e}`);
    rowStability.set(e, diag?.transitionProb ?? 0);
  }

  // Color scale: white at 0 → magenta at 1
  const colorFor = (p: number) => {
    const alpha = Math.min(Math.max(p, 0), 1);
    return `rgba(255, 102, 204, ${(0.08 + 0.82 * alpha).toFixed(3)})`;
  };

  const cellSize = 84;

  if (rows.length === 0) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center" }}>No transition data.</p>
    );
  }

  const maxOff = Math.max(
    0,
    ...rows.filter((r) => !r.isStable).map((r) => r.transitionProb),
  );

  return (
    <div style={{ overflowX: "auto" }}>
      <div
        style={{
          display: "inline-grid",
          gridTemplateColumns: `120px repeat(${labels.length}, ${cellSize}px)`,
          gap: 2,
          fontSize: 10,
          fontFamily: '"Zpix", monospace',
        }}
      >
        <div />
        {labels.map((lbl) => (
          <div
            key={`head-${lbl}`}
            style={{
              color: "var(--muted)",
              textAlign: "center",
              padding: "4px 2px",
              writingMode: "vertical-rl",
              transform: "rotate(180deg)",
              height: 90,
              fontSize: 10,
              whiteSpace: "nowrap",
              overflow: "hidden",
            }}
          >
            {lbl}
          </div>
        ))}

        {labels.map((early) => (
          <div key={`row-${early}`} style={{ display: "contents" }}>
            <div
              style={{
                color: "var(--fg)",
                textAlign: "right",
                padding: "4px 8px",
                fontSize: 10,
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
              title={`early: ${early}, stability (diag): ${((rowStability.get(early) ?? 0) * 100).toFixed(0)}%`}
            >
              {early}
            </div>
            {labels.map((late) => {
              const c = cell.get(`${early}→${late}`);
              const p = c?.transitionProb ?? 0;
              const stable = early === late;
              return (
                <div
                  key={`${early}-${late}`}
                  title={`${early} → ${late}: ${(p * 100).toFixed(1)}% (${c?.actors ?? 0}/${c?.rowTotal ?? 0})`}
                  style={{
                    background: stable
                      ? `rgba(51, 255, 87, ${(0.08 + 0.82 * p).toFixed(3)})`
                      : colorFor(maxOff > 0 ? p / maxOff : 0),
                    border: "1px solid rgba(255,255,255,0.06)",
                    textAlign: "center",
                    padding: "6px 2px",
                    fontSize: 11,
                    color: p > 0.4 ? "#000" : "#d4d4d4",
                    minHeight: 34,
                  }}
                >
                  {p > 0 ? `${(p * 100).toFixed(0)}%` : "·"}
                  {c && c.actors > 0 ? (
                    <div style={{ fontSize: 9, opacity: 0.7 }}>
                      {c.actors}
                    </div>
                  ) : null}
                </div>
              );
            })}
          </div>
        ))}
      </div>
      <p style={{ color: "var(--muted)", fontSize: 11, marginTop: 8 }}>
        Rows = early-window persona · Columns = late-window persona · Cell = P(late | early).
        Green diagonal = persona stability. Off-diagonal intensity normalised to max off-diagonal =
        {" "}
        {(maxOff * 100).toFixed(0)}%.
      </p>
    </div>
  );
}
