import type { CSSProperties, ReactNode } from "react";

import { type Tone, toneColor } from "./tokens";

type Props = {
  title: string;
  headline?: ReactNode;
  children: ReactNode;
  tone?: Tone;
  actions?: ReactNode;
  howToRead?: ReactNode;
  source?: string;
  techBadge?: string;
  findings?: ReactNode;
  style?: CSSProperties;
};

export function PixelSection({
  title,
  headline,
  children,
  tone = "positive",
  actions,
  howToRead,
  source,
  techBadge,
  findings,
  style,
}: Props) {
  return (
    <section
      className="nes-container with-title is-dark"
      style={{ minWidth: 0, marginBottom: "var(--pixel-space-5)", ...style }}
    >
      <p className="title" style={{ color: toneColor[tone] }}>
        {title}
      </p>

      {(headline || actions) && (
        <div
          style={{
            display: "flex",
            flexWrap: "wrap",
            alignItems: "flex-start",
            justifyContent: "space-between",
            gap: "var(--pixel-space-3)",
            marginBottom: "var(--pixel-space-3)",
          }}
        >
          {headline ? (
            <p
              style={{
                margin: 0,
                color: "var(--fg)",
                fontSize: "var(--fs-body)",
                lineHeight: "var(--lh-tight)",
                flex: 1,
                minWidth: 0,
              }}
            >
              {headline}
            </p>
          ) : (
            <span />
          )}
          {actions ? <div style={{ display: "flex", gap: "var(--pixel-space-2)" }}>{actions}</div> : null}
        </div>
      )}

      <div style={{ minWidth: 0 }}>{children}</div>

      {findings ? (
        <div
          style={{
            marginTop: "var(--pixel-space-4)",
            padding: "var(--pixel-space-3)",
            borderLeft: `3px solid ${toneColor[tone]}`,
            background: "rgba(255,255,255,0.03)",
            color: "var(--fg)",
            fontSize: "var(--fs-body)",
            lineHeight: "var(--lh-tight)",
          }}
        >
          <div
            style={{
              color: toneColor[tone],
              fontSize: "var(--fs-caption)",
              textTransform: "uppercase",
              letterSpacing: 1,
              marginBottom: "var(--pixel-space-2)",
            }}
          >
            What this chart tells us
          </div>
          {findings}
        </div>
      ) : null}

      {(howToRead || source || techBadge) && (
        <details
          style={{
            marginTop: "var(--pixel-space-4)",
            color: "var(--muted-strong)",
            fontSize: "var(--fs-caption)",
            lineHeight: "var(--lh-tight)",
          }}
        >
          <summary>How to read / source</summary>
          <div style={{ marginTop: "var(--pixel-space-2)" }}>
            {howToRead ? <div>{howToRead}</div> : null}
            {source ? (
              <div style={{ marginTop: "var(--pixel-space-2)", color: "var(--muted)" }}>
                Source: <code style={{ color: "var(--accent-info)" }}>{source}</code>
              </div>
            ) : null}
            {techBadge ? (
              <div style={{ marginTop: "var(--pixel-space-2)", color: "var(--muted)" }}>
                Tech: <code style={{ color: "var(--accent-purple)" }}>{techBadge}</code>
              </div>
            ) : null}
          </div>
        </details>
      )}
    </section>
  );
}
