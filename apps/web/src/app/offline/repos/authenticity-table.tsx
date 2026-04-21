"use client";

import { useMemo } from "react";

import { EntityLink } from "@/components/entity";
import {
  PixelBadge,
  PixelMiniBar,
  PixelSearchTable,
  type PixelSearchColumn,
} from "@/components/pixel";
import {
  SIGNAL_LABEL,
  type AuthenticityRow,
  type AuthenticitySignalKey,
} from "@/lib/authenticity";

type Props = {
  rows: AuthenticityRow[];
};

// Colour each signal consistently so the 4 mini-bars read like a fingerprint.
const SIGNAL_TONE: Record<AuthenticitySignalKey, "danger" | "change" | "info" | "magenta"> = {
  alertZ: "danger",
  dnaOutlierZ: "change",
  watcherBot: "magenta",
  communityBot: "info",
};

function compositeTone(value: number): "danger" | "change" | "info" | "muted" {
  if (value >= 0.6) return "danger";
  if (value >= 0.35) return "change";
  if (value > 0) return "info";
  return "muted";
}

function rawLabel(signal: AuthenticitySignalKey, raw: number): string {
  switch (signal) {
    case "alertZ":
      return `|z|=${raw.toFixed(2)}`;
    case "dnaOutlierZ":
      return `z=${raw.toFixed(2)}`;
    case "watcherBot":
    case "communityBot":
      return `${(raw * 100).toFixed(0)}%`;
  }
}

function SignalBars({ row }: { row: AuthenticityRow }) {
  const keys: AuthenticitySignalKey[] = [
    "alertZ",
    "dnaOutlierZ",
    "watcherBot",
    "communityBot",
  ];
  return (
    <div style={{ display: "grid", rowGap: 2 }}>
      {keys.map((k) => (
        <div
          key={k}
          style={{ display: "flex", alignItems: "center", gap: 6, fontSize: 10 }}
        >
          <span
            style={{
              color: "var(--muted-strong)",
              minWidth: 72,
              textAlign: "right",
              opacity: row.dominantSignal === k ? 1 : 0.7,
              fontWeight: row.dominantSignal === k ? 700 : 400,
            }}
          >
            {SIGNAL_LABEL[k]}
          </span>
          <PixelMiniBar
            value={row.normalized[k]}
            min={0}
            max={1}
            tone={SIGNAL_TONE[k]}
            width={110}
            height={8}
            title={`${SIGNAL_LABEL[k]}: normalised ${row.normalized[k].toFixed(2)} · raw ${rawLabel(k, row.raw[k])}`}
          />
          <span style={{ color: "var(--fg)", minWidth: 56, fontSize: 10 }}>
            {rawLabel(k, row.raw[k])}
          </span>
        </div>
      ))}
    </div>
  );
}

export function AuthenticityTable({ rows }: Props) {
  const columns = useMemo<PixelSearchColumn<AuthenticityRow>[]>(
    () => [
      {
        key: "repoName",
        header: "repo",
        align: "left",
        sortValue: (r) => r.repoName,
        searchValue: (r) => r.repoName,
        render: (r) => <EntityLink type="repo" id={r.repoName} />,
      },
      {
        key: "composite",
        header: "composite",
        align: "right",
        sortValue: (r) => r.composite,
        render: (r) => (
          <PixelBadge tone={compositeTone(r.composite)} size="sm">
            {r.composite.toFixed(2)}
          </PixelBadge>
        ),
      },
      {
        key: "signals",
        header: "signal breakdown (normalised 0..1 · raw)",
        align: "left",
        render: (r) => <SignalBars row={r} />,
      },
      {
        key: "dominantSignal",
        header: "dominant",
        align: "left",
        sortValue: (r) => r.dominantSignal,
        render: (r) => (
          <PixelBadge tone={SIGNAL_TONE[r.dominantSignal] === "magenta" ? "magenta" : SIGNAL_TONE[r.dominantSignal]} size="sm">
            {SIGNAL_LABEL[r.dominantSignal]}
          </PixelBadge>
        ),
      },
      {
        key: "watchers",
        header: "watchers",
        align: "right",
        sortValue: (r) => r.watchers,
      },
    ],
    [],
  );

  if (rows.length === 0) {
    return (
      <p style={{ color: "var(--muted)", textAlign: "center", padding: 12 }}>
        No fused signals available yet.
      </p>
    );
  }

  return (
    <PixelSearchTable
      csvFilename="authenticity_top.csv"
      rows={rows}
      columns={columns}
      getRowKey={(r) => r.repoName}
      initialSort={{ key: "composite", desc: true }}
      pageSize={10}
      searchPlaceholder="filter repo..."
      fontSize={11}
    />
  );
}
