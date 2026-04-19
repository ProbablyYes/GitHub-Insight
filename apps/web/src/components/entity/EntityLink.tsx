"use client";

import type { CSSProperties, ReactNode } from "react";

import { type EntityType, useEntityDrawer } from "./entity-drawer-context";

type Props = {
  type: EntityType;
  id: string;
  label?: ReactNode;
  style?: CSSProperties;
  title?: string;
  muted?: boolean;
};

function githubHref(type: EntityType, id: string): string {
  if (type === "repo") return `https://github.com/${id}`;
  return `https://github.com/${id}`;
}

export function EntityLink({ type, id, label, style, title, muted }: Props) {
  const { open } = useEntityDrawer();

  return (
    <a
      href={githubHref(type, id)}
      target="_blank"
      rel="noreferrer noopener"
      onClick={(e) => {
        if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey || e.button === 1) return;
        e.preventDefault();
        open({ type, id });
      }}
      title={title ?? `${type === "repo" ? "Repo" : "Actor"}: ${id} — click to open analysis drawer, ⌘/Ctrl+click to open on GitHub`}
      style={{
        color: muted ? "var(--muted-strong)" : "var(--accent-info)",
        textDecoration: "none",
        borderBottom: "1px dotted currentColor",
        cursor: "pointer",
        fontSize: "inherit",
        fontFamily: "inherit",
        ...style,
      }}
    >
      {label ?? id}
    </a>
  );
}
