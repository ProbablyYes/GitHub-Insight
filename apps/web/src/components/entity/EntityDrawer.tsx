"use client";

import { useEffect, useState } from "react";

import { ActorDrawerContent } from "./actor-drawer-content";
import { RepoDrawerContent } from "./repo-drawer-content";
import { useEntityDrawer } from "./entity-drawer-context";

export function EntityDrawer() {
  const { target, close } = useEntityDrawer();
  const [data, setData] = useState<unknown>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!target) return;
    let abort = false;
    // Intentional loading-state reset on target change — the drawer must show a
    // spinner while the new entity is being fetched. Grouped into one microtask
    // via React auto-batching so this is a single render, not a cascade.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setLoading(true);
    setError(null);
    setData(null);
    const parts = target.id.split("/").map(encodeURIComponent).join("/");
    fetch(`/api/entity/${target.type}/${parts}`)
      .then((r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((payload) => {
        if (abort) return;
        setData(payload);
      })
      .catch((err) => {
        if (abort) return;
        setError((err as Error).message ?? "failed to load");
      })
      .finally(() => {
        if (abort) return;
        setLoading(false);
      });
    return () => {
      abort = true;
    };
  }, [target]);

  useEffect(() => {
    if (!target) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [target, close]);

  if (!target) return null;

  const type = target.type;
  const id = target.id;

  return (
    <div
      onClick={close}
      style={{
        position: "fixed",
        inset: 0,
        background: "rgba(0,0,0,0.55)",
        zIndex: 2000,
        display: "flex",
        justifyContent: "flex-end",
      }}
    >
      <aside
        onClick={(e) => e.stopPropagation()}
        className="nes-container is-dark pixel-scroll"
        style={{
          width: "min(460px, 95vw)",
          height: "100vh",
          overflowY: "auto",
          padding: "var(--pixel-space-4)",
          borderLeft: "3px solid var(--accent-positive)",
          display: "flex",
          flexDirection: "column",
          gap: "var(--pixel-space-3)",
          background: "var(--panel)",
        }}
      >
        <header
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-start",
            gap: "var(--pixel-space-2)",
          }}
        >
          <div style={{ minWidth: 0 }}>
            <div
              style={{
                fontSize: "var(--fs-caption)",
                color: "var(--muted-strong)",
                letterSpacing: 1,
                textTransform: "uppercase",
              }}
            >
              {type}
            </div>
            <div
              style={{
                color: "var(--accent-positive)",
                fontSize: "var(--fs-title)",
                lineHeight: 1.2,
                wordBreak: "break-all",
              }}
            >
              {id}
            </div>
          </div>
          <button
            type="button"
            onClick={close}
            className="nes-btn"
            style={{ padding: "2px 8px", fontSize: 12 }}
            aria-label="Close"
          >
            ✕
          </button>
        </header>

        {loading ? (
          <div style={{ color: "var(--accent-change)", fontSize: 12 }} className="animate-blink">
            loading {type} summary...
          </div>
        ) : null}
        {error ? (
          <div style={{ color: "var(--accent-danger)", fontSize: 12 }}>error: {error}</div>
        ) : null}

        {data != null && !loading ? (
          type === "repo" ? (
            <RepoDrawerContent summary={data as Parameters<typeof RepoDrawerContent>[0]["summary"]} />
          ) : (
            <ActorDrawerContent summary={data as Parameters<typeof ActorDrawerContent>[0]["summary"]} />
          )
        ) : null}
      </aside>
    </div>
  );
}
