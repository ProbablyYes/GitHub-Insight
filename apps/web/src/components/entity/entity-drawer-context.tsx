"use client";

import { createContext, useCallback, useContext, useMemo, useState, type ReactNode } from "react";

export type EntityType = "repo" | "actor";

export type EntityTarget = { type: EntityType; id: string };

type Ctx = {
  target: EntityTarget | null;
  open: (t: EntityTarget) => void;
  close: () => void;
};

const EntityDrawerContext = createContext<Ctx | null>(null);

export function EntityDrawerProvider({ children }: { children: ReactNode }) {
  const [target, setTarget] = useState<EntityTarget | null>(null);
  const open = useCallback((t: EntityTarget) => setTarget(t), []);
  const close = useCallback(() => setTarget(null), []);
  const value = useMemo(() => ({ target, open, close }), [target, open, close]);
  return <EntityDrawerContext.Provider value={value}>{children}</EntityDrawerContext.Provider>;
}

export function useEntityDrawer(): Ctx {
  const ctx = useContext(EntityDrawerContext);
  if (!ctx) throw new Error("useEntityDrawer must be used inside EntityDrawerProvider");
  return ctx;
}
