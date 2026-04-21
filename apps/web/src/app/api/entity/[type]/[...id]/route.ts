import { NextResponse } from "next/server";

import { getActorSummary, getRepoSummary } from "@/lib/entity-summary";

export const dynamic = "force-dynamic";

type RouteParams = { params: Promise<{ type: string; id: string[] }> };

export async function GET(_request: Request, { params }: RouteParams) {
  const { type, id } = await params;
  const rawId = (id ?? []).join("/");
  if (!rawId) {
    return NextResponse.json({ error: "missing id" }, { status: 400 });
  }

  try {
    if (type === "repo") {
      const summary = await getRepoSummary(decodeURIComponent(rawId));
      return NextResponse.json(summary, {
        headers: { "Cache-Control": "public, max-age=30, stale-while-revalidate=120" },
      });
    }
    if (type === "actor") {
      const summary = await getActorSummary(decodeURIComponent(rawId));
      return NextResponse.json(summary, {
        headers: { "Cache-Control": "public, max-age=30, stale-while-revalidate=120" },
      });
    }
    return NextResponse.json({ error: `unknown entity type '${type}'` }, { status: 400 });
  } catch (err) {
    return NextResponse.json(
      { error: (err as Error).message ?? "failed to load entity" },
      { status: 500 }
    );
  }
}
