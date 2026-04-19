import { getSummaryMetrics } from "@/lib/dashboard";
import { PixelPageShell } from "@/components/pixel-shell";
import Link from "next/link";

function fmt(v: number) {
  return new Intl.NumberFormat("en-US").format(v);
}

export default async function Home() {
  const s = await getSummaryMetrics();

  return (
    <PixelPageShell
      title="GitHub Open-Source Ecosystem Console"
      subtitle="Unified streaming + batch analytics — developer behavior observatory powered by GH Archive"
    >
      {/* ── Status ── */}
      <section className="nes-container with-title is-dark animate-float delay-1" style={{ marginBottom: 24 }}>
        <p className="title">System status</p>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 24 }}>
          <div>
            <p style={{ color: "var(--green)", fontSize: 14, marginBottom: 6 }}>♥ Realtime pipeline</p>
            <progress className="nes-progress is-success" value={s.realtimeEventRows > 0 ? 80 : 10} max={100} />
            <p style={{ color: "var(--fg)", marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.realtimeEventRows)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>rows · {fmt(s.realtimeRepos)} repos</span>
            </p>
          </div>

          <div>
            <p style={{ color: "var(--yellow)", fontSize: 14, marginBottom: 6 }}>⚠ Anomaly alerts</p>
            <progress className="nes-progress is-warning" value={Math.min(s.anomalyAlerts * 5, 100)} max={100} />
            <p style={{ marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.anomalyAlerts)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>alerts</span>
            </p>
          </div>

          <div>
            <p style={{ color: "var(--cyan)", fontSize: 14, marginBottom: 6 }}>◆ Batch pipeline</p>
            <progress className="nes-progress is-primary" value={s.batchMetricRows > 0 ? 70 : 5} max={100} />
            <p style={{ marginTop: 6 }}>
              <span style={{ color: "var(--fg-strong)", fontSize: 18 }}>{fmt(s.batchMetricRows)}</span>{" "}
              <span style={{ color: "var(--muted)" }}>rows</span>
            </p>
          </div>
        </div>
      </section>

      {/* ── Navigation ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-2">
          <p className="title" style={{ color: "var(--green)" }}>♦ Realtime war room</p>
          <p style={{ fontSize: 15, color: "var(--fg-strong)", marginBottom: 12 }}>
            Kafka → Consumer → ClickHouse
          </p>
          <p style={{ color: "var(--muted)", marginBottom: 16 }}>
            Minute-level windows · Hot repo tracking · Spike alerts
          </p>
          <Link href="/realtime">
            <button type="button" className="nes-btn is-success" style={{ width: "100%" }}>
              Enter Realtime &gt;&gt;
            </button>
          </Link>
        </section>

        <section className="nes-container with-title is-dark animate-float delay-3">
          <p className="title" style={{ color: "var(--cyan)" }}>♠ Offline lab</p>
          <p style={{ fontSize: 15, color: "var(--fg-strong)", marginBottom: 12 }}>
            Spark → Parquet → ClickHouse
          </p>
          <p style={{ color: "var(--muted)", marginBottom: 16 }}>
            Daily trends · Bot/Human profiles · Activity rhythm maps
          </p>
          <Link href="/offline">
            <button type="button" className="nes-btn is-primary" style={{ width: "100%" }}>
              Enter Offline &gt;&gt;
            </button>
          </Link>
        </section>
      </div>

      {/* ── Architecture ── */}
      <section className="nes-container with-title is-dark animate-float delay-4" style={{ marginBottom: 24 }}>
        <p className="title">System architecture</p>
        <pre style={{ color: "var(--green)", lineHeight: 1.5, overflow: "auto", margin: 0, fontSize: 13 }}>
{`  ┌──────────┐     ┌───────┐     ┌──────────┐     ┌────────────┐
  │ GH       │────▶│ Kafka │────▶│ Realtime │────▶│            │
  │ Archive  │     └───────┘     │ Consumer │     │ ClickHouse │
  │          │                    └──────────┘     │            │
  │          │     ┌───────┐     ┌──────────┐     │            │
  │          │────▶│ Spark │────▶│ Parquet  │────▶│            │
  └──────────┘     └───────┘     └──────────┘     └─────┬──────┘
                                                        │
                                                        ▼
                                                  ┌──────────┐
                                                  │ Next.js  │
                                                  └──────────┘`}
        </pre>
      </section>

      {/* ── Quest Board ── */}
      <section className="nes-container with-title is-dark animate-float delay-5" style={{ marginBottom: 24 }}>
        <p className="title">Quest board</p>
        <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
          {[
            { done: true, text: "Realtime radar online", sub: "Stream · Hotspots · Alerts", color: "var(--green)" },
            { done: true, text: "Offline lab online", sub: "Trends · Profiles · Rhythm", color: "var(--cyan)" },
            { done: false, text: "Exploration zone", sub: "LOCKED", color: "var(--muted)" },
            { done: false, text: "Defense storyline", sub: "LOCKED", color: "var(--muted)" },
          ].map((q) => (
            <div key={q.text} style={{ display: "flex", alignItems: "center", gap: 16 }}>
              <label style={{ display: "flex", alignItems: "center" }}>
                <input type="checkbox" className="nes-checkbox is-dark" checked={q.done} readOnly />
                <span style={{ color: q.color, fontSize: 15 }}>{q.text}</span>
              </label>
              <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: 13 }}>{q.sub}</span>
            </div>
          ))}
        </div>
      </section>

      {/* ── Tech Stack (flat grid, no nested boxes) ── */}
      <section style={{ marginBottom: 24 }} className="animate-float delay-5">
        <p style={{ color: "var(--muted)", fontSize: 12, letterSpacing: 4, marginBottom: 12 }}>TECH STACK</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(160px, 1fr))", gap: 12 }}>
          {[
            { name: "Kafka", desc: "Event bus", c: "var(--green)" },
            { name: "ClickHouse", desc: "Analytical DB", c: "var(--green)" },
            { name: "Spark", desc: "Batch processing", c: "var(--cyan)" },
            { name: "Next.js", desc: "UI layer", c: "var(--cyan)" },
            { name: "MinIO", desc: "Object storage", c: "var(--yellow)" },
            { name: "Docker", desc: "Orchestration", c: "var(--yellow)" },
          ].map((t) => (
            <div key={t.name} style={{ background: "var(--panel)", border: "2px solid var(--divider)", padding: "12px 16px" }}>
              <p style={{ color: t.c, fontSize: 15, marginBottom: 2 }}>{t.name}</p>
              <p style={{ color: "var(--muted)", fontSize: 13 }}>{t.desc}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Footer */}
      <p style={{ textAlign: "center", color: "var(--muted)", fontSize: 13, padding: "16px 0" }}>
        ♥ GitHub Insight — Big Data Processing course project
        <span className="animate-blink" style={{ marginLeft: 8, color: "var(--green)" }}>_</span>
      </p>
    </PixelPageShell>
  );
}
