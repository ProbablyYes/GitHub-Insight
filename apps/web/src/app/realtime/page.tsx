import { DashboardCharts } from "@/components/dashboard-charts";
import { PixelPageShell } from "@/components/pixel-shell";
import { getAlerts, getEventTrend, getHotRepos } from "@/lib/dashboard";

function fmt(v: number) {
  return new Intl.NumberFormat("en-US").format(v);
}

export default async function RealtimePage() {
  const [eventTrend, hotRepos, alerts] = await Promise.all([
    getEventTrend(),
    getHotRepos(),
    getAlerts(),
  ]);

  const eventTrendData = eventTrend.map((item) => ({
    label: item.windowStart.slice(11, 16),
    value: item.totalEvents,
  }));

  const maxHotness = hotRepos[0]?.hotnessScore || 1;

  return (
    <PixelPageShell
      title="Realtime Event War Room"
      subtitle="Kafka stream → realtime consumer → ClickHouse · minute-level window aggregation"
    >
      {/* ── Event Trend ── */}
      <section className="nes-container with-title is-dark animate-float delay-1" style={{ marginBottom: 24 }}>
        <p className="title" style={{ color: "var(--green)" }}>♦ Event trend</p>
        {eventTrendData.length > 0 ? (
          <DashboardCharts variant="line" data={eventTrendData} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center", padding: 40 }}>
            <span className="animate-blink">▶</span> Waiting for realtime data...
          </p>
        )}
      </section>

      {/* ── Hot Repos + Alerts ── */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 24 }}>
        <section className="nes-container with-title is-dark animate-float delay-2">
          <p className="title" style={{ color: "var(--green)" }}>Hot repos</p>
          {hotRepos.length > 0 ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 14 }}>
              {hotRepos.map((repo, idx) => (
                <div key={repo.repoName}>
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 4 }}>
                    <span>
                      <span style={{ color: "var(--green)", fontSize: 16 }}>#{String(idx + 1).padStart(2, "0")}</span>{" "}
                      <span style={{ color: "var(--fg-strong)", fontSize: 14 }}>{repo.repoName}</span>
                    </span>
                    <span style={{ color: "var(--green)", fontSize: 15 }}>
                      {repo.hotnessScore.toFixed(1)}
                    </span>
                  </div>
                  <progress className="nes-progress is-success" value={repo.hotnessScore} max={maxHotness} style={{ height: 16 }} />
                  <p style={{ color: "var(--muted)", fontSize: 13, marginTop: 4 }}>
                    Push {fmt(repo.pushEvents)} · Watch {fmt(repo.watchEvents)} · Fork {fmt(repo.forkEvents)}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No data.</p>
          )}
        </section>

        <section className="nes-container with-title is-dark animate-float delay-3">
          <p className="title" style={{ color: "var(--yellow)" }}>⚠ Anomaly alerts</p>
          {alerts.length > 0 ? (
            <div style={{ display: "flex", flexDirection: "column", gap: 14, maxHeight: 420, overflowY: "auto" }}>
              {alerts.map((alert) => (
                <div
                  key={`${alert.windowStart}-${alert.repoName}`}
                  style={{ background: "rgba(0,0,0,0.3)", padding: "12px 16px", border: "2px solid var(--divider)" }}
                >
                  <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
                    <span style={{ color: "var(--fg-strong)", fontSize: 14 }}>{alert.repoName}</span>
                    <span style={{ color: alert.alertLevel === "high" ? "var(--red)" : "var(--yellow)", fontSize: 14, fontWeight: "bold" }}>
                      {alert.alertLevel.toUpperCase()}
                    </span>
                  </div>
                  <p style={{ color: "var(--muted)", fontSize: 13, marginTop: 6 }}>
                    Ratio {alert.anomalyRatio.toFixed(1)} · Current {alert.currentEvents} · Baseline {alert.baselineEvents.toFixed(1)}
                  </p>
                  <progress
                    className={`nes-progress ${alert.alertLevel === "high" ? "is-error" : "is-warning"}`}
                    value={Math.min(alert.anomalyRatio * 20, 100)}
                    max={100}
                    style={{ height: 10, marginTop: 6 }}
                  />
                </div>
              ))}
            </div>
          ) : (
            <p style={{ color: "var(--muted)", textAlign: "center" }}>No anomaly alerts.</p>
          )}
        </section>
      </div>

      {/* ── Locked slots (flat, no nested container) ── */}
      <section style={{ marginBottom: 24 }} className="animate-float delay-4">
        <p style={{ color: "var(--muted)", fontSize: 12, letterSpacing: 4, marginBottom: 12 }}>LOCKED MODULES</p>
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))", gap: 10 }}>
          {[
            "Event-type distribution toggle",
            "Repo detail drawer",
            "Realtime terminal feed",
            "Bot vs Human realtime comparison",
          ].map((slot) => (
            <div key={slot} style={{ background: "var(--panel)", border: "2px solid var(--divider)", padding: "10px 14px", opacity: 0.5 }}>
              <span style={{ color: "var(--muted)" }}>🔒 {slot}</span>
            </div>
          ))}
        </div>
      </section>
    </PixelPageShell>
  );
}
