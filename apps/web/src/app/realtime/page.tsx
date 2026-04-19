import { DashboardCharts } from "@/components/dashboard-charts";
import { PixelPageShell } from "@/components/pixel-shell";
import { RealtimeAutoRefresh } from "@/components/realtime-auto-refresh";
import {
  getActiveOwners,
  getAlerts,
  getEventTrend,
  getHotRepos,
  getRealtimeAnomalyAlertRows,
  getRealtimeEventMetricRows,
  getRealtimeRepoScoreRows,
} from "@/lib/dashboard";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function fmt(v: number) {
  return new Intl.NumberFormat("zh-CN").format(v);
}

function repoUrl(repoName: string) {
  return `https://github.com/${repoName}`;
}

function ownerUrl(owner: string) {
  return `https://github.com/${owner}`;
}

const tableHeaders = {
  eventMetrics: ["window_start", "event_type", "repo_name", "actor_category", "event_count"],
  repoScores: ["window_start", "repo_name", "hotness_score", "push_events", "watch_events", "fork_events"],
  alerts: ["window_start", "repo_name", "current_events", "baseline_events", "anomaly_ratio", "alert_level"],
};

export default async function RealtimePage() {
  const [eventTrend, hotRepos, activeOwners, alerts, eventRows, repoRows, alertRows] = await Promise.all([
    getEventTrend(),
    getHotRepos(),
    getActiveOwners(12),
    getAlerts(),
    getRealtimeEventMetricRows(30),
    getRealtimeRepoScoreRows(30),
    getRealtimeAnomalyAlertRows(30),
  ]);

  const eventTrendData = eventTrend.map((item) => ({
    label: (item.windowStartDisplay || item.windowStart).slice(11, 16),
    value: item.totalEvents,
  }));

  const maxHotness = hotRepos[0]?.hotnessScore || 1;
  const maxOwnerHotness = activeOwners[0]?.totalHotness || 1;
  const latestWindow = eventRows[0]?.windowStart || "-";
  const totalTopRepoEvents = hotRepos.reduce(
    (acc, cur) => acc + cur.pushEvents + cur.watchEvents + cur.forkEvents,
    0,
  );

  return (
    <PixelPageShell
      title="实时事件作战室"
      subtitle="Kafka 事件流 -> 实时聚合 -> ClickHouse 实时结果表（UTC+8 展示）"
    >
      <RealtimeAutoRefresh intervalMs={5000} />

      <section className="rt-kpis animate-float delay-2">
        <div className="rt-kpi">
          <p className="rt-kpi-label">最新窗口</p>
          <p className="rt-kpi-value">{latestWindow}</p>
        </div>
        <div className="rt-kpi">
          <p className="rt-kpi-label">趋势点位</p>
          <p className="rt-kpi-value">{fmt(eventTrendData.length)}</p>
        </div>
        <div className="rt-kpi">
          <p className="rt-kpi-label">热点仓库</p>
          <p className="rt-kpi-value">{fmt(hotRepos.length)}</p>
        </div>
        <div className="rt-kpi">
          <p className="rt-kpi-label">活跃用户/组织</p>
          <p className="rt-kpi-value">{fmt(activeOwners.length)}</p>
        </div>
        <div className="rt-kpi">
          <p className="rt-kpi-label">异常告警</p>
          <p className="rt-kpi-value rt-kpi-warn">{fmt(alerts.length)}</p>
        </div>
      </section>

      <section className="nes-container with-title is-dark animate-float delay-3" style={{ marginBottom: 20 }}>
        <p className="title rt-title-lg">事件趋势（UTC+8）</p>
        {eventTrendData.length > 0 ? (
          <DashboardCharts variant="line" data={eventTrendData} />
        ) : (
          <p style={{ color: "var(--muted)", textAlign: "center", padding: 40 }}>
            <span className="animate-blink">*</span> 等待实时数据写入...
          </p>
        )}
      </section>

      <section className="rt-grid-2 animate-float delay-4">
        <article className="nes-container with-title is-dark">
          <p className="title rt-title-lg">热点仓库 Top</p>
          {hotRepos.length > 0 ? (
            <div className="rt-list">
              {hotRepos.map((repo, idx) => (
                <div key={repo.repoName} className="rt-item-card">
                  <div className="rt-item-row">
                    <span className="rt-rank">#{String(idx + 1).padStart(2, "0")}</span>
                    <a className="rt-link" href={repoUrl(repo.repoName)} target="_blank" rel="noreferrer">
                      {repo.repoName}
                    </a>
                    <span className="rt-score">{repo.hotnessScore.toFixed(1)}</span>
                  </div>
                  <progress className="nes-progress is-success" value={repo.hotnessScore} max={maxHotness} style={{ height: 12 }} />
                  <p className="rt-item-sub">
                    Push {fmt(repo.pushEvents)} | Watch {fmt(repo.watchEvents)} | Fork {fmt(repo.forkEvents)}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <p className="rt-empty">暂无热点仓库数据</p>
          )}
        </article>

        <article className="nes-container with-title is-dark">
          <p className="title rt-title-lg">活跃用户/组织 Top</p>
          <p className="rt-note">说明：当前按仓库 owner 聚合（无需改实时表结构）</p>
          {activeOwners.length > 0 ? (
            <div className="rt-list">
              {activeOwners.map((owner, idx) => (
                <div key={owner.ownerName} className="rt-item-card">
                  <div className="rt-item-row">
                    <span className="rt-rank">#{String(idx + 1).padStart(2, "0")}</span>
                    <a className="rt-link" href={ownerUrl(owner.ownerName)} target="_blank" rel="noreferrer">
                      @{owner.ownerName}
                    </a>
                    <span className="rt-score">{owner.totalHotness.toFixed(1)}</span>
                  </div>
                  <progress className="nes-progress is-primary" value={owner.totalHotness} max={maxOwnerHotness} style={{ height: 12 }} />
                  <p className="rt-item-sub">
                    事件 {fmt(owner.totalEvents)} | 关联仓库 {fmt(owner.repoCount)}
                  </p>
                </div>
              ))}
            </div>
          ) : (
            <p className="rt-empty">暂无活跃用户/组织数据</p>
          )}
        </article>
      </section>

      <section className="nes-container with-title is-dark animate-float delay-5" style={{ marginTop: 20, marginBottom: 20 }}>
        <p className="title rt-title-lg">异常预警</p>
        {alerts.length > 0 ? (
          <div className="rt-alert-list">
            {alerts.map((alert) => (
              <div key={`${alert.windowStart}-${alert.repoName}`} className="rt-alert-card">
                <div className="rt-item-row">
                  <a className="rt-link" href={repoUrl(alert.repoName)} target="_blank" rel="noreferrer">
                    {alert.repoName}
                  </a>
                  <span className={alert.alertLevel === "high" ? "rt-pill rt-pill-high" : "rt-pill rt-pill-medium"}>
                    {alert.alertLevel.toUpperCase()}
                  </span>
                </div>
                <p className="rt-item-sub">
                  比值 {alert.anomalyRatio.toFixed(2)} | 当前 {fmt(alert.currentEvents)} | 基线 {alert.baselineEvents.toFixed(2)}
                </p>
              </div>
            ))}
          </div>
        ) : (
          <p className="rt-empty">暂无异常告警</p>
        )}
      </section>

      <section className="nes-container with-title is-dark animate-float delay-5" style={{ marginBottom: 20 }}>
        <p className="title rt-title-lg">实时结果明细表</p>
        <p className="rt-note">来自 ClickHouse：`realtime_event_metrics` / `realtime_repo_scores` / `realtime_anomaly_alerts`</p>

        <details className="rt-details" open>
          <summary>realtime_event_metrics（最近 30 条）</summary>
          <div className="rt-table-wrap">
            <table className="rt-table">
              <thead>
                <tr>{tableHeaders.eventMetrics.map((h) => <th key={h}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {eventRows.map((row, idx) => (
                  <tr key={`${row.windowStart}-${row.repoName}-${idx}`}>
                    <td>{row.windowStart}</td>
                    <td>{row.eventType}</td>
                    <td>
                      <a className="rt-link" href={repoUrl(row.repoName)} target="_blank" rel="noreferrer">
                        {row.repoName}
                      </a>
                    </td>
                    <td>{row.actorCategory}</td>
                    <td>{row.eventCount}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>

        <details className="rt-details">
          <summary>realtime_repo_scores（最新窗口 Top 30）</summary>
          <div className="rt-table-wrap">
            <table className="rt-table">
              <thead>
                <tr>{tableHeaders.repoScores.map((h) => <th key={h}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {repoRows.map((row, idx) => (
                  <tr key={`${row.repoName}-${idx}`}>
                    <td>{row.windowStart}</td>
                    <td>
                      <a className="rt-link" href={repoUrl(row.repoName)} target="_blank" rel="noreferrer">
                        {row.repoName}
                      </a>
                    </td>
                    <td>{row.hotnessScore.toFixed(2)}</td>
                    <td>{row.pushEvents}</td>
                    <td>{row.watchEvents}</td>
                    <td>{row.forkEvents}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>

        <details className="rt-details">
          <summary>realtime_anomaly_alerts（最近 30 条）</summary>
          <div className="rt-table-wrap">
            <table className="rt-table">
              <thead>
                <tr>{tableHeaders.alerts.map((h) => <th key={h}>{h}</th>)}</tr>
              </thead>
              <tbody>
                {alertRows.map((row, idx) => (
                  <tr key={`${row.windowStart}-${row.repoName}-${idx}`}>
                    <td>{row.windowStart}</td>
                    <td>
                      <a className="rt-link" href={repoUrl(row.repoName)} target="_blank" rel="noreferrer">
                        {row.repoName}
                      </a>
                    </td>
                    <td>{row.currentEvents}</td>
                    <td>{row.baselineEvents.toFixed(2)}</td>
                    <td>{row.anomalyRatio.toFixed(2)}</td>
                    <td>
                      <span className={row.alertLevel === "high" ? "rt-pill rt-pill-high" : "rt-pill rt-pill-medium"}>
                        {row.alertLevel}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      </section>

      <p className="rt-note" style={{ marginBottom: 8 }}>
        当前 Top 仓库事件总量（Push + Watch + Fork）：{fmt(totalTopRepoEvents)}
      </p>
    </PixelPageShell>
  );
}
