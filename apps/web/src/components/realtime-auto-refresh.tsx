"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";

type RealtimeAutoRefreshProps = {
  intervalMs?: number;
};

export function RealtimeAutoRefresh({ intervalMs = 5000 }: RealtimeAutoRefreshProps) {
  const router = useRouter();
  const refreshingRef = useRef(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [now, setNow] = useState(() => Date.now());
  const [lastRefreshAt, setLastRefreshAt] = useState(() => Date.now());
  const [autoRefresh, setAutoRefresh] = useState(false);

  const triggerRefresh = useCallback(() => {
    if (refreshingRef.current) {
      return;
    }
    refreshingRef.current = true;
    setIsRefreshing(true);
    router.refresh();
    window.setTimeout(() => {
      refreshingRef.current = false;
      setIsRefreshing(false);
      setLastRefreshAt(Date.now());
    }, 1200);
  }, [router]);

  useEffect(() => {
    const ticker = setInterval(() => {
      setNow(Date.now());
    }, 1000);

    return () => clearInterval(ticker);
  }, []);

  useEffect(() => {
    if (!autoRefresh) {
      return;
    }

    const timer = setInterval(() => {
      triggerRefresh();
    }, intervalMs);

    return () => clearInterval(timer);
  }, [autoRefresh, intervalMs, triggerRefresh]);

  const secondsLeft = useMemo(() => {
    if (!autoRefresh) {
      return "--";
    }
    const remainder = now % intervalMs;
    return String(Math.ceil((intervalMs - remainder) / 1000)).padStart(2, "0");
  }, [autoRefresh, intervalMs, now]);

  const timeText = useMemo(
    () =>
      new Date(now).toLocaleTimeString("zh-CN", {
        hour12: false,
      }),
    [now],
  );
  const lastRefreshText = useMemo(
    () =>
      new Date(lastRefreshAt).toLocaleTimeString("zh-CN", {
        hour12: false,
      }),
    [lastRefreshAt],
  );

  return (
    <section className="rt-refresh-panel animate-float delay-1">
      <button
        type="button"
        className={`nes-btn ${isRefreshing ? "is-disabled" : "is-warning"} rt-refresh-button`}
        onClick={triggerRefresh}
      >
        {isRefreshing ? "REFRESHING..." : "立即刷新"}
      </button>

      <button
        type="button"
        className={`nes-btn ${autoRefresh ? "is-success" : "is-primary"}`}
        onClick={() => setAutoRefresh((prev) => !prev)}
      >
        {autoRefresh ? "自动刷新: ON" : "自动刷新: OFF"}
      </button>

      <div className="rt-refresh-meta">
        <span>当前时间 {timeText}</span>
        <span>上次刷新 {lastRefreshText}</span>
        <span>下次刷新 {secondsLeft}s</span>
      </div>
    </section>
  );
}
