"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { TypingText } from "./typing-text";

const navItems = [
  { href: "/", label: "★ 总览" },
  { href: "/realtime", label: "♦ 实时" },
  { href: "/offline", label: "♠ 离线" },
];

export function PixelPageShell({
  title,
  subtitle,
  children,
}: {
  title: string;
  subtitle: string;
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  return (
    <main style={{ minHeight: "100vh", padding: "20px 24px", maxWidth: 1200, margin: "0 auto" }}>
      {/* ── Slim nav row ── */}
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 20, flexWrap: "wrap" }}>
        {navItems.map((item) => {
          const active = pathname === item.href;
          return (
            <Link key={item.href} href={item.href}>
              <button
                type="button"
                className={`nes-btn ${active ? "is-success" : ""}`}
                style={{ padding: "6px 16px" }}
              >
                {active ? ">> " : ""}{item.label}
              </button>
            </Link>
          );
        })}
        <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: 12 }}>
          v1.0 <span className="animate-blink" style={{ color: "var(--green)" }}>●</span>
        </span>
      </div>

      {/* ── Title area (no nes-container, keep it clean) ── */}
      <div style={{ marginBottom: 28 }}>
        <p style={{ color: "var(--muted)", fontSize: 12, letterSpacing: 4, marginBottom: 4 }}>
          GITHUB INSIGHT
        </p>
        <h1 style={{ color: "var(--green)", fontSize: 24, margin: 0, lineHeight: 1.5 }} className="animate-glow">
          <TypingText text={title} speed={50} />
        </h1>
        <p style={{ color: "var(--muted)", fontSize: 14, marginTop: 8 }}>
          {subtitle}
        </p>
      </div>

      {children}
    </main>
  );
}
