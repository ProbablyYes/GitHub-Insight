"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

import { TypingText } from "./typing-text";

const navItems = [
  { href: "/", label: "★ Overview" },
  { href: "/realtime", label: "♦ Realtime" },
  { href: "/offline", label: "♠ Offline" },
];

export type Breadcrumb = { label: string; href?: string };

export function PixelPageShell({
  title,
  subtitle,
  breadcrumbs,
  tldr,
  children,
}: {
  title: string;
  subtitle: string;
  breadcrumbs?: Breadcrumb[];
  tldr?: React.ReactNode;
  children: React.ReactNode;
}) {
  const pathname = usePathname();

  const isActive = (href: string) => {
    if (href === "/") {
      return pathname === "/";
    }
    return pathname === href || pathname.startsWith(`${href}/`);
  };

  return (
    <main
      style={{
        minHeight: "100vh",
        padding: "20px 24px",
        maxWidth: 1200,
        margin: "0 auto",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          marginBottom: 16,
          flexWrap: "wrap",
        }}
      >
        {navItems.map((item) => {
          const active = isActive(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`nes-btn ${active ? "is-success" : ""}`}
              style={{
                padding: "6px 16px",
                textDecoration: "none",
                display: "inline-block",
                cursor: "pointer",
              }}
            >
              {active ? ">> " : ""}
              {item.label}
            </Link>
          );
        })}
        <span style={{ marginLeft: "auto", color: "var(--muted)", fontSize: 12 }}>
          v1.0 <span className="animate-blink" style={{ color: "var(--accent-positive)" }}>●</span>
        </span>
      </div>

      {breadcrumbs && breadcrumbs.length > 0 ? (
        <div
          style={{
            color: "var(--muted-strong)",
            fontSize: "var(--fs-caption)",
            letterSpacing: 1,
            marginBottom: 6,
          }}
        >
          {breadcrumbs.map((b, i) => (
            <span key={`${b.label}-${i}`}>
              {i > 0 ? <span style={{ color: "var(--muted)" }}> › </span> : null}
              {b.href ? (
                <Link href={b.href} style={{ color: "var(--accent-info)" }}>
                  {b.label}
                </Link>
              ) : (
                <span>{b.label}</span>
              )}
            </span>
          ))}
        </div>
      ) : null}

      <div style={{ marginBottom: 24 }}>
        <p
          style={{
            color: "var(--muted)",
            fontSize: "var(--fs-caption)",
            letterSpacing: 4,
            marginBottom: 4,
          }}
        >
          GITHUB INSIGHT
        </p>
        <h1
          style={{
            color: "var(--accent-positive)",
            fontSize: "var(--fs-hero)",
            margin: 0,
            lineHeight: 1.4,
          }}
          className="animate-glow"
        >
          <TypingText text={title} speed={50} />
        </h1>
        <p
          style={{
            color: "var(--muted-strong)",
            fontSize: "var(--fs-body)",
            marginTop: 6,
            lineHeight: "var(--lh-tight)",
          }}
        >
          {subtitle}
        </p>
        {tldr ? (
          <div
            style={{
              marginTop: 10,
              padding: "8px 12px",
              border: "2px dashed var(--divider)",
              color: "var(--fg)",
              fontSize: "var(--fs-body)",
              lineHeight: "var(--lh-tight)",
            }}
          >
            <span style={{ color: "var(--accent-change)", marginRight: 6 }}>TL;DR</span>
            {tldr}
          </div>
        ) : null}
      </div>

      {children}
    </main>
  );
}
