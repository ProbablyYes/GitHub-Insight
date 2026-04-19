"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const items = [
  { href: "/offline/ecosystem", label: "Ecosystem" },
  { href: "/offline/repos", label: "Repos" },
  { href: "/offline/people", label: "People" },
  { href: "/offline/network", label: "Network" },
  { href: "/offline/ml", label: "ML Lab" },
] as const;

export function OfflineSubnav() {
  const pathname = usePathname();

  return (
    <div
      style={{
        display: "flex",
        flexWrap: "wrap",
        gap: 10,
        alignItems: "center",
        marginBottom: 16,
      }}
    >
      {items.map((it) => {
        const active =
          pathname === it.href ||
          pathname.startsWith(`${it.href}/`) ||
          (it.href === "/offline/ecosystem" && pathname === "/offline/overview") ||
          (it.href === "/offline/people" && pathname === "/offline/org-users") ||
          (it.href === "/offline/repos" && pathname === "/offline/risk");
        return (
          <Link
            key={it.href}
            href={it.href}
            className={`nes-btn ${active ? "is-primary" : ""}`}
            style={{
              padding: "6px 12px",
              textDecoration: "none",
              display: "inline-block",
              cursor: "pointer",
            }}
          >
            {active ? ">> " : ""}
            {it.label}
          </Link>
        );
      })}
    </div>
  );
}
