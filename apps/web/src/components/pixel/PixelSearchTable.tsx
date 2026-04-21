"use client";

import { useMemo, useState, type ReactNode } from "react";

type ColumnAlign = "left" | "right" | "center";

export type PixelSearchColumn<T> = {
  key: string;
  header: string;
  align?: ColumnAlign;
  width?: number | string;
  render?: (row: T, index: number) => ReactNode;
  sortValue?: (row: T) => number | string;
  searchValue?: (row: T) => string;
};

type Props<T> = {
  rows: T[];
  columns: PixelSearchColumn<T>[];
  searchable?: boolean;
  searchPlaceholder?: string;
  pageSize?: number;
  emptyHint?: string;
  initialSort?: { key: string; desc?: boolean };
  getRowKey?: (row: T, index: number) => string | number;
  stickyHeader?: boolean;
  fontSize?: number | string;
  csvFilename?: string;
};

function csvCell(value: unknown): string {
  if (value == null) return "";
  const s = String(value);
  if (/[,\n"]/.test(s)) return `"${s.replaceAll('"', '""')}"`;
  return s;
}

function rowsToCsv<T>(rows: T[], columns: PixelSearchColumn<T>[]): string {
  const header = columns.map((c) => csvCell(c.header)).join(",");
  const body = rows
    .map((r) =>
      columns
        .map((c) => {
          const v = c.sortValue
            ? c.sortValue(r)
            : c.searchValue
            ? c.searchValue(r)
            : (r as unknown as Record<string, unknown>)[c.key];
          return csvCell(v);
        })
        .join(","),
    )
    .join("\n");
  return `${header}\n${body}`;
}

function downloadCsv(filename: string, csv: string) {
  if (typeof window === "undefined") return;
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

function defaultKey<T>(row: T, index: number): string {
  void row;
  return `r${index}`;
}

export function PixelSearchTable<T>({
  rows,
  columns,
  searchable = true,
  searchPlaceholder = "filter...",
  pageSize = 20,
  emptyHint = "No data.",
  initialSort,
  getRowKey = defaultKey,
  stickyHeader = true,
  fontSize = 12,
  csvFilename,
}: Props<T>) {
  const [query, setQuery] = useState("");
  const [sortKey, setSortKey] = useState<string | null>(initialSort?.key ?? null);
  const [sortDesc, setSortDesc] = useState<boolean>(initialSort?.desc ?? false);
  const [page, setPage] = useState(0);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q || !searchable) return rows;
    return rows.filter((row) =>
      columns.some((c) => {
        const v = c.searchValue ? c.searchValue(row) : String((row as unknown as Record<string, unknown>)[c.key] ?? "");
        return v.toLowerCase().includes(q);
      })
    );
  }, [rows, query, columns, searchable]);

  const sorted = useMemo(() => {
    if (!sortKey) return filtered;
    const col = columns.find((c) => c.key === sortKey);
    if (!col) return filtered;
    const copy = [...filtered];
    copy.sort((a, b) => {
      const va = col.sortValue
        ? col.sortValue(a)
        : ((a as unknown as Record<string, unknown>)[sortKey] as number | string);
      const vb = col.sortValue
        ? col.sortValue(b)
        : ((b as unknown as Record<string, unknown>)[sortKey] as number | string);
      if (va == null && vb == null) return 0;
      if (va == null) return 1;
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number") {
        return sortDesc ? vb - va : va - vb;
      }
      const sa = String(va);
      const sb = String(vb);
      return sortDesc ? sb.localeCompare(sa) : sa.localeCompare(sb);
    });
    return copy;
  }, [filtered, sortKey, sortDesc, columns]);

  const totalPages = Math.max(Math.ceil(sorted.length / pageSize), 1);
  const curPage = Math.min(page, totalPages - 1);
  const pageRows = sorted.slice(curPage * pageSize, (curPage + 1) * pageSize);

  function toggleSort(key: string) {
    if (sortKey === key) {
      setSortDesc((d) => !d);
    } else {
      setSortKey(key);
      setSortDesc(true);
    }
  }

  const toolbarShown = searchable || Boolean(csvFilename);

  return (
    <div style={{ minWidth: 0 }}>
      {toolbarShown ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "var(--pixel-space-3)",
            marginBottom: "var(--pixel-space-3)",
            flexWrap: "wrap",
          }}
        >
          {searchable ? (
            <>
              <input
                className="nes-input"
                type="text"
                placeholder={searchPlaceholder}
                value={query}
                onChange={(e) => {
                  setQuery(e.target.value);
                  setPage(0);
                }}
                style={{
                  fontFamily: '"Zpix", monospace',
                  fontSize: 12,
                  padding: "4px 8px",
                  maxWidth: 320,
                }}
              />
              <span style={{ color: "var(--muted)", fontSize: "var(--fs-caption)" }}>
                {sorted.length} row{sorted.length === 1 ? "" : "s"}
              </span>
            </>
          ) : null}
          {csvFilename ? (
            <button
              type="button"
              className="nes-btn"
              style={{ padding: "2px 10px", fontSize: 11, marginLeft: "auto" }}
              onClick={() => downloadCsv(csvFilename, rowsToCsv(sorted, columns))}
              title="Download current view (filtered + sorted) as CSV"
            >
              ⇣ CSV
            </button>
          ) : null}
        </div>
      ) : null}

      <div
        style={{
          overflowX: "auto",
          overflowY: sorted.length > pageSize ? "auto" : "visible",
          maxHeight: sorted.length > pageSize ? 520 : undefined,
        }}
        className="pixel-scroll"
      >
        <table
          suppressHydrationWarning
          style={{ width: "100%", borderCollapse: "collapse", fontSize }}
        >
          <thead
            style={
              stickyHeader
                ? { position: "sticky", top: 0, background: "var(--panel)", zIndex: 1 }
                : undefined
            }
          >
            <tr style={{ color: "var(--muted-strong)", borderBottom: "1px solid var(--divider)" }}>
              {columns.map((c) => {
                const sortable = Boolean(c.sortValue) || c.key;
                const isSorted = sortKey === c.key;
                return (
                  <th
                    key={c.key}
                    style={{
                      textAlign: c.align ?? "left",
                      padding: "6px 6px",
                      width: c.width,
                      cursor: sortable ? "pointer" : "default",
                      userSelect: "none",
                      whiteSpace: "nowrap",
                    }}
                    onClick={() => sortable && toggleSort(c.key)}
                  >
                    {c.header}
                    {isSorted ? (
                      <span style={{ color: "var(--accent-info)", marginLeft: 4 }}>
                        {sortDesc ? "▼" : "▲"}
                      </span>
                    ) : null}
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody>
            {pageRows.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  style={{ padding: "var(--pixel-space-4)", textAlign: "center", color: "var(--muted)" }}
                >
                  {emptyHint}
                </td>
              </tr>
            ) : null}
            {pageRows.map((row, idx) => (
              <tr
                key={getRowKey(row, curPage * pageSize + idx)}
                style={{
                  borderBottom: "1px solid var(--divider)",
                }}
              >
                {columns.map((c) => {
                  const rendered = c.render
                    ? c.render(row, curPage * pageSize + idx)
                    : (row as unknown as Record<string, unknown>)[c.key];
                  return (
                    <td
                      key={c.key}
                      style={{
                        textAlign: c.align ?? "left",
                        padding: "5px 6px",
                        color: "var(--fg)",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {rendered as ReactNode}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {totalPages > 1 ? (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "flex-end",
            gap: "var(--pixel-space-2)",
            marginTop: "var(--pixel-space-2)",
            color: "var(--muted-strong)",
            fontSize: "var(--fs-caption)",
          }}
        >
          <button
            type="button"
            className="nes-btn"
            style={{ padding: "2px 8px", fontSize: 11 }}
            disabled={curPage === 0}
            onClick={() => setPage((p) => Math.max(0, p - 1))}
          >
            «
          </button>
          <span>
            {curPage + 1} / {totalPages}
          </span>
          <button
            type="button"
            className="nes-btn"
            style={{ padding: "2px 8px", fontSize: 11 }}
            disabled={curPage >= totalPages - 1}
            onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
          >
            »
          </button>
        </div>
      ) : null}
    </div>
  );
}
