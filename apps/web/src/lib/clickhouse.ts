import { createClient } from "@clickhouse/client";

const url = process.env.CLICKHOUSE_URL ?? "http://127.0.0.1:8123";
const username = process.env.CLICKHOUSE_USER ?? "analytics";
const password = process.env.CLICKHOUSE_PASSWORD ?? "analytics";
const database = process.env.CLICKHOUSE_DATABASE ?? "github_analytics";

export const clickhouse = createClient({
  url,
  username,
  password,
  database,
});
