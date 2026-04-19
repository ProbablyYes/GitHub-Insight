from __future__ import annotations

from typing import Iterable, Mapping, Sequence

import clickhouse_connect

from jobs.common.config import (
    CLICKHOUSE_DATABASE,
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def insert_records(table: str, records: Sequence[Mapping[str, object]], *, truncate: bool = False) -> None:
    if not records:
        return
    client = get_clickhouse_client()
    if truncate:
        client.command(f"TRUNCATE TABLE {table}")
    client.insert_df(table, _records_to_dataframe(records))


def _records_to_dataframe(records: Iterable[Mapping[str, object]]):
    import pandas as pd

    return pd.DataFrame.from_records(list(records))
