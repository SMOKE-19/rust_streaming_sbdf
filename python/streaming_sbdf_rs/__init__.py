"""Rust-backed streaming SBDF writer and Parquet export helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb

from .streaming_sbdf_rs import SBDFError, StreamingSbdfWriter

_DUCKDB_MEMORY_LIMIT = "4GB"


def parquet_to_sbdf_streaming(
    parquet_path: str | Path,
    sbdf_path: str | Path,
    *,
    batch_size: int = 50_000,
    where_sql: str | None = None,
    order_by_sql: str | None = None,
    select_sql: str = "*",
    column_types: dict[str, str] | None = None,
) -> Path:
    """Stream Parquet rows into SBDF table slices."""
    parquet_path = Path(parquet_path)
    sbdf_path = Path(sbdf_path)
    sbdf_path.parent.mkdir(parents=True, exist_ok=True)

    sql = _build_parquet_select_sql(
        parquet_path,
        where_sql=where_sql,
        order_by_sql=order_by_sql,
        select_sql=select_sql,
    )
    with _connect_duckdb() as connection:
        if column_types is None:
            column_types = _infer_spotfire_types(connection, sql)
        cursor = connection.execute(sql)
        columns = [item[0] for item in cursor.description]
        writer = StreamingSbdfWriter(
            str(sbdf_path),
            columns=columns,
            column_types=column_types,
        )
        try:
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch = {column: [] for column in columns}
                for row in rows:
                    for index, column in enumerate(columns):
                        batch[column].append(row[index])
                writer.write_batch(batch)
        finally:
            writer.close()
    return sbdf_path


def _build_parquet_select_sql(
    parquet_path: Path,
    *,
    where_sql: str | None,
    order_by_sql: str | None,
    select_sql: str,
) -> str:
    source_sql = _sql_literal(str(parquet_path))
    query = [f"SELECT {select_sql}", f"FROM read_parquet({source_sql})"]
    if where_sql:
        query.append(f"WHERE {where_sql}")
    if order_by_sql:
        query.append(f"ORDER BY {order_by_sql}")
    return "\n".join(query)


def _connect_duckdb() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect(database=":memory:")
    connection.execute("SET memory_limit = ?", [_DUCKDB_MEMORY_LIMIT])
    return connection


def _infer_spotfire_types(
    connection: duckdb.DuckDBPyConnection,
    sql: str,
) -> dict[str, str]:
    describe_sql = f"DESCRIBE {sql}"
    rows = connection.execute(describe_sql).fetchall()
    inferred: dict[str, str] = {}
    for column_name, column_type, *_ in rows:
        inferred[str(column_name)] = _map_duckdb_type_to_spotfire(
            str(column_name),
            str(column_type),
        )
    return inferred


def _map_duckdb_type_to_spotfire(column_name: str, column_type: str) -> str:
    normalized = column_type.upper().strip()

    if normalized in {"BOOLEAN", "BOOL"}:
        return "Boolean"
    if normalized in {"TINYINT", "SMALLINT", "INTEGER", "INT", "UTINYINT", "USMALLINT"}:
        return "Integer"
    if normalized in {"BIGINT", "UBIGINT", "HUGEINT", "UHUGEINT", "UINTEGER"}:
        return "LongInteger"
    if normalized in {"FLOAT", "REAL"}:
        return "SingleReal"
    if normalized in {"DOUBLE", "DOUBLE PRECISION"} or normalized.startswith("DECIMAL("):
        return "Real"
    if normalized == "DATE":
        return "Date"
    if normalized == "TIME" or normalized.startswith("TIME "):
        return "Time"
    if normalized == "INTERVAL":
        return "TimeSpan"
    if normalized.startswith("TIMESTAMP"):
        return "DateTime"
    if normalized in {"VARCHAR", "CHAR", "BPCHAR", "TEXT", "STRING", "UUID", "JSON"}:
        return "String"
    if normalized == "BLOB":
        return "Binary"

    if normalized.endswith("[]") or normalized.startswith(("LIST", "ARRAY", "STRUCT", "MAP", "UNION")):
        raise ValueError(
            "Nested Parquet types are not supported for SBDF export. "
            f"Column '{column_name}' has DuckDB type '{column_type}'. "
            "Cast the column to a supported scalar type in select_sql."
        )

    raise ValueError(
        "Automatic Spotfire type mapping is not available for this Parquet type. "
        f"Column '{column_name}' has DuckDB type '{column_type}'. "
        "Cast the column to a supported scalar type in select_sql or handle it before SBDF export."
    )


def _sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


__all__ = ["SBDFError", "StreamingSbdfWriter", "parquet_to_sbdf_streaming"]
