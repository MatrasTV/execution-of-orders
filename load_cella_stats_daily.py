#!/usr/bin/env python3
"""Daily Cella statistics loader.

This script reads daily stats for a specific Cella from two XLS reports and one
CSV forecast file and stores the aggregated result in PostgreSQL.

Configuration is taken from environment variables. At minimum set ``CELLA`` and
PostgreSQL connection variables (``PGHOST``, ``PGPORT``, ``PGDATABASE``,
``PGUSER``, ``PGPASSWORD``). File paths default to ``Частично.xls``,
``Целиком.xls`` and ``Почасовой прогноз прихода заказов на склад.csv``.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Optional

import pandas as pd
import psycopg2
from psycopg2 import sql
from dateutil import parser as date_parser, tz


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def normalize_colname(name: str) -> str:
    """Normalize a column name for comparison.

    Lower case, strip spaces, replace "ё" with "е".
    """
    return name.lower().replace("ё", "е").replace(" ", "")


def find_expected_column(df: pd.DataFrame) -> str:
    """Find the column representing expected quantity in the forecast file.

    Exact match is performed on the normalized name. If no exact match is
    found, a substring search for "ожид" is used.
    """
    normalized = {col: normalize_colname(col) for col in df.columns}
    for col, norm in normalized.items():
        if norm == "ожидается":
            return col
    for col, norm in normalized.items():
        if "ожид" in norm:
            return col
    raise ValueError("Column 'Ожидается' not found in forecast file")


def determine_stats_date(date_str: Optional[str], tz_name: Optional[str]) -> date:
    """Determine the statistics date based on argument and current day."""
    if date_str:
        return date_parser.isoparse(date_str).date()

    tzinfo = tz.gettz(tz_name) if tz_name else None
    today = datetime.now(tzinfo).date()
    if today.weekday() == 0:  # Monday -> use previous Friday
        return today - timedelta(days=3)
    return today - timedelta(days=1)


def count_xls_rows(path: str, date_col: str, cella_col: str, cella: str, stats_date: date) -> int:
    """Count rows in an XLS file for given Cella and date."""
    df = pd.read_excel(path, engine="xlrd")
    df = df[df[cella_col] == cella]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    return int((df[date_col] == stats_date).sum())


def compute_expected(path: str, cella: str, cella_col: Optional[str]) -> Decimal:
    """Compute expected value from CSV forecast file."""
    df = pd.read_csv(path, sep=None, engine="python")
    if cella_col:
        df = df[df[cella_col] == cella]
    if df.empty:
        return Decimal("0")
    col = find_expected_column(df)
    values = pd.to_numeric(df[col], errors="coerce").dropna()
    if values.empty:
        return Decimal("0")
    if len(values) == 1:
        return Decimal(str(values.iloc[0]))
    return Decimal(str(values.sum()))


def upsert_stats(
    conn: psycopg2.extensions.connection,
    schema: str,
    table: str,
    stats_date: date,
    cella: str,
    partial_count: int,
    full_count: int,
    expected: Decimal,
):
    """Create tables if necessary and upsert statistics."""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}" ).format(sql.Identifier(schema)))
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id BIGSERIAL PRIMARY KEY,
                    run_ts TIMESTAMPTZ DEFAULT now(),
                    stats_date DATE NOT NULL,
                    cella TEXT NOT NULL,
                    partial_count INT NOT NULL,
                    full_count INT NOT NULL,
                    expected NUMERIC(18,2) NOT NULL,
                    UNIQUE (cella, stats_date)
                )
                """
            ).format(sql.Identifier(schema), sql.Identifier(table))
        )
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {}.{} (stats_date, cella, partial_count, full_count, expected)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (cella, stats_date) DO UPDATE
                SET partial_count = EXCLUDED.partial_count,
                    full_count = EXCLUDED.full_count,
                    expected = EXCLUDED.expected
                RETURNING id, run_ts
                """
            ).format(sql.Identifier(schema), sql.Identifier(table)),
            (stats_date, cella, partial_count, full_count, expected),
        )
        row = cur.fetchone()
        conn.commit()
        return row


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def getenv(name: str, default: Optional[str] = None, *, required: bool = False) -> str:
    """Return environment variable value or exit if required and missing."""
    val = os.getenv(name, default)
    if required and val is None:
        raise SystemExit(f"Environment variable {name} is required")
    return val


def main() -> None:
    cella = getenv("CELLA", required=True)
    tz_name = getenv("TZ", "Europe/Moscow")
    stats_date = determine_stats_date(os.getenv("STATS_DATE"), tz_name)

    partial_path = getenv("PARTIAL_XLS", "Частично.xls")
    full_path = getenv("FULL_XLS", "Целиком.xls")
    forecast_path = getenv("FORECAST_CSV", "Почасовой прогноз прихода заказов на склад.csv")

    date_col = getenv("DATE_COL", "Плановая дата поставки")
    cella_col = getenv("CELLA_COL", "Cella")
    csv_cella_col = os.getenv("CSV_CELLA_COL")

    host = getenv("PGHOST", "localhost")
    port = int(getenv("PGPORT", "5432"))
    dbname = getenv("PGDATABASE", "postgres")
    user = getenv("PGUSER", "postgres")
    password = getenv("PGPASSWORD", "")
    schema = getenv("SCHEMA", "REPORT")
    table = getenv("TABLE", "execution-of-orders")

    print(f"Stats date: {stats_date}")
    print(
        "Parameters:",
        {
            "cella": cella,
            "partial": partial_path,
            "full": full_path,
            "forecast": forecast_path,
            "date_col": date_col,
            "cella_col": cella_col,
            "csv_cella_col": csv_cella_col,
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "schema": schema,
            "table": table,
            "tz": tz_name,
        },
    )

    partial_count = count_xls_rows(partial_path, date_col, cella_col, cella, stats_date)
    full_count = count_xls_rows(full_path, date_col, cella_col, cella, stats_date)
    expected = compute_expected(forecast_path, cella, csv_cella_col)

    print(
        "Computed metrics:",
        {
            "partial_count": partial_count,
            "full_count": full_count,
            "expected": float(expected),
        },
    )

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    row = upsert_stats(
        conn,
        schema,
        table,
        stats_date,
        cella,
        partial_count,
        full_count,
        expected,
    )
    conn.close()

    print("DB record:", row)


if __name__ == "__main__":
    main()
