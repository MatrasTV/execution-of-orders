#!/usr/bin/env python3
"""Daily Cella statistics loader.

This script reads daily stats from two XLS reports and one CSV forecast file
and stores the aggregated result in PostgreSQL. When the ``CELLA`` environment
variable is set, only that Cella is processed; otherwise statistics for all
Cellas found in the reports are loaded.

Configuration is taken from environment variables. Specify PostgreSQL
connection variables (``PGHOST``, ``PGPORT``, ``PGDATABASE``, ``PGUSER``,
``PGPASSWORD``). File paths default to ``Частично.xls``, ``Целиком.xls`` and
``Почасовой прогноз прихода заказов на склад.csv``.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Dict, Optional

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


def resolve_path(path: str, base_dir: Optional[str]) -> str:
    """Return an absolute path by prepending ``base_dir`` if provided."""
    if os.path.isabs(path) or not base_dir:
        return path
    return os.path.join(base_dir, path)


def count_xls_rows(
    path: str, date_col: str, cella_col: str, stats_date: date, cella: Optional[str]
) -> pd.Series:
    """Count rows in an XLS file for the given date grouped by Cella."""
    try:
        df = pd.read_excel(path, engine="xlrd")
    except ImportError as exc:  # pragma: no cover - dependency check
        raise SystemExit(
            "Missing optional dependency 'xlrd'. Install it with 'pip install xlrd'."
        ) from exc
    if cella:
        df = df[df[cella_col] == cella]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    df = df[df[date_col] == stats_date]
    return df.groupby(cella_col).size()


def compute_expected(
    path: str, cella_col: Optional[str], cella: Optional[str]
) -> Dict[str, Decimal]:
    """Compute expected value from CSV forecast file grouped by Cella."""
    df = pd.read_csv(path, sep=None, engine="python")
    col = find_expected_column(df)
    df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=[col])

    if cella_col and cella_col in df.columns:
        if cella:
            df = df[df[cella_col] == cella]
        grouped = df.groupby(cella_col)[col].sum()
        return {c: Decimal(str(v)) for c, v in grouped.items()}

    total = Decimal(str(df[col].sum()))
    key = cella if cella else "__all__"
    return {key: total}


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
    cella = os.getenv("CELLA")
    tz_name = getenv("TZ", "Europe/Moscow")
    stats_date = determine_stats_date(os.getenv("STATS_DATE"), tz_name)

    base_dir = os.getenv("DATA_DIR")
    partial_path = resolve_path(getenv("PARTIAL_XLS", "Частично.xls"), base_dir)
    full_path = resolve_path(getenv("FULL_XLS", "Целиком.xls"), base_dir)
    forecast_path = resolve_path(
        getenv("FORECAST_CSV", "Почасовой прогноз прихода заказов на склад.csv"),
        base_dir,
    )

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
            "data_dir": base_dir,
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

    partial_counts = count_xls_rows(
        partial_path, date_col, cella_col, stats_date, cella
    )
    full_counts = count_xls_rows(full_path, date_col, cella_col, stats_date, cella)
    expected_map = compute_expected(forecast_path, csv_cella_col, cella)
    default_expected = expected_map.pop("__all__", Decimal("0"))

    if cella:
        cellas = {cella}
    else:
        cellas = set(partial_counts.index) | set(full_counts.index) | set(expected_map.keys())

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )

    for c in sorted(cellas):
        partial_count = int(partial_counts.get(c, 0))
        full_count = int(full_counts.get(c, 0))
        expected = expected_map.get(c, default_expected)

        print(
            "Computed metrics:",
            {
                "cella": c,
                "partial_count": partial_count,
                "full_count": full_count,
                "expected": float(expected),
            },
        )

        row = upsert_stats(
            conn,
            schema,
            table,
            stats_date,
            c,
            partial_count,
            full_count,
            expected,
        )
        print("DB record:", row)

    conn.close()


if __name__ == "__main__":
    main()
