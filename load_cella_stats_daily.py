#!/usr/bin/env python3
"""Daily Cella statistics loader.

This script reads daily stats from two XLS reports and one CSV forecast file
and stores the aggregated result in PostgreSQL. All configuration values are
hard coded below so the script can be launched without any command line
options or environment variables.
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
# Configuration
# ---------------------------------------------------------------------------

# Base directory containing the input files
DATA_DIR = r"\\192.168.3.7\ul\Закупки\DATA"

# Input file paths
PARTIAL_XLS = os.path.join(DATA_DIR, "Частично.xls")
FULL_XLS = os.path.join(DATA_DIR, "Целиком.xls")
FORECAST_CSV = os.path.join(DATA_DIR, "Почасовой прогноз прихода заказов на склад.csv")

# Report parameters
DATE_COL = "Плановая дата поставки"
CELLA_COL = "Cella"
CSV_CELLA_COL: Optional[str] = None
CELLA: Optional[str] = None  # Process all Cellas by default
TZ_NAME = "Europe/Moscow"

# PostgreSQL connection
HOST = "192.168.3.19"
PORT = 5432
DBNAME = "postgres"
USER = "Admin"
PASSWORD = "0782"
SCHEMA = "REPORT"
TABLE = "execution-of-orders"


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
    # Dates in the reports use day-first formatting (e.g. 26.08.2025),
    # so explicitly enable dayfirst parsing to avoid ambiguous warnings.
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", dayfirst=True).dt.date
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

def main() -> None:
    cella = CELLA
    tz_name = TZ_NAME
    stats_date = determine_stats_date(None, tz_name)

    partial_path = PARTIAL_XLS
    full_path = FULL_XLS
    forecast_path = FORECAST_CSV

    date_col = DATE_COL
    cella_col = CELLA_COL
    csv_cella_col = CSV_CELLA_COL

    host = HOST
    port = PORT
    dbname = DBNAME
    user = USER
    password = PASSWORD
    schema = SCHEMA
    table = TABLE

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
