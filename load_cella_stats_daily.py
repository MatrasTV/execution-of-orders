#!/usr/bin/env python3
"""Daily Cella statistics loader.

This script reads daily stats for a specific Cella from two XLS reports and one
CSV forecast file and stores the aggregated result in PostgreSQL.

See README or script docstring for usage details.
"""
from __future__ import annotations

import argparse
import os
import sys
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
    """Determine the statistics date based on CLI argument and current day."""
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

def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load daily Cella statistics")
    parser.add_argument("--cella", required=True, help="Cella value to filter")
    parser.add_argument("--date", help="Stats date YYYY-MM-DD")
    parser.add_argument("--partial", required=True, help="Path to 'Частично.xls'")
    parser.add_argument("--full", required=True, help="Path to 'Целиком.xls'")
    parser.add_argument(
        "--forecast", required=True, help="Path to forecast CSV file"
    )
    parser.add_argument(
        "--date-col",
        default="Плановая дата поставки",
        help="Column name with planned delivery date",
    )
    parser.add_argument(
        "--cella-col", default="Cella", help="Column name with Cella in XLS"
    )
    parser.add_argument(
        "--csv-cella-col",
        help="Optional column name with Cella in forecast CSV",
    )
    parser.add_argument("--host", default=os.getenv("PGHOST"), help="DB host")
    parser.add_argument(
        "--port", type=int, default=int(os.getenv("PGPORT", 5432)), help="DB port"
    )
    parser.add_argument("--dbname", default=os.getenv("PGDATABASE"), help="DB name")
    parser.add_argument("--user", default=os.getenv("PGUSER"), help="DB user")
    parser.add_argument(
        "--password", default=os.getenv("PGPASSWORD"), help="DB password"
    )
    parser.add_argument("--schema", default="REPORT", help="DB schema")
    parser.add_argument(
        "--table", default="execution-of-orders", help="DB table name"
    )
    parser.add_argument(
        "--tz", default="Europe/Moscow", help="Timezone for date calculations"
    )

    if argv is None:
        argv = sys.argv[1:]
    if not argv:
        parser.print_help()
        parser.exit(1)

    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)

    stats_date = determine_stats_date(args.date, args.tz)
    print(f"Stats date: {stats_date}")
    print(f"Parameters: {args}")

    partial_count = count_xls_rows(
        args.partial, args.date_col, args.cella_col, args.cella, stats_date
    )
    full_count = count_xls_rows(
        args.full, args.date_col, args.cella_col, args.cella, stats_date
    )
    expected = compute_expected(args.forecast, args.cella, args.csv_cella_col)

    print(
        "Computed metrics:",
        {
            "partial_count": partial_count,
            "full_count": full_count,
            "expected": float(expected),
        },
    )

    conn = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )
    row = upsert_stats(
        conn,
        args.schema,
        args.table,
        stats_date,
        args.cella,
        partial_count,
        full_count,
        expected,
    )
    conn.close()

    print("DB record:", row)


if __name__ == "__main__":
    main()
