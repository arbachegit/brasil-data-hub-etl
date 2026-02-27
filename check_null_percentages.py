#!/usr/bin/env python3
"""
Check NULL percentages across all main tables in Supabase.
Fetches total row counts and samples up to 5000 rows per table
to compute NULL % per column.
"""

import os
import sys
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLES = [
    "dim_politicos",
    "fato_politicos_mandatos",
    "fato_bens_candidato",
    "fato_receitas_campanha",
    "fato_votos_legislativos",
    "fato_perfil_filiacao_partidaria",
    "idh_municipios",
    "emprego_municipios",
    "saneamento_municipios",
    "pib_municipios",
    "pop_municipios",
    "mortalidade_municipios",
    "financas_municipios",
    "fato_emendas_parlamentares",
]

SAMPLE_SIZE = 5000


def get_row_count(table_name):
    """Get exact row count for a table."""
    try:
        resp = supabase.table(table_name).select("id", count="exact").limit(1).execute()
        return resp.count if resp.count is not None else 0
    except Exception as e:
        try:
            resp = supabase.table(table_name).select("*", count="exact").limit(1).execute()
            return resp.count if resp.count is not None else 0
        except Exception as e2:
            print(f"  ERROR counting rows: {e2}")
            return 0


def get_sample(table_name):
    """Fetch a sample of up to SAMPLE_SIZE rows."""
    try:
        resp = supabase.table(table_name).select("*").range(0, SAMPLE_SIZE - 1).execute()
        return resp.data
    except Exception as e:
        print(f"  ERROR fetching sample: {e}")
        return []


def analyze_nulls(rows):
    """Compute NULL count and percentage per column from a list of row dicts."""
    if not rows:
        return {}

    sample_size = len(rows)
    all_columns = set()
    for row in rows:
        all_columns.update(row.keys())

    results = {}
    for col in sorted(all_columns):
        null_count = sum(1 for row in rows if row.get(col) is None)
        pct = (null_count / sample_size) * 100
        results[col] = {"null_count": null_count, "sample_size": sample_size, "pct": pct}

    return results


def main():
    print("=" * 90)
    print("  NULL PERCENTAGE REPORT - Brasil Data Hub")
    print("=" * 90)

    summary = []

    for table in TABLES:
        print(f"\n{'─' * 90}")
        print(f"  TABLE: {table}")
        print(f"{'─' * 90}")

        total_rows = get_row_count(table)
        print(f"  Total rows: {total_rows:,}")

        if total_rows == 0:
            print("  (empty table - skipping)")
            summary.append((table, total_rows, 0, 0.0))
            continue

        rows = get_sample(table)
        sample_size = len(rows)
        print(f"  Sample size: {sample_size:,}")

        if sample_size == 0:
            summary.append((table, total_rows, 0, 0.0))
            continue

        col_stats = analyze_nulls(rows)

        cols_with_nulls = {k: v for k, v in col_stats.items() if v["null_count"] > 0}
        cols_without_nulls = {k: v for k, v in col_stats.items() if v["null_count"] == 0}

        if cols_with_nulls:
            print(f"\n  {'Column':<45} {'Nulls':>8} {'of Sample':>10} {'Null %':>10}")
            print(f"  {'─' * 45} {'─' * 8} {'─' * 10} {'─' * 10}")
            for col in sorted(cols_with_nulls.keys()):
                s = cols_with_nulls[col]
                marker = " <<<" if s["pct"] >= 90 else (" !!" if s["pct"] >= 50 else "")
                print(f"  {col:<45} {s['null_count']:>8,} {s['sample_size']:>10,} {s['pct']:>9.1f}%{marker}")

        if cols_without_nulls:
            print(f"\n  {len(cols_without_nulls)} column(s) with 0% nulls: {', '.join(sorted(cols_without_nulls.keys()))}")

        total_cells = sum(v["sample_size"] for v in col_stats.values())
        total_nulls = sum(v["null_count"] for v in col_stats.values())
        overall_pct = (total_nulls / total_cells * 100) if total_cells > 0 else 0.0

        summary.append((table, total_rows, len(cols_with_nulls), overall_pct))

    print(f"\n\n{'=' * 90}")
    print("  SUMMARY")
    print(f"{'=' * 90}")
    print(f"\n  {'Table':<40} {'Rows':>12} {'Cols w/ Nulls':>14} {'Overall Null %':>15}")
    print(f"  {'─' * 40} {'─' * 12} {'─' * 14} {'─' * 15}")
    for table, total_rows, null_cols, overall_pct in summary:
        print(f"  {table:<40} {total_rows:>12,} {null_cols:>14} {overall_pct:>14.2f}%")

    print(f"\n{'=' * 90}")
    print("  Done.")
    print(f"{'=' * 90}\n")


if __name__ == "__main__":
    main()
