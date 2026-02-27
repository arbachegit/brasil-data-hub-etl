#!/usr/bin/env python3
"""
Utilitários compartilhados para scripts de projeção econométrica.
"""

import os
import time
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()


def setup_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    return create_client(url, key)


def fetch_all(supabase, table, select="*", filters=None, order=None):
    """Busca todos os registros com paginação (range 0-999)."""
    all_rows = []
    offset = 0
    page_size = 999
    while True:
        q = supabase.table(table).select(select)
        if filters:
            for col, op, val in filters:
                q = getattr(q, op)(col, val)
        if order:
            q = q.order(order)
        q = q.range(offset, offset + page_size)
        resp = q.execute()
        rows = resp.data or []
        all_rows.extend(rows)
        if len(rows) <= page_size:
            break
        offset += page_size + 1
    return all_rows


def batch_insert(supabase, table, records, batch_size=500,
                 on_conflict=None, sleep_time=0.3):
    """Inserção em lotes com upsert opcional."""
    inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            if on_conflict:
                supabase.table(table).upsert(
                    batch, on_conflict=on_conflict
                ).execute()
            else:
                supabase.table(table).insert(batch).execute()
            inserted += len(batch)
            if (i // batch_size) % 10 == 0:
                print(f"  {inserted}/{len(records)} inseridos...")
        except Exception as e:
            print(f"  Erro batch {i}: {e}")
        time.sleep(sleep_time)
    return inserted


def classificar_idh(valor):
    """Classificação PNUD do IDH."""
    if valor is None:
        return None
    if valor >= 0.800:
        return "Muito Alto"
    elif valor >= 0.700:
        return "Alto"
    elif valor >= 0.600:
        return "Médio"
    elif valor >= 0.500:
        return "Baixo"
    else:
        return "Muito Baixo"


# Salário mínimo por ano (valores nominais em R$)
SALARIO_MINIMO = {
    2010: 510.00,
    2011: 545.00,
    2012: 622.00,
    2013: 678.00,
    2014: 724.00,
    2015: 788.00,
    2016: 880.00,
    2017: 937.00,
    2018: 954.00,
    2019: 998.00,
    2020: 1045.00,
    2021: 1100.00,
    2022: 1212.00,
    2023: 1320.00,
    2024: 1412.00,
    2025: 1518.00,
}


# Mapa UF -> Região
UF_REGIAO = {
    11: "Norte", 12: "Norte", 13: "Norte", 14: "Norte",
    15: "Norte", 16: "Norte", 17: "Norte",
    21: "Nordeste", 22: "Nordeste", 23: "Nordeste", 24: "Nordeste",
    25: "Nordeste", 26: "Nordeste", 27: "Nordeste", 28: "Nordeste",
    29: "Nordeste",
    31: "Sudeste", 32: "Sudeste", 33: "Sudeste", 35: "Sudeste",
    41: "Sul", 42: "Sul", 43: "Sul",
    50: "Centro-Oeste", 51: "Centro-Oeste", 52: "Centro-Oeste",
    53: "Centro-Oeste",
}
