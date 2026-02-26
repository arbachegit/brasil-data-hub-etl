#!/usr/bin/env python3
"""
Preenche pib_agropecuaria_pct, pib_industria_pct, pib_servicos_pct,
pib_adm_publica_pct na tabela pib_estados para 2015-2021.
Fonte: SIDRA tabela 5938 - PIB dos Municípios.
"""

import os
import requests
import time
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SIDRA_BASE = "https://apisidra.ibge.gov.br/values"

# Variáveis: participação % no VA total
VAR_MAP = {
    "516": "pib_agropecuaria_pct",
    "520": "pib_industria_pct",
    "6574": "pib_servicos_pct",
    "528": "pib_adm_publica_pct",
}


def main():
    print("=" * 60)
    print("PIB ESTADOS - SETOR % (2015-2021)")
    print("=" * 60)

    # Buscar dados do SIDRA
    anos = ",".join(str(a) for a in range(2015, 2022))
    vars_str = ",".join(VAR_MAP.keys())
    url = f"{SIDRA_BASE}/t/5938/n3/all/v/{vars_str}/p/{anos}"

    print(f"\nBuscando SIDRA t/5938...")
    resp = requests.get(url, timeout=120)
    data = resp.json()
    registros = data[1:] if len(data) > 1 else []
    print(f"  {len(registros)} registros retornados")

    # Organizar: {(uf, ano): {campo: valor}}
    updates = {}
    for r in registros:
        uf = int(r["D1C"])
        ano = int(r["D3C"])
        var = r["D2C"]
        val = r.get("V", "")

        if val in ("...", "-", "", None):
            continue

        campo = VAR_MAP.get(var)
        if not campo:
            continue

        key = (uf, ano)
        if key not in updates:
            updates[key] = {}
        try:
            updates[key][campo] = float(val)
        except (ValueError, TypeError):
            pass

    print(f"  {len(updates)} combinações (uf, ano) com dados")

    # Buscar registros existentes do pib_estados
    all_recs = []
    offset = 0
    while True:
        resp = supabase.table("pib_estados").select(
            "id,codigo_ibge_uf,ano,pib_agropecuaria_pct"
        ).range(offset, offset + 999).execute()
        all_recs.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000

    print(f"  {len(all_recs)} registros no pib_estados")

    # Atualizar
    filled = 0
    errors = 0
    for rec in all_recs:
        uf = rec["codigo_ibge_uf"]
        ano = rec["ano"]

        if rec.get("pib_agropecuaria_pct") is not None:
            continue  # já tem dados

        data = updates.get((uf, ano))
        if not data:
            continue

        try:
            supabase.table("pib_estados").update(data).eq("id", rec["id"]).execute()
            filled += 1
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"  Erro: {e}")

    print(f"\n{'='*60}")
    print(f"TOTAL: {filled} registros atualizados, {errors} erros")
    print("=" * 60)

    # Verificar SP
    r = supabase.table("pib_estados").select(
        "ano,pib_agropecuaria_pct,pib_industria_pct,pib_servicos_pct,pib_adm_publica_pct"
    ).eq("codigo_ibge_uf", 35).order("ano").execute()
    print("\nSão Paulo (35):")
    for rec in r.data:
        if rec.get("pib_agropecuaria_pct"):
            print(f"  {rec['ano']}: agro={rec['pib_agropecuaria_pct']}% "
                  f"ind={rec['pib_industria_pct']}% "
                  f"serv={rec['pib_servicos_pct']}% "
                  f"adm={rec['pib_adm_publica_pct']}%")


if __name__ == "__main__":
    main()
