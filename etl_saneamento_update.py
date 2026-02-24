#!/usr/bin/env python3
"""
Atualiza tabela saneamento_municipios com dados de esgoto e lixo.
  - Esgoto: SIDRA t/3154 c299 (Censo 2010)
  - Lixo: SIDRA t/6892 c67 (Censo 2022 - único disponível municipal)
"""

import os
import time
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SIDRA_BASE = "https://apisidra.ibge.gov.br/values"
UFS = [11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]


def fetch_sidra(url):
    for tentativa in range(3):
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                registros = data[1:] if len(data) > 1 else []
                return [r for r in registros if r.get("V") and r["V"] not in ("...", "-", "")]
        except Exception as e:
            if tentativa < 2:
                print(f"    Erro: {e} - retry {tentativa+1}")
        time.sleep(3 * (tentativa + 1))
    return []


def main():
    print("=" * 60)
    print("ATUALIZANDO SANEAMENTO - ESGOTO + LIXO")
    print("=" * 60)

    # 1. ESGOTO - Censo 2010, t/3154, c299
    # Codes: 10941=rede geral, 10942=fossa séptica, 10962=outro, 10963=sem banheiro
    print("\n1. Buscando ESGOTO (t/3154 c299, 27 UFs)...")
    esgoto = {}

    for code, field in [("10941", "esgoto_rede_geral"), ("10942", "esgoto_fossa_septica"),
                         ("10962", "esgoto_outro"), ("10963", "esgoto_sem")]:
        print(f"  Buscando {field}...")
        for uf in UFS:
            url = f"{SIDRA_BASE}/t/3154/n6/in%20n3%20{uf}/v/96/p/2010/c299/{code}"
            registros = fetch_sidra(url)
            for r in registros:
                cod = r["D1C"]
                if cod not in esgoto:
                    esgoto[cod] = {}
                try:
                    esgoto[cod][field] = int(r["V"])
                except (ValueError, TypeError):
                    pass
            time.sleep(0.3)
        count = sum(1 for d in esgoto.values() if field in d)
        print(f"    -> {count} municípios")

    # 2. LIXO - Censo 2022, t/6892, c67
    # Codes: 2520=Coletado, 10972=Total
    print("\n2. Buscando LIXO (t/6892 c67, Censo 2022, 27 UFs)...")
    lixo = {}
    for uf in UFS:
        url = f"{SIDRA_BASE}/t/6892/n6/in%20n3%20{uf}/v/381/p/2022/c67/2520"
        registros = fetch_sidra(url)
        for r in registros:
            cod = r["D1C"]
            try:
                lixo[cod] = int(r["V"])
            except (ValueError, TypeError):
                pass
        time.sleep(0.3)
    print(f"  -> {len(lixo)} municípios com dados de lixo coletado")

    # 3. Atualizar registros no Supabase
    print("\n3. Atualizando registros no Supabase...")
    updated = 0
    errors = 0

    # Get all existing records
    all_records = []
    offset = 0
    while True:
        resp = supabase.table("saneamento_municipios").select(
            "id,codigo_ibge,domicilios_total"
        ).range(offset, offset + 999).execute()
        all_records.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000

    print(f"  {len(all_records)} registros existentes")

    for rec in all_records:
        cod = str(rec["codigo_ibge"])
        dom_total = rec.get("domicilios_total", 0) or 0
        update_data = {}

        # Esgoto
        if cod in esgoto:
            eg = esgoto[cod]
            update_data["esgoto_rede_geral"] = eg.get("esgoto_rede_geral")
            update_data["esgoto_fossa_septica"] = eg.get("esgoto_fossa_septica")
            update_data["esgoto_outro"] = eg.get("esgoto_outro")
            update_data["esgoto_sem"] = eg.get("esgoto_sem")
            rede = eg.get("esgoto_rede_geral", 0) or 0
            fossa = eg.get("esgoto_fossa_septica", 0) or 0
            if dom_total > 0:
                update_data["pct_esgoto_adequado"] = round((rede + fossa) / dom_total * 100, 2)

        # Lixo
        if cod in lixo:
            update_data["lixo_coletado"] = lixo[cod]
            if dom_total > 0:
                update_data["pct_lixo_coletado"] = round(lixo[cod] / dom_total * 100, 2)
                update_data["lixo_outro"] = max(0, dom_total - lixo[cod])

        if update_data:
            try:
                supabase.table("saneamento_municipios").update(
                    update_data
                ).eq("id", rec["id"]).execute()
                updated += 1
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"  Erro: {e}")

        if updated % 1000 == 0 and updated > 0:
            print(f"    {updated} atualizados...")

    print(f"\n{'='*60}")
    print(f"TOTAL: {updated} registros atualizados, {errors} erros")
    print("=" * 60)

    # Verify
    r = supabase.table("saneamento_municipios").select("*").eq("codigo_ibge", 3550308).execute()
    if r.data:
        print("\nSão Paulo (SP):")
        for k, v in r.data[0].items():
            if k not in ("id", "created_at"):
                print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
