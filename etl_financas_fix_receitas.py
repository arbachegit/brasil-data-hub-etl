#!/usr/bin/env python3
"""
Corrige receita_corrente, receita_capital e transferencias_correntes
para financas_municipios 2014-2017.

O formato dos códigos DCA mudou:
  2014-2017: RO1.0.0.0.00.00.00 (7 segmentos, últimos com 2 dígitos)
  2018+:     RO1.0.0.0.00.0.0   (7 segmentos, últimos com 1 dígito)

O ETL original só usava o formato 2018+, então esses campos ficaram NULL.
Este script re-busca a DCA Anexo I-C para os entes com NULLs e extrai
os valores usando ambos os formatos de código.
"""

import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SICONFI_BASE = "https://apidatalake.tesouro.gov.br/ords/siconfi/tt"

# Ambos os formatos de código de conta
RECEITA_KEYS = {
    # 2018+
    "RO1.0.0.0.00.0.0": "receita_corrente",
    "RO2.0.0.0.00.0.0": "receita_capital",
    "RO1.7.0.0.00.0.0": "transferencias_correntes",
    # 2014-2017
    "RO1.0.0.0.00.00.00": "receita_corrente",
    "RO2.0.0.0.00.00.00": "receita_capital",
    "RO1.7.0.0.00.00.00": "transferencias_correntes",
}


def fetch_dca_receitas(ente_id, ano):
    """Busca receitas do DCA Anexo I-C para um ente/ano."""
    url = f"{SICONFI_BASE}/dca?an_exercicio={ano}&id_ente={ente_id}&no_anexo=DCA-Anexo%20I-C"
    for tentativa in range(3):
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                result = {}
                for item in items:
                    cod = item.get("cod_conta", "")
                    if cod in RECEITA_KEYS:
                        campo = RECEITA_KEYS[cod]
                        val = item.get("valor")
                        if val is not None and campo not in result:
                            result[campo] = float(val)
                return result
            elif resp.status_code == 404:
                return {}
        except Exception as e:
            if tentativa < 2:
                time.sleep(2 * (tentativa + 1))
    return {}


def process_record(rec):
    """Processa um registro: busca DCA e retorna update."""
    result = fetch_dca_receitas(rec["codigo_ibge"], rec["ano"])
    if result:
        return (rec["id"], result)
    return None


def main():
    print("=" * 60)
    print("FINANÇAS - CORREÇÃO RECEITAS 2014-2017")
    print("=" * 60)

    # Buscar registros com receita_corrente NULL
    all_nulls = []
    for ano in range(2014, 2018):
        offset = 0
        while True:
            resp = supabase.table("financas_municipios").select(
                "id,codigo_ibge,ano,receita_corrente"
            ).eq("ano", ano).is_("receita_corrente", "null").range(offset, offset + 999).execute()
            all_nulls.extend(resp.data)
            if len(resp.data) < 1000:
                break
            offset += 1000
        print(f"  {ano}: {len([r for r in all_nulls if r['ano'] == ano])} com receita_corrente NULL")

    print(f"\n  Total a processar: {len(all_nulls)} registros")

    if not all_nulls:
        print("  Nada a fazer!")
        return

    # Processar com ThreadPoolExecutor
    filled = 0
    errors = 0

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {executor.submit(process_record, rec): rec for rec in all_nulls}
        done = 0
        for future in as_completed(futures):
            done += 1
            result = future.result()
            if result:
                rec_id, update_data = result
                try:
                    supabase.table("financas_municipios").update(
                        update_data
                    ).eq("id", rec_id).execute()
                    filled += 1
                except Exception as e:
                    errors += 1
                    if errors <= 3:
                        print(f"  Erro update: {e}")

            if done % 500 == 0:
                print(f"    {done}/{len(all_nulls)} processados, {filled} atualizados...")

    print(f"\n{'='*60}")
    print(f"TOTAL: {filled} atualizados, {errors} erros (de {len(all_nulls)} processados)")
    print("=" * 60)

    # Verificar SP
    r = supabase.table("financas_municipios").select(
        "ano,receita_total,receita_corrente,receita_capital,transferencias_correntes"
    ).eq("codigo_ibge", 3550308).order("ano").execute()
    print("\nSão Paulo (SP):")
    for rec in r.data:
        print(f"  {rec['ano']}: total={rec.get('receita_total')} "
              f"corrente={rec.get('receita_corrente')} "
              f"capital={rec.get('receita_capital')} "
              f"transf={rec.get('transferencias_correntes')}")


if __name__ == "__main__":
    main()
