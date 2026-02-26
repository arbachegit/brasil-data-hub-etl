#!/usr/bin/env python3
"""
Preenche gaps restantes em pop_municipios e pib_municipios:

1. pop_municipios: 8 capitais sem vital stats (todos os anos)
   - Busca dados de mortalidade_municipios
2. pop_municipios: 398 NULLs em 2015, 90 em 2016
   - Busca dados de mortalidade_municipios
3. pib_municipios: renda_per_capita NULL para 2010 (5,570 registros)
   - Busca do IPEA/Atlas Brasil via API
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


def fetch_all(table, select, filters=None):
    """Busca todos os registros com paginação."""
    all_data = []
    offset = 0
    while True:
        q = supabase.table(table).select(select)
        if filters:
            for k, v in filters.items():
                q = q.eq(k, v)
        resp = q.range(offset, offset + 999).execute()
        all_data.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000
    return all_data


def main():
    print("=" * 60)
    print("PREENCHIMENTO DE GAPS RESTANTES")
    print("=" * 60)
    total_filled = 0

    # =========================================================
    # 1. pop_municipios: vital stats faltantes (2014-2023)
    #    Copiar de mortalidade_municipios
    # =========================================================
    print("\n" + "=" * 50)
    print("1. POP MUNICIPIOS - vital stats faltantes 2014-2023")
    print("=" * 50)

    filled_vital = 0
    for ano in range(2014, 2024):
        # Buscar pop_municipios com obitos_total NULL
        pop_nulls = fetch_all("pop_municipios", "id,codigo_ibge,populacao,obitos_total", {"ano": ano})
        nulls = [r for r in pop_nulls if r.get("obitos_total") is None]

        if not nulls:
            continue

        # Buscar mortalidade para esses municípios
        mort_recs = fetch_all("mortalidade_municipios",
                              "codigo_ibge,obitos_total,obitos_masculinos,obitos_femininos,nascimentos,taxa_mortalidade",
                              {"ano": ano})
        mort_map = {r["codigo_ibge"]: r for r in mort_recs}

        count = 0
        for r in nulls:
            mort = mort_map.get(r["codigo_ibge"])
            if not mort:
                continue

            update = {}
            if mort.get("obitos_total") is not None:
                update["obitos_total"] = mort["obitos_total"]
            if mort.get("obitos_masculinos") is not None:
                update["obitos_masculinos"] = mort["obitos_masculinos"]
            if mort.get("obitos_femininos") is not None:
                update["obitos_femininos"] = mort["obitos_femininos"]
            if mort.get("nascimentos") is not None:
                update["nascimentos"] = mort["nascimentos"]
            if mort.get("taxa_mortalidade") is not None:
                update["taxa_mortalidade"] = float(mort["taxa_mortalidade"])

            if update:
                try:
                    supabase.table("pop_municipios").update(update).eq("id", r["id"]).execute()
                    count += 1
                except Exception as e:
                    print(f"  Erro {r['codigo_ibge']}/{ano}: {e}")

        if count > 0:
            print(f"  {ano}: +{count} municípios atualizados (de {len(nulls)} NULLs)")
            filled_vital += count

    print(f"  Total vital stats: +{filled_vital}")
    total_filled += filled_vital

    # =========================================================
    # 2. pib_municipios: renda_per_capita 2010
    #    Buscar do IPEA/Atlas Brasil
    # =========================================================
    print("\n" + "=" * 50)
    print("2. PIB MUNICIPIOS - renda_per_capita 2010")
    print("=" * 50)

    # Tentar Atlas Brasil via IPEA API (IpeaData)
    # Código IPEA para renda per capita domiciliar: RDPC (Atlas Brasil)
    # A API do IPEA é: http://www.ipeadata.gov.br/api/odata4/
    print("  Buscando renda per capita 2010 do IPEA...")

    renda_map = {}

    # Tentar API IPEA
    try:
        url = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='ADH_RDPC')"
        resp = requests.get(url, timeout=120)
        if resp.status_code == 200:
            data = resp.json()
            valores = data.get("value", [])
            print(f"  {len(valores)} registros retornados do IPEA")
            for v in valores:
                # Filtrar só 2010 e nível municipal
                cod_territorial = v.get("TERCODIGO", "")
                # Código municipal tem 7 dígitos
                if len(str(cod_territorial)) == 7:
                    val = v.get("VALVALOR")
                    data_ref = v.get("VALDATA", "")
                    # Filtrar por data (2010)
                    if "2010" in str(data_ref) and val is not None:
                        renda_map[int(cod_territorial)] = round(float(val), 2)
            print(f"  {len(renda_map)} municípios com renda 2010")
        else:
            print(f"  IPEA retornou HTTP {resp.status_code}")
    except Exception as e:
        print(f"  Erro IPEA: {e}")

    # Se IPEA não funcionou, tentar método alternativo:
    # usar a renda_per_capita de 2011 como proxy
    if len(renda_map) < 100:
        print("  IPEA insuficiente. Usando renda de 2011 como proxy para 2010...")
        renda_2011 = fetch_all("pib_municipios", "codigo_ibge,renda_per_capita", {"ano": 2011})
        for r in renda_2011:
            if r.get("renda_per_capita") is not None:
                renda_map[r["codigo_ibge"]] = float(r["renda_per_capita"])
        print(f"  {len(renda_map)} municípios com renda proxy de 2011")

    # Atualizar registros 2010
    if renda_map:
        pib_2010 = fetch_all("pib_municipios", "id,codigo_ibge,renda_per_capita", {"ano": 2010})
        nulls = [r for r in pib_2010 if r.get("renda_per_capita") is None]
        print(f"  {len(nulls)} registros sem renda_per_capita em 2010")

        filled_renda = 0
        for r in nulls:
            renda = renda_map.get(r["codigo_ibge"])
            if renda is not None:
                try:
                    supabase.table("pib_municipios").update(
                        {"renda_per_capita": renda}
                    ).eq("id", r["id"]).execute()
                    filled_renda += 1
                except Exception as e:
                    if filled_renda < 3:
                        print(f"    Erro: {e}")

            if filled_renda % 1000 == 0 and filled_renda > 0:
                print(f"    {filled_renda} atualizados...")

        print(f"  renda_per_capita 2010: +{filled_renda}")
        total_filled += filled_renda

    # =========================================================
    # RESUMO
    # =========================================================
    print(f"\n{'='*60}")
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  pop_municipios vital stats: +{filled_vital}")
    print(f"  pib_municipios renda 2010:  +{filled_renda if renda_map else 0}")
    print(f"  TOTAL:                      +{total_filled}")
    print("=" * 60)


if __name__ == "__main__":
    main()
