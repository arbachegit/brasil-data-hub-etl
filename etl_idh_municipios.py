#!/usr/bin/env python3
"""
ETL IDH Municipal - IDHM e componentes (1991, 2000, 2010)
Fonte: IPEA Data (Atlas Brasil / PNUD)
Séries: ADH_IDHM, ADH_IDHM_E, ADH_IDHM_L, ADH_IDHM_R, ADH_GINI
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

IPEA_API = "http://www.ipeadata.gov.br/api/odata4/ValoresSerie(SERCODIGO='{serie}')"

CREATE_TABLE_SQL = """
-- IDH Municipal (Atlas Brasil / PNUD)
CREATE TABLE IF NOT EXISTS idh_municipios (
    id BIGSERIAL PRIMARY KEY,
    codigo_ibge INTEGER NOT NULL,
    codigo_ibge_uf INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    idhm NUMERIC(6,4),
    idhm_educacao NUMERIC(6,4),
    idhm_longevidade NUMERIC(6,4),
    idhm_renda NUMERIC(6,4),
    indice_gini NUMERIC(6,4),
    classificacao VARCHAR(20),
    fonte VARCHAR(50) DEFAULT 'Atlas Brasil/PNUD',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(codigo_ibge, ano)
);
"""


def classificar_idh(valor):
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


def fetch_serie_ipea(serie):
    """Busca série completa do IPEA Data."""
    url = IPEA_API.format(serie=serie)
    for tentativa in range(3):
        try:
            print(f"  Buscando {serie}...")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                values = data.get("value", [])
                # Filtrar apenas municípios (codigo >= 100000)
                mun = [r for r in values if len(str(r.get("TERCODIGO", ""))) >= 6]
                print(f"  {len(mun)} registros municipais")
                return mun
            print(f"  HTTP {resp.status_code} - tentativa {tentativa+1}")
        except Exception as e:
            print(f"  Erro: {e} - tentativa {tentativa+1}")
        time.sleep(5)
    return []


def main():
    print("=" * 60)
    print("ETL IDH MUNICIPAL - Atlas Brasil / PNUD")
    print("=" * 60)
    print(f"\nSQL para criar tabela:\n{CREATE_TABLE_SQL}")

    # Verificar se tabela existe e já tem dados
    try:
        r = supabase.table("idh_municipios").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"Tabela já tem {r.count} registros. Abortando.")
            return
    except Exception as e:
        if "does not exist" in str(e) or "schema cache" in str(e):
            print("*** TABELA NÃO EXISTE! Execute o SQL acima no Supabase SQL Editor ***")
            return

    # Buscar todas as séries
    series = {
        "ADH_IDHM": "idhm",
        "ADH_IDHM_E": "idhm_educacao",
        "ADH_IDHM_L": "idhm_longevidade",
        "ADH_IDHM_R": "idhm_renda",
        "ADH_GINI": "indice_gini",
    }

    # Estrutura: {(codigo, ano): {campo: valor}}
    dados = {}

    for serie_cod, campo in series.items():
        raw = fetch_serie_ipea(serie_cod)
        for r in raw:
            cod = int(r["TERCODIGO"])
            ano = int(r["VALDATA"][:4])
            valor = r.get("VALVALOR")
            chave = (cod, ano)
            if chave not in dados:
                dados[chave] = {"codigo_ibge": cod, "ano": ano}
            dados[chave][campo] = valor
        time.sleep(2)

    # Montar registros
    registros = []
    for (cod, ano), d in dados.items():
        idhm = d.get("idhm")
        registros.append({
            "codigo_ibge": d["codigo_ibge"],
            "codigo_ibge_uf": int(str(d["codigo_ibge"])[:2]),
            "ano": d["ano"],
            "idhm": d.get("idhm"),
            "idhm_educacao": d.get("idhm_educacao"),
            "idhm_longevidade": d.get("idhm_longevidade"),
            "idhm_renda": d.get("idhm_renda"),
            "indice_gini": d.get("indice_gini"),
            "classificacao": classificar_idh(idhm),
            "fonte": "Atlas Brasil/PNUD",
        })

    print(f"\nTotal: {len(registros)} registros para inserir")

    # Agrupar por ano
    from collections import Counter
    por_ano = Counter(r["ano"] for r in registros)
    for ano in sorted(por_ano):
        print(f"  {ano}: {por_ano[ano]} municípios")

    # Inserir em batches
    batch_size = 500
    inseridos = 0
    for i in range(0, len(registros), batch_size):
        batch = registros[i:i + batch_size]
        try:
            supabase.table("idh_municipios").insert(batch).execute()
            inseridos += len(batch)
            if (i // batch_size) % 5 == 0:
                print(f"  {inseridos}/{len(registros)} inseridos...")
        except Exception as e:
            print(f"  Erro batch {i}: {e}")
        time.sleep(0.3)

    print(f"\nInseridos: {inseridos}/{len(registros)}")

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    for ano in [1991, 2000, 2010]:
        r = supabase.table("idh_municipios").select("id", count="exact").eq("ano", ano).execute()
        print(f"  {ano}: {r.count} municípios")
    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
