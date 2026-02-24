#!/usr/bin/env python3
"""
ETL BCB Macro - IPCA, SELIC e Câmbio USD/BRL (2010-2025)
Fonte: API BCB SGS (Sistema Gerenciador de Séries Temporais)
https://dadosabertos.bcb.gov.br/
"""

import os
import time
import requests
from datetime import datetime
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

BCB_SGS = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{serie}/dados"


def fetch_bcb(serie, data_ini, data_fim, descricao):
    """Busca série temporal do BCB SGS."""
    url = f"{BCB_SGS.format(serie=serie)}?formato=json&dataInicial={data_ini}&dataFinal={data_fim}"
    for tentativa in range(3):
        try:
            print(f"  Buscando {descricao}...")
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                print(f"  {len(data)} registros obtidos")
                return data
            print(f"  HTTP {resp.status_code} - tentativa {tentativa+1}")
        except Exception as e:
            print(f"  Erro: {e} - tentativa {tentativa+1}")
        time.sleep(3)
    return []


def parse_data_bcb(data_str):
    """Converte dd/mm/aaaa para YYYY-MM-DD."""
    return datetime.strptime(data_str, "%d/%m/%Y").strftime("%Y-%m-%d")


def criar_tabela_via_insert(tabela, registros):
    """Insere registros. Se a tabela não existir, informa o erro."""
    print(f"  Inserindo {len(registros)} registros em {tabela}...")
    batch_size = 500
    inseridos = 0
    for i in range(0, len(registros), batch_size):
        batch = registros[i:i + batch_size]
        try:
            supabase.table(tabela).insert(batch).execute()
            inseridos += len(batch)
        except Exception as e:
            err_str = str(e)
            if "does not exist" in err_str or "relation" in err_str:
                print(f"  ERRO: Tabela '{tabela}' não existe. Precisa criar via SQL.")
                return -1
            # Tentar upsert se houver duplicatas
            try:
                supabase.table(tabela).upsert(batch).execute()
                inseridos += len(batch)
            except Exception as e2:
                print(f"  Erro batch {i}: {e2}")
    print(f"  OK: {inseridos} inseridos")
    return inseridos


# =============================================================
# SQL para criar as tabelas (executar no Supabase SQL Editor)
# =============================================================
CREATE_TABLES_SQL = """
-- IPCA Mensal
CREATE TABLE IF NOT EXISTS ipca_mensal (
    id BIGSERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    variacao_mensal NUMERIC(8,4),
    variacao_acumulada_ano NUMERIC(8,4),
    variacao_12_meses NUMERIC(8,4),
    indice_numero NUMERIC(12,4),
    fonte VARCHAR(50) DEFAULT 'BCB/SGS',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ano, mes)
);

-- SELIC Meta
CREATE TABLE IF NOT EXISTS selic_meta (
    id BIGSERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    taxa_meta_anual NUMERIC(8,4),
    taxa_diaria NUMERIC(12,8),
    fonte VARCHAR(50) DEFAULT 'BCB/SGS',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ano, mes)
);

-- Câmbio USD/BRL (médias mensais)
CREATE TABLE IF NOT EXISTS cambio_usd_mensal (
    id BIGSERIAL PRIMARY KEY,
    data_referencia DATE NOT NULL,
    ano INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    taxa_media NUMERIC(10,4),
    taxa_min NUMERIC(10,4),
    taxa_max NUMERIC(10,4),
    variacao_mensal NUMERIC(8,4),
    fonte VARCHAR(50) DEFAULT 'BCB/SGS',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(ano, mes)
);
"""


# =============================================================
# 1. IPCA MENSAL
# =============================================================
def etl_ipca():
    print("\n" + "=" * 60)
    print("1. IPCA MENSAL (série 433 - variação mensal)")
    print("=" * 60)

    # Série 433 = IPCA variação mensal %
    raw = fetch_bcb(433, "01/01/2010", "31/12/2025", "IPCA mensal 2010-2025")
    if not raw:
        return

    # Série 13522 = IPCA acumulado 12 meses
    time.sleep(1)
    raw_12m = fetch_bcb(13522, "01/01/2010", "31/12/2025", "IPCA acumulado 12 meses")
    acum_12m = {}
    for r in raw_12m:
        dt = parse_data_bcb(r["data"])
        acum_12m[dt[:7]] = float(r["valor"])

    # Série 10764 = Número índice IPCA
    time.sleep(1)
    raw_idx = fetch_bcb(10764, "01/01/2010", "31/12/2025", "IPCA número-índice")
    indice = {}
    for r in raw_idx:
        dt = parse_data_bcb(r["data"])
        indice[dt[:7]] = float(r["valor"])

    # Calcular acumulado no ano
    registros = []
    acum_ano = {}
    for r in raw:
        dt = parse_data_bcb(r["data"])
        ano = int(dt[:4])
        mes = int(dt[5:7])
        var_mensal = float(r["valor"])

        if ano not in acum_ano:
            acum_ano[ano] = 1.0
        acum_ano[ano] *= (1 + var_mensal / 100)
        var_acum = round((acum_ano[ano] - 1) * 100, 4)

        chave = dt[:7]
        registros.append({
            "data_referencia": dt,
            "ano": ano,
            "mes": mes,
            "variacao_mensal": var_mensal,
            "variacao_acumulada_ano": var_acum,
            "variacao_12_meses": acum_12m.get(chave),
            "indice_numero": indice.get(chave),
            "fonte": "BCB/SGS",
        })

    return criar_tabela_via_insert("ipca_mensal", registros)


# =============================================================
# 2. SELIC META (mensal - último valor do mês)
# =============================================================
def etl_selic():
    print("\n" + "=" * 60)
    print("2. SELIC META (série 432 - meta SELIC anualizada)")
    print("=" * 60)

    # Buscar por ano para evitar HTTP 406 (resposta muito grande)
    raw = []
    raw_diaria = []
    for ano in range(2010, 2026):
        ini = f"01/01/{ano}"
        fim = f"31/12/{ano}"
        r1 = fetch_bcb(432, ini, fim, f"SELIC meta {ano}")
        if r1:
            raw.extend(r1)
        time.sleep(0.5)
        r2 = fetch_bcb(11, ini, fim, f"SELIC diária {ano}")
        if r2:
            raw_diaria.extend(r2)
        time.sleep(0.5)

    if not raw:
        return

    selic_dia = {}
    for r in raw_diaria:
        dt = parse_data_bcb(r["data"])
        selic_dia[dt[:7]] = float(r["valor"])

    # Agregar por mês (último valor do mês)
    por_mes = {}
    for r in raw:
        dt = parse_data_bcb(r["data"])
        chave = dt[:7]
        por_mes[chave] = float(r["valor"])

    registros = []
    for chave in sorted(por_mes.keys()):
        ano = int(chave[:4])
        mes = int(chave[5:7])
        registros.append({
            "data_referencia": f"{chave}-01",
            "ano": ano,
            "mes": mes,
            "taxa_meta_anual": por_mes[chave],
            "taxa_diaria": selic_dia.get(chave),
            "fonte": "BCB/SGS",
        })

    return criar_tabela_via_insert("selic_meta", registros)


# =============================================================
# 3. CÂMBIO USD/BRL (médias mensais)
# =============================================================
def etl_cambio():
    print("\n" + "=" * 60)
    print("3. CÂMBIO USD/BRL (série 1 - PTAX venda)")
    print("=" * 60)

    # Buscar por ano para evitar HTTP 406
    raw = []
    for ano in range(2010, 2026):
        r = fetch_bcb(1, f"01/01/{ano}", f"31/12/{ano}", f"Câmbio USD {ano}")
        if r:
            raw.extend(r)
        time.sleep(0.5)

    if not raw:
        return

    # Agregar por mês
    from collections import defaultdict
    por_mes = defaultdict(list)
    for r in raw:
        dt = parse_data_bcb(r["data"])
        chave = dt[:7]
        por_mes[chave].append(float(r["valor"]))

    registros = []
    prev_media = None
    for chave in sorted(por_mes.keys()):
        ano = int(chave[:4])
        mes = int(chave[5:7])
        valores = por_mes[chave]
        media = round(sum(valores) / len(valores), 4)
        variacao = None
        if prev_media:
            variacao = round(((media / prev_media) - 1) * 100, 4)

        registros.append({
            "data_referencia": f"{chave}-01",
            "ano": ano,
            "mes": mes,
            "taxa_media": media,
            "taxa_min": round(min(valores), 4),
            "taxa_max": round(max(valores), 4),
            "variacao_mensal": variacao,
            "fonte": "BCB/SGS",
        })
        prev_media = media

    return criar_tabela_via_insert("cambio_usd_mensal", registros)


# =============================================================
# MAIN
# =============================================================
def main():
    print("=" * 60)
    print("ETL BCB MACRO - IPCA, SELIC, Câmbio")
    print("=" * 60)

    # Mostrar SQL para criar tabelas
    print("\nSQL para criar tabelas (executar no Supabase SQL Editor se necessário):")
    print(CREATE_TABLES_SQL)
    print("-" * 60)

    # Verificar se IPCA já foi carregado
    try:
        r = supabase.table("ipca_mensal").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"\n  ipca_mensal já tem {r.count} registros, pulando.")
        else:
            etl_ipca()
    except:
        resultado_ipca = etl_ipca()
        if resultado_ipca == -1:
            print("\n*** ATENÇÃO: As tabelas precisam ser criadas primeiro! ***")
            return

    time.sleep(1)

    try:
        r = supabase.table("selic_meta").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"\n  selic_meta já tem {r.count} registros, pulando.")
        else:
            etl_selic()
    except:
        etl_selic()

    time.sleep(1)

    try:
        r = supabase.table("cambio_usd_mensal").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"\n  cambio_usd_mensal já tem {r.count} registros, pulando.")
        else:
            etl_cambio()
    except:
        etl_cambio()

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    for tabela in ["ipca_mensal", "selic_meta", "cambio_usd_mensal"]:
        try:
            r = supabase.table(tabela).select("id", count="exact").execute()
            print(f"  {tabela}: {r.count} registros")
        except:
            print(f"  {tabela}: erro ao consultar")
    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
