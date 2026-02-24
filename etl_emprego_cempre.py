#!/usr/bin/env python3
"""
ETL Emprego Municipal - CEMPRE (2010-2021)
Fonte: IBGE/SIDRA - Cadastro Central de Empresas (CEMPRE)
  - Tabela 1685: Dados por município
  - Variáveis: empresas, empregos, salários
Busca UF por UF para evitar timeout da API.
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

# Variáveis CEMPRE tabela 1685
VARS = "706,367,707,708,662,1606,10143"
VAR_MAP = {
    "706": "unidades_locais",
    "367": "empresas",
    "707": "pessoal_ocupado_total",
    "708": "pessoal_assalariado",
    "662": "salarios_mil_reais",
    "1606": "salario_medio_sm",
    "10143": "salario_medio_reais",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS emprego_municipios (
    id BIGSERIAL PRIMARY KEY,
    codigo_ibge INTEGER NOT NULL,
    codigo_ibge_uf INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    unidades_locais INTEGER,
    empresas INTEGER,
    pessoal_ocupado_total INTEGER,
    pessoal_assalariado INTEGER,
    salarios_mil_reais NUMERIC(14,0),
    salario_medio_sm NUMERIC(6,2),
    salario_medio_reais NUMERIC(10,2),
    fonte VARCHAR(60) DEFAULT 'IBGE/CEMPRE',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(codigo_ibge, ano)
);
"""


def fetch_sidra(url):
    """Fetch com retry."""
    for tentativa in range(3):
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                registros = data[1:] if len(data) > 1 else []
                return [r for r in registros if r.get("V") and r["V"] not in ("...", "-", "")]
            if tentativa < 2:
                print(f"    HTTP {resp.status_code} - retry {tentativa+1}")
        except Exception as e:
            if tentativa < 2:
                print(f"    Erro: {e} - retry {tentativa+1}")
        time.sleep(3 * (tentativa + 1))
    return []


def fetch_ano(ano):
    """Busca todas as variáveis CEMPRE para um ano, UF por UF."""
    municipios = {}
    for uf in UFS:
        url = f"{SIDRA_BASE}/t/1685/n6/in%20n3%20{uf}/v/{VARS}/p/{ano}"
        registros = fetch_sidra(url)
        for r in registros:
            cod = r["D1C"]
            var_code = r["D2C"]
            campo = VAR_MAP.get(var_code)
            if not campo:
                continue
            if cod not in municipios:
                municipios[cod] = {"codigo_ibge": int(cod)}
            val = r["V"]
            try:
                if campo in ("salario_medio_sm", "salario_medio_reais"):
                    municipios[cod][campo] = float(val)
                else:
                    municipios[cod][campo] = int(float(val))
            except (ValueError, TypeError):
                pass  # "X", "...", etc = dado sigiloso
        time.sleep(0.5)
    return municipios


def main():
    print("=" * 60)
    print("ETL EMPREGO MUNICIPAL - CEMPRE")
    print("=" * 60)

    # Verificar tabela
    try:
        r = supabase.table("emprego_municipios").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"Tabela já tem {r.count} registros.")
            for ano in range(2010, 2022):
                rc = supabase.table("emprego_municipios").select(
                    "id", count="exact"
                ).eq("ano", ano).execute()
                if rc.count and rc.count > 0:
                    print(f"  {ano}: {rc.count}")
    except Exception as e:
        if "does not exist" in str(e).lower() or "schema" in str(e).lower():
            print("*** TABELA NÃO EXISTE! Execute o SQL no Supabase SQL Editor ***")
            print(CREATE_TABLE_SQL)
            return

    anos = list(range(2010, 2022))

    for ano in anos:
        print(f"\n{'='*40}")
        print(f"ANO {ano}")
        print(f"{'='*40}")

        # Verificar se já existe
        rc = supabase.table("emprego_municipios").select(
            "id", count="exact"
        ).eq("ano", ano).execute()
        if rc.count and rc.count > 0:
            print(f"  Já tem {rc.count} registros, pulando.")
            continue

        # Buscar dados
        print(f"  Buscando CEMPRE {ano} (27 UFs, 7 variáveis)...")
        municipios = fetch_ano(ano)
        print(f"  -> {len(municipios)} municípios com dados")

        # Montar registros
        registros = []
        for cod, dados in municipios.items():
            if not dados.get("pessoal_ocupado_total"):
                continue
            registros.append({
                "codigo_ibge": int(cod),
                "codigo_ibge_uf": int(str(cod)[:2]),
                "ano": ano,
                "unidades_locais": dados.get("unidades_locais"),
                "empresas": dados.get("empresas"),
                "pessoal_ocupado_total": dados.get("pessoal_ocupado_total"),
                "pessoal_assalariado": dados.get("pessoal_assalariado"),
                "salarios_mil_reais": dados.get("salarios_mil_reais"),
                "salario_medio_sm": dados.get("salario_medio_sm"),
                "salario_medio_reais": dados.get("salario_medio_reais"),
                "fonte": "IBGE/CEMPRE",
            })

        # Inserir
        print(f"  Inserindo {len(registros)} registros...")
        batch_size = 500
        inseridos = 0
        for i in range(0, len(registros), batch_size):
            batch = registros[i:i + batch_size]
            try:
                supabase.table("emprego_municipios").insert(batch).execute()
                inseridos += len(batch)
            except Exception as e:
                print(f"  Erro batch {i}: {e}")
            time.sleep(0.3)
        print(f"  OK: {inseridos} inseridos para {ano}")

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    total = 0
    for ano in range(2010, 2022):
        rc = supabase.table("emprego_municipios").select(
            "id", count="exact"
        ).eq("ano", ano).execute()
        c = rc.count or 0
        total += c
        if c > 0:
            print(f"  {ano}: {c} municípios")
    print(f"  TOTAL: {total} registros")
    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
