#!/usr/bin/env python3
"""
ETL Mortalidade/Natalidade Municipal (2010-2023)
Fonte: IBGE/SIDRA - Estatísticas do Registro Civil
  - Tabela 2681: Óbitos (v/343), por sexo (c2: 4=Homens, 5=Mulheres)
  - Tabela 2612: Nascidos vivos (v/218)
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

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS mortalidade_municipios (
    id BIGSERIAL PRIMARY KEY,
    codigo_ibge INTEGER NOT NULL,
    codigo_ibge_uf INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    obitos_total INTEGER,
    obitos_masculinos INTEGER,
    obitos_femininos INTEGER,
    nascimentos INTEGER,
    taxa_mortalidade NUMERIC(8,2),
    taxa_natalidade NUMERIC(8,2),
    crescimento_vegetativo INTEGER,
    fonte VARCHAR(60) DEFAULT 'IBGE/Registro Civil',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(codigo_ibge, ano)
);
"""


def fetch_sidra(url, descricao):
    """Fetch com retry."""
    for tentativa in range(3):
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                registros = data[1:] if len(data) > 1 else []
                validos = [r for r in registros if r.get("V") and r["V"] not in ("...", "-", "")]
                return validos
            if tentativa < 2:
                print(f"    HTTP {resp.status_code} - retry {tentativa+1}")
        except Exception as e:
            if tentativa < 2:
                print(f"    Erro: {e} - retry {tentativa+1}")
        time.sleep(3 * (tentativa + 1))
    return []


def fetch_all_ufs(table, var, ano, classificacao=""):
    """Busca dados de todos os municípios, UF por UF."""
    todos = []
    for uf in UFS:
        url = f"{SIDRA_BASE}/t/{table}/n6/in%20n3%20{uf}/v/{var}/p/{ano}"
        if classificacao:
            url += f"/{classificacao}"
        resultado = fetch_sidra(url, f"UF {uf}")
        todos.extend(resultado)
        time.sleep(0.5)
    return todos


def main():
    print("=" * 60)
    print("ETL MORTALIDADE/NATALIDADE MUNICIPAL")
    print("=" * 60)

    # Verificar tabela
    try:
        r = supabase.table("mortalidade_municipios").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"Tabela já tem {r.count} registros.")
            for ano in range(2010, 2024):
                rc = supabase.table("mortalidade_municipios").select(
                    "id", count="exact"
                ).eq("ano", ano).execute()
                if rc.count and rc.count > 0:
                    print(f"  {ano}: {rc.count}")
    except Exception as e:
        if "does not exist" in str(e).lower() or "schema" in str(e).lower():
            print("*** TABELA NÃO EXISTE! Execute o SQL no Supabase SQL Editor ***")
            print(CREATE_TABLE_SQL)
            return

    anos = list(range(2010, 2024))

    for ano in anos:
        print(f"\n{'='*40}")
        print(f"ANO {ano}")
        print(f"{'='*40}")

        # Verificar se já existe
        rc = supabase.table("mortalidade_municipios").select(
            "id", count="exact"
        ).eq("ano", ano).execute()
        if rc.count and rc.count > 0:
            print(f"  Já tem {rc.count} registros, pulando.")
            continue

        # 1. Óbitos totais (UF por UF)
        print(f"  Buscando óbitos totais {ano} (27 UFs)...")
        obitos_raw = fetch_all_ufs("2681", "343", ano)
        obitos = {}
        for r in obitos_raw:
            cod = r["D1C"]
            obitos[cod] = {
                "codigo_ibge": int(cod),
                "obitos_total": int(r["V"]),
            }
        print(f"  -> {len(obitos)} municípios com óbitos")

        # 2. Óbitos masculinos (UF por UF)
        time.sleep(2)
        print(f"  Buscando óbitos masculinos {ano} (27 UFs)...")
        obitos_masc_raw = fetch_all_ufs("2681", "343", ano, "c2/4")
        masc_count = 0
        for r in obitos_masc_raw:
            cod = r["D1C"]
            if cod in obitos:
                obitos[cod]["obitos_masculinos"] = int(r["V"])
                masc_count += 1
        print(f"  -> {masc_count} com dados masculinos")

        # 3. Óbitos femininos (UF por UF)
        time.sleep(2)
        print(f"  Buscando óbitos femininos {ano} (27 UFs)...")
        obitos_fem_raw = fetch_all_ufs("2681", "343", ano, "c2/5")
        fem_count = 0
        for r in obitos_fem_raw:
            cod = r["D1C"]
            if cod in obitos:
                obitos[cod]["obitos_femininos"] = int(r["V"])
                fem_count += 1
        print(f"  -> {fem_count} com dados femininos")

        # 4. Nascimentos (UF por UF)
        time.sleep(2)
        print(f"  Buscando nascimentos {ano} (27 UFs)...")
        nasc_raw = fetch_all_ufs("2612", "218", ano)
        nascimentos = {}
        for r in nasc_raw:
            cod = r["D1C"]
            nascimentos[cod] = int(r["V"])
        print(f"  -> {len(nascimentos)} com nascimentos")

        # 5. Buscar população para calcular taxas
        pop = {}
        try:
            offset = 0
            while True:
                resp = supabase.table("pop_municipios").select(
                    "codigo_ibge,populacao"
                ).eq("ano", ano).range(offset, offset + 999).execute()
                for r in resp.data:
                    pop[str(r["codigo_ibge"])] = r["populacao"]
                if len(resp.data) < 1000:
                    break
                offset += 1000
            print(f"  -> {len(pop)} com população")
        except:
            print(f"  Sem dados de população para {ano}")

        # 6. Montar registros
        registros = []
        codigos = set(list(obitos.keys()) + list(nascimentos.keys()))
        for cod in codigos:
            ob = obitos.get(cod, {})
            nasc = nascimentos.get(cod)
            ob_total = ob.get("obitos_total")
            populacao = pop.get(cod)

            if not ob_total and not nasc:
                continue

            taxa_mort = None
            taxa_nat = None
            if populacao and populacao > 0:
                if ob_total:
                    taxa_mort = round((ob_total / populacao) * 1000, 2)
                if nasc:
                    taxa_nat = round((nasc / populacao) * 1000, 2)

            cresc_veg = None
            if nasc and ob_total:
                cresc_veg = nasc - ob_total

            registros.append({
                "codigo_ibge": int(cod),
                "codigo_ibge_uf": int(str(cod)[:2]),
                "ano": ano,
                "obitos_total": ob_total,
                "obitos_masculinos": ob.get("obitos_masculinos"),
                "obitos_femininos": ob.get("obitos_femininos"),
                "nascimentos": nasc,
                "taxa_mortalidade": taxa_mort,
                "taxa_natalidade": taxa_nat,
                "crescimento_vegetativo": cresc_veg,
                "fonte": "IBGE/Registro Civil",
            })

        # 7. Inserir
        print(f"  Inserindo {len(registros)} registros...")
        batch_size = 500
        inseridos = 0
        for i in range(0, len(registros), batch_size):
            batch = registros[i:i + batch_size]
            try:
                supabase.table("mortalidade_municipios").insert(batch).execute()
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
    for ano in range(2010, 2024):
        rc = supabase.table("mortalidade_municipios").select(
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
