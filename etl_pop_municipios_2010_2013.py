#!/usr/bin/env python3
"""
ETL - População Municipal 2010-2013
Preenche lacuna na tabela pop_municipios usando:
  - Censo 2010 (SIDRA Tabela 202): pop total, por sexo, urbana/rural
  - Estimativas 2011-2013 (SIDRA Tabela 6579): pop total
  - Proporções do Censo 2010 para derivar sexo/urbana-rural em 2011-2013
"""

import os
import json
import time
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SIDRA_BASE = "https://apisidra.ibge.gov.br/values"


def classificar_faixa(pop):
    if pop <= 5000:
        return "Até 5.000"
    elif pop <= 10000:
        return "5.001 a 10.000"
    elif pop <= 20000:
        return "10.001 a 20.000"
    elif pop <= 50000:
        return "20.001 a 50.000"
    elif pop <= 100000:
        return "50.001 a 100.000"
    elif pop <= 500000:
        return "100.001 a 500.000"
    elif pop <= 1000000:
        return "500.001 a 1.000.000"
    else:
        return "Acima de 1.000.000"


def extrair_uf_do_codigo(codigo_ibge):
    """Extrai código UF dos 2 primeiros dígitos do código IBGE."""
    return int(str(codigo_ibge)[:2])


def extrair_nome_municipio(nome_sidra):
    """Remove ' - UF' do nome retornado pelo SIDRA."""
    if " - " in nome_sidra:
        return nome_sidra.rsplit(" - ", 1)[0].strip()
    return nome_sidra.strip()


def fetch_sidra(url, descricao):
    """Fetch com retry da API SIDRA."""
    for tentativa in range(3):
        try:
            print(f"  Buscando {descricao}...")
            resp = requests.get(url, timeout=300)
            if resp.status_code == 200:
                data = resp.json()
                # Primeira linha é header
                return data[1:] if len(data) > 1 else []
            else:
                print(f"  HTTP {resp.status_code} - tentativa {tentativa+1}")
        except Exception as e:
            print(f"  Erro: {e} - tentativa {tentativa+1}")
        time.sleep(5 * (tentativa + 1))
    return []


def fetch_censo_2010():
    """Busca dados do Censo 2010 (Tabela 202) para todos os municípios."""
    print("\n=== CENSO 2010 (Tabela 202) ===")

    # Tabela 202: v/93 = pop residente, c1 = situação domicílio, c2 = sexo
    # c1: 0=Total, 1=Urbana, 2=Rural
    # c2: 0=Total, 4=Homens, 5=Mulheres
    dados = {}

    # 1) Pop total por município
    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/202/n6/all/v/93/p/2010/c1/0/c2/0",
        "pop total"
    )
    for r in raw:
        cod = r["D1C"]
        nome = extrair_nome_municipio(r["D1N"])
        val = r["V"]
        if val and val != "..." and val != "-":
            dados[cod] = {
                "codigo_ibge": int(cod),
                "nome_municipio": nome,
                "populacao": int(val),
            }
    print(f"  Total: {len(dados)} municípios")

    # 2) Pop masculina
    time.sleep(2)
    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/202/n6/all/v/93/p/2010/c1/0/c2/4",
        "pop masculina"
    )
    for r in raw:
        cod = r["D1C"]
        val = r["V"]
        if cod in dados and val and val != "..." and val != "-":
            dados[cod]["populacao_masculina"] = int(val)

    # 3) Pop feminina
    time.sleep(2)
    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/202/n6/all/v/93/p/2010/c1/0/c2/5",
        "pop feminina"
    )
    for r in raw:
        cod = r["D1C"]
        val = r["V"]
        if cod in dados and val and val != "..." and val != "-":
            dados[cod]["populacao_feminina"] = int(val)

    # 4) Pop urbana
    time.sleep(2)
    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/202/n6/all/v/93/p/2010/c1/1/c2/0",
        "pop urbana"
    )
    for r in raw:
        cod = r["D1C"]
        val = r["V"]
        if cod in dados and val and val != "..." and val != "-":
            dados[cod]["populacao_urbana"] = int(val)

    # 5) Pop rural
    time.sleep(2)
    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/202/n6/all/v/93/p/2010/c1/2/c2/0",
        "pop rural"
    )
    for r in raw:
        cod = r["D1C"]
        val = r["V"]
        if cod in dados and val and val != "..." and val != "-":
            dados[cod]["populacao_rural"] = int(val)

    return dados


def fetch_estimativas(ano):
    """Busca estimativas populacionais (Tabela 6579) para um ano."""
    print(f"\n=== ESTIMATIVAS {ano} (Tabela 6579) ===")

    raw = fetch_sidra(
        f"{SIDRA_BASE}/t/6579/n6/all/v/9324/p/{ano}",
        f"estimativas {ano}"
    )

    dados = {}
    for r in raw:
        cod = r["D1C"]
        nome = extrair_nome_municipio(r["D1N"])
        val = r["V"]
        if val and val != "..." and val != "-":
            dados[cod] = {
                "codigo_ibge": int(cod),
                "nome_municipio": nome,
                "populacao": int(val),
            }
    print(f"  Total: {len(dados)} municípios")
    return dados


def aplicar_proporcoes(est_data, censo_data):
    """Aplica proporções do Censo 2010 aos dados de estimativas."""
    for cod, est in est_data.items():
        censo = censo_data.get(cod)
        if not censo or censo["populacao"] == 0:
            # Sem dados do censo, usar 50/50 e 80/20
            pop = est["populacao"]
            est["populacao_masculina"] = int(pop * 0.49)
            est["populacao_feminina"] = pop - int(pop * 0.49)
            est["populacao_urbana"] = int(pop * 0.84)
            est["populacao_rural"] = pop - int(pop * 0.84)
            continue

        pop_total_censo = censo["populacao"]
        pop = est["populacao"]

        # Proporções do censo
        pct_masc = censo.get("populacao_masculina", 0) / pop_total_censo
        pct_urb = censo.get("populacao_urbana", 0) / pop_total_censo

        est["populacao_masculina"] = int(pop * pct_masc)
        est["populacao_feminina"] = pop - int(pop * pct_masc)
        est["populacao_urbana"] = int(pop * pct_urb)
        est["populacao_rural"] = pop - int(pop * pct_urb)

    return est_data


def preparar_registros(dados, ano, fonte):
    """Converte dicionário de dados em lista de registros para upsert."""
    registros = []
    for cod, d in dados.items():
        pop = d.get("populacao", 0)
        if pop == 0:
            continue

        registros.append({
            "codigo_ibge": d["codigo_ibge"],
            "nome_municipio": d["nome_municipio"],
            "ano": ano,
            "populacao": pop,
            "populacao_masculina": d.get("populacao_masculina"),
            "populacao_feminina": d.get("populacao_feminina"),
            "populacao_urbana": d.get("populacao_urbana"),
            "populacao_rural": d.get("populacao_rural"),
            "faixa_populacional": classificar_faixa(pop),
            "codigo_ibge_uf": extrair_uf_do_codigo(d["codigo_ibge"]),
            "fonte": fonte,
        })
    return registros


def inserir_batch(registros, ano):
    """Insere registros no Supabase em batches."""
    # Verificar se já existem registros para este ano
    existing = supabase.table("pop_municipios").select(
        "id", count="exact"
    ).eq("ano", ano).execute()

    if existing.count and existing.count > 0:
        print(f"\n  Ano {ano} já tem {existing.count} registros. Pulando.")
        return 0

    print(f"\nInserindo {len(registros)} registros para {ano}...")

    batch_size = 200
    inseridos = 0
    erros = 0

    for i in range(0, len(registros), batch_size):
        batch = registros[i:i + batch_size]
        try:
            supabase.table("pop_municipios").insert(batch).execute()
            inseridos += len(batch)
            if (i // batch_size) % 5 == 0:
                print(f"  {inseridos}/{len(registros)} inseridos...")
        except Exception as e:
            erros += len(batch)
            print(f"  Erro no batch {i}-{i+batch_size}: {e}")
        time.sleep(0.3)

    print(f"  Resultado {ano}: {inseridos} inseridos, {erros} erros")
    return inseridos


def main():
    print("=" * 60)
    print("ETL - População Municipal 2010-2013")
    print("=" * 60)

    # 1. Buscar dados do Censo 2010
    censo_2010 = fetch_censo_2010()

    # 2. Montar e inserir registros do Censo 2010
    registros_2010 = preparar_registros(censo_2010, 2010, "IBGE/Censo-2010")
    total_2010 = inserir_batch(registros_2010, 2010)

    # 3. Buscar e inserir estimativas 2011, 2012, 2013
    for ano in [2011, 2012, 2013]:
        time.sleep(3)
        est_data = fetch_estimativas(ano)
        est_data = aplicar_proporcoes(est_data, censo_2010)
        registros = preparar_registros(est_data, ano, f"IBGE/Estimativas-{ano}")
        inserir_batch(registros, ano)

    # 4. Resumo final
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    for ano in [2010, 2011, 2012, 2013]:
        try:
            resp = supabase.table("pop_municipios").select(
                "id", count="exact"
            ).eq("ano", ano).execute()
            print(f"  {ano}: {resp.count} municípios")
        except:
            pass
    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
