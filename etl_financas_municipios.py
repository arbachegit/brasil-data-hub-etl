#!/usr/bin/env python3
"""
ETL Finanças Municipais (2014-2023)
Fonte: Tesouro Nacional / SICONFI - DCA (Declaração de Contas Anuais)
  - Anexo I-C: Receitas
  - Anexo I-D: Despesas
Usa requests concorrentes (ThreadPoolExecutor) para viabilizar 5500+ municípios.
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

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS financas_municipios (
    id BIGSERIAL PRIMARY KEY,
    codigo_ibge INTEGER NOT NULL,
    codigo_ibge_uf INTEGER NOT NULL,
    ano INTEGER NOT NULL,
    receita_total NUMERIC(16,2),
    receita_corrente NUMERIC(16,2),
    receita_capital NUMERIC(16,2),
    transferencias_correntes NUMERIC(16,2),
    despesa_total NUMERIC(16,2),
    despesa_corrente NUMERIC(16,2),
    despesa_pessoal NUMERIC(16,2),
    despesa_capital NUMERIC(16,2),
    investimentos NUMERIC(16,2),
    resultado_orcamentario NUMERIC(16,2),
    fonte VARCHAR(60) DEFAULT 'Tesouro/SICONFI-DCA',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(codigo_ibge, ano)
);
"""

# Contas-chave para extrair
RECEITA_KEYS = {
    "TotalReceitas": "receita_total",
    "RO1.0.0.0.00.0.0": "receita_corrente",
    "RO2.0.0.0.00.0.0": "receita_capital",
    "RO1.7.0.0.00.0.0": "transferencias_correntes",
}

DESPESA_KEYS = {
    "TotalDespesas": "despesa_total",
    "DO3.0.00.00.00.00": "despesa_corrente",
    "DO3.1.00.00.00.00": "despesa_pessoal",
    "DO4.0.00.00.00.00": "despesa_capital",
    "DO4.4.00.00.00.00": "investimentos",
}


def fetch_dca(id_ente, ano, anexo):
    """Fetch DCA data for one ente/year/anexo."""
    url = f"{SICONFI_BASE}/dca?an_exercicio={ano}&no_anexo={anexo}&id_ente={id_ente}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json().get("items", [])
    except:
        pass
    return []


def extract_financas(id_ente, ano):
    """Extract key financial data for one municipality/year."""
    dados = {"codigo_ibge": id_ente}

    # Receitas (Anexo I-C)
    items = fetch_dca(id_ente, ano, "DCA-Anexo%20I-C")
    for r in items:
        cod = r.get("cod_conta", "")
        coluna = r.get("coluna", "")
        valor = r.get("valor")
        if "Brut" in coluna and cod in RECEITA_KEYS and valor:
            dados[RECEITA_KEYS[cod]] = valor

    # Despesas (Anexo I-D)
    items = fetch_dca(id_ente, ano, "DCA-Anexo%20I-D")
    for r in items:
        cod = r.get("cod_conta", "")
        coluna = r.get("coluna", "")
        valor = r.get("valor")
        if "Empenha" in coluna and cod in DESPESA_KEYS and valor:
            dados[DESPESA_KEYS[cod]] = valor

    return dados


def get_entes_municipais():
    """Get list of municipal entes from SICONFI."""
    url = f"{SICONFI_BASE}/entes?an_referencia=2022&in_tipo_ente=M"
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 200:
            return resp.json().get("items", [])
    except:
        pass
    return []


def main():
    print("=" * 60)
    print("ETL FINANÇAS MUNICIPAIS - SICONFI DCA")
    print("=" * 60)

    # Verificar tabela
    try:
        r = supabase.table("financas_municipios").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"Tabela já tem {r.count} registros.")
            for ano in range(2014, 2024):
                rc = supabase.table("financas_municipios").select(
                    "id", count="exact"
                ).eq("ano", ano).execute()
                if rc.count and rc.count > 0:
                    print(f"  {ano}: {rc.count}")
    except Exception as e:
        if "does not exist" in str(e).lower() or "schema" in str(e).lower():
            print("*** TABELA NÃO EXISTE! Execute o SQL no Supabase SQL Editor ***")
            print(CREATE_TABLE_SQL)
            return

    # Buscar lista de entes municipais
    print("\nBuscando lista de municípios do SICONFI...")
    entes = get_entes_municipais()
    if not entes:
        print("Erro: não conseguiu lista de entes")
        return
    print(f"  {len(entes)} municípios encontrados")
    ente_ids = [e["cod_ibge"] for e in entes]

    anos = list(range(2014, 2024))

    for ano in anos:
        print(f"\n{'='*40}")
        print(f"ANO {ano}")
        print(f"{'='*40}")

        # Verificar se já existe
        rc = supabase.table("financas_municipios").select(
            "id", count="exact"
        ).eq("ano", ano).execute()
        if rc.count and rc.count > 0:
            print(f"  Já tem {rc.count} registros, pulando.")
            continue

        # Fetch concorrente (8 threads)
        print(f"  Buscando finanças {ano} para {len(ente_ids)} municípios (8 threads)...")
        resultados = []
        errors = 0

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = {
                executor.submit(extract_financas, eid, ano): eid
                for eid in ente_ids
            }
            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 500 == 0:
                    print(f"    {done}/{len(ente_ids)} processados...")
                try:
                    dados = future.result()
                    if dados.get("receita_total") or dados.get("despesa_total"):
                        resultados.append(dados)
                except:
                    errors += 1

        print(f"  -> {len(resultados)} municípios com dados, {errors} erros")

        # Montar registros
        registros = []
        for dados in resultados:
            cod = dados["codigo_ibge"]
            rec_total = dados.get("receita_total")
            desp_total = dados.get("despesa_total")

            resultado_orc = None
            if rec_total and desp_total:
                resultado_orc = round(rec_total - desp_total, 2)

            registros.append({
                "codigo_ibge": cod,
                "codigo_ibge_uf": int(str(cod)[:2]),
                "ano": ano,
                "receita_total": rec_total,
                "receita_corrente": dados.get("receita_corrente"),
                "receita_capital": dados.get("receita_capital"),
                "transferencias_correntes": dados.get("transferencias_correntes"),
                "despesa_total": desp_total,
                "despesa_corrente": dados.get("despesa_corrente"),
                "despesa_pessoal": dados.get("despesa_pessoal"),
                "despesa_capital": dados.get("despesa_capital"),
                "investimentos": dados.get("investimentos"),
                "resultado_orcamentario": resultado_orc,
                "fonte": "Tesouro/SICONFI-DCA",
            })

        # Inserir
        print(f"  Inserindo {len(registros)} registros...")
        batch_size = 500
        inseridos = 0
        for i in range(0, len(registros), batch_size):
            batch = registros[i:i + batch_size]
            try:
                supabase.table("financas_municipios").insert(batch).execute()
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
    for ano in range(2014, 2024):
        rc = supabase.table("financas_municipios").select(
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
