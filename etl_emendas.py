#!/usr/bin/env python3
"""
ETL Emendas Parlamentares - Brasil Data Hub
Popula fato_emendas_parlamentares com dados do Portal da Transparência.
Fonte: API Portal da Transparência (requer token gratuito)

Para obter o token: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email
Defina TRANSPARENCIA_TOKEN no .env
"""

import os
import hashlib
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TRANSPARENCIA_TOKEN = os.getenv("TRANSPARENCIA_TOKEN", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

TRANSPARENCIA_API = "https://api.portaldatransparencia.gov.br/api-de-dados"

BATCH_SIZE = 500
REQUEST_DELAY = 1.0  # Portal da Transparência rate limit
START_YEAR = int(os.getenv("START_YEAR", "2015"))
END_YEAR = int(os.getenv("END_YEAR", "2025"))

cpf_to_id = {}
nome_to_pid = {}  # nome_upper -> politico_id (fuzzy match for authors)


# ============================================================
# Helpers
# ============================================================

def retry_get(url, max_retries=3, timeout=60, **kwargs):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp
            if resp.status_code == 429:
                time.sleep((2 ** attempt) * 10)
                continue
            if resp.status_code >= 500:
                time.sleep((2 ** attempt) * 2)
                continue
            if resp.status_code == 404:
                return None
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** attempt)
    return None


def carregar_cache_politicos():
    global cpf_to_id, nome_to_pid
    print("Carregando cache de políticos...")
    last_id = 0
    total = 0
    page_size = 1000
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/dim_politicos"
               f"?select=id,cpf,nome_completo&id=gt.{last_id}&order=id.asc&limit={page_size}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=120)
            if resp.status_code != 200:
                break
            data = resp.json()
        except Exception:
            break
        if not data:
            break
        for r in data:
            if r.get("cpf"):
                cpf_to_id[r["cpf"]] = r["id"]
            nome = (r.get("nome_completo") or "").strip().upper()
            if nome and r.get("id"):
                nome_to_pid[nome] = r["id"]
        last_id = data[-1]["id"]
        total += len(data)
        if total % 50000 == 0:
            print(f"    ...{total} carregados")
        if len(data) < page_size:
            break
    print(f"  {total} políticos ({len(cpf_to_id)} com CPF, {len(nome_to_pid)} com nome)")


def parse_brl_number(val):
    """Parse Brazilian number format '1.234.567,89' to float."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if not isinstance(val, str):
        return None
    val = val.strip()
    if not val:
        return None
    try:
        return float(val.replace(".", "").replace(",", "."))
    except (ValueError, TypeError):
        return None


def find_politico_by_name(autor):
    """Try to match an emenda author to a politico_id by name."""
    if not autor:
        return None
    nome_upper = autor.strip().upper()
    # Direct match
    pid = nome_to_pid.get(nome_upper)
    if pid:
        return pid
    return None


def batch_insert_emendas(records):
    if not records:
        return 0

    url = (f"{SUPABASE_URL}/rest/v1/fato_emendas_parlamentares"
           f"?on_conflict=codigo_emenda,ano")
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=headers-only"}

    count = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        try:
            resp = requests.post(url, json=batch, headers=h, timeout=120)
            if resp.status_code in (200, 201):
                count += len(batch)
            else:
                for j in range(0, len(batch), 50):
                    mini = batch[j:j + 50]
                    try:
                        r = requests.post(url, json=mini, headers=h, timeout=60)
                        if r.status_code in (200, 201):
                            count += len(mini)
                        else:
                            print(f"      WARN: {r.status_code} {r.text[:200]}")
                    except Exception as e:
                        print(f"      WARN: {e}")
        except Exception as e:
            print(f"    ERRO: {e}")
    return count


# ============================================================
# Fetch emendas from Portal da Transparência
# ============================================================

def fetch_emendas_ano(ano):
    """Fetch all emendas for a given year."""
    print(f"\n  [{ano}] Buscando emendas...")

    if not TRANSPARENCIA_TOKEN:
        print(f"  [{ano}] ERRO: TRANSPARENCIA_TOKEN não definido no .env")
        print("    Registre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email")
        return 0

    api_headers = {
        "chave-api-dados": TRANSPARENCIA_TOKEN,
        "Accept": "application/json",
    }

    records = []
    pagina = 1
    total_pages = 0

    while True:
        url = (f"{TRANSPARENCIA_API}/emendas"
               f"?ano={ano}&pagina={pagina}")
        resp = retry_get(url, timeout=30, headers=api_headers)
        if not resp:
            if pagina == 1:
                print(f"  [{ano}] ERRO: API não respondeu")
            break

        try:
            data = resp.json()
        except Exception:
            break

        if not data:
            break

        if not isinstance(data, list):
            if pagina == 1:
                print(f"  [{ano}] Formato inesperado: {type(data)}")
            break

        for emenda in data:
            codigo = emenda.get("codigoEmenda", "")
            if not codigo:
                continue

            autor = emenda.get("nomeAutor", "") or emenda.get("autor", "")
            tipo = emenda.get("tipoEmenda", "")
            valor_emp = parse_brl_number(emenda.get("valorEmpenhado"))
            valor_liq = parse_brl_number(emenda.get("valorLiquidado"))
            valor_pago = parse_brl_number(emenda.get("valorPago"))
            localidade = emenda.get("localidadeDoGasto", "") or emenda.get("localidade", "")
            funcao = emenda.get("funcao", "")
            subfuncao = emenda.get("subfuncao", "")

            pid = find_politico_by_name(autor) if autor else None

            records.append({
                "politico_id": pid,
                "autor": (autor[:200] if autor else "Desconhecido"),
                "ano": ano,
                "codigo_emenda": str(codigo),
                "tipo_emenda": tipo if tipo else None,
                "valor_empenhado": valor_emp,
                "valor_liquidado": valor_liq,
                "valor_pago": valor_pago,
                "localidade": (localidade[:200] if localidade else None),
                "funcao": (funcao[:100] if funcao else None),
                "subfuncao": (subfuncao[:100] if subfuncao else None),
            })

        total_pages += 1
        if len(data) < 15:  # Default page size is 15
            break

        pagina += 1
        time.sleep(REQUEST_DELAY)

    if not records:
        print(f"  [{ano}] Nenhuma emenda encontrada")
        return 0

    # Deduplicate by (codigo_emenda, ano) - keep last occurrence
    seen = {}
    for r in records:
        key = (r["codigo_emenda"], r["ano"])
        seen[key] = r
    records = list(seen.values())

    matched = sum(1 for r in records if r["politico_id"])
    print(f"  [{ano}] {len(records)} emendas ({matched} com politico_id)")

    inserted = batch_insert_emendas(records)
    print(f"  [{ano}] {inserted:,} emendas inseridas")
    return inserted


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL EMENDAS PARLAMENTARES")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    carregar_cache_politicos()

    if not TRANSPARENCIA_TOKEN:
        print("\n⚠ TRANSPARENCIA_TOKEN não definido!")
        print("  Registre-se em: https://portaldatransparencia.gov.br/api-de-dados/cadastrar-email")
        print("  Adicione ao .env: TRANSPARENCIA_TOKEN=seu_token_aqui")
        print("=" * 60)

    anos = list(range(START_YEAR, END_YEAR + 1))
    print(f"\nAnos: {anos}")
    print("=" * 60)

    total = 0
    for ano in anos:
        n = fetch_emendas_ano(ano)
        total += n

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Total emendas inseridas: {total:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
