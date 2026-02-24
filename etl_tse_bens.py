#!/usr/bin/env python3
"""
ETL TSE Bens de Candidato - Brasil Data Hub
Popula fato_bens_candidato com declarações de bens dos candidatos.
Fonte: TSE bem_candidato (2006-2024)
"""

import os
import csv
import io
import zipfile
import hashlib
import requests
import time
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

TSE_CAND_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
TSE_BENS_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/bem_candidato"

ANOS_GERAIS = [2006, 2010, 2014, 2018, 2022]
ANOS_MUNICIPAIS = [2008, 2012, 2016, 2020, 2024]
TODOS_ANOS = sorted(set(ANOS_GERAIS + ANOS_MUNICIPAIS))

CARGOS_VALIDOS = {
    "PRESIDENTE", "VICE-PRESIDENTE",
    "GOVERNADOR", "VICE-GOVERNADOR",
    "SENADOR", "1º SUPLENTE", "2º SUPLENTE",
    "DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL",
    "PREFEITO", "VICE-PREFEITO", "VEREADOR",
}

BATCH_SIZE = 500
START_YEAR = int(os.getenv("START_YEAR", "2006"))

cpf_to_id = {}


# ============================================================
# Helpers
# ============================================================

def retry_get(url, max_retries=3, timeout=60, **kwargs):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp
            if resp.status_code >= 500 or resp.status_code == 429:
                time.sleep((2 ** attempt) * 1)
                continue
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** attempt)
    return None


def is_nulo(val):
    return not val or val in ("#NULO", "#NE#", "#NULO#", "#NE", "-1", "-3", "-4", "")


def is_valid_cpf(val):
    if not val:
        return False
    cleaned = val.strip()
    return len(cleaned) == 11 and cleaned.isdigit()


def fake_cpf(nome, cargo, ano, src="TSE"):
    h = hashlib.md5(f"{nome}|{cargo}|{ano}|{src}".encode()).hexdigest()
    return ''.join(str(int(c, 16) % 10) for c in h[:11])


def parse_valor_br(val):
    """Parse Brazilian monetary value: '350000,00' -> 350000.00"""
    if not val or is_nulo(val):
        return None
    try:
        return float(val.strip().replace('.', '').replace(',', '.'))
    except ValueError:
        return None


def carregar_cache_politicos():
    global cpf_to_id
    print("Carregando cache de políticos...")
    last_id = 0
    total = 0
    page_size = 1000
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/dim_politicos"
               f"?select=id,cpf&id=gt.{last_id}&order=id.asc&limit={page_size}")
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
        last_id = data[-1]["id"]
        total += len(data)
        if total % 50000 == 0:
            print(f"    ...{total} carregados")
        if len(data) < page_size:
            break
    print(f"  {total} políticos ({len(cpf_to_id)} com CPF)")


# ============================================================
# Build SQ_CANDIDATO -> cpf_key mapping
# ============================================================

def build_sq_map(ano):
    """Download candidatos CSV, return {SQ_CANDIDATO: cpf_key}."""
    url = f"{TSE_CAND_URL}/consulta_cand_{ano}.zip"
    resp = retry_get(url, timeout=180)
    if not resp:
        return {}

    sq_map = {}
    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        for csv_file in zf.namelist():
            if not csv_file.endswith('.csv') or '_BRASIL.' in csv_file.upper():
                continue
            with zf.open(csv_file) as f:
                content = f.read().decode('latin-1')
                reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
                header = next(reader)
                idx = {col: i for i, col in enumerate(header)}

                for row in reader:
                    try:
                        sq = row[idx["SQ_CANDIDATO"]].strip()
                        if not sq or is_nulo(sq):
                            continue
                        nome = row[idx["NM_CANDIDATO"]].strip()
                        cargo = row[idx["DS_CARGO"]].strip()
                        if cargo.upper() not in CARGOS_VALIDOS or not nome:
                            continue

                        cpf_raw = row[idx["NR_CPF_CANDIDATO"]].strip() if "NR_CPF_CANDIDATO" in idx else None
                        cpf = cpf_raw if is_valid_cpf(cpf_raw) else None
                        cpf_key = cpf or fake_cpf(nome, cargo, ano)
                        sq_map[sq] = cpf_key
                    except (IndexError, ValueError, KeyError):
                        continue
    except Exception as e:
        print(f"    ERRO candidatos: {e}")
    return sq_map


# ============================================================
# Batch insert bens
# ============================================================

def batch_insert_bens(records):
    if not records:
        return 0

    url = (f"{SUPABASE_URL}/rest/v1/fato_bens_candidato"
           f"?on_conflict=politico_id,ano_eleicao,ordem")
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
# Process bens for one year
# ============================================================

def processar_bens_ano(ano):
    # Step 1: Build SQ_CANDIDATO -> cpf_key
    print(f"\n  [{ano}] Mapeando candidatos...")
    sq_map = build_sq_map(ano)
    if not sq_map:
        print(f"  [{ano}] Sem candidatos - pulando")
        return 0
    print(f"  [{ano}] {len(sq_map):,} candidatos")

    # Step 2: Download bens ZIP
    print(f"  [{ano}] Baixando bens...")
    bens_url = f"{TSE_BENS_URL}/bem_candidato_{ano}.zip"
    resp = retry_get(bens_url, timeout=180)
    if not resp:
        print(f"  [{ano}] ERRO: download falhou")
        return 0

    # Step 3: Parse bens CSV
    records = []
    sem_match = 0

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        print(f"  [{ano}] ERRO: ZIP corrompido")
        return 0

    csv_files = sorted([f for f in zf.namelist()
                        if f.endswith('.csv') and '_BRASIL.' not in f.upper()])

    # Track ordem per (politico_id, ano) for unique constraint
    ordem_counter = {}

    for csv_file in csv_files:
        with zf.open(csv_file) as f:
            content = f.read().decode('latin-1')
            reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
            header = next(reader)
            idx = {col: i for i, col in enumerate(header)}

            for row in reader:
                try:
                    sq = row[idx["SQ_CANDIDATO"]].strip()
                    cpf_key = sq_map.get(sq)
                    if not cpf_key:
                        sem_match += 1
                        continue

                    pid = cpf_to_id.get(cpf_key)
                    if not pid:
                        sem_match += 1
                        continue

                    # Ordem from CSV or auto-increment
                    ordem_raw = row[idx["NR_ORDEM_BEM_CANDIDATO"]].strip() if "NR_ORDEM_BEM_CANDIDATO" in idx else None
                    if ordem_raw and ordem_raw.isdigit():
                        ordem = int(ordem_raw)
                    else:
                        key = (pid, ano)
                        ordem_counter[key] = ordem_counter.get(key, 0) + 1
                        ordem = ordem_counter[key]

                    tipo = row[idx["DS_TIPO_BEM_CANDIDATO"]].strip() if "DS_TIPO_BEM_CANDIDATO" in idx else None
                    descricao = row[idx["DS_BEM_CANDIDATO"]].strip() if "DS_BEM_CANDIDATO" in idx else None
                    valor = parse_valor_br(row[idx["VR_BEM_CANDIDATO"]]) if "VR_BEM_CANDIDATO" in idx else None

                    if is_nulo(tipo):
                        tipo = None
                    if is_nulo(descricao):
                        descricao = None

                    records.append({
                        "politico_id": pid,
                        "ano_eleicao": ano,
                        "ordem": ordem,
                        "tipo_bem": tipo,
                        "descricao": descricao[:500] if descricao else None,
                        "valor": valor,
                    })
                except (IndexError, ValueError, KeyError):
                    continue

    if sem_match:
        print(f"  [{ano}] {sem_match:,} bens sem match")

    # Step 4: Batch insert
    inserted = batch_insert_bens(records)
    print(f"  [{ano}] {inserted:,} bens inseridos")
    return inserted


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL TSE BENS DE CANDIDATO")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    carregar_cache_politicos()

    anos = [a for a in TODOS_ANOS if a >= START_YEAR]
    print(f"\nAnos: {anos}")
    print("=" * 60)

    total = 0
    for ano in anos:
        n = processar_bens_ano(ano)
        total += n

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Total bens inseridos: {total:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
