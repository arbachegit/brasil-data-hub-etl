#!/usr/bin/env python3
"""
ETL TSE Receitas de Campanha - Brasil Data Hub
Popula fato_receitas_campanha com doações recebidas por candidatos.
Fonte: TSE prestacao_contas (2006-2024)

File format varies significantly across years:
- 2006: Single CSV, columns like SEQUENCIAL_CANDIDATO, VALOR_RECEITA
- 2008: Single CSV with _brasil suffix, columns like SEQUENCIAL_CANDIDATO, VR_RECEITA
- 2010: Per-state TXT in nested dirs, columns with spaces
- 2012-2016: Per-state TXT, columns with spaces
- 2018+: Per-state CSV, standard SQ_CANDIDATO/VR_RECEITA columns
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
TSE_RECEITAS_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/prestacao_contas"

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
    """Parse Brazilian monetary value: '1500,00' -> 1500.00"""
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
# Batch insert receitas
# ============================================================

def batch_insert_receitas(records):
    if not records:
        return 0

    url = (f"{SUPABASE_URL}/rest/v1/fato_receitas_campanha"
           f"?on_conflict=politico_id,ano_eleicao,sequencial")
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
# Download receitas ZIP (URL varies by year)
# ============================================================

def download_receitas_zip(ano):
    """Try multiple URL patterns to download prestação de contas."""
    url_patterns = [
        f"{TSE_RECEITAS_URL}/prestacao_de_contas_eleitorais_candidatos_{ano}.zip",
        f"{TSE_RECEITAS_URL}/prestacao_contas_{ano}.zip",
        f"{TSE_RECEITAS_URL}/prestacao_final_{ano}.zip",
        f"{TSE_RECEITAS_URL}/prestacao_de_contas_{ano}.zip",
        f"{TSE_RECEITAS_URL}/prestacao_contas_eleitorais_{ano}.zip",
    ]
    for url in url_patterns:
        resp = retry_get(url, timeout=300)
        if resp:
            return resp
    return None


# ============================================================
# Find receitas files in ZIP (format varies by year)
# ============================================================

def find_receitas_files(zf):
    """Find receitas_candidatos files in ZIP. Handles .csv and .txt, various naming."""
    all_files = zf.namelist()

    # Get all data files (csv + txt)
    data_files = [f for f in all_files if f.endswith('.csv') or f.endswith('.txt')]

    # Find files containing 'receita' and 'candidato' (but not 'doador_originario')
    receitas = [f for f in data_files
                if 'receita' in f.lower() and 'candidato' in f.lower()
                and 'doador_originario' not in f.lower()]

    # If we have per-state files, exclude BR/BRASIL aggregate
    if len(receitas) > 2:
        non_br = [f for f in receitas
                  if '/BR/' not in f and '_BRASIL.' not in f.upper()
                  and '_brasil.' not in f]
        if non_br:
            receitas = non_br

    if receitas:
        return sorted(receitas)

    # Fallback: any file containing 'receita' or 'Receita'
    receitas = [f for f in data_files if 'receita' in f.lower()]
    if len(receitas) > 2:
        non_br = [f for f in receitas
                  if '/BR/' not in f and '_BRASIL.' not in f.upper()
                  and '_brasil.' not in f]
        if non_br:
            receitas = non_br

    return sorted(receitas)


# ============================================================
# Column name resolver (handles 3+ naming conventions)
# ============================================================

def resolve_col(idx, *candidates):
    """Find the first matching column name from candidates."""
    for name in candidates:
        if name in idx:
            return idx[name]
    return None


# ============================================================
# Process receitas for one year
# ============================================================

def processar_receitas_ano(ano):
    # Step 1: Build SQ_CANDIDATO -> cpf_key
    print(f"\n  [{ano}] Mapeando candidatos...")
    sq_map = build_sq_map(ano)
    if not sq_map:
        print(f"  [{ano}] Sem candidatos - pulando")
        return 0
    print(f"  [{ano}] {len(sq_map):,} candidatos")

    # Step 2: Download receitas ZIP
    print(f"  [{ano}] Baixando receitas...")
    resp = download_receitas_zip(ano)
    if not resp:
        print(f"  [{ano}] ERRO: download falhou")
        return 0

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        print(f"  [{ano}] ERRO: ZIP corrompido")
        return 0

    # Step 3: Find receitas files
    csv_files = find_receitas_files(zf)
    if not csv_files:
        print(f"  [{ano}] Nenhum arquivo de receitas encontrado")
        all_files = [f for f in zf.namelist() if not f.endswith('/')]
        if all_files:
            print(f"    Arquivos disponíveis: {all_files[:5]}...")
        return 0
    print(f"  [{ano}] {len(csv_files)} arquivos de receitas")

    # Step 4: Parse receitas
    records = []
    sem_match = 0
    seen_seq = set()

    for csv_file in csv_files:
        with zf.open(csv_file) as f:
            content = f.read().decode('latin-1')
            reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
            header = next(reader)
            idx = {col: i for i, col in enumerate(header)}

            # Resolve column indices across all naming conventions
            i_sq = resolve_col(idx,
                "SQ_CANDIDATO", "SEQUENCIAL_CANDIDATO",
                "Sequencial Candidato", "SQ_PRESTADOR_CONTAS")
            i_valor = resolve_col(idx,
                "VR_RECEITA", "VALOR_RECEITA", "Valor receita")
            i_seq = resolve_col(idx,
                "SQ_RECEITA", "NR_RECIBO_ELEITORAL",
                "Numero Recibo Eleitoral", "Número Recibo Eleitoral")
            i_tipo = resolve_col(idx,
                "DS_ORIGEM_RECEITA", "DS_FONTE_RECEITA",
                "TIPO_RECEITA", "Tipo receita", "DS_TITULO")
            i_fonte = resolve_col(idx,
                "DS_FONTE_RECEITA", "DESCRICAO_TIPO_RECURSO",
                "Fonte recurso")
            i_cpf_doador = resolve_col(idx,
                "NR_CPF_CNPJ_DOADOR", "NUMERO_CPF_CGC_DOADOR",
                "CD_CPF_CNPJ_DOADOR", "CPF/CNPJ do doador")
            i_nome_doador = resolve_col(idx,
                "NM_DOADOR", "NOME_DOADOR", "Nome do doador")

            if i_sq is None or i_valor is None:
                print(f"    SKIP {csv_file}: SQ={i_sq} VALOR={i_valor}")
                continue

            for row in reader:
                try:
                    sq = row[i_sq].strip() if i_sq is not None else None
                    if not sq:
                        continue

                    cpf_key = sq_map.get(sq)
                    if not cpf_key:
                        sem_match += 1
                        continue

                    pid = cpf_to_id.get(cpf_key)
                    if not pid:
                        sem_match += 1
                        continue

                    valor = parse_valor_br(row[i_valor]) if i_valor is not None else None
                    if valor is None:
                        continue

                    # Sequencial (unique key) - use SQ_RECEITA or generate from row
                    seq = row[i_seq].strip() if i_seq is not None else None
                    if not seq or is_nulo(seq):
                        seq = hashlib.md5(f"{sq}|{ano}|{len(records)}".encode()).hexdigest()[:16]

                    # Dedup within year
                    dedup_key = (pid, ano, seq)
                    if dedup_key in seen_seq:
                        continue
                    seen_seq.add(dedup_key)

                    tipo = row[i_tipo].strip() if i_tipo is not None else None
                    fonte = row[i_fonte].strip() if i_fonte is not None else None
                    cpf_doador = row[i_cpf_doador].strip() if i_cpf_doador is not None else None
                    nome_doador = row[i_nome_doador].strip() if i_nome_doador is not None else None

                    if is_nulo(tipo):
                        tipo = None
                    if is_nulo(fonte):
                        fonte = None
                    if is_nulo(cpf_doador):
                        cpf_doador = None
                    if is_nulo(nome_doador):
                        nome_doador = None

                    records.append({
                        "politico_id": pid,
                        "ano_eleicao": ano,
                        "tipo_receita": tipo,
                        "fonte_recurso": fonte,
                        "valor": valor,
                        "cpf_cnpj_doador": cpf_doador,
                        "nome_doador": nome_doador[:200] if nome_doador else None,
                        "sequencial": seq,
                    })
                except (IndexError, ValueError, KeyError):
                    continue

    if sem_match:
        print(f"  [{ano}] {sem_match:,} receitas sem match")

    print(f"  [{ano}] {len(records):,} receitas para inserir")

    # Step 5: Batch insert
    inserted = batch_insert_receitas(records)
    print(f"  [{ano}] {inserted:,} receitas inseridas")
    return inserted


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL TSE RECEITAS DE CAMPANHA")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    carregar_cache_politicos()

    anos = [a for a in TODOS_ANOS if a >= START_YEAR]
    print(f"\nAnos: {anos}")
    print("=" * 60)

    total = 0
    for ano in anos:
        n = processar_receitas_ano(ano)
        total += n

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Total receitas inseridas: {total:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
