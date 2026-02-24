#!/usr/bin/env python3
"""
ETL TSE Resultados - Brasil Data Hub
Enriquece fato_politicos_mandatos com votos_nominais e percentual_votos.
Fonte: TSE votacao_candidato_munzona (1998-2024)
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
TSE_VOTOS_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/votacao_candidato_munzona"
TSE_MAP_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/municipio_tse_ibge/municipio_tse_ibge.zip"

ANOS_GERAIS = [1998, 2002, 2006, 2010, 2014, 2018, 2022]
ANOS_MUNICIPAIS = [2000, 2004, 2008, 2012, 2016, 2020, 2024]
TODOS_ANOS = sorted(set(ANOS_GERAIS + ANOS_MUNICIPAIS))

CARGOS_VALIDOS = {
    "PRESIDENTE", "VICE-PRESIDENTE",
    "GOVERNADOR", "VICE-GOVERNADOR",
    "SENADOR", "1º SUPLENTE", "2º SUPLENTE",
    "DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL",
    "PREFEITO", "VICE-PREFEITO", "VEREADOR",
}
CARGOS_MUNICIPAIS = {"PREFEITO", "VICE-PREFEITO", "VEREADOR"}

UF_IBGE = {
    "AC": 12, "AL": 27, "AM": 13, "AP": 16, "BA": 29, "CE": 23,
    "DF": 53, "ES": 32, "GO": 52, "MA": 21, "MG": 31, "MS": 50,
    "MT": 51, "PA": 15, "PB": 25, "PE": 26, "PI": 22, "PR": 41,
    "RJ": 33, "RN": 24, "RO": 11, "RR": 14, "RS": 43, "SC": 42,
    "SE": 28, "SP": 35, "TO": 17,
}

BATCH_SIZE = 500
START_YEAR = int(os.getenv("START_YEAR", "1998"))

cpf_to_id = {}
tse_to_ibge = {}  # cd_municipio_tse -> (cd_ibge, cd_uf_ibge)
uf_tse_map = {}   # SG_UF -> CD_UF_TSE


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


def carregar_mapeamento_tse_ibge():
    global tse_to_ibge, uf_tse_map
    print("Carregando mapeamento TSE -> IBGE...")
    resp = retry_get(TSE_MAP_URL, timeout=30)
    if not resp:
        print("  ERRO: download falhou")
        return
    zf = zipfile.ZipFile(io.BytesIO(resp.content))
    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
    with zf.open(csv_files[0]) as f:
        content = f.read().decode('latin-1')
        reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
        header = next(reader)
        idx = {col: i for i, col in enumerate(header)}
        for row in reader:
            cd_tse = row[idx["CD_MUNICIPIO_TSE"]].strip()
            cd_ibge = row[idx["CD_MUNICIPIO_IBGE"]].strip()
            cd_uf_ibge = row[idx["CD_UF_IBGE"]].strip()
            sg_uf = row[idx["SG_UF"]].strip()
            cd_uf_tse = row[idx["CD_UF_TSE"]].strip()
            if cd_tse and cd_ibge:
                tse_to_ibge[cd_tse] = (int(cd_ibge), int(cd_uf_ibge))
            if sg_uf and cd_uf_tse:
                uf_tse_map[sg_uf] = int(cd_uf_tse)
    print(f"  {len(tse_to_ibge)} municípios mapeados")


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
# Build SQ_CANDIDATO -> cpf_key mapping from candidatos CSV
# ============================================================

def build_sq_map(ano):
    """Download candidatos CSV, return {SQ_CANDIDATO: {cpf_key, cargo, codigo_ibge}}."""
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

                        sg_uf = row[idx["SG_UF"]].strip() if "SG_UF" in idx else ""
                        codigo_ibge_uf = UF_IBGE.get(sg_uf, 0)
                        codigo_tse = uf_tse_map.get(sg_uf, 0)

                        if cargo.upper() in CARGOS_MUNICIPAIS:
                            sg_ue = row[idx["SG_UE"]].strip() if "SG_UE" in idx else ""
                            ibge_info = tse_to_ibge.get(sg_ue)
                            codigo_ibge = ibge_info[0] if ibge_info else 0
                        else:
                            codigo_ibge = 0

                        sq_map[sq] = {
                            'cpf_key': cpf_key,
                            'cargo': cargo,
                            'codigo_ibge': codigo_ibge,
                            'codigo_ibge_uf': codigo_ibge_uf,
                            'codigo_tse': codigo_tse,
                        }
                    except (IndexError, ValueError, KeyError):
                        continue
    except Exception as e:
        print(f"    ERRO candidatos: {e}")
    return sq_map


# ============================================================
# Batch upsert votos
# ============================================================

def batch_upsert_votos(records):
    if not records:
        return 0

    url = (f"{SUPABASE_URL}/rest/v1/fato_politicos_mandatos"
           f"?on_conflict=politico_id,codigo_ibge,ano_eleicao,cargo,turno")
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
# Process votes for one year
# ============================================================

def processar_votos_ano(ano):
    # Step 1: Build SQ_CANDIDATO mapping from candidatos CSV
    print(f"\n  [{ano}] Mapeando candidatos...")
    sq_map = build_sq_map(ano)
    if not sq_map:
        print(f"  [{ano}] Sem candidatos - pulando")
        return 0
    print(f"  [{ano}] {len(sq_map):,} candidatos")

    # Step 2: Download votos ZIP
    print(f"  [{ano}] Baixando votos...")
    votos_url = f"{TSE_VOTOS_URL}/votacao_candidato_munzona_{ano}.zip"
    resp = retry_get(votos_url, timeout=600)
    if not resp:
        print(f"  [{ano}] ERRO: download votos falhou")
        return 0
    size_mb = len(resp.content) / (1024 * 1024)
    print(f"  [{ano}] ZIP: {size_mb:.0f} MB")

    # Step 3: Aggregate votes across all state CSVs
    cand_votos = {}   # (sq, turno) -> total
    ctx_totais = {}   # (cargo, turno, unit) -> total
    cand_unit = {}    # (sq, turno) -> unit

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        print(f"  [{ano}] ERRO: ZIP corrompido")
        return 0

    csv_files = sorted([f for f in zf.namelist()
                        if f.endswith('.csv') and '_BRASIL.' not in f.upper()])

    total_linhas = 0
    for csv_file in csv_files:
        with zf.open(csv_file) as f:
            content = f.read().decode('latin-1')
            reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
            header = next(reader)
            idx = {col: i for i, col in enumerate(header)}

            i_sq = idx.get("SQ_CANDIDATO")
            i_turno = idx.get("NR_TURNO")
            i_votos = idx.get("QT_VOTOS_NOMINAIS")
            i_cargo = idx.get("DS_CARGO")
            i_uf = idx.get("SG_UF")
            i_mun = idx.get("CD_MUNICIPIO")

            if None in (i_sq, i_turno, i_votos, i_cargo):
                continue

            for row in reader:
                try:
                    votos_str = row[i_votos].strip()
                    if is_nulo(votos_str):
                        continue
                    votos = int(votos_str)
                    if votos < 0:
                        continue

                    sq = row[i_sq].strip()
                    turno = int(row[i_turno])
                    cargo = row[i_cargo].strip()
                    cargo_upper = cargo.upper()
                    if cargo_upper not in CARGOS_VALIDOS:
                        continue

                    uf = row[i_uf].strip() if i_uf is not None else ""
                    if cargo_upper in ("PRESIDENTE", "VICE-PRESIDENTE"):
                        unit = "BR"
                    elif cargo_upper in CARGOS_MUNICIPAIS:
                        unit = row[i_mun].strip() if i_mun is not None else uf
                    else:
                        unit = uf

                    key = (sq, turno)
                    cand_votos[key] = cand_votos.get(key, 0) + votos
                    cand_unit[key] = unit

                    ctx = (cargo, turno, unit)
                    ctx_totais[ctx] = ctx_totais.get(ctx, 0) + votos

                    total_linhas += 1
                except (IndexError, ValueError):
                    continue

    print(f"  [{ano}] {len(csv_files)} arquivos, {total_linhas:,} linhas, "
          f"{len(cand_votos):,} candidatos com votos")

    # Step 4: Build upsert records
    records = []
    sem_sq = 0
    sem_pid = 0
    for (sq, turno), votos in cand_votos.items():
        info = sq_map.get(sq)
        if not info:
            sem_sq += 1
            continue

        pid = cpf_to_id.get(info['cpf_key'])
        if not pid:
            sem_pid += 1
            continue

        unit = cand_unit.get((sq, turno), "")
        ctx = (info['cargo'], turno, unit)
        total = ctx_totais.get(ctx, 0)
        pct = round((votos / total) * 100, 4) if total > 0 else 0

        records.append({
            "politico_id": pid,
            "codigo_ibge": info['codigo_ibge'],
            "codigo_ibge_uf": info['codigo_ibge_uf'],
            "codigo_tse": info['codigo_tse'],
            "ano_eleicao": ano,
            "cargo": info['cargo'],
            "turno": turno,
            "votos_nominais": votos,
            "percentual_votos": pct,
        })

    if sem_sq or sem_pid:
        print(f"  [{ano}] Skip: {sem_sq} sem SQ match, {sem_pid} sem politico_id")

    # Step 5: Batch upsert
    updated = batch_upsert_votos(records)
    print(f"  [{ano}] {updated:,} mandatos atualizados")
    return updated


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL TSE RESULTADOS - votos_nominais + percentual_votos")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    carregar_mapeamento_tse_ibge()
    carregar_cache_politicos()

    anos = [a for a in TODOS_ANOS if a >= START_YEAR]
    print(f"\nAnos: {anos}")
    print("=" * 60)

    total = 0
    for ano in anos:
        n = processar_votos_ano(ano)
        total += n

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Total mandatos atualizados: {total:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
