#!/usr/bin/env python3
"""
ETL TSE Perfil Filiação Partidária - Brasil Data Hub
Popula fato_perfil_filiacao_partidaria com dados agregados de filiados.
Fonte: TSE perfil_filiacao_partidaria (atualização semanal)
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

TSE_FILIACAO_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/filiacao_partidaria/perfil_filiacao_partidaria.zip"

BATCH_SIZE = 500

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fato_perfil_filiacao_partidaria (
  id BIGSERIAL PRIMARY KEY,
  ano_mes INTEGER NOT NULL,
  sigla_partido TEXT NOT NULL,
  nome_partido TEXT,
  uf TEXT NOT NULL,
  cod_municipio TEXT,
  nome_municipio TEXT,
  zona INTEGER,
  genero TEXT,
  faixa_etaria TEXT,
  estado_civil TEXT,
  grau_instrucao TEXT,
  ocupacao TEXT,
  raca_cor TEXT,
  qt_filiados INTEGER NOT NULL,
  hash_key TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_filiacao_partido ON fato_perfil_filiacao_partidaria(sigla_partido);
CREATE INDEX IF NOT EXISTS idx_filiacao_uf ON fato_perfil_filiacao_partidaria(uf);
CREATE INDEX IF NOT EXISTS idx_filiacao_ano_mes ON fato_perfil_filiacao_partidaria(ano_mes);
"""


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


def make_hash_key(ano_mes, partido, uf, mun, zona, genero, faixa, civil, instrucao, ocup, raca):
    raw = f"{ano_mes}|{partido}|{uf}|{mun}|{zona}|{genero}|{faixa}|{civil}|{instrucao}|{ocup}|{raca}"
    return hashlib.md5(raw.encode()).hexdigest()


# ============================================================
# Batch insert
# ============================================================

def batch_insert(records):
    if not records:
        return 0

    # Deduplicate by hash_key within the batch (PG rejects dupes in same INSERT)
    seen = {}
    for r in records:
        seen[r["hash_key"]] = r
    records = list(seen.values())

    url = (f"{SUPABASE_URL}/rest/v1/fato_perfil_filiacao_partidaria"
           f"?on_conflict=hash_key")
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
# Download and process
# ============================================================

def processar_filiacoes():
    print("\nBaixando ZIP de filiação partidária (~229 MB)...")
    resp = retry_get(TSE_FILIACAO_URL, timeout=300)
    if not resp:
        print("ERRO: download falhou")
        return 0

    print(f"  Download OK ({len(resp.content) / 1024 / 1024:.0f} MB)")

    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
    except zipfile.BadZipFile:
        print("ERRO: ZIP corrompido")
        return 0

    csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
    if not csv_files:
        print("ERRO: nenhum CSV no ZIP")
        return 0

    csv_file = csv_files[0]
    print(f"  Processando: {csv_file}")

    total_inserted = 0
    total_rows = 0
    records = []

    with zf.open(csv_file) as f:
        text_stream = io.TextIOWrapper(f, encoding='latin-1')
        reader = csv.reader(text_stream, delimiter=';', quotechar='"')
        header = next(reader)
        idx = {col.strip(): i for i, col in enumerate(header)}

        for row in reader:
            try:
                ano_mes_raw = row[idx["NR_ANO_MES"]].strip()
                if is_nulo(ano_mes_raw):
                    continue
                ano_mes = int(ano_mes_raw)

                sg_partido = row[idx["SG_PARTIDO"]].strip()
                nm_partido = row[idx["NM_PARTIDO"]].strip()
                uf = row[idx["SG_UF"]].strip()
                cod_mun = row[idx["CD_MUNICIPIO"]].strip()
                nm_mun = row[idx["NM_MUNICIPIO"]].strip()

                zona_raw = row[idx["NR_ZONA"]].strip()
                zona = int(zona_raw) if zona_raw and zona_raw.isdigit() else None

                genero = row[idx["DS_GENERO"]].strip() if "DS_GENERO" in idx else None
                faixa = row[idx["DS_FAIXA_ETARIA"]].strip() if "DS_FAIXA_ETARIA" in idx else None
                civil = row[idx["DS_ESTADO_CIVIL"]].strip() if "DS_ESTADO_CIVIL" in idx else None
                instrucao = row[idx["DS_GRAU_INSTRUCAO"]].strip() if "DS_GRAU_INSTRUCAO" in idx else None
                ocupacao = row[idx["NM_OCUPACAO"]].strip() if "NM_OCUPACAO" in idx else None
                raca = row[idx["DS_RACA_COR"]].strip() if "DS_RACA_COR" in idx else None

                qt_raw = row[idx["QT_FILIADO"]].strip()
                if not qt_raw or not qt_raw.isdigit():
                    continue
                qt_filiados = int(qt_raw)

                if is_nulo(genero):
                    genero = None
                if is_nulo(faixa):
                    faixa = None
                if is_nulo(civil):
                    civil = None
                if is_nulo(instrucao):
                    instrucao = None
                if is_nulo(ocupacao):
                    ocupacao = None
                if is_nulo(raca):
                    raca = None
                if is_nulo(cod_mun):
                    cod_mun = None
                if is_nulo(nm_mun):
                    nm_mun = None
                if is_nulo(nm_partido):
                    nm_partido = None

                hash_key = make_hash_key(
                    ano_mes, sg_partido, uf, cod_mun or "", zona or "",
                    genero or "", faixa or "", civil or "",
                    instrucao or "", ocupacao or "", raca or ""
                )

                records.append({
                    "ano_mes": ano_mes,
                    "sigla_partido": sg_partido,
                    "nome_partido": nm_partido,
                    "uf": uf,
                    "cod_municipio": cod_mun,
                    "nome_municipio": nm_mun,
                    "zona": zona,
                    "genero": genero,
                    "faixa_etaria": faixa,
                    "estado_civil": civil,
                    "grau_instrucao": instrucao,
                    "ocupacao": ocupacao,
                    "raca_cor": raca,
                    "qt_filiados": qt_filiados,
                    "hash_key": hash_key,
                })

                total_rows += 1

                # Flush in batches to avoid memory buildup
                if len(records) >= BATCH_SIZE * 10:
                    n = batch_insert(records)
                    total_inserted += n
                    records.clear()

                if total_rows % 100000 == 0:
                    print(f"    ...{total_rows:,} linhas lidas, {total_inserted:,} inseridas")

            except (IndexError, ValueError, KeyError):
                continue

    # Flush remaining
    if records:
        n = batch_insert(records)
        total_inserted += n

    print(f"  Total: {total_rows:,} linhas lidas, {total_inserted:,} inseridas")
    return total_inserted


# ============================================================
# Main
# ============================================================

def verificar_tabela():
    """Check if the table exists. Print SQL if not."""
    url = (f"{SUPABASE_URL}/rest/v1/fato_perfil_filiacao_partidaria"
           f"?select=id&limit=1")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            return True
    except Exception:
        pass
    print("*** TABELA NÃO EXISTE! Execute o SQL no Supabase SQL Editor ***")
    print(CREATE_TABLE_SQL)
    return False


if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL TSE PERFIL FILIAÇÃO PARTIDÁRIA")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    if not verificar_tabela():
        exit(1)

    total = processar_filiacoes()

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Total registros inseridos: {total:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
