#!/usr/bin/env python3
"""
ETL Mandatos 1994/1996 - Preenche os anos antigos com formato de cargo diferente.
Usa return=representation para obter IDs imediatamente.
"""

import os
import csv
import io
import zipfile
import hashlib
import requests
import time
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
TSE_MAP_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/municipio_tse_ibge/municipio_tse_ibge.zip"

CARGOS_VALIDOS = {
    "PRESIDENTE", "VICE-PRESIDENTE", "VICE PRESIDENTE",
    "GOVERNADOR", "VICE-GOVERNADOR", "VICE GOVERNADOR",
    "SENADOR", "1º SUPLENTE", "2º SUPLENTE", "SUPLENTE DE SENADOR",
    "DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL",
    "PREFEITO", "VICE-PREFEITO", "VICE PREFEITO", "VEREADOR",
}
CARGOS_MUNICIPAIS = {"PREFEITO", "VICE-PREFEITO", "VICE PREFEITO", "VEREADOR"}
CARGO_NORMALIZE = {
    "VICE PRESIDENTE": "VICE-PRESIDENTE",
    "VICE GOVERNADOR": "VICE-GOVERNADOR",
    "VICE PREFEITO": "VICE-PREFEITO",
    "SUPLENTE DE SENADOR": "1º SUPLENTE",
}

UF_IBGE = {
    "AC": 12, "AL": 27, "AM": 13, "AP": 16, "BA": 29, "CE": 23,
    "DF": 53, "ES": 32, "GO": 52, "MA": 21, "MG": 31, "MS": 50,
    "MT": 51, "PA": 15, "PB": 25, "PE": 26, "PI": 22, "PR": 41,
    "RJ": 33, "RN": 24, "RO": 11, "RR": 14, "RS": 43, "SC": 42,
    "SE": 28, "SP": 35, "TO": 17,
}

BATCH_SIZE = 500
cpf_to_id = {}
tse_to_ibge = {}
uf_tse_map = {}


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


def determinar_eleito(sit):
    if not sit:
        return False
    s = sit.upper()
    return any(x in s for x in ("ELEITO", "MÉDIA", "QP", "QUOCIENTE"))


def calcular_mandato(cargo, ano):
    a = ano + 1
    if cargo in ("PRESIDENTE", "VICE-PRESIDENTE", "GOVERNADOR", "VICE-GOVERNADOR"):
        return f"{a}-01-01", f"{a + 3}-12-31"
    elif cargo in ("SENADOR", "1º SUPLENTE", "2º SUPLENTE"):
        return f"{a}-02-01", f"{a + 7}-01-31"
    elif cargo in ("DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"):
        return f"{a}-02-01", f"{a + 3}-01-31"
    elif cargo in ("PREFEITO", "VICE-PREFEITO", "VEREADOR"):
        return f"{a}-01-01", f"{a + 3}-12-31"
    return None, None


def carregar_mapeamento_tse_ibge():
    global tse_to_ibge, uf_tse_map
    print("Carregando mapeamento TSE -> IBGE...")
    resp = requests.get(TSE_MAP_URL, timeout=60)
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


def upsert_politicos_with_ids(records):
    """Upsert políticos e retorna mapeamento cpf->id usando return=representation."""
    if not records:
        return

    # Deduplicar por CPF
    seen = set()
    unique = []
    for r in records:
        if r["cpf"] not in seen:
            seen.add(r["cpf"])
            unique.append(r)

    url = f"{SUPABASE_URL}/rest/v1/dim_politicos?on_conflict=cpf"
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=representation"}

    for i in range(0, len(unique), 100):
        batch = unique[i:i + 100]
        for attempt in range(3):
            try:
                resp = requests.post(url, json=batch, headers=h, timeout=120)
                if resp.status_code in (200, 201):
                    for r in resp.json():
                        if r.get("cpf"):
                            cpf_to_id[r["cpf"]] = r["id"]
                    break
                elif resp.status_code >= 500:
                    time.sleep(2 ** attempt)
                else:
                    print(f"    WARN upsert: {resp.status_code} {resp.text[:200]}")
                    break
            except Exception as e:
                print(f"    WARN: {e}")
                time.sleep(2 ** attempt)
        time.sleep(0.2)


def batch_insert_mandatos(records):
    if not records:
        return 0

    seen = set()
    unique = []
    for r in records:
        key = (r["politico_id"], r["codigo_ibge"], r["ano_eleicao"], r["cargo"], r["turno"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    url = (f"{SUPABASE_URL}/rest/v1/fato_politicos_mandatos"
           f"?on_conflict=politico_id,codigo_ibge,ano_eleicao,cargo,turno")
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=headers-only"}

    print(f"      insert_mandatos: {len(unique)} unique de {len(records)} records")
    count = 0
    for i in range(0, len(unique), BATCH_SIZE):
        batch = unique[i:i + BATCH_SIZE]
        for attempt in range(3):
            try:
                resp = requests.post(url, json=batch, headers=h, timeout=120)
                if resp.status_code in (200, 201):
                    count += len(batch)
                    break
                elif resp.status_code >= 500:
                    print(f"      HTTP {resp.status_code} retry {attempt+1}: {resp.text[:200]}")
                    time.sleep(2 ** attempt)
                else:
                    print(f"      HTTP {resp.status_code}: {resp.text[:300]}")
                    # Fallback smaller batches
                    for j in range(0, len(batch), 50):
                        mini = batch[j:j + 50]
                        try:
                            r2 = requests.post(url, json=mini, headers=h, timeout=60)
                            if r2.status_code in (200, 201):
                                count += len(mini)
                        except Exception:
                            pass
                    break
            except Exception as e:
                print(f"    ERRO: {e}")
                time.sleep(2 ** attempt)
        time.sleep(0.1)
    return count


def processar_ano(ano):
    print(f"\n{'='*50}")
    print(f"ANO {ano}")
    print(f"{'='*50}")

    # Tentar arquivo local primeiro, senão baixar
    local_path = f"/tmp/consulta_cand_{ano}.zip"
    if os.path.exists(local_path) and os.path.getsize(local_path) > 10000:
        print(f"  Usando ZIP local: {local_path}")
        zf = zipfile.ZipFile(local_path)
    else:
        url = f"{TSE_CAND_URL}/consulta_cand_{ano}.zip"
        print(f"  Baixando {url}...")
        for attempt in range(5):
            try:
                resp = requests.get(url, timeout=300)
                if resp.status_code == 200:
                    break
            except Exception as e:
                print(f"  Retry {attempt+1}: {e}")
                time.sleep(10 * (attempt + 1))
        else:
            print(f"  ERRO: download falhou")
            return 0
        try:
            zf = zipfile.ZipFile(io.BytesIO(resp.content))
        except zipfile.BadZipFile:
            print(f"  ERRO: ZIP corrompido")
            return 0

    csv_files = sorted([f for f in zf.namelist()
                        if f.endswith('.csv') and '_BRASIL.' not in f.upper()])
    print(f"  {len(csv_files)} arquivos CSV")

    total_ano = 0

    for csv_file in csv_files:
        uf_label = csv_file.split('_')[-1].replace('.csv', '')

        with zf.open(csv_file) as f:
            content = f.read().decode('latin-1')
            reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
            header = next(reader)
            idx = {col: i for i, col in enumerate(header)}

            pols_unique = {}
            mandatos_raw = []

            for row in reader:
                try:
                    cargo = row[idx["DS_CARGO"]]
                    if cargo not in CARGOS_VALIDOS:
                        continue
                    cargo = CARGO_NORMALIZE.get(cargo, cargo)

                    nome = row[idx["NM_CANDIDATO"]].strip()
                    if not nome:
                        continue

                    cpf_raw = row[idx.get("NR_CPF_CANDIDATO", -1)] if "NR_CPF_CANDIDATO" in idx else None
                    cpf = cpf_raw if is_valid_cpf(cpf_raw) else None
                    cpf_key = cpf or fake_cpf(nome, cargo, ano)

                    if cpf_key not in cpf_to_id and cpf_key not in pols_unique:
                        nome_urna = row[idx["NM_URNA_CANDIDATO"]] if "NM_URNA_CANDIDATO" in idx else None
                        if is_nulo(nome_urna):
                            nome_urna = nome

                        sexo_raw = row[idx["DS_GENERO"]] if "DS_GENERO" in idx else None
                        sexo = None
                        if sexo_raw and not is_nulo(sexo_raw):
                            sexo = "M" if "MASC" in sexo_raw.upper() else ("F" if "FEM" in sexo_raw.upper() else None)

                        dt_nasc = row[idx["DT_NASCIMENTO"]] if "DT_NASCIMENTO" in idx else None
                        if dt_nasc and not is_nulo(dt_nasc):
                            try:
                                from datetime import datetime
                                dt_nasc = datetime.strptime(dt_nasc, "%d/%m/%Y").strftime("%Y-%m-%d")
                            except ValueError:
                                dt_nasc = None
                        else:
                            dt_nasc = None

                        grau = row[idx["DS_GRAU_INSTRUCAO"]] if "DS_GRAU_INSTRUCAO" in idx else None
                        ocup = row[idx["DS_OCUPACAO"]] if "DS_OCUPACAO" in idx else None

                        pols_unique[cpf_key] = {
                            "cpf": cpf_key,
                            "nome_completo": nome,
                            "nome_urna": nome_urna,
                            "data_nascimento": dt_nasc,
                            "sexo": sexo,
                            "grau_instrucao": grau if not is_nulo(grau) else None,
                            "ocupacao": ocup if not is_nulo(ocup) else None,
                        }

                    turno = int(row[idx["NR_TURNO"]] or 1)
                    uf = row[idx["SG_UF"]]
                    sg_ue = row[idx["SG_UE"]].strip()
                    nm_ue = row[idx["NM_UE"]].strip()
                    nr_cand = row[idx["NR_CANDIDATO"]]
                    partido_sigla = row[idx["SG_PARTIDO"]]
                    partido_nome = row[idx["NM_PARTIDO"]]

                    coligacao = None
                    for col_name in ("NM_COLIGACAO", "NM_FEDERACAO"):
                        if col_name in idx:
                            v = row[idx[col_name]]
                            if not is_nulo(v):
                                coligacao = v
                                break

                    sit_idx = idx.get("DS_SIT_TOT_TURNO")
                    situacao = row[sit_idx] if sit_idx is not None and len(row) > sit_idx else None
                    if is_nulo(situacao):
                        situacao = None

                    is_municipal = cargo in CARGOS_MUNICIPAIS
                    codigo_ibge = 0
                    codigo_ibge_uf = UF_IBGE.get(uf, 0)
                    codigo_tse_val = 0
                    municipio = None

                    if is_municipal and sg_ue.isdigit():
                        codigo_tse_val = int(sg_ue)
                        ibge_info = tse_to_ibge.get(sg_ue)
                        if ibge_info:
                            codigo_ibge = ibge_info[0]
                            codigo_ibge_uf = ibge_info[1]
                        municipio = nm_ue if not is_nulo(nm_ue) else None
                    else:
                        codigo_tse_val = uf_tse_map.get(uf, 0)
                        municipio = uf if not is_nulo(uf) and uf != "BR" else None

                    inicio, fim = calcular_mandato(cargo, ano)

                    mandatos_raw.append({
                        "cpf_key": cpf_key,
                        "rec": {
                            "codigo_ibge": codigo_ibge,
                            "codigo_ibge_uf": codigo_ibge_uf,
                            "codigo_tse": codigo_tse_val,
                            "municipio": municipio,
                            "cargo": cargo,
                            "ano_eleicao": ano,
                            "turno": turno,
                            "numero_candidato": int(nr_cand) if nr_cand and nr_cand.isdigit() else None,
                            "partido_sigla": partido_sigla if not is_nulo(partido_sigla) else None,
                            "partido_nome": partido_nome if not is_nulo(partido_nome) else None,
                            "coligacao": coligacao,
                            "situacao_turno": situacao,
                            "eleito": determinar_eleito(situacao),
                            "data_inicio_mandato": inicio,
                            "data_fim_mandato": fim,
                        }
                    })

                except (IndexError, ValueError, KeyError):
                    continue

            # 1) Upsert políticos COM return=representation
            new_pols = list(pols_unique.values())
            cache_before = len(cpf_to_id)
            if new_pols:
                upsert_politicos_with_ids(new_pols)
            cache_after = len(cpf_to_id)

            # 2) Montar mandatos com politico_id
            mandatos = []
            skipped = 0
            for m in mandatos_raw:
                pid = cpf_to_id.get(m["cpf_key"])
                if not pid:
                    skipped += 1
                    continue
                rec = m["rec"]
                rec["politico_id"] = pid
                mandatos.append(rec)

            # 3) Insert mandatos
            n = batch_insert_mandatos(mandatos)
            total_ano += n

            print(f"    {uf_label}: {n} mandatos"
                  + (f" (+{len(new_pols)} pol, cache {cache_before}->{cache_after})" if new_pols else "")
                  + (f" [raw={len(mandatos_raw)}, skip={skipped}]"))

    print(f"  [{ano}] TOTAL: {total_ano:,} mandatos")
    return total_ano


if __name__ == "__main__":
    print("=" * 60)
    print("ETL MANDATOS 1994/1996")
    print("=" * 60)

    carregar_mapeamento_tse_ibge()

    total = 0
    for ano in [1994, 1996]:
        n = processar_ano(ano)
        total += n

    print(f"\nTOTAL GERAL: {total:,} mandatos")

    # Verificar
    for ano in [1994, 1996]:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/fato_politicos_mandatos?select=id&ano_eleicao=eq.{ano}&limit=1",
            headers={**HEADERS, "Prefer": "count=exact"}, timeout=30
        )
        c = r.headers.get("content-range", "*/0").split("/")[-1]
        print(f"  {ano}: {c} registros no banco")

    print("Concluído!")
