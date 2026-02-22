#!/usr/bin/env python3
"""
ETL Políticos e Mandatos - Brasil Data Hub
Fontes: TSE (Dados Abertos CSV), Câmara dos Deputados API, Senado Federal API

Busca 100% dos dados de candidaturas e mandatos políticos brasileiros.
Popula dim_politicos + fato_politicos_mandatos no Supabase.
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

# ============================================================
# Constantes
# ============================================================

TSE_CAND_URL = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"
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

CAMARA_API = "https://dadosabertos.camara.leg.br/api/v2"
SENADO_API = "https://legis.senado.leg.br/dadosabertos"

BATCH_SIZE = 500  # Bigger batches for speed
POL_BATCH_SIZE = 200  # Smaller batches for politician upsert (return=representation is heavy)
REQUEST_DELAY = 0.1

# Ano inicial (para reprocessar apenas anos pendentes)
START_YEAR = int(os.getenv("START_YEAR", "1998"))

# Caches globais
cpf_to_id = {}  # cpf -> politico_id
tse_to_ibge = {}  # cd_municipio_tse -> (cd_ibge, cd_uf_ibge)
uf_tse_map = {}  # SG_UF -> CD_UF_TSE


# ============================================================
# Supabase REST helpers (direto, sem ORM)
# ============================================================

def sb_post(table, data, prefer="resolution=merge-duplicates,return=representation"):
    """POST direto no Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    h = {**HEADERS, "Prefer": prefer}
    resp = requests.post(url, json=data, headers=h, timeout=60)
    if resp.status_code in (200, 201):
        return resp.json()
    return None


def sb_get(table, params="", limit=1000):
    """GET direto no Supabase REST API."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?{params}&limit={limit}"
    resp = requests.get(url, headers=HEADERS, timeout=60)
    if resp.status_code == 200:
        return resp.json()
    return []


def retry_get(url, max_retries=3, timeout=60, **kwargs):
    """GET com retry."""
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


# ============================================================
# Mapeamentos e cache
# ============================================================

def carregar_mapeamento_tse_ibge():
    """Baixa e carrega mapeamento TSE -> IBGE."""
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
    """Carrega cache CPF -> politico_id usando keyset pagination."""
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


def fetch_ids_by_cpf(cpfs):
    """Busca IDs de políticos pelo CPF e atualiza cache."""
    if not cpfs:
        return
    QUERY_BATCH = 200
    for i in range(0, len(cpfs), QUERY_BATCH):
        batch = cpfs[i:i + QUERY_BATCH]
        cpf_list = ",".join(batch)
        try:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/dim_politicos?select=id,cpf&cpf=in.({cpf_list})",
                headers=HEADERS, timeout=60
            )
            if resp.status_code == 200:
                for r in resp.json():
                    if r.get("cpf"):
                        cpf_to_id[r["cpf"]] = r["id"]
        except Exception:
            pass


def batch_upsert_politicos(records):
    """Upsert batch de políticos e atualiza cache cpf->id."""
    if not records:
        return

    # Deduplicar por CPF dentro do batch
    seen_cpfs = set()
    unique = []
    for r in records:
        if r["cpf"] not in seen_cpfs:
            seen_cpfs.add(r["cpf"])
            unique.append(r)

    url = f"{SUPABASE_URL}/rest/v1/dim_politicos?on_conflict=cpf"
    # Fase 1: upsert com headers-only (rápido, sem body pesado)
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=headers-only"}

    for i in range(0, len(unique), POL_BATCH_SIZE):
        batch = unique[i:i + POL_BATCH_SIZE]
        try:
            resp = requests.post(url, json=batch, headers=h, timeout=120)
            if resp.status_code not in (200, 201):
                # Fallback: smaller batches
                for j in range(0, len(batch), 50):
                    mini = batch[j:j + 50]
                    try:
                        r2 = requests.post(url, json=mini, headers=h, timeout=60)
                        if r2.status_code not in (200, 201):
                            print(f"      WARN pol upsert: {r2.status_code}")
                    except Exception as e:
                        print(f"      WARN pol upsert: {e}")
        except Exception as e:
            print(f"      WARN pol upsert: {e}")

    # Fase 2: buscar IDs dos CPFs que ainda não estão no cache
    missing_cpfs = [r["cpf"] for r in records if r["cpf"] not in cpf_to_id]
    if missing_cpfs:
        fetch_ids_by_cpf(missing_cpfs)


def batch_insert_mandatos(records):
    """Insert batch de mandatos com upsert (ignora duplicatas). Retorna contagem."""
    if not records:
        return 0

    # Deduplicar por chave única dentro do batch
    seen = set()
    unique = []
    for r in records:
        key = (r["politico_id"], r["codigo_ibge"], r["ano_eleicao"], r["cargo"], r["turno"])
        if key not in seen:
            seen.add(key)
            unique.append(r)

    count = 0
    # on_conflict como query param é essencial para PostgREST
    url = (f"{SUPABASE_URL}/rest/v1/fato_politicos_mandatos"
           f"?on_conflict=politico_id,codigo_ibge,ano_eleicao,cargo,turno")
    h = {**HEADERS, "Prefer": "resolution=merge-duplicates,return=headers-only"}

    for i in range(0, len(unique), BATCH_SIZE):
        batch = unique[i:i + BATCH_SIZE]
        try:
            resp = requests.post(url, json=batch, headers=h, timeout=120)
            if resp.status_code in (200, 201):
                count += len(batch)
            else:
                # Fallback: batches menores
                for j in range(0, len(batch), 50):
                    mini = batch[j:j + 50]
                    try:
                        r = requests.post(url, json=mini, headers=h, timeout=60)
                        if r.status_code in (200, 201):
                            count += len(mini)
                    except Exception:
                        pass
        except Exception as e:
            print(f"      ERRO: {e}")
    return count


# ============================================================
# Helpers
# ============================================================

def is_nulo(val):
    return not val or val in ("#NULO", "#NE#", "#NULO#", "-1", "-3", "-4", "")


def is_valid_cpf(val):
    """Verifica se parece um CPF válido (11 dígitos)."""
    if not val:
        return False
    cleaned = val.strip()
    return len(cleaned) == 11 and cleaned.isdigit()


def determinar_eleito(sit):
    if not sit:
        return False
    s = sit.upper()
    return any(x in s for x in ("ELEITO", "MÉDIA", "QP", "QUOCIENTE"))


def calcular_mandato(cargo_tse, ano):
    a = ano + 1
    if cargo_tse in ("PRESIDENTE", "VICE-PRESIDENTE", "GOVERNADOR", "VICE-GOVERNADOR"):
        return f"{a}-01-01", f"{a + 3}-12-31"
    elif cargo_tse in ("SENADOR", "1º SUPLENTE", "2º SUPLENTE"):
        return f"{a}-02-01", f"{a + 7}-01-31"
    elif cargo_tse in ("DEPUTADO FEDERAL", "DEPUTADO ESTADUAL", "DEPUTADO DISTRITAL"):
        return f"{a}-02-01", f"{a + 3}-01-31"
    elif cargo_tse in ("PREFEITO", "VICE-PREFEITO", "VEREADOR"):
        return f"{a}-01-01", f"{a + 3}-12-31"
    return None, None


def fake_cpf(nome, cargo, ano, src="TSE"):
    """Gera CPF fake determinístico (11 dígitos) para candidatos sem CPF real."""
    h = hashlib.md5(f"{nome}|{cargo}|{ano}|{src}".encode()).hexdigest()
    # Converter hex para dígitos, garantindo 11 chars
    digits = ''.join(str(int(c, 16) % 10) for c in h[:11])
    return digits


# ============================================================
# Fonte 1: TSE Dados Abertos
# ============================================================

def processar_tse_ano(ano):
    """Baixa e processa CSV de candidatos do TSE para um ano."""
    url = f"{TSE_CAND_URL}/consulta_cand_{ano}.zip"
    print(f"\n  [{ano}] Baixando...")

    resp = retry_get(url, timeout=180)
    if not resp:
        print(f"  [{ano}] ERRO: download falhou")
        return 0

    total_ano = 0
    try:
        zf = zipfile.ZipFile(io.BytesIO(resp.content))
        csv_files = sorted([f for f in zf.namelist()
                            if f.endswith('.csv') and '_BRASIL.' not in f.upper()])
        print(f"  [{ano}] {len(csv_files)} arquivos CSV")

        for csv_file in csv_files:
            uf_label = csv_file.split('_')[-1].replace('.csv', '')

            with zf.open(csv_file) as f:
                content = f.read().decode('latin-1')
                reader = csv.reader(io.StringIO(content), delimiter=';', quotechar='"')
                header = next(reader)
                idx = {col: i for i, col in enumerate(header)}

                # Coletar tudo do arquivo
                pols_unique = {}  # cpf -> record
                mandatos_raw = []

                for row in reader:
                    try:
                        cargo_tse = row[idx.get("DS_CARGO", 14)]
                        if cargo_tse not in CARGOS_VALIDOS:
                            continue

                        nome = row[idx.get("NM_CANDIDATO", 17)].strip()
                        if not nome:
                            continue

                        cpf_raw = row[idx.get("NR_CPF_CANDIDATO", 20)] if "NR_CPF_CANDIDATO" in idx else None
                        cpf = cpf_raw if is_valid_cpf(cpf_raw) else None

                        # CPF real ou fake
                        cpf_key = cpf or fake_cpf(nome, cargo_tse, ano)

                        # Político
                        if cpf_key not in cpf_to_id and cpf_key not in pols_unique:
                            nome_urna = row[idx.get("NM_URNA_CANDIDATO", 18)] if "NM_URNA_CANDIDATO" in idx else None
                            if is_nulo(nome_urna):
                                nome_urna = nome

                            sexo_raw = row[idx.get("DS_GENERO", 39)] if "DS_GENERO" in idx else None
                            sexo = None
                            if sexo_raw and not is_nulo(sexo_raw):
                                sexo = "M" if "MASC" in sexo_raw.upper() else ("F" if "FEM" in sexo_raw.upper() else None)

                            dt_nasc = row[idx.get("DT_NASCIMENTO", 36)] if "DT_NASCIMENTO" in idx else None
                            if dt_nasc and not is_nulo(dt_nasc):
                                try:
                                    dt_nasc = datetime.strptime(dt_nasc, "%d/%m/%Y").strftime("%Y-%m-%d")
                                except ValueError:
                                    dt_nasc = None
                            else:
                                dt_nasc = None

                            grau = row[idx.get("DS_GRAU_INSTRUCAO", 41)] if "DS_GRAU_INSTRUCAO" in idx else None
                            ocup = row[idx.get("DS_OCUPACAO", 47)] if "DS_OCUPACAO" in idx else None

                            pols_unique[cpf_key] = {
                                "cpf": cpf_key,
                                "nome_completo": nome,
                                "nome_urna": nome_urna,
                                "data_nascimento": dt_nasc,
                                "sexo": sexo,
                                "grau_instrucao": grau if not is_nulo(grau) else None,
                                "ocupacao": ocup if not is_nulo(ocup) else None,
                            }

                        # Mandato
                        turno = int(row[idx.get("NR_TURNO", 5)] or 1)
                        uf = row[idx.get("SG_UF", 10)]
                        sg_ue = row[idx.get("SG_UE", 11)].strip()
                        nm_ue = row[idx.get("NM_UE", 12)].strip()
                        nr_cand = row[idx.get("NR_CANDIDATO", 16)]
                        partido_sigla = row[idx.get("SG_PARTIDO", 26)]
                        partido_nome = row[idx.get("NM_PARTIDO", 27)]

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

                        is_municipal = cargo_tse in CARGOS_MUNICIPAIS
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

                        inicio, fim = calcular_mandato(cargo_tse, ano)

                        mandatos_raw.append({
                            "cpf_key": cpf_key,
                            "rec": {
                                "codigo_ibge": codigo_ibge,
                                "codigo_ibge_uf": codigo_ibge_uf,
                                "codigo_tse": codigo_tse_val,
                                "municipio": municipio,
                                "cargo": cargo_tse,
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

                    except (IndexError, ValueError):
                        continue

                # 1) Batch upsert novos políticos
                new_pols = list(pols_unique.values())
                if new_pols:
                    batch_upsert_politicos(new_pols)

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

                # 3) Batch insert mandatos
                n = batch_insert_mandatos(mandatos)
                total_ano += n

                if n > 0 or skipped > 0:
                    print(f"    {uf_label}: {n} mandatos"
                          + (f" (+{len(new_pols)} novos pol.)" if new_pols else "")
                          + (f" [{skipped} skip]" if skipped else ""))

    except zipfile.BadZipFile:
        print(f"  [{ano}] ERRO: ZIP corrompido")
    except Exception as e:
        print(f"  [{ano}] ERRO: {e}")

    print(f"  [{ano}] TOTAL: {total_ano:,} mandatos")
    return total_ano


def fetch_tse_todos_anos():
    """Busca todos os anos eleitorais do TSE."""
    anos = [a for a in TODOS_ANOS if a >= START_YEAR]
    print("\n" + "=" * 60)
    print("FONTE 1: TSE Dados Abertos (CSV)")
    print(f"Anos: {anos}")
    print("=" * 60)

    total = 0
    for ano in anos:
        n = processar_tse_ano(ano)
        total += n
        time.sleep(REQUEST_DELAY)

    print(f"\n  TOTAL TSE: {total:,}")
    return total


# ============================================================
# Fonte 2: Câmara dos Deputados
# ============================================================

def fetch_deputados_camara():
    """Busca deputados de todas as legislaturas."""
    print("\n" + "=" * 60)
    print("FONTE 2: Câmara dos Deputados API")
    print("=" * 60)

    # Info legislaturas
    resp = retry_get(f"{CAMARA_API}/legislaturas?itens=100&ordem=DESC&ordenarPor=id", timeout=30)
    leg_info = {}
    if resp:
        for l in resp.json().get("dados", []):
            leg_info[l["id"]] = (l["dataInicio"], l["dataFim"])

    total = 0
    for leg_id in range(51, 58):
        print(f"\n  Legislatura {leg_id}...")

        # Buscar todos deputados paginando
        deputados = []
        pagina = 1
        while True:
            url = (f"{CAMARA_API}/deputados"
                   f"?idLegislatura={leg_id}"
                   f"&itens=100&pagina={pagina}&ordem=ASC&ordenarPor=nome")
            resp = retry_get(url, timeout=30)
            if not resp:
                break
            data = resp.json()
            dados = data.get("dados", [])
            if not dados:
                break
            deputados.extend(dados)
            if not any(l.get("rel") == "next" for l in data.get("links", [])):
                break
            pagina += 1
            time.sleep(REQUEST_DELAY)

        if not deputados:
            continue

        info = leg_info.get(leg_id, (None, None))
        dt_ini, dt_fim = info
        ano_eleicao = int(dt_ini[:4]) - 1 if dt_ini else (1998 + (leg_id - 51) * 4)

        # Upsert políticos (dedup por cpf_key para evitar duplicatas da API)
        pols = []
        seen_cpfs = set()
        for dep in deputados:
            nome = (dep.get("nome") or "").strip()
            if not nome:
                continue
            cpf_key = fake_cpf(nome, "DEPUTADO FEDERAL", ano_eleicao, "CAMARA")
            if cpf_key not in cpf_to_id and cpf_key not in seen_cpfs:
                seen_cpfs.add(cpf_key)
                pols.append({
                    "cpf": cpf_key,
                    "nome_completo": nome,
                    "nome_urna": nome,
                })

        if pols:
            batch_upsert_politicos(pols)

        # Mandatos (dedup por nome para evitar duplicatas de partidos)
        mandatos = []
        seen_mand = set()
        for dep in deputados:
            nome = (dep.get("nome") or "").strip()
            if not nome:
                continue
            cpf_key = fake_cpf(nome, "DEPUTADO FEDERAL", ano_eleicao, "CAMARA")
            if cpf_key in seen_mand:
                continue
            seen_mand.add(cpf_key)
            pid = cpf_to_id.get(cpf_key)
            if not pid:
                continue

            uf = dep.get("siglaUf", "")
            mandatos.append({
                "politico_id": pid,
                "codigo_ibge": 0,
                "codigo_ibge_uf": UF_IBGE.get(uf, 0),
                "codigo_tse": uf_tse_map.get(uf, 0),
                "municipio": uf,
                "cargo": "DEPUTADO FEDERAL",
                "ano_eleicao": ano_eleicao,
                "turno": 1,
                "numero_candidato": None,
                "partido_sigla": dep.get("siglaPartido"),
                "partido_nome": None,
                "coligacao": None,
                "situacao_turno": "ELEITO",
                "eleito": True,
                "data_inicio_mandato": dt_ini,
                "data_fim_mandato": dt_fim,
            })

        n = batch_insert_mandatos(mandatos)
        total += n
        print(f"    {len(deputados)} deputados -> {n} mandatos")

    print(f"\n  TOTAL Câmara: {total:,}")
    return total


# ============================================================
# Fonte 3: Senado Federal
# ============================================================

def fetch_senadores_senado():
    """Busca senadores de todas as legislaturas."""
    print("\n" + "=" * 60)
    print("FONTE 3: Senado Federal API")
    print("=" * 60)

    total = 0
    for leg_id in range(51, 58):
        print(f"\n  Legislatura {leg_id}...")
        url = f"{SENADO_API}/senador/lista/legislatura/{leg_id}"
        resp = retry_get(url, timeout=30, headers={"Accept": "application/json"})
        if not resp:
            continue

        try:
            data = resp.json()
            parlams = (data.get("ListaParlamentarLegislatura", {})
                       .get("Parlamentares", {})
                       .get("Parlamentar", []))
            if not isinstance(parlams, list):
                parlams = [parlams]
        except Exception:
            continue

        # Coletar nomes e mandatos
        pols = []
        mandatos_raw = []

        for p in parlams:
            ident = p.get("IdentificacaoParlamentar", {})
            nome = ident.get("NomeParlamentar", "").strip()
            if not nome:
                continue

            mand_list = p.get("Mandatos", {}).get("Mandato", [])
            if not isinstance(mand_list, list):
                mand_list = [mand_list]

            for mand in mand_list:
                uf = mand.get("UfParlamentar", "")
                part = mand.get("DescricaoParticipacao", "Titular")
                cargo = "SENADOR" if "Titular" in part else "SUPLENTE"

                prim = mand.get("PrimeiraLegislaturaDoMandato", {})
                seg = mand.get("SegundaLegislaturaDoMandato", {})
                m_ini = prim.get("DataInicio")
                m_fim = seg.get("DataFim") or prim.get("DataFim")

                ano_el = int(m_ini[:4]) - 1 if m_ini else (1998 + (leg_id - 51) * 4)

                cpf_key = fake_cpf(nome, "SENADOR", ano_el, "SENADO")
                if cpf_key not in cpf_to_id:
                    pols.append({
                        "cpf": cpf_key,
                        "nome_completo": nome,
                        "nome_urna": nome,
                    })

                mandatos_raw.append({
                    "cpf_key": cpf_key,
                    "uf": uf,
                    "cargo": cargo,
                    "ano_el": ano_el,
                    "part": part,
                    "m_ini": m_ini,
                    "m_fim": m_fim,
                })

        # Dedup e upsert políticos
        seen = set()
        unique_pols = []
        for pol in pols:
            if pol["cpf"] not in seen:
                seen.add(pol["cpf"])
                unique_pols.append(pol)
        if unique_pols:
            batch_upsert_politicos(unique_pols)

        # Montar mandatos
        mandatos = []
        for m in mandatos_raw:
            pid = cpf_to_id.get(m["cpf_key"])
            if not pid:
                continue
            mandatos.append({
                "politico_id": pid,
                "codigo_ibge": 0,
                "codigo_ibge_uf": UF_IBGE.get(m["uf"], 0),
                "codigo_tse": uf_tse_map.get(m["uf"], 0),
                "municipio": m["uf"],
                "cargo": m["cargo"],
                "ano_eleicao": m["ano_el"],
                "turno": 1,
                "numero_candidato": None,
                "partido_sigla": None,
                "partido_nome": None,
                "coligacao": None,
                "situacao_turno": "ELEITO" if m["cargo"] == "SENADOR" else m["part"].upper(),
                "eleito": m["cargo"] == "SENADOR",
                "data_inicio_mandato": m["m_ini"],
                "data_fim_mandato": m["m_fim"],
            })

        n = batch_insert_mandatos(mandatos)
        total += n
        print(f"    {len(parlams)} parlamentares -> {n} mandatos")
        time.sleep(REQUEST_DELAY)

    print(f"\n  TOTAL Senado: {total:,}")
    return total


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL POLÍTICOS E MANDATOS - Brasil Data Hub")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # Carregar mapeamentos
    carregar_mapeamento_tse_ibge()
    carregar_cache_politicos()

    total_geral = 0

    # Fonte 1: TSE (todos os cargos e anos 1998-2024)
    total_tse = fetch_tse_todos_anos()
    total_geral += total_tse

    # Fonte 2: Câmara dos Deputados (legislaturas 51-57)
    total_camara = fetch_deputados_camara()
    total_geral += total_camara

    # Fonte 3: Senado Federal (legislaturas 51-57)
    total_senado = fetch_senadores_senado()
    total_geral += total_senado

    fim = datetime.now()
    duracao = fim - inicio

    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  TSE Dados Abertos:    {total_tse:>12,}")
    print(f"  Câmara dos Deputados: {total_camara:>12,}")
    print(f"  Senado Federal:       {total_senado:>12,}")
    print(f"  {'─' * 44}")
    print(f"  TOTAL:                {total_geral:>12,}")
    print(f"  Duração: {duracao}")
    print("=" * 60)
