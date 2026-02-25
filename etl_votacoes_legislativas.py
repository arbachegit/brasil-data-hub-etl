#!/usr/bin/env python3
"""
ETL Votações Legislativas - Brasil Data Hub
Popula fato_votos_legislativos com votos nominais de deputados e senadores.
Fontes: Câmara dos Deputados API + Senado Federal API
"""

import os
import csv
import io
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

CAMARA_API = "https://dadosabertos.camara.leg.br/api/v2"
CAMARA_BULK = "https://dadosabertos.camara.leg.br/arquivos"
SENADO_API = "https://legis.senado.leg.br/dadosabertos"

BATCH_SIZE = 500
REQUEST_DELAY = 0.3  # Respectful rate limit

# Legislaturas 51-57 (1999-2027)
START_LEG = int(os.getenv("START_LEG", "51"))
END_LEG = int(os.getenv("END_LEG", "57"))

cpf_to_id = {}
nome_to_id = {}  # nome_upper -> politico_id (for senator name matching)
dep_cpf_cache = {}  # dep_api_id -> cpf (cached across legislatures)


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
                time.sleep((2 ** attempt) * 5)
                continue
            if resp.status_code >= 500:
                time.sleep((2 ** attempt) * 2)
                continue
            return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** attempt)
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


def build_senator_name_cache():
    """Build nome_upper -> politico_id from fato_politicos_mandatos + dim_politicos."""
    global nome_to_id
    print("Carregando cache de senadores por nome...")

    # Step 1: Get all unique politico_ids for SENADOR mandatos
    seen_pids = set()
    offset = 0
    page_size = 1000
    while True:
        url = (f"{SUPABASE_URL}/rest/v1/fato_politicos_mandatos"
               f"?select=politico_id&cargo=eq.SENADOR"
               f"&order=politico_id.asc&offset={offset}&limit={page_size}")
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
            seen_pids.add(r["politico_id"])
        offset += len(data)
        if len(data) < page_size:
            break

    if not seen_pids:
        print("  0 senadores encontrados")
        return

    # Step 2: Fetch names from dim_politicos in batches
    pid_list = sorted(seen_pids)
    for i in range(0, len(pid_list), 200):
        batch_pids = pid_list[i:i + 200]
        pid_filter = ",".join(str(p) for p in batch_pids)
        url = (f"{SUPABASE_URL}/rest/v1/dim_politicos"
               f"?select=id,nome_completo,nome_urna"
               f"&id=in.({pid_filter})")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=120)
            if resp.status_code != 200:
                continue
            for r in resp.json():
                pid = r["id"]
                nome = (r.get("nome_completo") or "").strip().upper()
                nome_urna = (r.get("nome_urna") or "").strip().upper()
                if nome_urna:
                    nome_to_id[nome_urna] = pid
                if nome and nome != nome_urna:
                    nome_to_id[nome] = pid
        except Exception:
            continue

    print(f"  {len(nome_to_id)} nomes mapeados ({len(seen_pids)} senadores)")


def batch_insert_votos(records):
    if not records:
        return 0

    url = (f"{SUPABASE_URL}/rest/v1/fato_votos_legislativos"
           f"?on_conflict=politico_id,votacao_id")
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
# Câmara dos Deputados - Votações
# ============================================================

def fetch_dep_cache_for_legislature(leg_id):
    """Fetch deputy list and map dep_id -> politico_id via CPF from API."""
    dep_cache = {}
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

    seen = set()
    for dep in deputados:
        dep_id = dep.get("id")
        if not dep_id or dep_id in seen:
            continue
        seen.add(dep_id)

        # Check global cache first
        if dep_id in dep_cpf_cache:
            cpf = dep_cpf_cache[dep_id]
            pid = cpf_to_id.get(cpf)
            if pid:
                dep_cache[dep_id] = pid
            continue

        # Fetch deputy detail to get CPF
        detail_resp = retry_get(f"{CAMARA_API}/deputados/{dep_id}", timeout=30)
        if detail_resp:
            cpf_raw = (detail_resp.json().get("dados", {}).get("cpf") or "")
            cpf = cpf_raw.strip().replace(".", "").replace("-", "").replace(" ", "")
            if cpf and len(cpf) >= 10:
                cpf = cpf.zfill(11)
                dep_cpf_cache[dep_id] = cpf
                pid = cpf_to_id.get(cpf)
                if pid:
                    dep_cache[dep_id] = pid
        time.sleep(REQUEST_DELAY)

    print(f"    {len(deputados)} deputados, {len(dep_cache)} mapeados")
    return dep_cache


def fetch_votacoes_camara():
    """Fetch all roll call votes from Câmara using bulk CSV downloads."""
    print("\n" + "=" * 60)
    print("CÂMARA DOS DEPUTADOS - Votações (bulk CSV)")
    print("=" * 60)

    total = 0
    for leg_id in range(START_LEG, END_LEG + 1):
        print(f"\n  Legislatura {leg_id}...")

        ano_inicio = 1999 + (leg_id - 51) * 4
        ano_fim = ano_inicio + 3

        # Build deputy cache for this legislature
        dep_cache = fetch_dep_cache_for_legislature(leg_id)

        # Download votações metadata CSV and build votacao_id -> metadata map
        leg_votos = 0
        for ano in range(ano_inicio, min(ano_fim + 1, 2026)):
            # Download votacoes metadata
            vot_meta = {}  # votacao_id -> {descricao, proposicao}
            meta_url = f"{CAMARA_BULK}/votacoes/csv/votacoes-{ano}.csv"
            meta_resp = retry_get(meta_url, timeout=120)
            if meta_resp:
                content = meta_resp.content.decode("utf-8-sig")
                reader = csv.DictReader(io.StringIO(content), delimiter=";")
                for row in reader:
                    vot_id = (row.get("id") or "").strip()
                    if vot_id:
                        desc = (row.get("descricao") or "")[:300]
                        prop = (row.get("ultimaApresentacaoProposicao_descricao") or "")[:200]
                        vot_meta[vot_id] = {"descricao": desc, "proposicao": prop}

            # Download bulk votes CSV
            votos_url = f"{CAMARA_BULK}/votacoesVotos/csv/votacoesVotos-{ano}.csv"
            votos_resp = retry_get(votos_url, timeout=120)
            if not votos_resp:
                print(f"    {ano}: CSV não disponível")
                continue

            content = votos_resp.content.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(content), delimiter=";")

            records = []
            for row in reader:
                dep_id_str = (row.get("deputado_id") or "").strip()
                if not dep_id_str:
                    continue
                try:
                    dep_id = int(dep_id_str)
                except ValueError:
                    continue

                pid = dep_cache.get(dep_id)
                if not pid:
                    continue

                voto = (row.get("voto") or "").strip()
                if not voto:
                    continue

                vot_id = (row.get("idVotacao") or "").strip()
                data_raw = (row.get("dataHoraVoto") or "").strip()
                data_vot = data_raw[:10] if data_raw else None

                meta = vot_meta.get(vot_id, {})

                records.append({
                    "politico_id": pid,
                    "data_votacao": data_vot,
                    "votacao_id": f"CAM-{vot_id}",
                    "voto": voto,
                    "proposicao": (meta.get("proposicao") or None),
                    "descricao_votacao": (meta.get("descricao") or None),
                    "fonte": "CAMARA",
                })

            if records:
                n = batch_insert_votos(records)
                leg_votos += n
                print(f"    {ano}: {len(vot_meta)} votações, {n} votos inseridos")
            else:
                print(f"    {ano}: 0 votos")

        total += leg_votos
        print(f"    TOTAL leg {leg_id}: {leg_votos:,}")

    print(f"\n  TOTAL Câmara: {total:,}")
    return total


# ============================================================
# Senado Federal - Votações
# ============================================================

def fetch_votacoes_senado():
    """Fetch all roll call votes from Senado API."""
    print("\n" + "=" * 60)
    print("SENADO FEDERAL - Votações")
    print("=" * 60)

    total = 0
    for leg_id in range(START_LEG, END_LEG + 1):
        print(f"\n  Legislatura {leg_id}...")

        ano_inicio = 1999 + (leg_id - 51) * 4
        ano_fim = ano_inicio + 3
        ano_eleicao = ano_inicio - 1

        # Build senador name -> politico_id cache via name matching
        sen_cache = {}  # nome_normalizado -> politico_id
        url = f"{SENADO_API}/senador/lista/legislatura/{leg_id}"
        resp = retry_get(url, timeout=30, headers={"Accept": "application/json"})
        if resp:
            try:
                data = resp.json()
                parlams = (data.get("ListaParlamentarLegislatura", {})
                           .get("Parlamentares", {})
                           .get("Parlamentar", []))
                if not isinstance(parlams, list):
                    parlams = [parlams]
                for p in parlams:
                    ident = p.get("IdentificacaoParlamentar", {})
                    nome = ident.get("NomeParlamentar", "").strip()
                    nome_completo = ident.get("NomeCompletoParlamentar", "").strip()
                    if not nome:
                        continue
                    # Try matching by nome_urna first, then full name
                    pid = (nome_to_id.get(nome.upper())
                           or nome_to_id.get(nome_completo.upper()))
                    if pid:
                        sen_cache[nome.upper()] = pid
            except Exception:
                pass

        print(f"    {len(sen_cache)} senadores mapeados")

        # Fetch votações by year (API max 60-day intervals)
        leg_votos = 0
        for ano in range(ano_inicio, min(ano_fim + 1, 2026)):
            # Split each year into 2-month intervals (max 60 days)
            intervals = [
                (f"{ano}0101", f"{ano}0228"),
                (f"{ano}0301", f"{ano}0430"),
                (f"{ano}0501", f"{ano}0630"),
                (f"{ano}0701", f"{ano}0831"),
                (f"{ano}0901", f"{ano}1031"),
                (f"{ano}1101", f"{ano}1231"),
            ]

            sessoes_total = []
            for dt_start, dt_end in intervals:
                url = f"{SENADO_API}/plenario/lista/votacao/{dt_start}/{dt_end}"
                resp = retry_get(url, timeout=30, headers={"Accept": "application/json"})
                if not resp:
                    continue
                try:
                    data = resp.json()
                    sessoes = (data.get("ListaVotacoes", {})
                               .get("Votacoes", {})
                               .get("Votacao", []))
                    if not isinstance(sessoes, list):
                        sessoes = [sessoes] if sessoes else []
                    sessoes_total.extend(sessoes)
                except Exception:
                    continue
                time.sleep(REQUEST_DELAY)

            sessoes = sessoes_total
            if not sessoes:
                continue

            records = []
            for sessao in sessoes:
                cod_sessao = sessao.get("CodigoSessaoVotacao", "")
                seq_votacao = sessao.get("SequencialSessao", "")
                data_sessao = sessao.get("DataSessao", "")
                descricao = sessao.get("DescricaoVotacao", "")
                materia = sessao.get("SiglaMateria", "")
                numero = sessao.get("NumeroMateria", "")
                proposicao_text = f"{materia} {numero}".strip() if materia else ""

                vot_id = f"{cod_sessao}-{seq_votacao}" if seq_votacao else cod_sessao

                # Parse date
                data_vot = None
                if data_sessao:
                    try:
                        data_vot = datetime.strptime(data_sessao, "%d/%m/%Y").strftime("%Y-%m-%d")
                    except ValueError:
                        data_vot = data_sessao[:10] if len(data_sessao) >= 10 else None

                # Get individual votes
                votos_list = sessao.get("Votos", {})
                if isinstance(votos_list, dict):
                    votos_list = votos_list.get("VotoParlamentar", [])
                if not isinstance(votos_list, list):
                    votos_list = [votos_list] if votos_list else []

                for v in votos_list:
                    nome_sen = (v.get("NomeParlamentar") or "").strip().upper()
                    voto = v.get("Voto", "")
                    if not nome_sen or not voto:
                        continue

                    pid = sen_cache.get(nome_sen)
                    if not pid:
                        continue

                    records.append({
                        "politico_id": pid,
                        "data_votacao": data_vot,
                        "votacao_id": f"SEN-{vot_id}",
                        "voto": voto,
                        "proposicao": (proposicao_text[:200] if proposicao_text else None),
                        "descricao_votacao": (descricao[:300] if descricao else None),
                        "fonte": "SENADO",
                    })

            if records:
                n = batch_insert_votos(records)
                leg_votos += n
                print(f"    {ano}: {len(sessoes)} votações, {n} votos inseridos")
            else:
                print(f"    {ano}: {len(sessoes)} votações, 0 votos")

            time.sleep(REQUEST_DELAY)

        total += leg_votos
        print(f"    TOTAL leg {leg_id}: {leg_votos:,}")

    print(f"\n  TOTAL Senado: {total:,}")
    return total


# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    inicio = datetime.now()
    print("=" * 60)
    print("ETL VOTAÇÕES LEGISLATIVAS - Câmara + Senado")
    print(f"Início: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    carregar_cache_politicos()
    build_senator_name_cache()

    total_camara = fetch_votacoes_camara()
    total_senado = fetch_votacoes_senado()

    fim = datetime.now()
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)
    print(f"  Câmara:  {total_camara:,}")
    print(f"  Senado:  {total_senado:,}")
    print(f"  Total:   {total_camara + total_senado:,}")
    print(f"  Duração: {fim - inicio}")
    print("=" * 60)
