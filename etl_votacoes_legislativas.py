#!/usr/bin/env python3
"""
ETL Votações Legislativas - Brasil Data Hub
Popula fato_votos_legislativos com votos nominais de deputados e senadores.
Fontes: Câmara dos Deputados API + Senado Federal API
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

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

CAMARA_API = "https://dadosabertos.camara.leg.br/api/v2"
SENADO_API = "https://legis.senado.leg.br/dadosabertos"

BATCH_SIZE = 500
REQUEST_DELAY = 0.3  # Respectful rate limit

# Legislaturas 51-57 (1999-2027)
START_LEG = int(os.getenv("START_LEG", "51"))
END_LEG = int(os.getenv("END_LEG", "57"))

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


def fake_cpf(nome, cargo, ano, src="TSE"):
    h = hashlib.md5(f"{nome}|{cargo}|{ano}|{src}".encode()).hexdigest()
    return ''.join(str(int(c, 16) % 10) for c in h[:11])


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

def fetch_votacoes_camara():
    """Fetch all roll call votes from Câmara API."""
    print("\n" + "=" * 60)
    print("CÂMARA DOS DEPUTADOS - Votações")
    print("=" * 60)

    total = 0
    for leg_id in range(START_LEG, END_LEG + 1):
        print(f"\n  Legislatura {leg_id}...")

        # Get ano range for this legislature
        ano_inicio = 1999 + (leg_id - 51) * 4
        ano_fim = ano_inicio + 3

        # Build deputy name -> politico_id cache for this legislature
        ano_eleicao = ano_inicio - 1
        dep_cache = {}  # dep_id -> politico_id

        # Fetch deputy list for legislature
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

        # Map dep API id -> politico_id
        seen = set()
        for dep in deputados:
            nome = (dep.get("nome") or "").strip()
            dep_id = dep.get("id")
            if not nome or not dep_id or dep_id in seen:
                continue
            seen.add(dep_id)
            cpf_key = fake_cpf(nome, "DEPUTADO FEDERAL", ano_eleicao, "CAMARA")
            pid = cpf_to_id.get(cpf_key)
            if pid:
                dep_cache[dep_id] = pid

        print(f"    {len(deputados)} deputados, {len(dep_cache)} mapeados")

        # Fetch votações by year range (API max 3-month intervals)
        leg_votos = 0
        for ano in range(ano_inicio, min(ano_fim + 1, 2026)):
            votacoes = []
            # Split each year into quarters (API rejects >3 month range)
            quarters = [
                (f"{ano}-01-01", f"{ano}-03-31"),
                (f"{ano}-04-01", f"{ano}-06-30"),
                (f"{ano}-07-01", f"{ano}-09-30"),
                (f"{ano}-10-01", f"{ano}-12-31"),
            ]
            for q_start, q_end in quarters:
                pagina = 1
                while True:
                    url = (f"{CAMARA_API}/votacoes"
                           f"?dataInicio={q_start}&dataFim={q_end}"
                           f"&itens=200&pagina={pagina}&ordem=ASC&ordenarPor=dataHoraRegistro")
                    resp = retry_get(url, timeout=30)
                    if not resp:
                        break
                    data = resp.json()
                    dados = data.get("dados", [])
                    if not dados:
                        break
                    votacoes.extend(dados)
                    if not any(l.get("rel") == "next" for l in data.get("links", [])):
                        break
                    pagina += 1
                    time.sleep(REQUEST_DELAY)

            if not votacoes:
                continue

            # For each votação, fetch individual votes
            records = []
            for vot in votacoes:
                vot_id = str(vot.get("id", ""))
                data_raw = vot.get("dataHoraRegistro") or vot.get("data") or ""
                data_vot = data_raw[:10] if data_raw else None
                descricao = vot.get("descricao", "")
                proposicao = vot.get("proposicao", "")
                if isinstance(proposicao, dict):
                    proposicao = proposicao.get("ementa", "")[:200] if proposicao else ""

                # Fetch votos for this votação
                url = f"{CAMARA_API}/votacoes/{vot_id}/votos"
                resp = retry_get(url, timeout=30)
                if not resp:
                    continue

                votos_data = resp.json().get("dados", [])
                for v in votos_data:
                    dep_info = v.get("deputado_", {})
                    dep_id = dep_info.get("id")
                    pid = dep_cache.get(dep_id)
                    if not pid:
                        continue

                    voto = v.get("tipoVoto", "")
                    if not voto:
                        continue

                    records.append({
                        "politico_id": pid,
                        "data_votacao": data_vot if data_vot else None,
                        "votacao_id": f"CAM-{vot_id}",
                        "voto": voto,
                        "proposicao": (proposicao[:200] if proposicao else None),
                        "descricao_votacao": (descricao[:300] if descricao else None),
                        "fonte": "CAMARA",
                    })

                time.sleep(REQUEST_DELAY)

            if records:
                n = batch_insert_votos(records)
                leg_votos += n
                print(f"    {ano}: {len(votacoes)} votações, {n} votos inseridos")
            else:
                print(f"    {ano}: {len(votacoes)} votações, 0 votos")

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

        # Build senador name -> politico_id cache
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
                    if not nome:
                        continue
                    cpf_key = fake_cpf(nome, "SENADOR", ano_eleicao, "SENADO")
                    pid = cpf_to_id.get(cpf_key)
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
