#!/usr/bin/env python3
"""
ETL Fix: Emendas Parlamentares com politico_id NULL
====================================================
Corrige ~13,800 emendas onde o matching de autor falhou por falta de
normalização Unicode (API Transparência usa nomes sem acento, dim_politicos
pode ter acentos).

Estratégia:
1. Carrega dim_politicos (id, nome_completo, nome_urna)
2. Indexa por normalizar_nome(nome_completo) e normalizar_nome(nome_urna)
3. Busca emendas com politico_id IS NULL
4. Filtra autores institucionais (COMISSÃO, RELATOR, etc.) — NULL é correto
5. Para autores pessoa, normaliza e busca no índice
6. Batch PATCH as emendas corrigidas
"""

import os
import time
import unicodedata
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

PAGE_SIZE = 1000

# Keywords que indicam autor institucional (NULL é correto)
INSTITUTIONAL_KEYWORDS = (
    "COMISS", "RELATOR", "MESA ", "BANCADA", "BLOCO",
    "GOVERNO", "PRESIDENCIA", "CONSELHO", "SECRETARIA",
    "LIDERANC", "MINISTERIO", "ORGAO", "EXECUTIVO",
)


def normalizar_nome(nome):
    if not nome:
        return ""
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if unicodedata.category(c) != 'Mn')
    return ' '.join(nome.upper().split())


def retry_request(method, url, max_retries=5, timeout=60, **kwargs):
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (200, 201, 204):
                return resp
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = min((2 ** attempt) * 2, 30)
                print(f"    [retry] {resp.status_code}, waiting {wait}s...")
                time.sleep(wait)
                continue
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = min(2 ** attempt, 30)
            print(f"    [retry] {type(e).__name__}, waiting {wait}s...")
            time.sleep(wait)
    return None


def is_institutional(autor_norm):
    """Retorna True se o autor parece institucional (não é pessoa)."""
    return any(kw in autor_norm for kw in INSTITUTIONAL_KEYWORDS)


def carregar_indice_politicos():
    """Carrega dim_politicos e indexa por nome normalizado."""
    print("Carregando dim_politicos...")
    nome_to_id = {}  # normalizar_nome -> politico_id
    last_id = 0
    total = 0

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/dim_politicos"
            f"?select=id,nome_completo,nome_urna"
            f"&id=gt.{last_id}&order=id.asc&limit={PAGE_SIZE}"
        )
        resp = retry_request("GET", url, headers=HEADERS, timeout=120)
        if not resp or resp.status_code != 200:
            print(f"  ERRO carregando página (last_id={last_id})")
            break
        data = resp.json()
        if not data:
            break

        for r in data:
            pid = r["id"]
            # Index by nome_completo
            nc = normalizar_nome(r.get("nome_completo"))
            if nc and nc not in nome_to_id:
                nome_to_id[nc] = pid
            # Index by nome_urna (lower priority, don't overwrite)
            nu = normalizar_nome(r.get("nome_urna"))
            if nu and nu not in nome_to_id:
                nome_to_id[nu] = pid

        last_id = data[-1]["id"]
        total += len(data)
        if total % 100000 == 0:
            print(f"  ...{total:,} carregados")
        if len(data) < PAGE_SIZE:
            break

    print(f"  {total:,} politicos -> {len(nome_to_id):,} nomes indexados")
    return nome_to_id


def carregar_emendas_null():
    """Busca todas as emendas com politico_id IS NULL."""
    print("Buscando emendas com politico_id NULL...")
    emendas = []
    last_id = 0

    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/fato_emendas_parlamentares"
            f"?select=id,autor"
            f"&politico_id=is.null"
            f"&id=gt.{last_id}&order=id.asc&limit={PAGE_SIZE}"
        )
        resp = retry_request("GET", url, headers=HEADERS, timeout=120)
        if not resp or resp.status_code != 200:
            print(f"  ERRO carregando página (last_id={last_id})")
            break
        data = resp.json()
        if not data:
            break
        emendas.extend(data)
        last_id = data[-1]["id"]
        if len(data) < PAGE_SIZE:
            break

    print(f"  {len(emendas):,} emendas com politico_id NULL")
    return emendas


def patch_emendas_batch(emenda_ids, politico_id):
    """PATCH um grupo de emendas setando politico_id."""
    BATCH = 200
    patched = 0
    for i in range(0, len(emenda_ids), BATCH):
        batch = emenda_ids[i:i + BATCH]
        ids_str = ",".join(str(x) for x in batch)
        url = f"{SUPABASE_URL}/rest/v1/fato_emendas_parlamentares?id=in.({ids_str})"
        resp = retry_request(
            "PATCH", url,
            json={"politico_id": politico_id},
            headers=HEADERS, timeout=120
        )
        if resp and resp.status_code in (200, 204):
            patched += len(batch)
        else:
            status = resp.status_code if resp else "timeout"
            print(f"    WARN: PATCH falhou ({status}) para {len(batch)} emendas")
    return patched


def main():
    inicio = datetime.now()
    print("=" * 60)
    print("FIX: Emendas Parlamentares com politico_id NULL")
    print(f"Inicio: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    # 1. Carregar indice de politicos por nome normalizado
    nome_to_id = carregar_indice_politicos()

    # 2. Carregar emendas NULL
    emendas = carregar_emendas_null()
    if not emendas:
        print("Nenhuma emenda NULL encontrada.")
        return

    # 3. Classificar e fazer matching
    institucional = 0
    matched = 0
    unmatched = 0
    unmatched_names = {}

    # Agrupar por politico_id para batch PATCH eficiente
    pid_to_emenda_ids = {}  # politico_id -> [emenda_id, ...]

    for e in emendas:
        autor = e.get("autor", "") or ""
        autor_norm = normalizar_nome(autor)

        if not autor_norm or is_institutional(autor_norm):
            institucional += 1
            continue

        pid = nome_to_id.get(autor_norm)
        if pid:
            pid_to_emenda_ids.setdefault(pid, []).append(e["id"])
            matched += 1
        else:
            unmatched += 1
            unmatched_names[autor_norm] = unmatched_names.get(autor_norm, 0) + 1

    total_emendas = sum(len(ids) for ids in pid_to_emenda_ids.values())
    print(f"\nResultado do matching:")
    print(f"  Institucional (NULL correto): {institucional:,}")
    print(f"  Matched:                      {matched:,} -> {len(pid_to_emenda_ids):,} politicos")
    print(f"  Unmatched:                    {unmatched:,}")

    # Mostrar top unmatched
    if unmatched_names:
        top = sorted(unmatched_names.items(), key=lambda x: -x[1])[:20]
        print(f"\n  Top {len(top)} autores não encontrados:")
        for nome, cnt in top:
            print(f"    {cnt:>4}x  {nome}")

    # 4. Batch PATCH
    if not pid_to_emenda_ids:
        print("\nNenhuma emenda para corrigir.")
        return

    print(f"\nPatcheando {total_emendas:,} emendas ({len(pid_to_emenda_ids):,} politicos)...")
    total_patched = 0
    done = 0
    for pid, emenda_ids in pid_to_emenda_ids.items():
        n = patch_emendas_batch(emenda_ids, pid)
        total_patched += n
        done += 1
        if done % 500 == 0:
            print(f"  ...{done:,}/{len(pid_to_emenda_ids):,} politicos ({total_patched:,} emendas)")

    fim = datetime.now()
    print(f"\n{'=' * 60}")
    print(f"RESULTADO")
    print(f"  Emendas corrigidas: {total_patched:,}")
    print(f"  Ainda NULL (institucional): {institucional:,}")
    print(f"  Ainda NULL (unmatched): {unmatched:,}")
    print(f"  Duracao: {fim - inicio}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
