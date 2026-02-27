#!/usr/bin/env python3
"""
ETL: Dedup Cleanup Phase 2 - Emendas, Enrichment, Delete
=========================================================
Runs after votos migration is complete.
Rebuilds mapping, patches emendas in bulk, enriches canonicals, deletes dups.
"""

import os
import sys
import time
import unicodedata

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


def normalizar_nome(nome):
    if not nome:
        return ""
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if unicodedata.category(c) != 'Mn')
    return ' '.join(nome.upper().split())


def is_real_cpf(cpf):
    if not cpf or len(cpf) != 11 or not cpf.isdigit():
        return False
    if cpf == cpf[0] * 11:
        return False
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto
    if int(cpf[9]) != d1:
        return False
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto
    if int(cpf[10]) != d2:
        return False
    return True


def retry_request(method, url, max_retries=5, timeout=60, **kwargs):
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (200, 201, 204):
                return resp
            if resp.status_code == 429 or resp.status_code >= 500:
                wait = min((2 ** attempt) * 2, 30)
                print(f"      [retry] {resp.status_code}, waiting {wait}s...")
                time.sleep(wait)
                continue
            return resp
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            wait = min(2 ** attempt, 30)
            print(f"      [retry] {type(e).__name__}, waiting {wait}s...")
            time.sleep(wait)
    return None


def retry_get(url, **kwargs):
    return retry_request("GET", url, **kwargs)


def carregar_todos_politicos():
    print("Fase 1: Carregando dim_politicos...")
    politicos = []
    last_id = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/dim_politicos"
            f"?select=id,cpf,nome_completo,data_nascimento,sexo,grau_instrucao,ocupacao"
            f"&id=gt.{last_id}&order=id.asc&limit={PAGE_SIZE}"
        )
        resp = retry_get(url, headers=HEADERS, timeout=120)
        if not resp or resp.status_code != 200:
            print(f"  ERRO carregando página (last_id={last_id})")
            break
        data = resp.json()
        if not data:
            break
        politicos.extend(data)
        last_id = data[-1]["id"]
        if len(politicos) % 50000 == 0:
            print(f"  ...{len(politicos):,} carregados")
        if len(data) < PAGE_SIZE:
            break
    print(f"  Total: {len(politicos):,} políticos carregados")
    return politicos


def _separar_homonimos(membros):
    por_dn = {}
    sem_dn = []
    for m in membros:
        dn = m.get("data_nascimento")
        if dn:
            por_dn.setdefault(dn, []).append(m)
        else:
            sem_dn.append(m)
    if not por_dn:
        return [membros]
    if len(por_dn) == 1:
        return [membros]
    sub_grupos = list(por_dn.values())
    if sem_dn:
        maior = max(sub_grupos, key=len)
        maior.extend(sem_dn)
    return sub_grupos


def agrupar_duplicatas(politicos):
    print("Fase 2: Agrupando por nome normalizado...")
    grupos = {}
    for p in politicos:
        nome_norm = normalizar_nome(p.get("nome_completo"))
        if not nome_norm:
            continue
        grupos.setdefault(nome_norm, []).append(p)

    dup_grupos = {}
    for nome, membros in grupos.items():
        if len(membros) < 2:
            continue
        sub_grupos = _separar_homonimos(membros)
        for sg in sub_grupos:
            if len(sg) >= 2:
                key = nome
                dn = next((m["data_nascimento"] for m in sg if m.get("data_nascimento")), None)
                if dn:
                    key = f"{nome}|{dn}"
                dup_grupos[key] = sg

    total_dups = sum(len(g) - 1 for g in dup_grupos.values())
    print(f"  {len(dup_grupos):,} grupos, {total_dups:,} duplicados")
    return dup_grupos


def eleger_canonico(membros):
    def score(p):
        cpf_real = 1 if is_real_cpf(p.get("cpf")) else 0
        campos = sum(1 for k in ("data_nascimento", "sexo", "grau_instrucao", "ocupacao") if p.get(k))
        return (cpf_real, campos, -p["id"])
    membros_sorted = sorted(membros, key=score, reverse=True)
    return membros_sorted[0], membros_sorted[1:]


def batch_delete_by_ids(tabela, row_ids):
    deleted = 0
    BATCH = 500
    for i in range(0, len(row_ids), BATCH):
        batch = row_ids[i:i + BATCH]
        ids_str = ",".join(str(x) for x in batch)
        url = f"{SUPABASE_URL}/rest/v1/{tabela}?id=in.({ids_str})"
        resp = retry_request("DELETE", url, headers=HEADERS, timeout=120)
        if resp and resp.status_code in (200, 204):
            deleted += len(batch)
        else:
            for j in range(0, len(batch), 50):
                mini = batch[j:j + 50]
                ids_str2 = ",".join(str(x) for x in mini)
                url2 = f"{SUPABASE_URL}/rest/v1/{tabela}?id=in.({ids_str2})"
                r2 = retry_request("DELETE", url2, headers=HEADERS, timeout=60)
                if r2 and r2.status_code in (200, 204):
                    deleted += len(mini)
        if deleted % 10000 == 0 and deleted > 0:
            print(f"    ...{deleted:,} deletados")
    return deleted


def batch_patch_politico_id(tabela, dup_ids, canonical_id):
    """PATCH all rows where politico_id in dup_ids, setting politico_id=canonical_id.
    Batches dup_ids into groups of 200 to keep URL length reasonable."""
    patched = 0
    BATCH = 200
    for i in range(0, len(dup_ids), BATCH):
        batch = dup_ids[i:i + BATCH]
        ids_str = ",".join(str(x) for x in batch)
        url = f"{SUPABASE_URL}/rest/v1/{tabela}?politico_id=in.({ids_str})"
        resp = retry_request("PATCH", url, json={"politico_id": canonical_id}, headers=HEADERS, timeout=120)
        if resp and resp.status_code in (200, 204):
            patched += len(batch)
    return patched


def main():
    print("=" * 60)
    print("ETL: Dedup Cleanup Phase 2")
    print("  (emendas, enrichment, delete duplicates)")
    print("=" * 60)

    # Rebuild mapping
    politicos = carregar_todos_politicos()
    dup_grupos = agrupar_duplicatas(politicos)

    # Build dup→canonical mapping and enrichment data
    dup_to_canonical = {}
    canonical_enrichment = {}
    # Also build reverse: canonical→[dup_ids]
    canonical_to_dups = {}

    for nome, membros in dup_grupos.items():
        canonical, dups = eleger_canonico(membros)
        dup_ids_list = []
        for d in dups:
            dup_to_canonical[d["id"]] = canonical["id"]
            dup_ids_list.append(d["id"])
        canonical_to_dups[canonical["id"]] = dup_ids_list

        patch_fields = {}
        for campo in ["data_nascimento", "sexo", "grau_instrucao", "ocupacao"]:
            if not canonical.get(campo):
                for d in dups:
                    if d.get(campo):
                        patch_fields[campo] = d[campo]
                        break
        if patch_fields:
            canonical_enrichment[canonical["id"]] = patch_fields

    all_dup_ids = set(dup_to_canonical.keys())
    print(f"  Mapeamento: {len(all_dup_ids):,} duplicados → {len(canonical_to_dups):,} canônicos")

    # ---- Migrate fato_emendas_parlamentares (bulk PATCH) ----
    # Download table to find which dup_ids actually have emendas
    print(f"\n  Migrando fato_emendas_parlamentares...")
    print(f"    Baixando tabela...")
    all_emendas = []
    last_id = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/fato_emendas_parlamentares"
            f"?select=id,politico_id&id=gt.{last_id}&order=id.asc&limit={PAGE_SIZE}"
        )
        resp = retry_get(url, headers=HEADERS, timeout=120)
        if not resp or resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        all_emendas.extend(data)
        last_id = data[-1]["id"]
        if len(all_emendas) % 50000 == 0:
            print(f"    ...{len(all_emendas):,} rows")
        if len(data) < PAGE_SIZE:
            break
    print(f"    Total: {len(all_emendas):,} rows")

    # Find affected dup_ids and group by canonical
    affected_dup_pids = set()
    for e in all_emendas:
        pid = e.get("politico_id")
        if pid and pid in all_dup_ids:
            affected_dup_pids.add(pid)
    del all_emendas

    # Group affected dup_ids by canonical
    canonical_dup_emendas = {}
    for dup_pid in affected_dup_pids:
        canon_id = dup_to_canonical[dup_pid]
        canonical_dup_emendas.setdefault(canon_id, []).append(dup_pid)

    print(f"    {len(affected_dup_pids):,} dup politico_ids com emendas → {len(canonical_dup_emendas):,} canônicos")

    # Bulk PATCH per canonical group
    patched = 0
    for canonical_id, dup_ids in canonical_dup_emendas.items():
        batch_patch_politico_id("fato_emendas_parlamentares", dup_ids, canonical_id)
        patched += 1
        if patched % 500 == 0:
            print(f"    ...{patched:,}/{len(canonical_dup_emendas):,} canônicos patcheados")
    print(f"    {patched:,} canônicos patcheados")

    # ---- Enrich canonicals (batch by grouping PATCHes) ----
    if canonical_enrichment:
        print(f"\n  Enriquecendo {len(canonical_enrichment):,} canônicos...")
        enriched = 0
        for canon_id, fields in canonical_enrichment.items():
            url = f"{SUPABASE_URL}/rest/v1/dim_politicos?id=eq.{canon_id}"
            resp = retry_request("PATCH", url, json=fields, headers=HEADERS, timeout=30)
            if resp and resp.status_code in (200, 204):
                enriched += 1
            if enriched % 5000 == 0 and enriched > 0:
                print(f"    ...{enriched:,}/{len(canonical_enrichment):,}")
        print(f"    {enriched:,} canônicos enriquecidos")

    # ---- Delete all duplicates from dim_politicos ----
    all_dup_ids_sorted = sorted(all_dup_ids)
    print(f"\n  Deletando {len(all_dup_ids_sorted):,} duplicados de dim_politicos...")
    deleted = batch_delete_by_ids("dim_politicos", all_dup_ids_sorted)
    print(f"    {deleted:,} duplicados deletados")

    print(f"\n{'=' * 60}")
    print(f"  Cleanup concluído!")
    print(f"  Duplicados removidos: {deleted:,} / {len(all_dup_ids_sorted):,}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
