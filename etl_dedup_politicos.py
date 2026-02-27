#!/usr/bin/env python3
"""
ETL: Deduplicação de dim_politicos
===================================
Identifica políticos duplicados (mesmo nome, fontes diferentes) e consolida
registros, migrando FKs nas tabelas de fato para o registro canônico.

Variáveis de ambiente:
  DRY_RUN=1 (default)  → Apenas relatório, sem modificações
  DRY_RUN=0            → Executa merge
  CHECKPOINT_FILE      → default: /tmp/dedup_checkpoint.json
"""

import json
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

DRY_RUN = os.getenv("DRY_RUN", "1") != "0"
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "/tmp/dedup_checkpoint.json")
REPORT_FILE = "/tmp/dedup_report.json"

PAGE_SIZE = 1000

# Tabelas de fato que referenciam politico_id
FACT_TABLES = [
    {
        "table": "fato_politicos_mandatos",
        "unique_cols": ["politico_id", "codigo_ibge", "ano_eleicao", "cargo", "turno"],
        "conflict_strategy": "delete_dup",  # se mandato já existe no canônico, deletar
    },
    {
        "table": "fato_bens_candidato",
        "unique_cols": ["politico_id", "ano_eleicao", "ordem"],
        "conflict_strategy": "reorder",  # recalcular ordem (MAX+1)
    },
    {
        "table": "fato_receitas_campanha",
        "unique_cols": ["politico_id", "ano_eleicao", "sequencial"],
        "conflict_strategy": "rename_seq",  # adicionar sufixo ao sequencial
    },
    {
        "table": "fato_votos_legislativos",
        "unique_cols": ["politico_id", "votacao_id"],
        "conflict_strategy": "delete_dup",  # mesmo voto, deletar duplicado
    },
    {
        "table": "fato_emendas_parlamentares",
        "unique_cols": ["codigo_emenda", "ano"],  # politico_id NOT in unique
        "conflict_strategy": "no_conflict",  # sem risco de conflito
    },
]


# ============================================================
# Funções utilitárias
# ============================================================

def normalizar_nome(nome):
    """Remove acentos, uppercase, normaliza espaços."""
    if not nome:
        return ""
    nome = unicodedata.normalize('NFD', nome)
    nome = ''.join(c for c in nome if unicodedata.category(c) != 'Mn')
    return ' '.join(nome.upper().split())


def is_real_cpf(cpf):
    """Valida dígitos verificadores do CPF brasileiro.

    Retorna True apenas para CPFs com check digits corretos,
    diferente do is_valid_cpf() dos outros ETLs que só checa formato.
    """
    if not cpf or len(cpf) != 11 or not cpf.isdigit():
        return False
    if cpf == cpf[0] * 11:
        return False
    # Primeiro dígito verificador
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    resto = soma % 11
    d1 = 0 if resto < 2 else 11 - resto
    if int(cpf[9]) != d1:
        return False
    # Segundo dígito verificador
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    resto = soma % 11
    d2 = 0 if resto < 2 else 11 - resto
    if int(cpf[10]) != d2:
        return False
    return True


def retry_request(method, url, max_retries=3, timeout=60, **kwargs):
    """HTTP request with exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = requests.request(method, url, timeout=timeout, **kwargs)
            if resp.status_code in (200, 201, 204):
                return resp
            if resp.status_code == 429 or resp.status_code >= 500:
                time.sleep((2 ** attempt) * 2)
                continue
            return resp  # 4xx errors, return as-is
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(2 ** attempt)
    return None


def retry_get(url, **kwargs):
    return retry_request("GET", url, **kwargs)


# ============================================================
# Fase 1: Carregar dim_politicos
# ============================================================

def carregar_todos_politicos():
    """Carrega todos os políticos via keyset pagination."""
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


# ============================================================
# Fase 2: Agrupar e identificar duplicatas
# ============================================================

def agrupar_duplicatas(politicos):
    """Agrupa por nome normalizado e filtra grupos com 2+ entradas.

    Exclui homônimos verificados (data_nascimento diferente).
    """
    print("Fase 2: Agrupando por nome normalizado...")
    grupos = {}
    for p in politicos:
        nome_norm = normalizar_nome(p.get("nome_completo"))
        if not nome_norm:
            continue
        grupos.setdefault(nome_norm, []).append(p)

    # Filtrar grupos com 2+ entradas
    dup_grupos = {}
    for nome, membros in grupos.items():
        if len(membros) < 2:
            continue
        # Separar homônimos por data_nascimento
        sub_grupos = _separar_homonimos(membros)
        for sg in sub_grupos:
            if len(sg) >= 2:
                # Usar nome + data_nascimento como chave do subgrupo
                key = nome
                dn = next((m["data_nascimento"] for m in sg if m.get("data_nascimento")), None)
                if dn:
                    key = f"{nome}|{dn}"
                dup_grupos[key] = sg

    print(f"  {len(dup_grupos):,} grupos de duplicatas encontrados")
    total_dups = sum(len(g) - 1 for g in dup_grupos.values())
    print(f"  {total_dups:,} registros duplicados a consolidar")
    return dup_grupos


def _separar_homonimos(membros):
    """Separa membros com data_nascimento diferente (homônimos reais).

    Regra:
    - Se ambos têm data_nascimento e são diferentes → NÃO são duplicatas
    - Se um ou ambos são None → considerar duplicata
    """
    # Agrupar por data_nascimento conhecida
    por_dn = {}  # data_nascimento -> [membros]
    sem_dn = []  # membros sem data_nascimento

    for m in membros:
        dn = m.get("data_nascimento")
        if dn:
            por_dn.setdefault(dn, []).append(m)
        else:
            sem_dn.append(m)

    if not por_dn:
        # Nenhum tem data_nascimento → todos são potencialmente o mesmo
        return [membros]

    if len(por_dn) == 1:
        # Todos com data_nascimento têm a mesma → um só grupo
        return [membros]

    # Múltiplas datas → cada data é um subgrupo separado
    # Membros sem data vão para o maior subgrupo (heurística)
    sub_grupos = list(por_dn.values())
    if sem_dn:
        # Juntar sem_dn ao maior subgrupo
        maior = max(sub_grupos, key=len)
        maior.extend(sem_dn)

    return sub_grupos


# ============================================================
# Fase 3: Eleger canônico
# ============================================================

def eleger_canonico(membros):
    """Elege o registro canônico de um grupo de duplicatas.

    Prioridade:
    1. CPF real (passa validação de dígitos verificadores)
    2. Mais campos preenchidos
    3. Menor id (mais antigo)

    Retorna (canonical, [duplicates])
    """
    def score(p):
        cpf_real = 1 if is_real_cpf(p.get("cpf")) else 0
        campos = sum(1 for k in ("data_nascimento", "sexo", "grau_instrucao", "ocupacao") if p.get(k))
        return (cpf_real, campos, -p["id"])  # -id para que menor id tenha maior score

    membros_sorted = sorted(membros, key=score, reverse=True)
    canonical = membros_sorted[0]
    duplicates = membros_sorted[1:]
    return canonical, duplicates


# ============================================================
# Fase 4: Dry Run (relatório)
# ============================================================

def contar_referencias(politico_id):
    """Conta referências de um politico_id em cada tabela de fato."""
    contagens = {}
    for ft in FACT_TABLES:
        tabela = ft["table"]
        url = (
            f"{SUPABASE_URL}/rest/v1/{tabela}"
            f"?select=id&politico_id=eq.{politico_id}&limit=1"
        )
        h = {**HEADERS, "Prefer": "count=exact"}
        resp = retry_get(url, headers=h, timeout=30)
        if resp and resp.status_code == 200:
            count_header = resp.headers.get("content-range", "")
            # Format: "0-0/123" or "*/0"
            if "/" in count_header:
                total = count_header.split("/")[1]
                contagens[tabela] = int(total) if total != "*" else 0
            else:
                contagens[tabela] = len(resp.json())
        else:
            contagens[tabela] = -1  # erro
    return contagens


def gerar_relatorio(dup_grupos):
    """Gera relatório de duplicatas e salva em arquivo."""
    print("\nFase 4: Gerando relatório...")

    report = {
        "total_grupos": len(dup_grupos),
        "total_duplicados": sum(len(g) - 1 for g in dup_grupos.values()),
        "maiores_grupos": [],
        "amostra_refs": {},
    }

    # Top 20 maiores grupos
    sorted_grupos = sorted(dup_grupos.items(), key=lambda x: len(x[1]), reverse=True)

    print(f"\n  Top 20 maiores grupos de duplicatas:")
    for i, (nome, membros) in enumerate(sorted_grupos[:20]):
        canonical, dups = eleger_canonico(membros)
        grupo_info = {
            "nome": nome,
            "tamanho": len(membros),
            "canonical_id": canonical["id"],
            "canonical_cpf": canonical.get("cpf"),
            "canonical_cpf_real": is_real_cpf(canonical.get("cpf")),
            "duplicate_ids": [d["id"] for d in dups],
        }
        report["maiores_grupos"].append(grupo_info)
        cpf_info = "CPF real" if is_real_cpf(canonical.get("cpf")) else "CPF fake"
        print(f"    {i+1}. {nome} ({len(membros)} entradas, canônico={canonical['id']} [{cpf_info}])")

    # Contar referências para os primeiros 5 grupos (amostra)
    print(f"\n  Contagem de referências (amostra dos 5 primeiros):")
    for nome, membros in sorted_grupos[:5]:
        canonical, dups = eleger_canonico(membros)
        refs_total = {}
        for d in dups:
            refs = contar_referencias(d["id"])
            for tabela, count in refs.items():
                refs_total[tabela] = refs_total.get(tabela, 0) + count
        report["amostra_refs"][nome] = refs_total
        refs_str = ", ".join(f"{t}: {c}" for t, c in refs_total.items() if c > 0)
        print(f"    {nome}: {refs_str or 'sem referências'}")

    # Salvar relatório completo
    # Adicionar todos os grupos ao relatório
    report["todos_grupos"] = []
    for nome, membros in sorted_grupos:
        canonical, dups = eleger_canonico(membros)
        report["todos_grupos"].append({
            "nome": nome,
            "tamanho": len(membros),
            "canonical_id": canonical["id"],
            "duplicate_ids": [d["id"] for d in dups],
        })

    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print(f"\n  Relatório salvo em {REPORT_FILE}")

    return report


# ============================================================
# Fase 5: Executar Merge (Bulk table-centric)
# ============================================================

# Columns to strip when re-POSTing rows (auto-generated)
STRIP_COLS = {"id", "criado_em", "atualizado_em", "created_at"}


def fetch_rows_for_ids(tabela, politico_ids):
    """Fetch all rows for a batch of politico_ids using in.() filter.

    Returns list of row dicts.
    """
    if not politico_ids:
        return []
    ids_str = ",".join(str(x) for x in politico_ids)
    rows = []
    last_id = 0
    while True:
        url = (
            f"{SUPABASE_URL}/rest/v1/{tabela}"
            f"?politico_id=in.({ids_str})&id=gt.{last_id}"
            f"&order=id.asc&limit={PAGE_SIZE}"
        )
        resp = retry_get(url, headers=HEADERS, timeout=120)
        if not resp or resp.status_code != 200:
            break
        data = resp.json()
        if not data:
            break
        rows.extend(data)
        last_id = data[-1]["id"]
        if len(data) < PAGE_SIZE:
            break
    return rows


def batch_delete_by_ids(tabela, row_ids):
    """Delete rows by id using batched in.() filter."""
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
            # Fallback: smaller batches
            for j in range(0, len(batch), 50):
                mini = batch[j:j + 50]
                ids_str2 = ",".join(str(x) for x in mini)
                url2 = f"{SUPABASE_URL}/rest/v1/{tabela}?id=in.({ids_str2})"
                r2 = retry_request("DELETE", url2, headers=HEADERS, timeout=60)
                if r2 and r2.status_code in (200, 204):
                    deleted += len(mini)
    return deleted


def batch_insert_rows(tabela, rows):
    """INSERT rows in batches. Plain INSERT (no merge-duplicates) is much
    faster than upsert because it doesn't need to check for existing rows.

    On 409 (unique constraint violation = duplicate), the row is silently
    skipped (canonical already has the data).

    Returns (inserted, skipped).
    """
    if not rows:
        return 0, 0
    url = f"{SUPABASE_URL}/rest/v1/{tabela}"
    h = {**HEADERS, "Prefer": "return=headers-only"}

    inserted = 0
    skipped = 0
    BATCH = 500
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        resp = retry_request("POST", url, json=batch, headers=h, timeout=120)
        if resp and resp.status_code in (200, 201):
            inserted += len(batch)
        elif resp and resp.status_code == 409:
            # At least one row conflicts. Fall back to smaller batches.
            for j in range(0, len(batch), 50):
                mini = batch[j:j + 50]
                r2 = retry_request("POST", url, json=mini, headers=h, timeout=60)
                if r2 and r2.status_code in (200, 201):
                    inserted += len(mini)
                elif r2 and r2.status_code == 409:
                    # Individual fallback
                    for row in mini:
                        r3 = retry_request("POST", url, json=[row], headers=h, timeout=30)
                        if r3 and r3.status_code in (200, 201):
                            inserted += 1
                        else:
                            skipped += 1  # conflict = canonical has it
                else:
                    skipped += len(mini)
        else:
            # Other error, try smaller batches
            for j in range(0, len(batch), 50):
                mini = batch[j:j + 50]
                r2 = retry_request("POST", url, json=mini, headers=h, timeout=60)
                if r2 and r2.status_code in (200, 201):
                    inserted += len(mini)
                else:
                    skipped += len(mini)
        if (i // BATCH) % 100 == 0 and i > 0:
            print(f"      Inserindo... {inserted:,} ok, {skipped:,} skip")
    return inserted, skipped


def migrar_tabela_bulk(ft, dup_to_canonical, all_dup_ids):
    """Migrate all rows in a fact table from dup_ids to canonical_ids.

    Strategy:
    1. Fetch all rows belonging to dup_ids (batched in.() queries)
    2. Transform: change politico_id → canonical, resolve conflicts
    3. POST transformed rows (upsert with merge-duplicates)
    4. DELETE original rows
    """
    tabela = ft["table"]
    unique_cols = ft["unique_cols"]
    strategy = ft["conflict_strategy"]
    on_conflict = ",".join(unique_cols)

    print(f"\n  Migrando {tabela}...")

    # Step 1: Fetch all affected rows in batches of dup_ids
    all_rows = []
    ID_BATCH = 300  # dup_ids per in.() query (URL length safe)
    for i in range(0, len(all_dup_ids), ID_BATCH):
        batch_ids = all_dup_ids[i:i + ID_BATCH]
        rows = fetch_rows_for_ids(tabela, batch_ids)
        all_rows.extend(rows)
        if (i // ID_BATCH) % 50 == 0 and i > 0:
            print(f"    Buscando... {i:,}/{len(all_dup_ids):,} IDs, {len(all_rows):,} rows")

    if not all_rows:
        print(f"    Nenhum registro encontrado")
        return

    print(f"    {len(all_rows):,} registros encontrados")

    # Step 2: Transform rows
    rows_to_upsert = []
    rows_to_delete_ids = []

    # For reorder/rename strategies, we need canonical's existing keys
    # Collect which canonical_ids are affected
    if strategy in ("reorder", "rename_seq"):
        affected_canonicals = set()
        for row in all_rows:
            cid = dup_to_canonical.get(row["politico_id"])
            if cid:
                affected_canonicals.add(cid)

        # Fetch canonical keys
        canon_keys = {}  # canonical_id -> set of unique key tuples (excl politico_id)
        print(f"    Buscando chaves dos {len(affected_canonicals):,} canônicos...")
        ac_list = sorted(affected_canonicals)
        for i in range(0, len(ac_list), ID_BATCH):
            batch_ids = ac_list[i:i + ID_BATCH]
            canon_rows = fetch_rows_for_ids(tabela, batch_ids)
            for cr in canon_rows:
                cid = cr["politico_id"]
                key_parts = tuple(str(cr.get(c, "")) for c in unique_cols if c != "politico_id")
                canon_keys.setdefault(cid, set()).add(key_parts)

        # Track new keys we're adding (for dedup within the batch)
        added_keys = {}  # canonical_id -> set of key tuples

    for row in all_rows:
        dup_pid = row["politico_id"]
        canonical_pid = dup_to_canonical.get(dup_pid)
        if not canonical_pid:
            continue  # shouldn't happen

        original_id = row["id"]
        rows_to_delete_ids.append(original_id)

        # Create transformed row (strip auto-generated columns)
        new_row = {k: v for k, v in row.items() if k not in STRIP_COLS}
        new_row["politico_id"] = canonical_pid

        if strategy in ("reorder", "rename_seq"):
            # Check for conflict with canonical's existing keys
            key_parts = tuple(str(new_row.get(c, "")) for c in unique_cols if c != "politico_id")
            existing = canon_keys.get(canonical_pid, set())
            added = added_keys.get(canonical_pid, set())

            if key_parts in existing or key_parts in added:
                if strategy == "reorder":
                    # Find next available ordem
                    ano = new_row.get("ano_eleicao")
                    max_ordem = 0
                    for ek in existing | added:
                        # ek = (ano_str, ordem_str) for bens
                        # unique_cols minus politico_id = ["ano_eleicao", "ordem"]
                        if str(ano) == ek[0]:
                            try:
                                max_ordem = max(max_ordem, int(ek[1]))
                            except (ValueError, IndexError):
                                pass
                    new_row["ordem"] = max_ordem + 1
                    key_parts = tuple(str(new_row.get(c, "")) for c in unique_cols if c != "politico_id")

                elif strategy == "rename_seq":
                    seq_orig = str(new_row.get("sequencial", ""))
                    for suffix in range(1, 1000):
                        new_seq = f"{seq_orig}_{suffix}"
                        test_parts = list(key_parts)
                        # sequencial is the last non-politico_id unique col
                        seq_idx = [c for c in unique_cols if c != "politico_id"].index("sequencial")
                        test_parts[seq_idx] = new_seq
                        if tuple(test_parts) not in existing and tuple(test_parts) not in added:
                            new_row["sequencial"] = new_seq
                            key_parts = tuple(test_parts)
                            break

            added_keys.setdefault(canonical_pid, set()).add(key_parts)

        rows_to_upsert.append(new_row)

    print(f"    {len(rows_to_upsert):,} registros transformados")

    # Step 3: INSERT transformed rows (plain INSERT, skip on conflict)
    inserted, skip = batch_insert_rows(tabela, rows_to_upsert)
    print(f"    {inserted:,} inseridos, {skip:,} skipped (já existem no canônico)")

    # Step 4: Delete originals
    deleted = batch_delete_by_ids(tabela, rows_to_delete_ids)
    print(f"    {deleted:,} registros antigos deletados")


def executar_merge_bulk(dup_grupos):
    """Bulk table-centric merge."""
    print("\nFase 5: Executando merge (bulk)...")

    # Step 1: Build dup→canonical mapping
    dup_to_canonical = {}
    canonical_enrichment = {}  # canonical_id -> {field: value}

    for nome, membros in dup_grupos.items():
        canonical, dups = eleger_canonico(membros)
        for d in dups:
            dup_to_canonical[d["id"]] = canonical["id"]
        # Compute enrichment fields
        patch_fields = {}
        for campo in ["data_nascimento", "sexo", "grau_instrucao", "ocupacao"]:
            if not canonical.get(campo):
                for d in dups:
                    if d.get(campo):
                        patch_fields[campo] = d[campo]
                        break
        if patch_fields:
            canonical_enrichment[canonical["id"]] = patch_fields

    all_dup_ids = sorted(dup_to_canonical.keys())
    print(f"  Mapeamento: {len(all_dup_ids):,} duplicados → {len(set(dup_to_canonical.values())):,} canônicos")

    # Step 2: Migrate each fact table
    for ft in FACT_TABLES:
        migrar_tabela_bulk(ft, dup_to_canonical, all_dup_ids)

    # Step 3: Enrich canonicals
    enrich_count = len(canonical_enrichment)
    if enrich_count:
        print(f"\n  Enriquecendo {enrich_count:,} canônicos...")
        enriched = 0
        for canon_id, fields in canonical_enrichment.items():
            url = f"{SUPABASE_URL}/rest/v1/dim_politicos?id=eq.{canon_id}"
            resp = retry_request("PATCH", url, json=fields, headers=HEADERS, timeout=30)
            if resp and resp.status_code in (200, 204):
                enriched += 1
            if enriched % 5000 == 0 and enriched > 0:
                print(f"    ...{enriched:,}/{enrich_count:,}")
        print(f"    {enriched:,} canônicos enriquecidos")

    # Step 4: Bulk delete all duplicates from dim_politicos
    print(f"\n  Deletando {len(all_dup_ids):,} duplicados de dim_politicos...")
    deleted = batch_delete_by_ids("dim_politicos", all_dup_ids)
    print(f"    {deleted:,} duplicados deletados")

    return {
        "duplicados_removidos": deleted,
        "total_duplicados": len(all_dup_ids),
    }


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 60)
    print("ETL: Deduplicação de dim_politicos")
    print(f"  Modo: {'DRY RUN (apenas relatório)' if DRY_RUN else 'EXECUÇÃO (vai modificar dados!)'}")
    print("=" * 60)

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERRO: SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")
        sys.exit(1)

    # Fase 1: Carregar
    politicos = carregar_todos_politicos()
    if not politicos:
        print("ERRO: Nenhum político carregado")
        sys.exit(1)

    # Fase 2: Agrupar
    dup_grupos = agrupar_duplicatas(politicos)
    if not dup_grupos:
        print("Nenhuma duplicata encontrada!")
        return

    # Fase 4: Relatório (sempre roda)
    report = gerar_relatorio(dup_grupos)

    print(f"\n{'=' * 60}")
    print(f"  Grupos de duplicatas: {report['total_grupos']:,}")
    print(f"  Registros a consolidar: {report['total_duplicados']:,}")
    print(f"{'=' * 60}")

    if DRY_RUN:
        print("\n  DRY RUN concluído. Para executar merge, rode com DRY_RUN=0")
        return

    # Fase 5: Merge (bulk)
    stats = executar_merge_bulk(dup_grupos)

    print(f"\n{'=' * 60}")
    print(f"  Merge concluído!")
    print(f"  Duplicados removidos: {stats['duplicados_removidos']:,} / {stats['total_duplicados']:,}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
