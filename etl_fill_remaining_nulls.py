#!/usr/bin/env python3
"""
Preenchimento abrangente de NULLs restantes em todas as tabelas.
Fases:
  1. saneamento_municipios  - esgoto_rede_geral (113 rows)
  2. financas_municipios     - receita_capital, resultado_orcamentario (~3140 rows)
  3. pib_municipios          - renda_fonte/renda_metodo (5570 rows)
  4. mortalidade_municipios  - obitos interpolados (4745 rows)
  5. pop_municipios          - demografias interpoladas (~1%)
  6. fato_emendas            - politico_id por nome do autor
  7. fato_votos_legislativos - proposicao via bulk CSV da Câmara
"""

import os
import csv
import io
import time
import math
import requests
import numpy as np
from collections import defaultdict
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=headers-only",
}
HEADERS_MERGE = {
    **HEADERS,
    "Prefer": "resolution=merge-duplicates,return=headers-only",
}

CAMARA_BULK = "https://dadosabertos.camara.leg.br/arquivos"


# ── Helpers ─────────────────────────────────────────────────────────────

def fetch_all(table, select="*", filters="", order=""):
    """Paginação via Range header com retry."""
    all_rows = []
    offset = 0
    while True:
        url = f"{SUPABASE_URL}/rest/v1/{table}?select={select}{filters}&order={order or 'id.asc'}"
        h = {**HEADERS, "Prefer": "", "Range": f"{offset}-{offset+999}"}
        rows = None
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=h, timeout=120)
                if resp.status_code in (200, 206):
                    rows = resp.json()
                    break
                elif resp.status_code == 416:  # Range not satisfiable
                    rows = []
                    break
            except Exception:
                time.sleep(5 * (attempt + 1))
        if rows is None:
            break
        if not rows:
            break
        all_rows.extend(rows)
        if len(rows) < 1000:
            break
        offset += 1000
        if offset % 10000 == 0:
            time.sleep(0.5)
    return all_rows


def batch_patch(table, updates, key_col="id", workers=4):
    """Atualiza registros em paralelo via PATCH com ThreadPool."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    counter = {"ok": 0, "fail": 0}
    lock = threading.Lock()

    def _patch_one(rec):
        key_val = rec[key_col]
        payload = {k: v for k, v in rec.items() if k != key_col}
        url = f"{SUPABASE_URL}/rest/v1/{table}?{key_col}=eq.{key_val}"
        try:
            resp = requests.patch(url, json=payload, headers=HEADERS, timeout=30)
            with lock:
                if resp.status_code in (200, 204):
                    counter["ok"] += 1
                else:
                    counter["fail"] += 1
                if counter["ok"] % 500 == 0 and counter["ok"] > 0:
                    print(f"    {counter['ok']}/{len(updates)} atualizados...")
        except Exception:
            with lock:
                counter["fail"] += 1

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_patch_one, rec) for rec in updates]
        for f in as_completed(futures):
            pass

    return counter["ok"]


def batch_upsert(table, records, on_conflict, batch_size=500):
    """Upsert em lotes via POST."""
    inserted = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict={on_conflict}"
        try:
            resp = requests.post(url, json=batch, headers=HEADERS_MERGE, timeout=60)
            if resp.status_code in (200, 201):
                inserted += len(batch)
        except Exception as e:
            print(f"    Erro batch {i}: {e}")
        time.sleep(0.3)
        if (i // batch_size) % 10 == 0 and i > 0:
            print(f"    {inserted}/{len(records)} upserted...")
    return inserted


# ════════════════════════════════════════════════════════════════════════
# FASE 1: saneamento_municipios - esgoto_rede_geral
# ════════════════════════════════════════════════════════════════════════

def fase1_saneamento():
    print("\n" + "=" * 70)
    print("FASE 1: saneamento_municipios - esgoto_rede_geral")
    print("=" * 70)

    rows = fetch_all("saneamento_municipios",
                     select="id,pct_esgoto_adequado,domicilios_total,esgoto_rede_geral",
                     filters="&esgoto_rede_geral=is.null")
    print(f"  Registros com esgoto_rede_geral NULL: {len(rows)}")

    updates = []
    for r in rows:
        pct = r.get("pct_esgoto_adequado")
        dom = r.get("domicilios_total")
        if pct is not None and dom is not None and dom > 0:
            val = int(round(float(pct) / 100 * int(dom)))
            updates.append({"id": r["id"], "esgoto_rede_geral": val})

    if updates:
        n = batch_patch("saneamento_municipios", updates)
        print(f"  Atualizados: {n}")
    else:
        print("  Nada a preencher.")

    # esgoto_fossa_septica
    rows2 = fetch_all("saneamento_municipios",
                      select="id,domicilios_total,esgoto_rede_geral,esgoto_outro,esgoto_sem,esgoto_fossa_septica",
                      filters="&esgoto_fossa_septica=is.null")
    print(f"  Registros com esgoto_fossa_septica NULL: {len(rows2)}")

    updates2 = []
    for r in rows2:
        dom = r.get("domicilios_total")
        rede = r.get("esgoto_rede_geral")
        outro = r.get("esgoto_outro") or 0
        sem = r.get("esgoto_sem") or 0
        if dom and rede is not None:
            fossa = max(0, int(dom) - int(rede) - int(outro) - int(sem))
            updates2.append({"id": r["id"], "esgoto_fossa_septica": fossa})

    if updates2:
        n = batch_patch("saneamento_municipios", updates2)
        print(f"  Atualizados: {n}")


# ════════════════════════════════════════════════════════════════════════
# FASE 2: financas_municipios - campos derivados
# ════════════════════════════════════════════════════════════════════════

def fase2_financas():
    print("\n" + "=" * 70)
    print("FASE 2: financas_municipios - campos derivados")
    print("=" * 70)

    # receita_capital = receita_total - receita_corrente
    rows = fetch_all("financas_municipios",
                     select="id,receita_total,receita_corrente,receita_capital",
                     filters="&receita_capital=is.null&receita_total=not.is.null&receita_corrente=not.is.null")
    print(f"  receita_capital NULL (derivável): {len(rows)}")

    updates = []
    for r in rows:
        rt = float(r["receita_total"])
        rc = float(r["receita_corrente"])
        updates.append({"id": r["id"], "receita_capital": round(rt - rc, 2)})

    if updates:
        n = batch_patch("financas_municipios", updates)
        print(f"  receita_capital preenchidos: {n}")

    # resultado_orcamentario = receita_total - despesa_total
    rows2 = fetch_all("financas_municipios",
                      select="id,receita_total,despesa_total,resultado_orcamentario",
                      filters="&resultado_orcamentario=is.null&receita_total=not.is.null&despesa_total=not.is.null")
    print(f"  resultado_orcamentario NULL (derivável): {len(rows2)}")

    updates2 = []
    for r in rows2:
        rt = float(r["receita_total"])
        dt = float(r["despesa_total"])
        updates2.append({"id": r["id"], "resultado_orcamentario": round(rt - dt, 2)})

    if updates2:
        n = batch_patch("financas_municipios", updates2)
        print(f"  resultado_orcamentario preenchidos: {n}")

    # transferencias_correntes NULL
    rows3 = fetch_all("financas_municipios",
                      select="id,transferencias_correntes",
                      filters="&transferencias_correntes=is.null")
    print(f"  transferencias_correntes NULL: {len(rows3)} (sem derivação possível, pulando)")


# ════════════════════════════════════════════════════════════════════════
# FASE 3: pib_municipios - renda_fonte/renda_metodo
# ════════════════════════════════════════════════════════════════════════

def fase3_pib():
    print("\n" + "=" * 70)
    print("FASE 3: pib_municipios - renda_fonte/renda_metodo")
    print("=" * 70)

    rows = fetch_all("pib_municipios",
                     select="id,ano,renda_fonte,renda_metodo,renda_per_capita",
                     filters="&renda_fonte=is.null&renda_per_capita=not.is.null")
    print(f"  Registros sem renda_fonte mas com renda_per_capita: {len(rows)}")

    updates = []
    for r in rows:
        ano = r["ano"]
        if ano == 2010:
            fonte = "Atlas Brasil/PNUD"
            metodo = "Censo 2010"
        else:
            fonte = "Atlas Brasil + VAR/Projecao"
            metodo = "Projeção PIB"
        updates.append({
            "id": r["id"],
            "renda_fonte": fonte,
            "renda_metodo": metodo,
        })

    if updates:
        n = batch_patch("pib_municipios", updates)
        print(f"  Atualizados: {n}")


# ════════════════════════════════════════════════════════════════════════
# FASE 4: mortalidade_municipios - interpolação temporal
# ════════════════════════════════════════════════════════════════════════

def fase4_mortalidade():
    print("\n" + "=" * 70)
    print("FASE 4: mortalidade_municipios - interpolação de óbitos")
    print("=" * 70)

    # Carregar tudo para interpolação
    all_rows = fetch_all("mortalidade_municipios",
                         select="id,codigo_ibge,codigo_ibge_uf,ano,obitos_total,obitos_masculinos,obitos_femininos,nascimentos,taxa_natalidade")
    print(f"  Total registros: {len(all_rows)}")

    # Organizar por município
    by_mun = defaultdict(dict)
    for r in all_rows:
        by_mun[r["codigo_ibge"]][r["ano"]] = r

    # Carregar população para calcular taxas
    pop_rows = fetch_all("pop_municipios",
                         select="codigo_ibge,ano,populacao")
    pop = defaultdict(dict)
    for r in pop_rows:
        if r.get("populacao"):
            pop[r["codigo_ibge"]][r["ano"]] = int(r["populacao"])

    # Identificar NULLs
    null_rows = [r for r in all_rows if r.get("obitos_total") is None]
    print(f"  Registros com obitos_total NULL: {len(null_rows)}")

    updates = []
    for r in null_rows:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        mun_data = by_mun[cod]

        # Buscar anos vizinhos com dados
        known = [(y, d["obitos_total"]) for y, d in mun_data.items()
                 if d.get("obitos_total") is not None]
        if not known:
            continue

        known.sort()

        # Interpolação linear
        before = [(y, v) for y, v in known if y < ano]
        after = [(y, v) for y, v in known if y > ano]

        if before and after:
            y0, v0 = before[-1]
            y1, v1 = after[0]
            obitos = int(round(v0 + (v1 - v0) * (ano - y0) / (y1 - y0)))
        elif before:
            # Extrapolar para frente
            if len(before) >= 2:
                y0, v0 = before[-2]
                y1, v1 = before[-1]
                slope = (v1 - v0) / (y1 - y0)
                obitos = int(round(v1 + slope * (ano - y1)))
            else:
                obitos = before[-1][1]
        elif after:
            if len(after) >= 2:
                y0, v0 = after[0]
                y1, v1 = after[1]
                slope = (v1 - v0) / (y1 - y0)
                obitos = int(round(v0 + slope * (ano - y0)))
            else:
                obitos = after[0][1]
        else:
            continue

        obitos = max(0, obitos)

        # Estimar split masculino/feminino (~55%/45% típico)
        ob_masc = int(round(obitos * 0.55))
        ob_fem = obitos - ob_masc

        # Taxas
        populacao = pop.get(cod, {}).get(ano)
        taxa_mort = round(obitos / populacao * 1000, 2) if populacao and populacao > 0 else None

        nasc = r.get("nascimentos")
        cresc_veg = (nasc - obitos) if nasc is not None else None

        updates.append({
            "id": r["id"],
            "obitos_total": obitos,
            "obitos_masculinos": ob_masc,
            "obitos_femininos": ob_fem,
            "taxa_mortalidade": taxa_mort,
            "crescimento_vegetativo": cresc_veg,
        })

    if updates:
        print(f"  Interpoláveis: {len(updates)}")
        n = batch_patch("mortalidade_municipios", updates)
        print(f"  Atualizados: {n}")
    else:
        print("  Nada a interpolar.")


# ════════════════════════════════════════════════════════════════════════
# FASE 5: pop_municipios - demografias interpoladas
# ════════════════════════════════════════════════════════════════════════

def fase5_pop():
    print("\n" + "=" * 70)
    print("FASE 5: pop_municipios - interpolação demográfica")
    print("=" * 70)

    # Focar em populacao_masculina/feminina/urbana/rural NULL
    cols = "id,codigo_ibge,ano,populacao,populacao_masculina,populacao_feminina,populacao_urbana,populacao_rural,faixa_populacional,nome_municipio"
    null_rows = fetch_all("pop_municipios",
                          select=cols,
                          filters="&populacao_masculina=is.null&populacao=not.is.null")
    print(f"  Registros com demografias NULL: {len(null_rows)}")

    if not null_rows:
        print("  Nada a preencher.")
        return

    # Carregar dados completos para interpolação
    all_rows = fetch_all("pop_municipios",
                         select="codigo_ibge,ano,populacao,populacao_masculina,populacao_feminina,populacao_urbana,populacao_rural,nome_municipio")
    by_mun = defaultdict(dict)
    for r in all_rows:
        by_mun[r["codigo_ibge"]][r["ano"]] = r

    updates = []
    for r in null_rows:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        pop_total = int(r["populacao"])
        mun_data = by_mun[cod]

        # Encontrar ano mais próximo com proporções conhecidas
        known = [(y, d) for y, d in mun_data.items()
                 if d.get("populacao_masculina") is not None
                 and d.get("populacao") and int(d["populacao"]) > 0]
        if not known:
            continue

        closest_y, closest_d = min(known, key=lambda x: abs(x[0] - ano))
        ref_pop = int(closest_d["populacao"])

        # Aplicar proporções
        pct_m = int(closest_d["populacao_masculina"]) / ref_pop
        pct_f = int(closest_d["populacao_feminina"]) / ref_pop
        pct_u = int(closest_d.get("populacao_urbana") or 0) / ref_pop
        pct_r = int(closest_d.get("populacao_rural") or 0) / ref_pop

        pop_m = int(round(pop_total * pct_m))
        pop_f = pop_total - pop_m
        pop_u = int(round(pop_total * pct_u))
        pop_r = pop_total - pop_u

        upd = {
            "id": r["id"],
            "populacao_masculina": pop_m,
            "populacao_feminina": pop_f,
            "populacao_urbana": pop_u,
            "populacao_rural": pop_r,
        }

        # faixa_populacional
        if r.get("faixa_populacional") is None:
            if pop_total <= 5000:
                faixa = "Até 5.000"
            elif pop_total <= 10000:
                faixa = "5.001 a 10.000"
            elif pop_total <= 20000:
                faixa = "10.001 a 20.000"
            elif pop_total <= 50000:
                faixa = "20.001 a 50.000"
            elif pop_total <= 100000:
                faixa = "50.001 a 100.000"
            elif pop_total <= 500000:
                faixa = "100.001 a 500.000"
            else:
                faixa = "Mais de 500.000"
            upd["faixa_populacional"] = faixa

        updates.append(upd)

    if updates:
        print(f"  Interpoláveis: {len(updates)}")
        n = batch_patch("pop_municipios", updates)
        print(f"  Atualizados: {n}")

    # nome_municipio NULL
    nome_null = fetch_all("pop_municipios",
                          select="id,codigo_ibge,ano,nome_municipio",
                          filters="&nome_municipio=is.null")
    print(f"\n  Registros com nome_municipio NULL: {len(nome_null)}")
    if nome_null:
        # Buscar nomes de anos que têm
        nomes = {}
        for r in all_rows:
            if r.get("nome_municipio"):
                nomes[r["codigo_ibge"]] = r["nome_municipio"]

        # Complementar com dim_politicos mandatos se necessário
        updates_n = []
        for r in nome_null:
            nome = nomes.get(r["codigo_ibge"])
            if nome:
                updates_n.append({"id": r["id"], "nome_municipio": nome})

        if updates_n:
            n = batch_patch("pop_municipios", updates_n)
            print(f"  nome_municipio preenchidos: {n}")


# ════════════════════════════════════════════════════════════════════════
# FASE 6: fato_emendas_parlamentares - politico_id por nome
# ════════════════════════════════════════════════════════════════════════

def fase6_emendas():
    print("\n" + "=" * 70)
    print("FASE 6: fato_emendas_parlamentares - match politico_id")
    print("=" * 70)

    # Carregar emendas sem politico_id
    emendas = fetch_all("fato_emendas_parlamentares",
                        select="id,autor,politico_id",
                        filters="&politico_id=is.null")
    print(f"  Emendas sem politico_id: {len(emendas)}")

    if not emendas:
        return

    # Extrair autores únicos (apenas nomes de pessoas, não comissões)
    autores = set()
    for r in emendas:
        autor = (r.get("autor") or "").strip()
        if autor and not any(kw in autor.upper() for kw in [
            "COMISS", "RELATOR", "MESA DIRETORA", "LIDERANC",
            "BANCADA", "BLOCO", "GOVERNO", "PRESIDENCIA"
        ]):
            autores.add(autor)

    print(f"  Autores individuais únicos: {len(autores)}")

    # Buscar políticos por nome (query individual - muito mais rápido que carregar 2M+)
    import unicodedata
    import urllib.parse

    def normalize(s):
        s = s.upper().strip()
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        return s

    print("  Buscando políticos por nome...")
    autor_to_pid = {}
    for autor in sorted(autores):
        encoded = urllib.parse.quote(autor)
        # Tentar match exato por nome_urna ou nome_completo
        url = (f"{SUPABASE_URL}/rest/v1/dim_politicos"
               f"?select=id&or=(nome_urna.eq.{encoded},nome_completo.eq.{encoded})"
               f"&limit=1")
        h = {**HEADERS, "Prefer": ""}
        try:
            resp = requests.get(url, headers=h, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data:
                    autor_to_pid[autor] = data[0]["id"]
        except Exception:
            pass
        time.sleep(0.05)

    matched = len(autor_to_pid)
    print(f"  Matched: {matched}/{len(autores)} autores")

    # Aplicar
    updates = []
    for r in emendas:
        autor = (r.get("autor") or "").strip()
        pid = autor_to_pid.get(autor)
        if pid:
            updates.append({"id": r["id"], "politico_id": pid})

    if updates:
        print(f"  Emendas a atualizar: {len(updates)}")
        n = batch_patch("fato_emendas_parlamentares", updates)
        print(f"  Atualizados: {n}")


# ════════════════════════════════════════════════════════════════════════
# FASE 7: fato_votos_legislativos - proposicao via bulk CSV
# ════════════════════════════════════════════════════════════════════════

def fase7_votos():
    print("\n" + "=" * 70)
    print("FASE 7: fato_votos_legislativos - proposição")
    print("=" * 70)

    # Legislaturas 55-57 → anos 2015-2026
    # Baixar TODOS os CSVs de votações e construir mapa completo
    # Depois atualizar por votacao_id (cada PATCH atualiza MUITOS votos de uma vez)
    anos = list(range(2015, 2026))
    print(f"  Anos a buscar: {anos}")

    vot_to_prop = {}
    for ano in anos:
        url = f"{CAMARA_BULK}/votacoes/csv/votacoes-{ano}.csv"
        print(f"  Baixando votacoes-{ano}.csv...")
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code != 200:
                print(f"    HTTP {resp.status_code}, pulando")
                continue
            content = resp.content.decode("utf-8-sig")
            reader = csv.DictReader(io.StringIO(content), delimiter=";")
            found = 0
            for row in reader:
                vot_id = (row.get("id") or "").strip()
                if not vot_id:
                    continue
                prop = (row.get("ultimaApresentacaoProposicao_descricao") or "").strip()
                if not prop:
                    prop = (row.get("proposicaoObjeto") or "").strip()
                if not prop:
                    prop_tipo = (row.get("ultimaApresentacaoProposicao_tipo") or "").strip()
                    prop_num = (row.get("ultimaApresentacaoProposicao_numero") or "").strip()
                    prop_ano = (row.get("ultimaApresentacaoProposicao_ano") or "").strip()
                    if prop_tipo and prop_num:
                        prop = f"{prop_tipo} {prop_num}/{prop_ano}".strip()
                if prop:
                    full_id = f"CAM-{vot_id}"
                    vot_to_prop[full_id] = prop[:200]
                    found += 1
            print(f"    {found} votações com proposição")
        except Exception as e:
            print(f"    Erro: {e}")
        time.sleep(1)

    print(f"\n  Total proposições no CSV: {len(vot_to_prop)}")

    if not vot_to_prop:
        print("  Nenhuma proposição disponível nos CSVs.")
        return

    # Atualizar em paralelo por votacao_id
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    counter = {"ok": 0, "fail": 0}
    lock = threading.Lock()
    items = list(vot_to_prop.items())

    def _patch_vot(vid_prop):
        vid, prop = vid_prop
        url = (f"{SUPABASE_URL}/rest/v1/fato_votos_legislativos"
               f"?votacao_id=eq.{vid}&proposicao=is.null")
        try:
            resp = requests.patch(url, json={"proposicao": prop},
                                  headers=HEADERS, timeout=30)
            with lock:
                if resp.status_code in (200, 204):
                    counter["ok"] += 1
                else:
                    counter["fail"] += 1
                if counter["ok"] % 1000 == 0 and counter["ok"] > 0:
                    print(f"    {counter['ok']}/{len(items)} votações atualizadas...")
        except Exception:
            with lock:
                counter["fail"] += 1

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = [pool.submit(_patch_vot, item) for item in items]
        for f in as_completed(futures):
            pass

    print(f"  Votações atualizadas com proposição: {counter['ok']}"
          f" (falhas: {counter['fail']})")


# ════════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════════

def main():
    import sys
    start = int(sys.argv[1]) if len(sys.argv) > 1 else 1

    print("=" * 70)
    print(f"PREENCHIMENTO ABRANGENTE DE NULLs (a partir da fase {start})")
    print("=" * 70)

    if start <= 1: fase1_saneamento()
    if start <= 2: fase2_financas()
    if start <= 3: fase3_pib()
    time.sleep(5)  # cooldown antes das fases pesadas
    if start <= 4: fase4_mortalidade()
    time.sleep(3)
    if start <= 5: fase5_pop()
    time.sleep(3)
    if start <= 6: fase6_emendas()
    time.sleep(3)
    if start <= 7: fase7_votos()

    print("\n" + "=" * 70)
    print("CONCLUÍDO")
    print("=" * 70)


if __name__ == "__main__":
    main()
