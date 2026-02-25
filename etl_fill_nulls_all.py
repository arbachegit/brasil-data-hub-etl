#!/usr/bin/env python3
"""
Preenchimento massivo de NULLs calculáveis a partir de dados existentes no banco.

Prioridade 1 - Cálculos internos (sem API externa):
  1. pib_estados.pib_per_capita = pib_nominal * 1e6 / populacao
  2. pib_estados.ranking_per_capita por ano
  3. pib_estados.variacao_anual para 2011-2015
  4. pib_brasil.pib_per_capita = pib_nominal * 1e9 / pop_total
  5. pib_brasil.variacao_acumulada_ano (acumulado desde 2010)
  6. pib_municipios.pib_per_capita = pib_total / populacao (2016-2020)
  7. pop_municipios vital stats 2010-2013 (copiar de mortalidade_municipios)
  8. pop_estados vital stats 2010-2013 (agregar de mortalidade_municipios)
"""

import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all(table, select, filters=None, order=None):
    """Busca todos os registros com paginação."""
    all_data = []
    offset = 0
    while True:
        q = supabase.table(table).select(select)
        if filters:
            for k, v in filters.items():
                q = q.eq(k, v)
        if order:
            q = q.order(order)
        resp = q.range(offset, offset + 999).execute()
        all_data.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000
    return all_data


def update_record(table, rec_id, data):
    """Update com tratamento de erro."""
    try:
        supabase.table(table).update(data).eq("id", rec_id).execute()
        return True
    except Exception as e:
        return False


def main():
    print("=" * 60)
    print("PREENCHIMENTO DE NULLS - TODAS AS TABELAS")
    print("=" * 60)
    total_filled = 0

    # ==========================================================
    # 1. pib_estados: pib_per_capita, ranking, variacao_anual
    # ==========================================================
    print("\n" + "=" * 50)
    print("1. PIB ESTADOS - per capita, ranking, variação")
    print("=" * 50)

    pib_est = fetch_all("pib_estados", "id,codigo_ibge_uf,ano,pib_nominal,pib_per_capita,variacao_anual")
    pop_est = fetch_all("pop_estados", "codigo_ibge_uf,ano,populacao")

    # Mapa pop: {(uf, ano): populacao}
    pop_est_map = {}
    for r in pop_est:
        pop_est_map[(r["codigo_ibge_uf"], r["ano"])] = r["populacao"]

    # Mapa pib por (uf, ano) para variação
    pib_est_map = {}
    for r in pib_est:
        pib_est_map[(r["codigo_ibge_uf"], r["ano"])] = r["pib_nominal"]

    filled_ppc = 0
    filled_var = 0

    # Calcular pib_per_capita e variacao_anual
    for r in pib_est:
        update = {}
        uf = r["codigo_ibge_uf"]
        ano = r["ano"]

        # pib_per_capita: pib_nominal está em milhões R$, pop em unidades
        if r.get("pib_per_capita") is None:
            pop = pop_est_map.get((uf, ano))
            if pop and pop > 0 and r["pib_nominal"]:
                update["pib_per_capita"] = round(r["pib_nominal"] * 1_000_000 / pop, 2)
                filled_ppc += 1

        # variacao_anual
        if r.get("variacao_anual") is None and ano > 2010:
            pib_ant = pib_est_map.get((uf, ano - 1))
            pib_atual = r["pib_nominal"]
            if pib_ant and pib_ant > 0 and pib_atual:
                update["variacao_anual"] = round((pib_atual / pib_ant - 1) * 100, 2)
                filled_var += 1

        if update:
            update_record("pib_estados", r["id"], update)

    # Ranking per capita por ano
    print(f"  pib_per_capita: +{filled_ppc}")
    print(f"  variacao_anual: +{filled_var}")

    # Recalcular ranking
    pib_est_updated = fetch_all("pib_estados", "id,ano,pib_per_capita")
    by_year = {}
    for r in pib_est_updated:
        if r.get("pib_per_capita") is not None:
            by_year.setdefault(r["ano"], []).append(r)

    filled_rank = 0
    for ano, recs in by_year.items():
        recs_sorted = sorted(recs, key=lambda x: x["pib_per_capita"], reverse=True)
        for i, r in enumerate(recs_sorted):
            update_record("pib_estados", r["id"], {"ranking_per_capita": i + 1})
            filled_rank += 1

    print(f"  ranking_per_capita: +{filled_rank}")
    total_filled += filled_ppc + filled_var + filled_rank

    # ==========================================================
    # 2. pib_brasil: pib_per_capita, variacao_acumulada
    # ==========================================================
    print("\n" + "=" * 50)
    print("2. PIB BRASIL - per capita, variação acumulada")
    print("=" * 50)

    pib_br = fetch_all("pib_brasil", "id,ano,pib_nominal,pib_per_capita,variacao_anual,variacao_acumulada_ano")

    # Pop total do Brasil por ano (somar pop_estados)
    pop_br_map = {}
    for r in pop_est:
        pop_br_map[r["ano"]] = pop_br_map.get(r["ano"], 0) + (r["populacao"] or 0)

    pib_br_sorted = sorted(pib_br, key=lambda x: x["ano"])
    filled_br = 0

    base_pib = None
    for r in pib_br_sorted:
        update = {}
        ano = r["ano"]

        # pib_per_capita: pib_nominal em trilhões, pop em unidades
        if r.get("pib_per_capita") is None:
            pop = pop_br_map.get(ano)
            if pop and pop > 0 and r["pib_nominal"]:
                # pib_nominal está em R$ (valor absoluto no banco)
                # Verificar a escala checando o valor
                pib_val = r["pib_nominal"]
                # Se pib_nominal > 1e12, está em R$; se < 100, está em trilhões
                if pib_val < 100:
                    pib_reais = pib_val * 1e12
                elif pib_val < 100000:
                    pib_reais = pib_val * 1e9
                else:
                    pib_reais = pib_val
                update["pib_per_capita"] = round(pib_reais / pop, 2)
                filled_br += 1

        # variacao_acumulada_ano (acumulado desde 2010)
        if r.get("variacao_acumulada_ano") is None:
            if base_pib is None and r["pib_nominal"]:
                base_pib = r["pib_nominal"]
                update["variacao_acumulada_ano"] = 0.0
                filled_br += 1
            elif base_pib and r["pib_nominal"]:
                update["variacao_acumulada_ano"] = round(
                    (r["pib_nominal"] / base_pib - 1) * 100, 2
                )
                filled_br += 1

        if update:
            update_record("pib_brasil", r["id"], update)

    print(f"  campos preenchidos: +{filled_br}")
    total_filled += filled_br

    # ==========================================================
    # 3. pib_municipios: pib_per_capita onde falta
    # ==========================================================
    print("\n" + "=" * 50)
    print("3. PIB MUNICIPIOS - pib_per_capita faltante")
    print("=" * 50)

    # Carregar pop_municipios para os anos 2016-2020
    pop_mun_map = {}
    for ano in range(2010, 2026):
        recs = fetch_all("pop_municipios", "codigo_ibge,populacao", {"ano": ano})
        for r in recs:
            pop_mun_map[(r["codigo_ibge"], ano)] = r["populacao"]
    print(f"  {len(pop_mun_map)} registros de população carregados")

    # Buscar pib_municipios com pib_per_capita NULL
    pib_mun_all = fetch_all("pib_municipios", "id,codigo_ibge,ano,pib_total,pib_per_capita")
    sem_ppc = [r for r in pib_mun_all if r.get("pib_per_capita") is None and r.get("pib_total")]

    print(f"  {len(sem_ppc)} registros sem pib_per_capita")

    filled_mun_ppc = 0
    for r in sem_ppc:
        pop = pop_mun_map.get((r["codigo_ibge"], r["ano"]))
        if not pop or pop <= 0:
            # Tentar interpolar dos anos vizinhos
            pop_anos = {}
            for a in range(2010, 2026):
                p = pop_mun_map.get((r["codigo_ibge"], a))
                if p and p > 0:
                    pop_anos[a] = p
            if pop_anos:
                anos_ord = sorted(pop_anos.keys())
                ant = [a for a in anos_ord if a <= r["ano"]]
                pos = [a for a in anos_ord if a >= r["ano"]]
                if ant and pos and ant[-1] != pos[0]:
                    a1, a2 = ant[-1], pos[0]
                    frac = (r["ano"] - a1) / (a2 - a1)
                    pop = int(pop_anos[a1] + (pop_anos[a2] - pop_anos[a1]) * frac)
                elif ant:
                    pop = pop_anos[ant[-1]]
                elif pos:
                    pop = pop_anos[pos[0]]

        if pop and pop > 0:
            # pib_total está em R$ mil
            ppc = round(r["pib_total"] * 1000 / pop, 2)
            update_record("pib_municipios", r["id"], {"pib_per_capita": ppc})
            filled_mun_ppc += 1

    print(f"  pib_per_capita: +{filled_mun_ppc}")
    total_filled += filled_mun_ppc

    # ==========================================================
    # 4. pop_municipios: copiar vital stats de mortalidade_municipios
    # ==========================================================
    print("\n" + "=" * 50)
    print("4. POP MUNICIPIOS - vital stats 2010-2013 (de mortalidade)")
    print("=" * 50)

    # Carregar mortalidade 2010-2013
    mort_map = {}
    for ano in range(2010, 2014):
        recs = fetch_all("mortalidade_municipios",
                         "codigo_ibge,obitos_total,obitos_masculinos,obitos_femininos,nascimentos,taxa_mortalidade",
                         {"ano": ano})
        for r in recs:
            mort_map[(r["codigo_ibge"], ano)] = r
    print(f"  {len(mort_map)} registros de mortalidade 2010-2013 carregados")

    # Buscar pop_municipios 2010-2013 com NULLs
    filled_vital = 0
    for ano in range(2010, 2014):
        recs = fetch_all("pop_municipios", "id,codigo_ibge,populacao,obitos_total,nascimentos", {"ano": ano})
        for r in recs:
            if r.get("obitos_total") is not None:
                continue

            mort = mort_map.get((r["codigo_ibge"], ano))
            if not mort:
                continue

            update = {}
            if mort.get("obitos_total") is not None:
                update["obitos_total"] = mort["obitos_total"]
            if mort.get("obitos_masculinos") is not None:
                update["obitos_masculinos"] = mort["obitos_masculinos"]
            if mort.get("obitos_femininos") is not None:
                update["obitos_femininos"] = mort["obitos_femininos"]
            if mort.get("nascimentos") is not None:
                update["nascimentos"] = mort["nascimentos"]
                # nascimentos_masculino/feminino não temos desagregado
            if mort.get("taxa_mortalidade") is not None:
                update["taxa_mortalidade"] = float(mort["taxa_mortalidade"])

            # taxa_natalidade
            pop = r.get("populacao")
            nasc = mort.get("nascimentos")
            if pop and pop > 0 and nasc:
                update["taxa_natalidade"] = round((nasc / pop) * 1000, 2)

            if update:
                update_record("pop_municipios", r["id"], update)
                filled_vital += 1

        print(f"    {ano}: preenchidos até agora: {filled_vital}")

    print(f"  vital stats: +{filled_vital}")
    total_filled += filled_vital

    # ==========================================================
    # 5. pop_estados: vital stats 2010-2013 (agregar mortalidade)
    # ==========================================================
    print("\n" + "=" * 50)
    print("5. POP ESTADOS - vital stats 2010-2013 (agregado)")
    print("=" * 50)

    filled_est_vital = 0
    for ano in range(2010, 2014):
        # Agregar mortalidade por UF
        mort_recs = fetch_all("mortalidade_municipios",
                              "codigo_ibge_uf,obitos_total,obitos_masculinos,obitos_femininos,nascimentos",
                              {"ano": ano})

        uf_agg = {}
        for r in mort_recs:
            uf = r["codigo_ibge_uf"]
            if uf not in uf_agg:
                uf_agg[uf] = {"obitos_total": 0, "obitos_masculinos": 0,
                              "obitos_femininos": 0, "nascimentos": 0}
            for campo in ["obitos_total", "obitos_masculinos", "obitos_femininos", "nascimentos"]:
                if r.get(campo):
                    uf_agg[uf][campo] += r[campo]

        # Buscar pop_estados com NULLs
        est_recs = fetch_all("pop_estados", "id,codigo_ibge_uf,populacao,obitos_total", {"ano": ano})
        for r in est_recs:
            if r.get("obitos_total") is not None:
                continue

            uf = r["codigo_ibge_uf"]
            agg = uf_agg.get(uf)
            if not agg:
                continue

            update = {}
            for campo in ["obitos_total", "obitos_masculinos", "obitos_femininos", "nascimentos"]:
                if agg[campo] > 0:
                    update[campo] = agg[campo]

            pop = r.get("populacao")
            if pop and pop > 0:
                if agg["obitos_total"] > 0:
                    update["taxa_mortalidade"] = round((agg["obitos_total"] / pop) * 1000, 2)
                if agg["nascimentos"] > 0:
                    update["taxa_natalidade"] = round((agg["nascimentos"] / pop) * 1000, 2)

            if update:
                update_record("pop_estados", r["id"], update)
                filled_est_vital += 1

        print(f"    {ano}: {filled_est_vital} estados atualizados")

    print(f"  vital stats estados: +{filled_est_vital}")
    total_filled += filled_est_vital

    # ==========================================================
    # RESUMO FINAL
    # ==========================================================
    print(f"\n{'='*60}")
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  pib_estados.pib_per_capita:       +{filled_ppc}")
    print(f"  pib_estados.variacao_anual:        +{filled_var}")
    print(f"  pib_estados.ranking_per_capita:    +{filled_rank}")
    print(f"  pib_brasil campos:                 +{filled_br}")
    print(f"  pib_municipios.pib_per_capita:     +{filled_mun_ppc}")
    print(f"  pop_municipios vital stats:        +{filled_vital}")
    print(f"  pop_estados vital stats:           +{filled_est_vital}")
    print(f"  ----------------------------------------")
    print(f"  TOTAL CAMPOS PREENCHIDOS:          +{total_filled}")
    print("=" * 60)


if __name__ == "__main__":
    main()
