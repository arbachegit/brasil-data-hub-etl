#!/usr/bin/env python3
"""
Projeção IDH Municipal (2011-2025)
Modelos:
  - IDHM_Renda: fórmula PNUD com renda_per_capita / pib_per_capita
  - IDHM_Longevidade: Preston curve simplificada com taxa_mortalidade
  - IDHM_Educação: crescimento geométrico com amortecimento (ceiling)
  - Gini: tendência linear com mean reversion
"""

import math
import numpy as np
import pandas as pd
from collections import defaultdict
from etl_projection_utils import (
    setup_supabase, fetch_all, batch_insert, classificar_idh
)


def clamp(val, lo, hi):
    return max(lo, min(hi, val))


def idhm_renda_pnud(renda):
    """Fórmula PNUD: IDHM_R = (ln(renda) - ln(8)) / (ln(4033) - ln(8))"""
    if renda is None or renda <= 8:
        return 0.0
    return clamp((math.log(renda) - math.log(8)) / (math.log(4033) - math.log(8)), 0, 1)


def main():
    sb = setup_supabase()

    print("=" * 70)
    print("PROJEÇÃO IDH MUNICIPAL (2011-2025)")
    print("=" * 70)

    # ── 1. Carregar dados âncora ────────────────────────────────────────
    print("\n1. Carregando dados âncora...")

    # IDH histórico (1991, 2000, 2010)
    idh_rows = fetch_all(sb, "idh_municipios",
                         select="codigo_ibge,codigo_ibge_uf,ano,idhm,idhm_educacao,idhm_longevidade,idhm_renda,indice_gini")
    print(f"  idh_municipios: {len(idh_rows)} registros")

    # Organizar por município
    idh = defaultdict(dict)  # {codigo: {ano: {campo: val}}}
    for r in idh_rows:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        # Pular projeções anteriores
        if ano not in (1991, 2000, 2010):
            continue
        idh[cod][ano] = {
            "uf": r["codigo_ibge_uf"],
            "idhm_e": float(r["idhm_educacao"]) if r.get("idhm_educacao") else None,
            "idhm_l": float(r["idhm_longevidade"]) if r.get("idhm_longevidade") else None,
            "idhm_r": float(r["idhm_renda"]) if r.get("idhm_renda") else None,
            "gini": float(r["indice_gini"]) if r.get("indice_gini") else None,
        }

    municipios = set(idh.keys())
    print(f"  Municípios com IDH: {len(municipios)}")

    # PIB (2010-2021): renda_per_capita, pib_per_capita
    pib_rows = fetch_all(sb, "pib_municipios",
                         select="codigo_ibge,ano,pib_per_capita,renda_per_capita")
    pib = defaultdict(dict)  # {codigo: {ano: {ppc, rpc}}}
    for r in pib_rows:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        pib[cod][ano] = {
            "ppc": float(r["pib_per_capita"]) if r.get("pib_per_capita") else None,
            "rpc": float(r["renda_per_capita"]) if r.get("renda_per_capita") else None,
        }
    print(f"  pib_municipios: {len(pib_rows)} registros")

    # Mortalidade (2010-2023)
    mort_rows = fetch_all(sb, "mortalidade_municipios",
                          select="codigo_ibge,ano,taxa_mortalidade")
    mort = defaultdict(dict)  # {codigo: {ano: taxa}}
    for r in mort_rows:
        if r.get("taxa_mortalidade") is not None:
            mort[r["codigo_ibge"]][r["ano"]] = float(r["taxa_mortalidade"])
    print(f"  mortalidade_municipios: {len(mort_rows)} registros")

    # População (2010-2025) - para referência
    pop_rows = fetch_all(sb, "pop_municipios",
                         select="codigo_ibge,ano,populacao")
    pop = defaultdict(dict)
    for r in pop_rows:
        if r.get("populacao"):
            pop[r["codigo_ibge"]][r["ano"]] = int(r["populacao"])
    print(f"  pop_municipios: {len(pop_rows)} registros")

    # ── 2. Calcular medianas estaduais (fallbacks) ──────────────────────
    print("\n2. Calculando medianas estaduais...")

    # Crescimento educação por UF
    uf_educ_growth = defaultdict(list)
    for cod, anos in idh.items():
        if 1991 in anos and 2010 in anos:
            e91 = anos[1991]["idhm_e"]
            e10 = anos[2010]["idhm_e"]
            uf = anos[2010]["uf"] if 2010 in anos else anos[1991]["uf"]
            if e91 and e10 and e91 > 0.01:
                g = (e10 / e91) ** (1 / 19) - 1
                uf_educ_growth[uf].append(g)

    uf_educ_median = {}
    for uf, gs in uf_educ_growth.items():
        uf_educ_median[uf] = float(np.median(gs))
    global_educ_median = float(np.median([g for gs in uf_educ_growth.values() for g in gs])) if uf_educ_growth else 0.01
    print(f"  Mediana educação global: {global_educ_median:.4f}")

    # Crescimento longevidade por UF
    uf_long_growth = defaultdict(list)
    for cod, anos in idh.items():
        if 2000 in anos and 2010 in anos:
            l00 = anos[2000]["idhm_l"]
            l10 = anos[2010]["idhm_l"]
            if l00 and l10 and l00 > 0:
                g = (l10 / l00) ** (1 / 10) - 1
                uf = anos[2010]["uf"]
                uf_long_growth[uf].append(g)

    uf_long_median = {}
    for uf, gs in uf_long_growth.items():
        uf_long_median[uf] = float(np.median(gs))
    global_long_median = float(np.median([g for gs in uf_long_growth.values() for g in gs])) if uf_long_growth else 0.005

    # ── 3. Projetar ─────────────────────────────────────────────────────
    print("\n3. Projetando 2011-2025...")

    records = []
    skipped = 0

    for cod in sorted(municipios):
        anos_data = idh[cod]
        if 2010 not in anos_data:
            skipped += 1
            continue

        base = anos_data[2010]
        uf = base["uf"]
        e_2010 = base["idhm_e"]
        l_2010 = base["idhm_l"]
        r_2010 = base["idhm_r"]
        gini_2010 = base["gini"]

        if not all([e_2010, l_2010, r_2010]):
            skipped += 1
            continue

        # ── IDHM_Educação: crescimento geométrico com amortecimento ──
        e_1991 = anos_data.get(1991, {}).get("idhm_e")
        if e_1991 and e_1991 > 0.01:
            g_base = (e_2010 / e_1991) ** (1 / 19) - 1
        else:
            g_base = uf_educ_median.get(uf, global_educ_median)

        educ_proj = {2010: e_2010}
        e_curr = e_2010
        for t in range(2011, 2026):
            g_t = g_base * max(0.05, 1 - e_curr)
            e_curr = clamp(e_curr * (1 + g_t), 0, 1)
            educ_proj[t] = e_curr

        # ── IDHM_Longevidade: Preston curve simplificada ──
        mort_2010 = mort.get(cod, {}).get(2010)
        long_proj = {2010: l_2010}

        if mort_2010 and mort_2010 > 0:
            l_curr = l_2010
            for t in range(2011, 2026):
                mort_t = mort.get(cod, {}).get(t)
                if mort_t is None:
                    # Extrapolar tendência linear de 2018-2023
                    recent = [(y, mort.get(cod, {}).get(y))
                              for y in range(2018, 2024)
                              if mort.get(cod, {}).get(y) is not None]
                    if len(recent) >= 2:
                        xs = [r[0] for r in recent]
                        ys = [r[1] for r in recent]
                        slope = np.polyfit(xs, ys, 1)[0]
                        mort_t = recent[-1][1] + slope * (t - recent[-1][0])
                        mort_t = max(0.5, mort_t)
                    else:
                        mort_t = mort_2010

                ratio = mort_2010 / max(mort_t, 0.5)
                l_curr = clamp(l_2010 * (ratio ** 0.15), 0, 1)
                long_proj[t] = l_curr
        else:
            # Fallback: crescimento médio estadual
            g_long = uf_long_median.get(uf, global_long_median)
            l_curr = l_2010
            for t in range(2011, 2026):
                l_curr = clamp(l_curr * (1 + g_long), 0, 1)
                long_proj[t] = l_curr

        # ── IDHM_Renda: fórmula PNUD ──
        renda_proj = {}
        renda_2010 = pib.get(cod, {}).get(2010, {}).get("rpc")
        ppc_2010 = pib.get(cod, {}).get(2010, {}).get("ppc")

        if renda_2010 and renda_2010 > 0:
            renda_proj[2010] = renda_2010
            for t in range(2011, 2026):
                renda_t = pib.get(cod, {}).get(t, {}).get("rpc")
                if renda_t and renda_t > 0:
                    renda_proj[t] = renda_t
                else:
                    # Extrapolar via tendência PIB per capita
                    ppc_t = pib.get(cod, {}).get(t, {}).get("ppc")
                    if ppc_t and ppc_2010 and ppc_2010 > 0:
                        growth = ppc_t / ppc_2010
                        renda_proj[t] = renda_2010 * growth
                    else:
                        # Usar último ano conhecido + crescimento 2%
                        last_year = max(y for y in renda_proj if y < t)
                        renda_proj[t] = renda_proj[last_year] * 1.02
        elif ppc_2010 and ppc_2010 > 0:
            # Sem renda_per_capita, estimar a partir de pib_per_capita
            # Renda ~ 40-60% do PIB per capita (aproximação)
            renda_proj[2010] = ppc_2010 * 0.5
            for t in range(2011, 2026):
                ppc_t = pib.get(cod, {}).get(t, {}).get("ppc")
                if ppc_t:
                    renda_proj[t] = ppc_t * 0.5
                else:
                    last_year = max(y for y in renda_proj if y < t)
                    renda_proj[t] = renda_proj[last_year] * 1.02
        else:
            # Fallback total: manter IDHM_R constante
            for t in range(2010, 2026):
                renda_proj[t] = None

        renda_idh = {}
        for t in range(2011, 2026):
            rpc = renda_proj.get(t)
            if rpc:
                renda_idh[t] = idhm_renda_pnud(rpc)
            else:
                renda_idh[t] = r_2010  # constante

        # ── Gini: tendência linear com mean reversion ──
        gini_1991 = anos_data.get(1991, {}).get("gini")
        gini_2000 = anos_data.get(2000, {}).get("gini")
        gini_proj = {}

        if gini_2010 is not None:
            # Calcular slope médio dos pontos disponíveis
            points = []
            if gini_1991 is not None:
                points.append((1991, gini_1991))
            if gini_2000 is not None:
                points.append((2000, gini_2000))
            points.append((2010, gini_2010))

            if len(points) >= 2:
                xs = [p[0] for p in points]
                ys = [p[1] for p in points]
                slope = np.polyfit(xs, ys, 1)[0]
            else:
                slope = 0.0

            for t in range(2011, 2026):
                damping = 0.95 ** (t - 2010)
                gini_t = gini_2010 + slope * (t - 2010) * damping
                gini_proj[t] = clamp(gini_t, 0.25, 0.80)

        # ── Montar registros ──
        for t in range(2011, 2026):
            e_t = educ_proj.get(t, e_2010)
            l_t = long_proj.get(t, l_2010)
            r_t = renda_idh.get(t, r_2010)
            idhm_t = (e_t * l_t * r_t) ** (1 / 3)
            gini_t = gini_proj.get(t)

            records.append({
                "codigo_ibge": cod,
                "codigo_ibge_uf": uf,
                "ano": t,
                "idhm": round(idhm_t, 4),
                "idhm_educacao": round(e_t, 4),
                "idhm_longevidade": round(l_t, 4),
                "idhm_renda": round(r_t, 4),
                "indice_gini": round(gini_t, 4) if gini_t is not None else None,
                "classificacao": classificar_idh(idhm_t),
                "fonte": "Projeção econométrica",
            })

    print(f"  Gerados: {len(records)} registros")
    print(f"  Municípios pulados (sem dados 2010): {skipped}")

    # Verificação rápida
    df = pd.DataFrame(records)
    print(f"\n  Estatísticas IDHM projetado:")
    print(f"    min={df['idhm'].min():.4f}  max={df['idhm'].max():.4f}  "
          f"mean={df['idhm'].mean():.4f}  median={df['idhm'].median():.4f}")

    por_ano = df.groupby("ano").size()
    for ano, n in por_ano.items():
        print(f"    {ano}: {n} municípios")

    # ── 4. Inserir ──────────────────────────────────────────────────────
    print("\n4. Inserindo no banco...")
    inserted = batch_insert(sb, "idh_municipios", records,
                            on_conflict="codigo_ibge,ano")
    print(f"\n  Total inseridos/atualizados: {inserted}")

    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    for ano in [1991, 2000, 2010, 2015, 2020, 2025]:
        r = sb.table("idh_municipios").select("id", count="exact").eq("ano", ano).execute()
        print(f"  {ano}: {r.count or 0} municípios")
    print("=" * 70)
    print("Projeção IDH concluída!")


if __name__ == "__main__":
    main()
