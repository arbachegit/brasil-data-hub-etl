#!/usr/bin/env python3
"""
Projeção Saneamento Municipal (2011-2025)
Modelo: Curva logística (S-curve) por serviço
  C[t] = K / (1 + ((K - C₀)/C₀) × exp(-r × (t - 2010)))
Parâmetros r variam por tier de riqueza (PIB per capita 2010) e região.
"""

import math
import numpy as np
import pandas as pd
from collections import defaultdict
from etl_projection_utils import (
    setup_supabase, fetch_all, batch_insert, UF_REGIAO
)

# Parâmetros da curva logística por serviço e tier
PARAMS = {
    "pct_agua_rede_geral": {
        "K": 98.0,
        "r": {"alto": 0.06, "medio": 0.04, "baixo": 0.03},
    },
    "pct_esgoto_adequado": {
        "K": 95.0,
        "r": {"alto": 0.05, "medio": 0.03, "baixo": 0.02},
    },
    "pct_lixo_coletado": {
        "K": 99.0,
        "r": {"alto": 0.08, "medio": 0.06, "baixo": 0.04},
    },
}

# Multiplicadores regionais
REGIAO_MULT = {
    "Norte": 0.80,
    "Nordeste": 0.85,
    "Centro-Oeste": 1.00,
    "Sul": 1.10,
    "Sudeste": 1.15,
}

# Tiers de riqueza (PIB per capita 2010 em R$)
TIER_ALTO = 25000
TIER_MEDIO = 10000


def logistic(c0, K, r, t, t0=2010):
    """Curva logística: C[t] = K / (1 + ((K-C0)/C0) * exp(-r*(t-t0)))"""
    if c0 is None or c0 <= 0:
        c0 = 1.0  # evitar divisão por zero
    if c0 >= K:
        return K
    ratio = (K - c0) / c0
    return K / (1 + ratio * math.exp(-r * (t - t0)))


def get_tier(pib_pc):
    if pib_pc is None:
        return "medio"
    if pib_pc > TIER_ALTO:
        return "alto"
    elif pib_pc >= TIER_MEDIO:
        return "medio"
    else:
        return "baixo"


def main():
    sb = setup_supabase()

    print("=" * 70)
    print("PROJEÇÃO SANEAMENTO MUNICIPAL (2011-2025)")
    print("=" * 70)

    # ── 1. Carregar dados ───────────────────────────────────────────────
    print("\n1. Carregando dados...")

    san_rows = fetch_all(
        sb, "saneamento_municipios",
        select="codigo_ibge,codigo_ibge_uf,ano,domicilios_total,"
               "agua_rede_geral,agua_poco_nascente,agua_outra,"
               "pct_agua_rede_geral,esgoto_rede_geral,esgoto_fossa_septica,"
               "esgoto_outro,esgoto_sem,pct_esgoto_adequado,"
               "lixo_coletado,lixo_outro,pct_lixo_coletado",
        filters=[("ano", "eq", 2010)]
    )
    print(f"  saneamento_municipios (2010): {len(san_rows)} registros")

    san = {}
    for r in san_rows:
        san[r["codigo_ibge"]] = r

    # PIB per capita 2010
    pib_rows = fetch_all(sb, "pib_municipios",
                         select="codigo_ibge,ano,pib_per_capita",
                         filters=[("ano", "eq", 2010)])
    pib_pc = {}
    for r in pib_rows:
        if r.get("pib_per_capita"):
            pib_pc[r["codigo_ibge"]] = float(r["pib_per_capita"])
    print(f"  pib_municipios (2010): {len(pib_pc)} municípios com pib_per_capita")

    # População (2010-2025)
    pop_rows = fetch_all(sb, "pop_municipios",
                         select="codigo_ibge,ano,populacao")
    pop = defaultdict(dict)
    for r in pop_rows:
        if r.get("populacao"):
            pop[r["codigo_ibge"]][r["ano"]] = int(r["populacao"])
    print(f"  pop_municipios: {len(pop_rows)} registros")

    # ── 2. Projetar ─────────────────────────────────────────────────────
    print("\n2. Projetando 2011-2025...")

    records = []
    skipped = 0

    for cod, base in sorted(san.items()):
        uf = base["codigo_ibge_uf"]
        regiao = UF_REGIAO.get(uf, "Centro-Oeste")
        reg_mult = REGIAO_MULT.get(regiao, 1.0)
        tier = get_tier(pib_pc.get(cod))

        dom_2010 = base.get("domicilios_total")
        pop_2010 = pop.get(cod, {}).get(2010)

        if not dom_2010 or dom_2010 <= 0:
            skipped += 1
            continue

        for t in range(2011, 2026):
            # ── Domicílios: escalar com população ──
            pop_t = pop.get(cod, {}).get(t)
            if pop_t and pop_2010 and pop_2010 > 0:
                dom_t = int(round(dom_2010 * pop_t / pop_2010))
            else:
                # Interpolar população
                known_years = sorted(pop.get(cod, {}).keys())
                if known_years:
                    closest = min(known_years, key=lambda y: abs(y - t))
                    pop_closest = pop[cod][closest]
                    dom_t = int(round(dom_2010 * pop_closest / pop_2010)) if pop_2010 else dom_2010
                else:
                    dom_t = dom_2010

            dom_t = max(dom_t, 1)

            rec = {
                "codigo_ibge": cod,
                "codigo_ibge_uf": uf,
                "ano": t,
                "domicilios_total": dom_t,
                "fonte": "Projeção econométrica",
            }

            # ── Projetar cada serviço ──
            for service in ["pct_agua_rede_geral", "pct_esgoto_adequado", "pct_lixo_coletado"]:
                c0 = base.get(service)
                if c0 is not None:
                    c0 = float(c0)
                else:
                    c0 = 0.0

                K = PARAMS[service]["K"]
                r_base = PARAMS[service]["r"][tier]
                r = r_base * reg_mult

                if c0 >= K:
                    pct_t = K
                else:
                    pct_t = logistic(c0, K, r, t)

                pct_t = max(0, min(100, round(pct_t, 2)))
                rec[service] = pct_t

            # ── Contagens absolutas ──
            pct_agua = rec["pct_agua_rede_geral"]
            pct_esgoto = rec["pct_esgoto_adequado"]
            pct_lixo = rec["pct_lixo_coletado"]

            rec["agua_rede_geral"] = int(round(pct_agua / 100 * dom_t))

            # Distribuir restante de água mantendo proporções de 2010
            agua_rest = dom_t - rec["agua_rede_geral"]
            poco_2010 = base.get("agua_poco_nascente") or 0
            outra_2010 = base.get("agua_outra") or 0
            total_rest_2010 = poco_2010 + outra_2010
            if total_rest_2010 > 0 and agua_rest > 0:
                rec["agua_poco_nascente"] = int(round(agua_rest * poco_2010 / total_rest_2010))
                rec["agua_outra"] = agua_rest - rec["agua_poco_nascente"]
            else:
                rec["agua_poco_nascente"] = 0
                rec["agua_outra"] = max(0, agua_rest)

            # Esgoto
            esgoto_adeq = int(round(pct_esgoto / 100 * dom_t))
            rec["esgoto_rede_geral"] = esgoto_adeq  # simplificação: adequado = rede_geral
            esgoto_rest = dom_t - esgoto_adeq
            fossa_2010 = base.get("esgoto_fossa_septica") or 0
            outro_2010 = base.get("esgoto_outro") or 0
            sem_2010 = base.get("esgoto_sem") or 0
            total_esg_rest_2010 = fossa_2010 + outro_2010 + sem_2010
            if total_esg_rest_2010 > 0 and esgoto_rest > 0:
                rec["esgoto_fossa_septica"] = int(round(esgoto_rest * fossa_2010 / total_esg_rest_2010))
                rec["esgoto_outro"] = int(round(esgoto_rest * outro_2010 / total_esg_rest_2010))
                rec["esgoto_sem"] = esgoto_rest - rec["esgoto_fossa_septica"] - rec["esgoto_outro"]
            else:
                rec["esgoto_fossa_septica"] = 0
                rec["esgoto_outro"] = 0
                rec["esgoto_sem"] = max(0, esgoto_rest)

            # Lixo
            rec["lixo_coletado"] = int(round(pct_lixo / 100 * dom_t))
            rec["lixo_outro"] = max(0, dom_t - rec["lixo_coletado"])

            records.append(rec)

    print(f"  Gerados: {len(records)} registros")
    print(f"  Municípios pulados (sem domicílios 2010): {skipped}")

    # Verificação
    df = pd.DataFrame(records)
    if not df.empty:
        print(f"\n  Estatísticas pct_esgoto_adequado:")
        print(f"    min={df['pct_esgoto_adequado'].min():.2f}  "
              f"max={df['pct_esgoto_adequado'].max():.2f}  "
              f"mean={df['pct_esgoto_adequado'].mean():.2f}")
        por_ano = df.groupby("ano").size()
        for ano, n in por_ano.items():
            print(f"    {ano}: {n} municípios")

    # ── 3. Inserir ──────────────────────────────────────────────────────
    print("\n3. Inserindo no banco...")
    inserted = batch_insert(sb, "saneamento_municipios", records,
                            on_conflict="codigo_ibge,ano")
    print(f"\n  Total inseridos/atualizados: {inserted}")

    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    for ano in [2010, 2015, 2020, 2025]:
        r = sb.table("saneamento_municipios").select("id", count="exact").eq("ano", ano).execute()
        print(f"  {ano}: {r.count or 0} municípios")
    print("=" * 70)
    print("Projeção saneamento concluída!")


if __name__ == "__main__":
    main()
