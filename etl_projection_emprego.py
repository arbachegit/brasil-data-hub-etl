#!/usr/bin/env python3
"""
Projeção Emprego Municipal (2022-2025)
Modelo: Regressão log-linear OLS por município
  ln(Y[t]) = β₀ + β₁·t + β₂·ln(pib_total[t]) + β₃·ln(populacao[t]) + ε
Fallback: tendência linear se <6 obs ou R²<0.3
"""

import math
import numpy as np
import pandas as pd
from collections import defaultdict
import statsmodels.api as sm
from etl_projection_utils import (
    setup_supabase, fetch_all, batch_insert, SALARIO_MINIMO
)


def main():
    sb = setup_supabase()

    print("=" * 70)
    print("PROJEÇÃO EMPREGO MUNICIPAL (2022-2025)")
    print("=" * 70)

    # ── 1. Carregar dados ───────────────────────────────────────────────
    print("\n1. Carregando dados...")

    emp_rows = fetch_all(sb, "emprego_municipios",
                         select="codigo_ibge,codigo_ibge_uf,ano,pessoal_ocupado_total,empresas,unidades_locais,pessoal_assalariado,salarios_mil_reais,salario_medio_sm,salario_medio_reais")
    print(f"  emprego_municipios: {len(emp_rows)} registros")

    pib_rows = fetch_all(sb, "pib_municipios",
                         select="codigo_ibge,ano,pib_total")
    print(f"  pib_municipios: {len(pib_rows)} registros")

    pop_rows = fetch_all(sb, "pop_municipios",
                         select="codigo_ibge,ano,populacao")
    print(f"  pop_municipios: {len(pop_rows)} registros")

    # Organizar emprego por município
    emp = defaultdict(dict)
    emp_uf = {}
    for r in emp_rows:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        emp[cod][ano] = r
        emp_uf[cod] = r["codigo_ibge_uf"]

    # PIB e pop por município/ano
    pib = defaultdict(dict)
    for r in pib_rows:
        if r.get("pib_total"):
            pib[r["codigo_ibge"]][r["ano"]] = float(r["pib_total"])

    pop = defaultdict(dict)
    for r in pop_rows:
        if r.get("populacao"):
            pop[r["codigo_ibge"]][r["ano"]] = int(r["populacao"])

    municipios = set(emp.keys())
    print(f"  Municípios com emprego: {len(municipios)}")

    # ── 2. Calcular taxas de crescimento estaduais (fallback) ───────────
    print("\n2. Calculando crescimentos estaduais (fallback)...")
    uf_growth = defaultdict(lambda: defaultdict(list))
    for cod in municipios:
        uf = emp_uf.get(cod, 0)
        anos_data = emp[cod]
        sorted_anos = sorted(anos_data.keys())
        for i in range(1, len(sorted_anos)):
            a0, a1 = sorted_anos[i - 1], sorted_anos[i]
            for var in ["pessoal_ocupado_total", "empresas", "unidades_locais", "pessoal_assalariado"]:
                v0 = anos_data[a0].get(var)
                v1 = anos_data[a1].get(var)
                if v0 and v1 and v0 > 0 and v1 > 0:
                    g = (v1 / v0) ** (1 / (a1 - a0)) - 1
                    if abs(g) < 0.5:  # filtrar outliers
                        uf_growth[uf][var].append(g)

    uf_median_growth = {}
    for uf, vars_dict in uf_growth.items():
        uf_median_growth[uf] = {}
        for var, gs in vars_dict.items():
            uf_median_growth[uf][var] = float(np.median(gs)) if gs else 0.02

    # ── 3. Projetar ─────────────────────────────────────────────────────
    print("\n3. Projetando 2022-2025...")

    proj_years = [2022, 2023, 2024, 2025]
    records = []
    stats = {"ols_full": 0, "ols_trend": 0, "fallback_uf": 0}

    for cod in sorted(municipios):
        anos_data = emp[cod]
        uf = emp_uf.get(cod, 0)

        # Preparar série temporal
        hist_years = sorted([a for a in anos_data if a <= 2021])
        if not hist_years:
            continue

        for var in ["pessoal_ocupado_total", "empresas", "unidades_locais", "pessoal_assalariado"]:
            # Coletar pares (ano, valor) não-nulos
            pairs = [(y, anos_data[y].get(var))
                     for y in hist_years if anos_data[y].get(var) is not None]
            valid = [(y, v) for y, v in pairs if v and v > 0]

            if len(valid) < 6:
                # Fallback: crescimento estadual
                if valid:
                    last_y, last_v = valid[-1]
                    g = uf_median_growth.get(uf, {}).get(var, 0.02)
                    for t in proj_years:
                        proj_val = last_v * (1 + g) ** (t - last_y)
                        _store_proj(emp, cod, t, var, max(0, int(round(proj_val))))
                    stats["fallback_uf"] += 1
                continue

            years_arr = np.array([p[0] for p in valid])
            vals_arr = np.array([p[1] for p in valid])
            ln_y = np.log(vals_arr)

            # Tentar OLS completo com covariáveis
            model_ok = False
            try:
                ln_pib = []
                ln_pop = []
                for y in years_arr:
                    p = pib.get(cod, {}).get(int(y))
                    q = pop.get(cod, {}).get(int(y))
                    ln_pib.append(math.log(p) if p and p > 0 else np.nan)
                    ln_pop.append(math.log(q) if q and q > 0 else np.nan)

                ln_pib = np.array(ln_pib)
                ln_pop = np.array(ln_pop)

                # Checar se temos covariáveis suficientes
                valid_mask = ~(np.isnan(ln_pib) | np.isnan(ln_pop))
                if valid_mask.sum() >= 6:
                    X = np.column_stack([
                        years_arr[valid_mask],
                        ln_pib[valid_mask],
                        ln_pop[valid_mask],
                    ])
                    X = sm.add_constant(X)
                    y_fit = ln_y[valid_mask]
                    model = sm.OLS(y_fit, X).fit()

                    if model.rsquared >= 0.3:
                        # Predizer
                        for t in proj_years:
                            p_t = pib.get(cod, {}).get(t)
                            q_t = pop.get(cod, {}).get(t)
                            # Se não temos pib/pop futuro, extrapolar
                            if not p_t or p_t <= 0:
                                last_pib_y = max(y for y in pib.get(cod, {}) if pib[cod][y] and y <= 2021)
                                p_t = pib[cod][last_pib_y] * 1.03 ** (t - last_pib_y)
                            if not q_t or q_t <= 0:
                                last_pop_y = max(y for y in pop.get(cod, {}) if pop[cod][y])
                                q_t = pop[cod][last_pop_y]

                            x_new = np.array([1, t, math.log(max(p_t, 1)), math.log(max(q_t, 1))])
                            pred = math.exp(min(model.predict(x_new)[0], 21))  # cap exp to avoid overflow
                            _store_proj(emp, cod, t, var, max(0, min(2_000_000_000, int(round(pred)))))
                        model_ok = True
                        stats["ols_full"] += 1
            except Exception:
                pass

            if not model_ok:
                # Trend-only: ln(Y) = β₀ + β₁·t
                try:
                    X = sm.add_constant(years_arr)
                    model = sm.OLS(ln_y, X).fit()
                    for t in proj_years:
                        x_new = np.array([1, t])
                        pred = math.exp(min(model.predict(x_new)[0], 21))
                        _store_proj(emp, cod, t, var, max(0, min(2_000_000_000, int(round(pred)))))
                    stats["ols_trend"] += 1
                except Exception:
                    # Ultimate fallback
                    last_y, last_v = valid[-1]
                    g = uf_median_growth.get(uf, {}).get(var, 0.02)
                    for t in proj_years:
                        proj_val = last_v * (1 + g) ** (t - last_y)
                        _store_proj(emp, cod, t, var, max(0, int(round(proj_val))))
                    stats["fallback_uf"] += 1

        # ── Salários: tendência linear dos últimos 5 anos ──
        sal_pairs = [(y, anos_data[y].get("salario_medio_reais"))
                     for y in hist_years if anos_data[y].get("salario_medio_reais")]
        sal_valid = [(y, v) for y, v in sal_pairs if v and v > 0]

        if len(sal_valid) >= 2:
            # Usar últimos 5 anos
            recent = sal_valid[-5:]
            xs = np.array([p[0] for p in recent])
            ys = np.array([p[1] for p in recent])
            coeffs = np.polyfit(xs, ys, 1)
            for t in proj_years:
                sal_t = max(0, coeffs[0] * t + coeffs[1])
                _store_proj(emp, cod, t, "salario_medio_reais", round(sal_t, 2))
        elif sal_valid:
            # Constante
            for t in proj_years:
                _store_proj(emp, cod, t, "salario_medio_reais", sal_valid[-1][1])

        # ── Montar registros finais ──
        for t in proj_years:
            if t not in emp[cod]:
                continue

            d = emp[cod][t]
            sal_r = d.get("salario_medio_reais")
            pessoal_ass = d.get("pessoal_assalariado")

            sal_sm = None
            if sal_r and t in SALARIO_MINIMO:
                sal_sm = round(sal_r / SALARIO_MINIMO[t], 2)

            sal_mil = None
            if pessoal_ass and sal_r and pessoal_ass > 0:
                sal_mil = int(round(pessoal_ass * sal_r * 12 / 1000))

            records.append({
                "codigo_ibge": cod,
                "codigo_ibge_uf": uf,
                "ano": t,
                "unidades_locais": d.get("unidades_locais"),
                "empresas": d.get("empresas"),
                "pessoal_ocupado_total": d.get("pessoal_ocupado_total"),
                "pessoal_assalariado": d.get("pessoal_assalariado"),
                "salarios_mil_reais": sal_mil,
                "salario_medio_sm": sal_sm,
                "salario_medio_reais": sal_r,
                "fonte": "Projeção econométrica",
            })

    print(f"\n  Modelos usados:")
    print(f"    OLS completo (R²≥0.3): {stats['ols_full']}")
    print(f"    OLS trend-only: {stats['ols_trend']}")
    print(f"    Fallback estadual: {stats['fallback_uf']}")
    print(f"  Total registros: {len(records)}")

    # Verificação
    df = pd.DataFrame(records)
    if not df.empty:
        print(f"\n  Estatísticas pessoal_ocupado_total:")
        col = "pessoal_ocupado_total"
        valid = df[col].dropna()
        print(f"    min={valid.min()}  max={valid.max()}  "
              f"mean={valid.mean():.0f}  median={valid.median():.0f}")

        por_ano = df.groupby("ano").size()
        for ano, n in por_ano.items():
            print(f"    {ano}: {n} municípios")

    # ── 4. Inserir ──────────────────────────────────────────────────────
    print("\n4. Inserindo no banco...")
    inserted = batch_insert(sb, "emprego_municipios", records,
                            on_conflict="codigo_ibge,ano")
    print(f"\n  Total inseridos/atualizados: {inserted}")

    # Resumo
    print("\n" + "=" * 70)
    print("RESUMO")
    print("=" * 70)
    for ano in range(2010, 2026):
        r = sb.table("emprego_municipios").select("id", count="exact").eq("ano", ano).execute()
        c = r.count or 0
        if c > 0:
            print(f"  {ano}: {c} municípios")
    print("=" * 70)
    print("Projeção emprego concluída!")


def _store_proj(emp, cod, t, var, val):
    """Armazena valor projetado no dicionário emprego."""
    if t not in emp[cod]:
        emp[cod][t] = {}
    emp[cod][t][var] = val


if __name__ == "__main__":
    main()
