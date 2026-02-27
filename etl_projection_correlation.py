#!/usr/bin/env python3
"""
Análise de correlação entre variáveis municipais (ano 2010).
READ-ONLY - não escreve no banco.
Objetivo: informar seleção de covariáveis para modelos de projeção.
"""

import pandas as pd
import numpy as np
from etl_projection_utils import setup_supabase, fetch_all

def main():
    sb = setup_supabase()

    print("=" * 70)
    print("ANÁLISE DE CORRELAÇÃO - VARIÁVEIS MUNICIPAIS (2010)")
    print("=" * 70)

    # ── Carregar dados do ano 2010 ──────────────────────────────────────
    tables = {
        "idh_municipios": "codigo_ibge,idhm,idhm_educacao,idhm_longevidade,idhm_renda,indice_gini",
        "pib_municipios": "codigo_ibge,pib_total,pib_per_capita,renda_per_capita",
        "pop_municipios": "codigo_ibge,populacao,populacao_urbana,populacao_rural",
        "mortalidade_municipios": "codigo_ibge,obitos_total,taxa_mortalidade,taxa_natalidade,nascimentos",
        "saneamento_municipios": "codigo_ibge,pct_agua_rede_geral,pct_esgoto_adequado,pct_lixo_coletado",
        "emprego_municipios": "codigo_ibge,pessoal_ocupado_total,empresas,salario_medio_reais",
        "financas_municipios": "codigo_ibge,receita_total,receita_corrente",
    }

    frames = {}
    for table, select in tables.items():
        print(f"  Carregando {table}...")
        rows = fetch_all(sb, table, select=select,
                         filters=[("ano", "eq", 2010)])
        df = pd.DataFrame(rows)
        if "codigo_ibge" in df.columns:
            df = df.set_index("codigo_ibge")
        frames[table] = df
        print(f"    {len(df)} registros")

    # ── Financas: tentar 2014 se 2010 vazio ─────────────────────────────
    if frames["financas_municipios"].empty:
        print("  financas_municipios sem dados 2010, tentando 2014...")
        rows = fetch_all(sb, "financas_municipios",
                         select="codigo_ibge,receita_total,receita_corrente",
                         filters=[("ano", "eq", 2014)])
        df = pd.DataFrame(rows)
        if "codigo_ibge" in df.columns:
            df = df.set_index("codigo_ibge")
        frames["financas_municipios"] = df
        print(f"    {len(df)} registros (2014)")

    # ── JOIN por codigo_ibge ────────────────────────────────────────────
    print("\nJoining por codigo_ibge...")
    merged = None
    for name, df in frames.items():
        if df.empty:
            print(f"  SKIP {name} (vazio)")
            continue
        # Remover colunas duplicadas
        df = df[~df.index.duplicated(keep="first")]
        if merged is None:
            merged = df
        else:
            merged = merged.join(df, how="inner", rsuffix=f"_{name}")

    print(f"  Interseção: {len(merged)} municípios")

    # Converter tudo para numérico
    for col in merged.columns:
        merged[col] = pd.to_numeric(merged[col], errors="coerce")

    # Dropar colunas com >50% NaN
    thresh = len(merged) * 0.5
    merged = merged.dropna(axis=1, thresh=int(thresh))
    print(f"  Colunas válidas: {list(merged.columns)}")

    # ── Matriz de correlação ────────────────────────────────────────────
    corr = merged.corr(method="pearson")

    print("\n" + "=" * 70)
    print("MATRIZ DE CORRELAÇÃO COMPLETA")
    print("=" * 70)
    pd.set_option("display.max_columns", 30)
    pd.set_option("display.width", 200)
    pd.set_option("display.float_format", lambda x: f"{x:.3f}")
    print(corr.to_string())

    # ── Top correlações por variável-alvo ───────────────────────────────
    targets = {
        "IDH": ["idhm", "idhm_educacao", "idhm_longevidade", "idhm_renda"],
        "Emprego": ["pessoal_ocupado_total", "empresas", "salario_medio_reais"],
        "Saneamento": ["pct_agua_rede_geral", "pct_esgoto_adequado", "pct_lixo_coletado"],
    }

    print("\n" + "=" * 70)
    print("TOP CORRELAÇÕES POR VARIÁVEL-ALVO")
    print("=" * 70)

    for grupo, vars_alvo in targets.items():
        print(f"\n── {grupo} ──")
        for var in vars_alvo:
            if var not in corr.columns:
                print(f"  {var}: NÃO DISPONÍVEL")
                continue
            series = corr[var].drop(var, errors="ignore").abs().sort_values(ascending=False)
            print(f"\n  {var}:")
            for other, val in series.head(8).items():
                sign = "+" if corr[var][other] > 0 else "-"
                print(f"    {sign}{val:.3f}  {other}")

    # ── Recomendações ───────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("RECOMENDAÇÕES DE COVARIÁVEIS")
    print("=" * 70)
    print("""
IDH Projeção:
  - idhm_renda: usar renda_per_capita (fórmula PNUD direta) ou pib_per_capita
  - idhm_longevidade: usar taxa_mortalidade (Preston curve inversa)
  - idhm_educacao: crescimento geométrico (sem covariável forte disponível anual)
  - gini: tendência linear com amortecimento dos 3 pontos históricos

Emprego Projeção:
  - pessoal_ocupado_total: regressão log-linear com pib_total + populacao
  - empresas: regressão log-linear com pib_total + populacao
  - salarios: tendência linear

Saneamento Projeção:
  - pct_agua/esgoto/lixo: curva logística (S) com tiers de riqueza (pib_per_capita)
  - domicilios: escalar proporcionalmente à população
""")


if __name__ == "__main__":
    main()
