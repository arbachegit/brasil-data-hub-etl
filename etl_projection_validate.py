#!/usr/bin/env python3
"""
Validação das projeções econométricas.
READ-ONLY - apenas consulta e imprime relatório.
"""

import random
import pandas as pd
import numpy as np
from etl_projection_utils import setup_supabase, fetch_all, SALARIO_MINIMO


def validate_idh(sb):
    print("\n" + "=" * 70)
    print("VALIDAÇÃO IDH")
    print("=" * 70)

    rows = fetch_all(sb, "idh_municipios",
                     select="codigo_ibge,ano,idhm,idhm_educacao,idhm_longevidade,idhm_renda,indice_gini,classificacao,fonte",
                     filters=[("fonte", "eq", "Projeção econométrica")])
    if not rows:
        print("  NENHUM registro de projeção encontrado!")
        return

    df = pd.DataFrame(rows)
    print(f"  Total registros projetados: {len(df)}")

    # Bounds check
    issues = 0
    for col in ["idhm", "idhm_educacao", "idhm_longevidade", "idhm_renda"]:
        out = df[(df[col] < 0) | (df[col] > 1)]
        if len(out) > 0:
            print(f"  ERRO: {len(out)} registros com {col} fora de [0,1]")
            issues += len(out)

    gini_out = df[(df["indice_gini"].notna()) & ((df["indice_gini"] < 0.25) | (df["indice_gini"] > 0.80))]
    if len(gini_out) > 0:
        print(f"  AVISO: {len(gini_out)} registros com gini fora de [0.25, 0.80]")

    # NOT NULL check
    for col in ["idhm", "idhm_educacao", "idhm_longevidade", "idhm_renda", "classificacao"]:
        nulls = df[col].isna().sum()
        if nulls > 0:
            print(f"  AVISO: {nulls} NULLs em {col}")
            issues += nulls

    # Classificação coerente
    for _, row in df.sample(min(1000, len(df))).iterrows():
        idhm = row["idhm"]
        cls = row["classificacao"]
        expected = None
        if idhm >= 0.800:
            expected = "Muito Alto"
        elif idhm >= 0.700:
            expected = "Alto"
        elif idhm >= 0.600:
            expected = "Médio"
        elif idhm >= 0.500:
            expected = "Baixo"
        else:
            expected = "Muito Baixo"
        if cls != expected:
            print(f"  ERRO: cod={row['codigo_ibge']} ano={row['ano']} "
                  f"idhm={idhm:.4f} cls={cls} esperado={expected}")
            issues += 1

    # Stats por ano
    print(f"\n  Estatísticas por ano:")
    for ano in [2011, 2015, 2020, 2025]:
        sub = df[df["ano"] == ano]
        if sub.empty:
            continue
        print(f"    {ano}: n={len(sub)}  "
              f"idhm min={sub['idhm'].min():.4f} max={sub['idhm'].max():.4f} "
              f"mean={sub['idhm'].mean():.4f}")

    if issues == 0:
        print("\n  OK: Nenhum problema encontrado!")
    else:
        print(f"\n  TOTAL ISSUES: {issues}")


def validate_emprego(sb):
    print("\n" + "=" * 70)
    print("VALIDAÇÃO EMPREGO")
    print("=" * 70)

    rows = fetch_all(sb, "emprego_municipios",
                     select="codigo_ibge,ano,pessoal_ocupado_total,empresas,salario_medio_sm,salario_medio_reais,fonte",
                     filters=[("fonte", "eq", "Projeção econométrica")])
    if not rows:
        print("  NENHUM registro de projeção encontrado!")
        return

    df = pd.DataFrame(rows)
    print(f"  Total registros projetados: {len(df)}")

    issues = 0

    # Valores positivos
    for col in ["pessoal_ocupado_total", "empresas"]:
        neg = df[(df[col].notna()) & (df[col] < 0)]
        if len(neg) > 0:
            print(f"  ERRO: {len(neg)} registros com {col} negativo")
            issues += len(neg)

    # Salário médio SM entre 0.5 e 50
    sal = df[(df["salario_medio_sm"].notna())]
    if not sal.empty:
        out = sal[(sal["salario_medio_sm"] < 0.5) | (sal["salario_medio_sm"] > 50)]
        if len(out) > 0:
            print(f"  AVISO: {len(out)} registros com salario_medio_sm fora de [0.5, 50]")

    # Stats por ano
    print(f"\n  Estatísticas por ano:")
    for ano in [2022, 2023, 2024, 2025]:
        sub = df[df["ano"] == ano]
        if sub.empty:
            continue
        ocu = sub["pessoal_ocupado_total"].dropna()
        print(f"    {ano}: n={len(sub)}  "
              f"ocupados min={ocu.min():.0f} max={ocu.max():.0f} "
              f"mean={ocu.mean():.0f}")

    if issues == 0:
        print("\n  OK: Nenhum problema encontrado!")
    else:
        print(f"\n  TOTAL ISSUES: {issues}")


def validate_saneamento(sb):
    print("\n" + "=" * 70)
    print("VALIDAÇÃO SANEAMENTO")
    print("=" * 70)

    rows = fetch_all(sb, "saneamento_municipios",
                     select="codigo_ibge,ano,domicilios_total,pct_agua_rede_geral,pct_esgoto_adequado,pct_lixo_coletado,agua_rede_geral,lixo_coletado,fonte",
                     filters=[("fonte", "eq", "Projeção econométrica")])
    if not rows:
        print("  NENHUM registro de projeção encontrado!")
        return

    df = pd.DataFrame(rows)
    print(f"  Total registros projetados: {len(df)}")

    issues = 0

    # Percentuais entre 0 e 100
    for col in ["pct_agua_rede_geral", "pct_esgoto_adequado", "pct_lixo_coletado"]:
        vals = df[col].dropna()
        out = vals[(vals < 0) | (vals > 100)]
        if len(out) > 0:
            print(f"  ERRO: {len(out)} registros com {col} fora de [0, 100]")
            issues += len(out)

    # Contagens positivas
    for col in ["domicilios_total", "agua_rede_geral", "lixo_coletado"]:
        vals = df[col].dropna()
        neg = vals[vals < 0]
        if len(neg) > 0:
            print(f"  ERRO: {len(neg)} registros com {col} negativo")
            issues += len(neg)

    # Stats por ano
    print(f"\n  Estatísticas por ano:")
    for ano in [2011, 2015, 2020, 2025]:
        sub = df[df["ano"] == ano]
        if sub.empty:
            continue
        print(f"    {ano}: n={len(sub)}  "
              f"esgoto mean={sub['pct_esgoto_adequado'].mean():.1f}%  "
              f"agua mean={sub['pct_agua_rede_geral'].mean():.1f}%  "
              f"lixo mean={sub['pct_lixo_coletado'].mean():.1f}%")

    if issues == 0:
        print("\n  OK: Nenhum problema encontrado!")
    else:
        print(f"\n  TOTAL ISSUES: {issues}")


def spot_check(sb):
    print("\n" + "=" * 70)
    print("SPOT CHECK - MUNICÍPIOS SELECIONADOS")
    print("=" * 70)

    # São Paulo, Brasília + 1 aleatório
    targets = [
        (3550308, "São Paulo"),
        (5300108, "Brasília"),
    ]

    # Buscar um município pequeno aleatório
    small = fetch_all(sb, "pop_municipios",
                      select="codigo_ibge,populacao",
                      filters=[("ano", "eq", 2010), ("populacao", "lt", 10000)])
    if small:
        pick = random.choice(small)
        targets.append((pick["codigo_ibge"], f"Município {pick['codigo_ibge']} (pop {pick['populacao']})"))

    for cod, nome in targets:
        print(f"\n── {nome} (cod={cod}) ──")

        # IDH
        idh_rows = fetch_all(sb, "idh_municipios",
                             select="ano,idhm,idhm_educacao,idhm_longevidade,idhm_renda,indice_gini,classificacao,fonte",
                             filters=[("codigo_ibge", "eq", cod)],
                             order="ano")
        if idh_rows:
            print("  IDH:")
            for r in idh_rows:
                src = "P" if "rojeção" in (r.get("fonte") or "") else "O"
                print(f"    {r['ano']} [{src}] IDHM={r['idhm']:.4f} "
                      f"E={r['idhm_educacao']:.4f} L={r['idhm_longevidade']:.4f} "
                      f"R={r['idhm_renda']:.4f} Gini={r.get('indice_gini','N/A')} "
                      f"({r['classificacao']})")

        # Emprego
        emp_rows = fetch_all(sb, "emprego_municipios",
                             select="ano,pessoal_ocupado_total,empresas,salario_medio_reais,fonte",
                             filters=[("codigo_ibge", "eq", cod)],
                             order="ano")
        if emp_rows:
            print("  Emprego:")
            for r in emp_rows[-8:]:  # últimos 8 anos
                src = "P" if "rojeção" in (r.get("fonte") or "") else "O"
                print(f"    {r['ano']} [{src}] ocupados={r.get('pessoal_ocupado_total','N/A')} "
                      f"empresas={r.get('empresas','N/A')} "
                      f"salário={r.get('salario_medio_reais','N/A')}")

        # Saneamento
        san_rows = fetch_all(sb, "saneamento_municipios",
                             select="ano,pct_agua_rede_geral,pct_esgoto_adequado,pct_lixo_coletado,domicilios_total,fonte",
                             filters=[("codigo_ibge", "eq", cod)],
                             order="ano")
        if san_rows:
            print("  Saneamento:")
            for r in [san_rows[0]] + san_rows[-4:]:  # base + últimos 4
                src = "P" if "rojeção" in (r.get("fonte") or "") else "O"
                print(f"    {r['ano']} [{src}] água={r.get('pct_agua_rede_geral','N/A')}% "
                      f"esgoto={r.get('pct_esgoto_adequado','N/A')}% "
                      f"lixo={r.get('pct_lixo_coletado','N/A')}% "
                      f"dom={r.get('domicilios_total','N/A')}")


def main():
    sb = setup_supabase()

    print("=" * 70)
    print("VALIDAÇÃO DAS PROJEÇÕES ECONOMÉTRICAS")
    print("=" * 70)

    validate_idh(sb)
    validate_emprego(sb)
    validate_saneamento(sb)
    spot_check(sb)

    print("\n" + "=" * 70)
    print("CONTAGENS FINAIS")
    print("=" * 70)
    for table in ["idh_municipios", "emprego_municipios", "saneamento_municipios"]:
        r = sb.table(table).select("id", count="exact").execute()
        proj = sb.table(table).select("id", count="exact").eq("fonte", "Projeção econométrica").execute()
        print(f"  {table}: {r.count or 0} total ({proj.count or 0} projetados)")

    print("\nValidação concluída!")


if __name__ == "__main__":
    main()
