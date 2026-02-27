"""
Comprehensive NULL analysis across all key Supabase tables.
Checks data patterns, NULL distributions, and sample rows to guide ETL fixes.
"""
import os, json
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
sb = create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_KEY"])

def header(title):
    print(f"\n{'='*80}")
    print(f"  {title}")
    print(f"{'='*80}")

def show_rows(rows, label="Sample rows"):
    if not rows:
        print(f"  {label}: (no rows returned)")
        return
    print(f"  {label} ({len(rows)} rows):")
    for i, r in enumerate(rows):
        print(f"    [{i}] {json.dumps(r, ensure_ascii=False, default=str)}")

from collections import Counter

# ---------------------------------------------------------------------------
# 1. fato_votos_legislativos
# ---------------------------------------------------------------------------
header("1. fato_votos_legislativos")

res = sb.table("fato_votos_legislativos").select("*").limit(5).execute()
show_rows(res.data, "5 sample rows (all columns)")

res2 = sb.table("fato_votos_legislativos").select("votacao_id").limit(1000).execute()
distinct_ids = set(r["votacao_id"] for r in res2.data)
print(f"\n  Distinct votacao_ids in first 1000 rows: {len(distinct_ids)}")

numeric_ids = [v for v in distinct_ids if v and v.isdigit()]
uuid_like = [v for v in distinct_ids if v and len(v) > 20 and not v.isdigit()]
other_ids = [v for v in distinct_ids if v and not v.isdigit() and len(v) <= 20]
print(f"  Numeric (Camara-style): {len(numeric_ids)} examples: {list(numeric_ids)[:3]}")
print(f"  UUID/long (Senado-style): {len(uuid_like)} examples: {list(uuid_like)[:3]}")
print(f"  Other format: {len(other_ids)} examples: {list(other_ids)[:3]}")

# ---------------------------------------------------------------------------
# 2. fato_emendas_parlamentares
# ---------------------------------------------------------------------------
header("2. fato_emendas_parlamentares")

res = (sb.table("fato_emendas_parlamentares")
       .select("id,politico_id,autor,ano,tipo_emenda,valor_empenhado")
       .is_("politico_id", "null")
       .limit(10)
       .execute())
show_rows(res.data, "10 rows where politico_id IS NULL (showing autor)")

res_count = (sb.table("fato_emendas_parlamentares")
             .select("id", count="exact")
             .is_("politico_id", "null")
             .limit(1)
             .execute())
print(f"\n  Total rows with politico_id IS NULL: {res_count.count}")

res = (sb.table("fato_emendas_parlamentares")
       .select("id,codigo_ibge,localidade,autor,ano")
       .is_("codigo_ibge", "null")
       .limit(10)
       .execute())
show_rows(res.data, "10 rows where codigo_ibge IS NULL (showing localidade)")

res_count2 = (sb.table("fato_emendas_parlamentares")
              .select("id", count="exact")
              .is_("codigo_ibge", "null")
              .limit(1)
              .execute())
print(f"\n  Total rows with codigo_ibge IS NULL: {res_count2.count}")

# ---------------------------------------------------------------------------
# 3. pop_municipios
# ---------------------------------------------------------------------------
header("3. pop_municipios")

res = (sb.table("pop_municipios")
       .select("*")
       .is_("data_referencia", "null")
       .limit(5)
       .execute())
show_rows(res.data, "5 rows where data_referencia IS NULL")

res_all = (sb.table("pop_municipios")
           .select("ano")
           .is_("data_referencia", "null")
           .limit(1000)
           .execute())
ano_counts = Counter(r["ano"] for r in res_all.data)
print(f"\n  NULLs in data_referencia by ano (from up to 1000 rows):")
for ano in sorted(ano_counts.keys()):
    print(f"    {ano}: {ano_counts[ano]}")

res_count3 = (sb.table("pop_municipios")
              .select("id", count="exact")
              .is_("data_referencia", "null")
              .limit(1)
              .execute())
print(f"  Total rows with data_referencia IS NULL: {res_count3.count}")

# ---------------------------------------------------------------------------
# 4. pib_municipios
# ---------------------------------------------------------------------------
header("4. pib_municipios")

res = (sb.table("pib_municipios")
       .select("*")
       .is_("renda_fonte", "null")
       .limit(5)
       .execute())
show_rows(res.data, "5 rows where renda_fonte IS NULL")

res_count4 = (sb.table("pib_municipios")
              .select("id", count="exact")
              .is_("renda_fonte", "null")
              .limit(1)
              .execute())
print(f"\n  Total rows with renda_fonte IS NULL: {res_count4.count}")

res_fontes = (sb.table("pib_municipios")
              .select("renda_fonte")
              .not_.is_("renda_fonte", "null")
              .limit(1000)
              .execute())
distinct_fontes = set(r["renda_fonte"] for r in res_fontes.data)
print(f"  Distinct renda_fonte values (NOT NULL): {distinct_fontes}")

# ---------------------------------------------------------------------------
# 5. mortalidade_municipios
# ---------------------------------------------------------------------------
header("5. mortalidade_municipios")

res_mort = (sb.table("mortalidade_municipios")
            .select("ano")
            .is_("obitos_total", "null")
            .limit(1000)
            .execute())
ano_mort = Counter(r["ano"] for r in res_mort.data)
print(f"  NULLs in obitos_total by ano (from up to 1000 rows):")
for ano in sorted(ano_mort.keys()):
    print(f"    {ano}: {ano_mort[ano]}")

res_count5 = (sb.table("mortalidade_municipios")
              .select("id", count="exact")
              .is_("obitos_total", "null")
              .limit(1)
              .execute())
print(f"  Total rows with obitos_total IS NULL: {res_count5.count}")

res = (sb.table("mortalidade_municipios")
       .select("*")
       .is_("obitos_total", "null")
       .limit(5)
       .execute())
show_rows(res.data, "5 rows where obitos_total IS NULL")

# ---------------------------------------------------------------------------
# 6. financas_municipios
# ---------------------------------------------------------------------------
header("6. financas_municipios")

res = (sb.table("financas_municipios")
       .select("*")
       .is_("receita_capital", "null")
       .limit(5)
       .execute())
show_rows(res.data, "5 rows where receita_capital IS NULL")

res_count6 = (sb.table("financas_municipios")
              .select("id", count="exact")
              .is_("receita_capital", "null")
              .limit(1)
              .execute())
print(f"\n  Total rows with receita_capital IS NULL: {res_count6.count}")

# ---------------------------------------------------------------------------
# 7. saneamento_municipios
# ---------------------------------------------------------------------------
header("7. saneamento_municipios")

res = (sb.table("saneamento_municipios")
       .select("*")
       .is_("esgoto_rede_geral", "null")
       .limit(5)
       .execute())
show_rows(res.data, "5 rows where esgoto_rede_geral IS NULL")

res_count7 = (sb.table("saneamento_municipios")
              .select("id", count="exact")
              .is_("esgoto_rede_geral", "null")
              .limit(1)
              .execute())
print(f"\n  Total rows with esgoto_rede_geral IS NULL: {res_count7.count}")

res_check = (sb.table("saneamento_municipios")
             .select("esgoto_rede_geral,pct_esgoto_adequado,domicilios_total")
             .is_("esgoto_rede_geral", "null")
             .not_.is_("pct_esgoto_adequado", "null")
             .not_.is_("domicilios_total", "null")
             .limit(5)
             .execute())
show_rows(res_check.data, "Rows with esgoto NULL but pct_esgoto_adequado & domicilios_total NOT NULL")

res_count7b = (sb.table("saneamento_municipios")
               .select("id", count="exact")
               .is_("esgoto_rede_geral", "null")
               .not_.is_("pct_esgoto_adequado", "null")
               .not_.is_("domicilios_total", "null")
               .limit(1)
               .execute())
print(f"  Rows where calculation is feasible (esgoto NULL, pct+dom NOT NULL): {res_count7b.count}")

# ---------------------------------------------------------------------------
# 8. fato_politicos_mandatos
# ---------------------------------------------------------------------------
header("8. fato_politicos_mandatos")

res_mand = (sb.table("fato_politicos_mandatos")
            .select("cargo")
            .is_("votos_nominais", "null")
            .limit(1000)
            .execute())
cargo_counts = Counter(r["cargo"] for r in res_mand.data)
print(f"  votos_nominais IS NULL by cargo (from up to 1000 rows):")
for cargo in sorted(cargo_counts.keys(), key=lambda x: -cargo_counts[x]):
    print(f"    {cargo}: {cargo_counts[cargo]}")

res_count8 = (sb.table("fato_politicos_mandatos")
              .select("id", count="exact")
              .is_("votos_nominais", "null")
              .limit(1)
              .execute())
print(f"  Total rows with votos_nominais IS NULL: {res_count8.count}")

res_total = (sb.table("fato_politicos_mandatos")
             .select("cargo")
             .limit(1000)
             .execute())
total_cargo = Counter(r["cargo"] for r in res_total.data)
print(f"\n  Total rows by cargo (from first 1000 rows, for ratio context):")
for cargo in sorted(total_cargo.keys(), key=lambda x: -total_cargo[x]):
    print(f"    {cargo}: {total_cargo[cargo]}")

print(f"\n{'='*80}")
print("  ANALYSIS COMPLETE")
print(f"{'='*80}")
