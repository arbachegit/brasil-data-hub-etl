#!/usr/bin/env python3
"""
ETL P2+P3+P4 - Preencher lacunas:
  P2: pop_estados 2010-2013 (~108 registros)
  P3: pib_estados 2010-2014 (~135 registros)
  P4: pib_brasil 2010-2014 (5 registros)
"""

import os
import time
import requests
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

SIDRA_BASE = "https://apisidra.ibge.gov.br/values"

# Mapeamento UF code -> estado_id, sigla, nome (do banco)
UF_MAP = {}


def load_uf_map():
    """Carrega mapeamento de UFs do banco."""
    global UF_MAP
    resp = supabase.table("pop_estados").select(
        "estado_id,codigo_ibge_uf,sigla,nome"
    ).eq("ano", 2015).execute()
    for r in resp.data:
        UF_MAP[str(r["codigo_ibge_uf"])] = {
            "estado_id": r["estado_id"],
            "codigo_ibge_uf": r["codigo_ibge_uf"],
            "sigla": r["sigla"],
            "nome": r["nome"],
        }
    print(f"  UF map carregado: {len(UF_MAP)} estados")


def fetch_sidra(url, descricao):
    """Fetch com retry da API SIDRA."""
    for tentativa in range(3):
        try:
            print(f"  Buscando {descricao}...")
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                return data[1:] if len(data) > 1 else []
            print(f"  HTTP {resp.status_code} - tentativa {tentativa+1}")
        except Exception as e:
            print(f"  Erro: {e} - tentativa {tentativa+1}")
        time.sleep(5 * (tentativa + 1))
    return []


def ano_ja_existe(tabela, ano, campo_ano="ano"):
    """Verifica se já existem dados para o ano na tabela."""
    resp = supabase.table(tabela).select("id", count="exact").eq(campo_ano, ano).execute()
    return resp.count and resp.count > 0


def inserir(tabela, registros, descricao):
    """Insere registros no Supabase."""
    print(f"  Inserindo {len(registros)} registros em {tabela}...")
    try:
        supabase.table(tabela).insert(registros).execute()
        print(f"  OK: {len(registros)} inseridos ({descricao})")
        return len(registros)
    except Exception as e:
        print(f"  Erro: {e}")
        # Tentar em batches menores
        inseridos = 0
        for i in range(0, len(registros), 50):
            batch = registros[i:i + 50]
            try:
                supabase.table(tabela).insert(batch).execute()
                inseridos += len(batch)
            except Exception as e2:
                print(f"  Erro batch {i}: {e2}")
        print(f"  Inseridos: {inseridos}/{len(registros)}")
        return inseridos


# =============================================================
# P2: POP_ESTADOS 2010-2013
# =============================================================
def p2_pop_estados():
    print("\n" + "=" * 60)
    print("P2: POP_ESTADOS 2010-2013")
    print("=" * 60)

    # --- Censo 2010 ---
    if ano_ja_existe("pop_estados", 2010):
        print("  2010 já existe, pulando.")
    else:
        # Pop total por sexo
        raw_total = fetch_sidra(
            f"{SIDRA_BASE}/t/202/n3/all/v/93/p/2010/c1/0/c2/0,4,5",
            "Censo 2010 pop por sexo"
        )
        # Pop urbana/rural
        time.sleep(2)
        raw_sit = fetch_sidra(
            f"{SIDRA_BASE}/t/202/n3/all/v/93/p/2010/c1/1,2/c2/0",
            "Censo 2010 urbana/rural"
        )

        dados = {}
        for r in raw_total:
            uf_code = r["D1C"]
            sexo = r.get("D5N", "")
            val = r["V"]
            if val in ("...", "-", "") or not val:
                continue
            if uf_code not in dados:
                dados[uf_code] = {}
            if sexo == "Total":
                dados[uf_code]["populacao"] = int(val)
            elif sexo == "Homens":
                dados[uf_code]["populacao_masculina"] = int(val)
            elif sexo == "Mulheres":
                dados[uf_code]["populacao_feminina"] = int(val)

        for r in raw_sit:
            uf_code = r["D1C"]
            sit = r.get("D4N", "")
            val = r["V"]
            if val in ("...", "-", "") or not val or uf_code not in dados:
                continue
            if sit == "Urbana":
                dados[uf_code]["populacao_urbana"] = int(val)
            elif sit == "Rural":
                dados[uf_code]["populacao_rural"] = int(val)

        registros = []
        for uf_code, d in dados.items():
            uf = UF_MAP.get(uf_code)
            if not uf:
                continue
            registros.append({
                "estado_id": uf["estado_id"],
                "codigo_ibge_uf": uf["codigo_ibge_uf"],
                "sigla": uf["sigla"],
                "nome": uf["nome"],
                "ano": 2010,
                "tipo": "censo",
                "populacao": d.get("populacao"),
                "populacao_masculina": d.get("populacao_masculina"),
                "populacao_feminina": d.get("populacao_feminina"),
                "populacao_urbana": d.get("populacao_urbana"),
                "populacao_rural": d.get("populacao_rural"),
                "fonte": "IBGE/Censo-2010",
            })
        inserir("pop_estados", registros, "Censo 2010")

    # --- Estimativas 2011-2013 ---
    # Buscar proporções do censo 2010 para derivar sexo/situação
    censo_props = {}
    resp = supabase.table("pop_estados").select("*").eq("ano", 2010).execute()
    for r in resp.data:
        pop = r["populacao"] or 1
        censo_props[r["codigo_ibge_uf"]] = {
            "pct_masc": (r.get("populacao_masculina") or 0) / pop,
            "pct_urb": (r.get("populacao_urbana") or 0) / pop,
        }

    for ano in [2011, 2012, 2013]:
        if ano_ja_existe("pop_estados", ano):
            print(f"  {ano} já existe, pulando.")
            continue

        time.sleep(2)
        raw = fetch_sidra(
            f"{SIDRA_BASE}/t/6579/n3/all/v/9324/p/{ano}",
            f"Estimativas {ano}"
        )

        registros = []
        for r in raw:
            uf_code = r["D1C"]
            val = r["V"]
            if val in ("...", "-", "") or not val:
                continue
            uf = UF_MAP.get(uf_code)
            if not uf:
                continue
            pop = int(val)
            props = censo_props.get(uf["codigo_ibge_uf"], {"pct_masc": 0.49, "pct_urb": 0.84})
            registros.append({
                "estado_id": uf["estado_id"],
                "codigo_ibge_uf": uf["codigo_ibge_uf"],
                "sigla": uf["sigla"],
                "nome": uf["nome"],
                "ano": ano,
                "tipo": "estimativa",
                "populacao": pop,
                "populacao_masculina": int(pop * props["pct_masc"]),
                "populacao_feminina": pop - int(pop * props["pct_masc"]),
                "populacao_urbana": int(pop * props["pct_urb"]),
                "populacao_rural": pop - int(pop * props["pct_urb"]),
                "fonte": f"IBGE/Estimativas-{ano}",
            })
        inserir("pop_estados", registros, f"Estimativas {ano}")


# =============================================================
# P3: PIB_ESTADOS 2010-2014
# =============================================================
def p3_pib_estados():
    print("\n" + "=" * 60)
    print("P3: PIB_ESTADOS 2010-2014")
    print("=" * 60)

    # Variáveis SIDRA t/5938:
    # 37=PIB total, 496=participação Brasil%
    # 516=agro%, 520=industria%, 6574=servicos%, 528=admin%
    vars_ids = "37,496,516,520,6574,528"

    for ano in [2010, 2011, 2012, 2013, 2014]:
        if ano_ja_existe("pib_estados", ano):
            print(f"  {ano} já existe, pulando.")
            continue

        time.sleep(2)
        raw = fetch_sidra(
            f"{SIDRA_BASE}/t/5938/n3/all/v/{vars_ids}/p/{ano}",
            f"PIB estados {ano}"
        )

        # Agrupar por UF
        dados = {}
        for r in raw:
            uf_code = r["D1C"]
            var_code = r["D2C"]
            val = r["V"]
            if val in ("...", "-", "") or not val:
                continue
            if uf_code not in dados:
                dados[uf_code] = {}
            dados[uf_code][var_code] = float(val)

        # Montar registros
        pib_list = []
        for uf_code, d in dados.items():
            uf = UF_MAP.get(uf_code)
            if not uf:
                continue
            pib_total = d.get("37", 0)  # em milhares de reais
            pib_list.append({
                "uf_code": uf_code,
                "uf": uf,
                "pib_nominal": pib_total,
                "participacao": d.get("496", 0),
                "agro_pct": d.get("516"),
                "ind_pct": d.get("520"),
                "serv_pct": d.get("6574"),
                "adm_pct": d.get("528"),
            })

        # Ranking
        pib_list.sort(key=lambda x: x["pib_nominal"], reverse=True)

        registros = []
        for rank, item in enumerate(pib_list, 1):
            uf = item["uf"]
            pib_mil = item["pib_nominal"]  # SIDRA retorna em milhares
            pib_bilhoes = round(pib_mil / 1_000_000, 2)
            registros.append({
                "codigo_ibge_uf": uf["codigo_ibge_uf"],
                "uf": uf["sigla"],
                "ano": ano,
                "pib_nominal": round(pib_mil, 2),
                "pib_bilhoes": pib_bilhoes,
                "participacao_nacional": item["participacao"],
                "ranking_nacional": rank,
                "pib_agropecuaria_pct": item.get("agro_pct"),
                "pib_industria_pct": item.get("ind_pct"),
                "pib_servicos_pct": item.get("serv_pct"),
                "pib_adm_publica_pct": item.get("adm_pct"),
                "fonte": "IBGE/SIDRA-5938",
            })
        inserir("pib_estados", registros, f"PIB estados {ano}")


# =============================================================
# P4: PIB_BRASIL 2010-2014
# =============================================================
def p4_pib_brasil():
    print("\n" + "=" * 60)
    print("P4: PIB_BRASIL 2010-2014")
    print("=" * 60)

    # Dados oficiais do PIB Brasil (IBGE/SCN)
    # Valores em bilhões de R$ correntes
    pib_dados = {
        2010: {"pib_trilhoes": 3.886, "variacao_anual": 7.5, "pib_nominal": 3886000.0},
        2011: {"pib_trilhoes": 4.376, "variacao_anual": 4.0, "pib_nominal": 4376000.0},
        2012: {"pib_trilhoes": 4.815, "variacao_anual": 1.9, "pib_nominal": 4815000.0},
        2013: {"pib_trilhoes": 5.332, "variacao_anual": 3.0, "pib_nominal": 5332000.0},
        2014: {"pib_trilhoes": 5.779, "variacao_anual": 0.5, "pib_nominal": 5779000.0},
    }

    registros = []
    for ano, d in pib_dados.items():
        if ano_ja_existe("pib_brasil", ano):
            print(f"  {ano} já existe, pulando.")
            continue
        registros.append({
            "ano": ano,
            "pib_nominal": d["pib_nominal"],
            "pib_trilhoes": d["pib_trilhoes"],
            "variacao_anual": d["variacao_anual"],
            "fonte": "IBGE/SCN",
        })

    if registros:
        inserir("pib_brasil", registros, "PIB Brasil 2010-2014")
    else:
        print("  Todos os anos já existem.")


# =============================================================
# MAIN
# =============================================================
def main():
    print("=" * 60)
    print("ETL P2+P3+P4 - Preenchimento de lacunas")
    print("=" * 60)

    load_uf_map()

    p2_pop_estados()
    p3_pib_estados()
    p4_pib_brasil()

    # Resumo
    print("\n" + "=" * 60)
    print("RESUMO FINAL")
    print("=" * 60)

    for tabela, anos in [
        ("pop_estados", range(2010, 2026)),
        ("pib_estados", range(2010, 2026)),
        ("pib_brasil", range(2010, 2026)),
    ]:
        counts = []
        for ano in anos:
            r = supabase.table(tabela).select("id", count="exact").eq("ano", ano).execute()
            c = r.count or 0
            if c > 0:
                counts.append(f"{ano}:{c}")
        print(f"  {tabela}: {', '.join(counts)}")

    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
