#!/usr/bin/env python3
"""
ETL Saneamento Municipal - Censo 2010
Fonte: IBGE/SIDRA - Censo Demográfico 2010
  - Tabela 3217: Domicílios por forma de abastecimento de água (c61)
  - Tabela 3218: Domicílios por tipo de esgotamento sanitário (c62 via v/all)
  - Tabela 1395: Domicílios com destino do lixo (c67)
Busca UF por UF.
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
UFS = [11,12,13,14,15,16,17,21,22,23,24,25,26,27,28,29,31,32,33,35,41,42,43,50,51,52,53]

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS saneamento_municipios (
    id BIGSERIAL PRIMARY KEY,
    codigo_ibge INTEGER NOT NULL,
    codigo_ibge_uf INTEGER NOT NULL,
    ano INTEGER NOT NULL DEFAULT 2010,
    domicilios_total INTEGER,
    agua_rede_geral INTEGER,
    agua_poco_nascente INTEGER,
    agua_outra INTEGER,
    pct_agua_rede_geral NUMERIC(5,2),
    esgoto_rede_geral INTEGER,
    esgoto_fossa_septica INTEGER,
    esgoto_outro INTEGER,
    esgoto_sem INTEGER,
    pct_esgoto_adequado NUMERIC(5,2),
    lixo_coletado INTEGER,
    lixo_outro INTEGER,
    pct_lixo_coletado NUMERIC(5,2),
    fonte VARCHAR(60) DEFAULT 'IBGE/Censo 2010',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(codigo_ibge, ano)
);
"""


def fetch_sidra(url):
    """Fetch com retry."""
    for tentativa in range(3):
        try:
            resp = requests.get(url, timeout=120)
            if resp.status_code == 200:
                data = resp.json()
                registros = data[1:] if len(data) > 1 else []
                return [r for r in registros if r.get("V") and r["V"] not in ("...", "-", "")]
            if tentativa < 2:
                print(f"    HTTP {resp.status_code} - retry {tentativa+1}")
        except Exception as e:
            if tentativa < 2:
                print(f"    Erro: {e} - retry {tentativa+1}")
        time.sleep(3 * (tentativa + 1))
    return []


def fetch_all_ufs(table, var, classificacao=""):
    """Busca Census 2010 por UF."""
    todos = []
    for uf in UFS:
        url = f"{SIDRA_BASE}/t/{table}/n6/in%20n3%20{uf}/v/{var}/p/2010"
        if classificacao:
            url += f"/{classificacao}"
        todos.extend(fetch_sidra(url))
        time.sleep(0.5)
    return todos


def main():
    print("=" * 60)
    print("ETL SANEAMENTO MUNICIPAL - CENSO 2010")
    print("=" * 60)

    # Verificar tabela
    try:
        r = supabase.table("saneamento_municipios").select("id", count="exact").execute()
        if r.count and r.count > 0:
            print(f"Tabela já tem {r.count} registros. Pulando.")
            return
    except Exception as e:
        if "does not exist" in str(e).lower() or "schema" in str(e).lower():
            print("*** TABELA NÃO EXISTE! Execute o SQL no Supabase SQL Editor ***")
            print(CREATE_TABLE_SQL)
            return

    municipios = {}

    # 1. Água - Tabela 3217, v/96 (domicílios), c61 por categoria
    # c61/0=Total, c61/92853=Rede geral, c61/10971=Poço/nascente propriedade
    print("\n1. Buscando dados de ÁGUA (27 UFs)...")

    # Total domicílios
    raw = fetch_all_ufs("3217", "96", "c61/0")
    for r in raw:
        cod = r["D1C"]
        try:
            municipios[cod] = {"domicilios_total": int(r["V"])}
        except (ValueError, TypeError):
            pass
    print(f"  -> {len(municipios)} municípios com domicílios")

    # Rede geral
    time.sleep(2)
    raw = fetch_all_ufs("3217", "96", "c61/92853")
    for r in raw:
        cod = r["D1C"]
        if cod in municipios:
            try:
                municipios[cod]["agua_rede_geral"] = int(r["V"])
            except (ValueError, TypeError):
                pass
    print(f"  -> água rede geral OK")

    # Poço/nascente
    time.sleep(2)
    raw = fetch_all_ufs("3217", "96", "c61/10971")
    for r in raw:
        cod = r["D1C"]
        if cod in municipios:
            try:
                municipios[cod]["agua_poco_nascente"] = int(r["V"])
            except (ValueError, TypeError):
                pass
    print(f"  -> água poço/nascente OK")

    # 2. Esgoto - Tabela 1395, c67 (que funciona nesta tabela)
    # Preciso descobrir os códigos. Vou buscar com v/96 e c67/all
    print("\n2. Buscando dados de ESGOTO (27 UFs)...")
    time.sleep(2)

    # Fetch esgoto by category for all UFs
    # c67 codes: need to discover them first from one municipality
    resp = requests.get(f"{SIDRA_BASE}/t/1395/n6/3550308/v/96/p/2010/c67/all", timeout=60)
    if resp.status_code == 200:
        test_data = resp.json()[1:]
        # Map c67 codes
        c67_map = {}
        for r in test_data:
            c67_map[r.get("D6C", "")] = r.get("D6N", "")
        print(f"  Categorias c67: {c67_map}")

        # Fetch rede geral/pluvial (esgoto adequado)
        # Usually code for "Rede geral de esgoto ou pluvial" varies
        # Let me fetch all categories and process
        for uf in UFS:
            url = f"{SIDRA_BASE}/t/1395/n6/in%20n3%20{uf}/v/96/p/2010/c67/all"
            registros = fetch_sidra(url)
            for r in registros:
                cod = r["D1C"]
                cat_name = r.get("D6N", "")
                try:
                    val = int(r["V"])
                except (ValueError, TypeError):
                    continue
                if cod not in municipios:
                    municipios[cod] = {"domicilios_total": 0}

                if "Rede geral" in cat_name and "esgoto" in cat_name.lower():
                    municipios[cod]["esgoto_rede_geral"] = val
                elif "Fossa" in cat_name and "séptica" in cat_name.lower():
                    municipios[cod]["esgoto_fossa_septica"] = val
                elif "Não tinham" in cat_name or "sem" in cat_name.lower():
                    municipios[cod]["esgoto_sem"] = val
            time.sleep(0.5)
        print(f"  -> esgoto OK")
    else:
        print(f"  -> esgoto FALHOU (HTTP {resp.status_code})")

    # 3. Lixo - Buscar via tabela 3217 não tem lixo. Usar 1395 com outra classificação
    # Vou tentar c68 (destino do lixo) ou procurar nos dados
    print("\n3. Buscando dados de LIXO (27 UFs)...")
    time.sleep(2)

    # Test which classification has lixo data in table 1395
    lixo_found = False
    for c_num in [68, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90]:
        resp = requests.get(f"{SIDRA_BASE}/t/1395/n6/3550308/v/96/p/2010/c{c_num}/all", timeout=15)
        if resp.status_code == 200:
            try:
                test = resp.json()
                if len(test) > 1:
                    dim_names = [v for k, v in test[0].items() if "lixo" in v.lower() or "Destino" in v]
                    if dim_names or any("lixo" in str(r).lower() for r in test[1:3]):
                        print(f"  -> c{c_num} tem dados de lixo!")
                        # Fetch all UFs
                        for uf in UFS:
                            url = f"{SIDRA_BASE}/t/1395/n6/in%20n3%20{uf}/v/96/p/2010/c{c_num}/all"
                            registros = fetch_sidra(url)
                            for r in registros:
                                cod = r["D1C"]
                                # Find the lixo dimension
                                for di in range(4, 10):
                                    dn_key = f"D{di}N"
                                    dc_key = f"D{di}C"
                                    val_name = r.get(dn_key, "")
                                    if "Coletado" in val_name:
                                        try:
                                            if cod in municipios:
                                                municipios[cod]["lixo_coletado"] = municipios[cod].get("lixo_coletado", 0) + int(r["V"])
                                        except (ValueError, TypeError):
                                            pass
                                        break
                            time.sleep(0.5)
                        lixo_found = True
                        break
            except:
                pass

    if not lixo_found:
        print("  -> Lixo não encontrado via classificações disponíveis")

    # 4. Calcular percentuais e montar registros
    print("\n4. Montando registros...")
    registros = []
    for cod, dados in municipios.items():
        dom_total = dados.get("domicilios_total", 0)
        if dom_total <= 0:
            continue

        agua_rede = dados.get("agua_rede_geral", 0)
        agua_poco = dados.get("agua_poco_nascente", 0)
        agua_outra = dom_total - agua_rede - agua_poco if dom_total else None

        esgoto_rede = dados.get("esgoto_rede_geral")
        esgoto_fossa = dados.get("esgoto_fossa_septica")
        esgoto_sem = dados.get("esgoto_sem")
        esgoto_outro = None
        if esgoto_rede is not None and esgoto_fossa is not None:
            esgoto_adequado = (esgoto_rede or 0) + (esgoto_fossa or 0)
            esgoto_outro = dom_total - esgoto_adequado - (esgoto_sem or 0)
            if esgoto_outro < 0:
                esgoto_outro = 0

        lixo_col = dados.get("lixo_coletado")
        lixo_outro = dom_total - lixo_col if lixo_col is not None else None

        registros.append({
            "codigo_ibge": int(cod),
            "codigo_ibge_uf": int(str(cod)[:2]),
            "ano": 2010,
            "domicilios_total": dom_total,
            "agua_rede_geral": agua_rede,
            "agua_poco_nascente": agua_poco,
            "agua_outra": agua_outra if agua_outra and agua_outra >= 0 else 0,
            "pct_agua_rede_geral": round(agua_rede / dom_total * 100, 2) if agua_rede else 0,
            "esgoto_rede_geral": esgoto_rede,
            "esgoto_fossa_septica": esgoto_fossa,
            "esgoto_outro": esgoto_outro,
            "esgoto_sem": esgoto_sem,
            "pct_esgoto_adequado": round(((esgoto_rede or 0) + (esgoto_fossa or 0)) / dom_total * 100, 2) if esgoto_rede is not None else None,
            "lixo_coletado": lixo_col,
            "lixo_outro": lixo_outro if lixo_outro and lixo_outro >= 0 else None,
            "pct_lixo_coletado": round(lixo_col / dom_total * 100, 2) if lixo_col else None,
            "fonte": "IBGE/Censo 2010",
        })

    # 5. Inserir
    print(f"  Inserindo {len(registros)} registros...")
    batch_size = 500
    inseridos = 0
    for i in range(0, len(registros), batch_size):
        batch = registros[i:i + batch_size]
        try:
            supabase.table("saneamento_municipios").insert(batch).execute()
            inseridos += len(batch)
        except Exception as e:
            print(f"  Erro batch {i}: {e}")
        time.sleep(0.3)

    print(f"\n{'='*60}")
    print(f"TOTAL: {inseridos} registros inseridos")
    print("=" * 60)
    print("ETL concluído!")


if __name__ == "__main__":
    main()
