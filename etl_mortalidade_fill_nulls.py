#!/usr/bin/env python3
"""
Inferência temporal para preencher NULLs na mortalidade_municipios.

Estratégias:
1. obitos_masculinos = obitos_total - obitos_femininos (e vice-versa)
2. taxa_mortalidade/natalidade: interpola população dos anos vizinhos
   na pop_municipios e calcula (obitos/pop)*1000
"""

import os
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all(table, select, filters=None):
    """Busca todos os registros com paginação."""
    all_data = []
    offset = 0
    while True:
        q = supabase.table(table).select(select)
        if filters:
            for k, v in filters.items():
                q = q.eq(k, v)
        resp = q.range(offset, offset + 999).execute()
        all_data.extend(resp.data)
        if len(resp.data) < 1000:
            break
        offset += 1000
    return all_data


def main():
    print("=" * 60)
    print("INFERÊNCIA TEMPORAL - MORTALIDADE MUNICIPIOS")
    print("=" * 60)

    # =========================================================
    # ETAPA 1: Preencher obitos_masculinos e obitos_femininos
    # =========================================================
    print("\n--- ETAPA 1: Inferir óbitos por sexo ---")

    # 1a. obitos_masculinos = obitos_total - obitos_femininos
    recs = fetch_all(
        "mortalidade_municipios",
        "id,codigo_ibge,ano,obitos_total,obitos_masculinos,obitos_femininos"
    )

    fill_masc = 0
    fill_fem = 0
    errors = 0

    for r in recs:
        update = {}
        ob_total = r.get("obitos_total")
        ob_masc = r.get("obitos_masculinos")
        ob_fem = r.get("obitos_femininos")

        if ob_total is not None:
            if ob_masc is None and ob_fem is not None:
                update["obitos_masculinos"] = ob_total - ob_fem
                fill_masc += 1
            elif ob_fem is None and ob_masc is not None:
                update["obitos_femininos"] = ob_total - ob_masc
                fill_fem += 1

        if update:
            try:
                supabase.table("mortalidade_municipios").update(
                    update
                ).eq("id", r["id"]).execute()
            except Exception as e:
                errors += 1
                if errors <= 3:
                    print(f"  Erro: {e}")

    print(f"  obitos_masculinos preenchidos: {fill_masc}")
    print(f"  obitos_femininos preenchidos:  {fill_fem}")
    print(f"  erros: {errors}")

    # =========================================================
    # ETAPA 2: Construir mapa de população por município/ano
    # =========================================================
    print("\n--- ETAPA 2: Carregando população para interpolação ---")

    pop_raw = fetch_all("pop_municipios", "codigo_ibge,ano,populacao")
    print(f"  {len(pop_raw)} registros de população carregados")

    # Mapa: {codigo_ibge: {ano: populacao}}
    pop_map = {}
    for r in pop_raw:
        cod = r["codigo_ibge"]
        ano = r["ano"]
        pop = r.get("populacao")
        if pop and pop > 0:
            if cod not in pop_map:
                pop_map[cod] = {}
            pop_map[cod][ano] = pop

    print(f"  {len(pop_map)} municípios com pelo menos 1 ano de população")

    # =========================================================
    # ETAPA 3: Interpolar população e calcular taxas
    # =========================================================
    print("\n--- ETAPA 3: Interpolação temporal de população ---")

    # Buscar registros com obitos_total mas sem taxa_mortalidade
    recs_sem_taxa = [
        r for r in recs
        if r.get("obitos_total") is not None and r.get("taxa_mortalidade") is None
    ]
    print(f"  {len(recs_sem_taxa)} registros com óbitos mas sem taxa_mortalidade")

    # Também buscar nascimentos para taxa_natalidade
    # Precisamos recarregar com nascimentos
    recs_full = fetch_all(
        "mortalidade_municipios",
        "id,codigo_ibge,ano,obitos_total,nascimentos,taxa_mortalidade,taxa_natalidade,crescimento_vegetativo"
    )
    recs_full_map = {r["id"]: r for r in recs_full}

    def interpolar_pop(codigo_ibge, ano_alvo):
        """Interpola população linear entre os 2 anos mais próximos disponíveis."""
        anos_disp = pop_map.get(codigo_ibge, {})
        if not anos_disp:
            return None

        # Se tem o ano exato
        if ano_alvo in anos_disp:
            return anos_disp[ano_alvo]

        # Encontrar ano anterior e posterior mais próximos
        anos = sorted(anos_disp.keys())
        ant = None
        pos = None
        for a in anos:
            if a <= ano_alvo:
                ant = a
            if a >= ano_alvo and pos is None:
                pos = a

        if ant is not None and pos is not None and ant != pos:
            # Interpolação linear
            pop_ant = anos_disp[ant]
            pop_pos = anos_disp[pos]
            frac = (ano_alvo - ant) / (pos - ant)
            return int(pop_ant + (pop_pos - pop_ant) * frac)
        elif ant is not None:
            # Só tem anterior - extrapolação com taxa do último período disponível
            # Buscar o ano anterior a 'ant' para calcular taxa
            antes_de_ant = [a for a in anos if a < ant]
            if antes_de_ant:
                a2 = antes_de_ant[-1]
                taxa_anual = (anos_disp[ant] / anos_disp[a2]) ** (1.0 / (ant - a2))
                return int(anos_disp[ant] * taxa_anual ** (ano_alvo - ant))
            return anos_disp[ant]  # Sem taxa, usa o valor mais próximo
        elif pos is not None:
            return anos_disp[pos]

        return None

    fill_taxa_mort = 0
    fill_taxa_nat = 0
    fill_cresc = 0
    errors2 = 0

    for r in recs_full:
        update = {}
        cod = r["codigo_ibge"]
        ano = r["ano"]
        ob_total = r.get("obitos_total")
        nasc = r.get("nascimentos")

        needs_taxa_mort = r.get("taxa_mortalidade") is None and ob_total is not None
        needs_taxa_nat = r.get("taxa_natalidade") is None and nasc is not None
        needs_cresc = r.get("crescimento_vegetativo") is None and ob_total is not None and nasc is not None

        if not needs_taxa_mort and not needs_taxa_nat and not needs_cresc:
            continue

        pop_interp = interpolar_pop(cod, ano)

        if pop_interp and pop_interp > 0:
            if needs_taxa_mort and ob_total is not None:
                update["taxa_mortalidade"] = round((ob_total / pop_interp) * 1000, 2)
                fill_taxa_mort += 1
            if needs_taxa_nat and nasc is not None:
                update["taxa_natalidade"] = round((nasc / pop_interp) * 1000, 2)
                fill_taxa_nat += 1

        if needs_cresc:
            update["crescimento_vegetativo"] = nasc - ob_total
            fill_cresc += 1

        if update:
            try:
                supabase.table("mortalidade_municipios").update(
                    update
                ).eq("id", r["id"]).execute()
            except Exception as e:
                errors2 += 1
                if errors2 <= 3:
                    print(f"  Erro: {e}")

        total_done = fill_taxa_mort + fill_taxa_nat + fill_cresc
        if total_done % 500 == 0 and total_done > 0:
            print(f"    {total_done} campos preenchidos...")

    print(f"\n  taxa_mortalidade preenchidos:      {fill_taxa_mort}")
    print(f"  taxa_natalidade preenchidos:       {fill_taxa_nat}")
    print(f"  crescimento_vegetativo preenchidos: {fill_cresc}")
    print(f"  erros: {errors2}")

    # =========================================================
    # RESUMO
    # =========================================================
    print(f"\n{'='*60}")
    print("RESUMO FINAL")
    print("=" * 60)
    print(f"  Etapa 1 - Óbitos por sexo:")
    print(f"    obitos_masculinos: +{fill_masc}")
    print(f"    obitos_femininos:  +{fill_fem}")
    print(f"  Etapa 2/3 - Taxas via interpolação temporal:")
    print(f"    taxa_mortalidade:      +{fill_taxa_mort}")
    print(f"    taxa_natalidade:       +{fill_taxa_nat}")
    print(f"    crescimento_vegetativo: +{fill_cresc}")

    # Verificar NULLs restantes
    print(f"\n  NULLs restantes (não calculáveis):")
    for campo in ["obitos_total", "obitos_masculinos", "taxa_mortalidade"]:
        all_r = fetch_all("mortalidade_municipios", "id")
        # Precisamos contar via API com filtro is.null
        # Fazemos uma query simples
        break

    # Amostra São Paulo
    r = supabase.table("mortalidade_municipios").select("*").eq(
        "codigo_ibge", 3550308
    ).order("ano").execute()
    if r.data:
        print(f"\n  Verificação - São Paulo (SP):")
        for rec in r.data[-3:]:
            print(f"    {rec['ano']}: óbitos={rec['obitos_total']} "
                  f"(M={rec.get('obitos_masculinos')}/F={rec.get('obitos_femininos')}) "
                  f"taxa_mort={rec.get('taxa_mortalidade')} "
                  f"nasc={rec.get('nascimentos')} taxa_nat={rec.get('taxa_natalidade')}")

    print(f"\n{'='*60}")
    print("Inferência concluída!")


if __name__ == "__main__":
    main()
