#!/usr/bin/env python3
"""
ETL TSE - Dimensão Políticos
Fonte: Dados Abertos TSE (https://dadosabertos.tse.jus.br)

Baixa dados de candidatos de TODAS as eleições disponíveis (1982-2024)
e cria tabela dim_politicos com políticos únicos deduplicados por CPF.

Uso:
    python etl_tse_politicos.py

Pré-requisito: criar a tabela dim_politicos no Supabase usando sql_dim_politicos.sql
"""

import os
import io
import csv
import json
import zipfile
import requests
import time
import sys
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# URL base do CDN do TSE
TSE_CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"

# Todos os anos de eleição disponíveis no TSE Dados Abertos
ANOS_ELEICOES = [
    1982, 1986, 1989, 1990, 1994, 1996, 1998, 2000,
    2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016,
    2018, 2020, 2022, 2024
]

# Mapeamento de colunas do CSV para nosso modelo
# Cada chave pode ter múltiplos nomes possíveis (variações entre anos)
COLUNAS_MAPA = {
    'cpf': ['NR_CPF_CANDIDATO'],
    'sq_candidato': ['SQ_CANDIDATO'],
    'titulo_eleitoral': ['NR_TITULO_ELEITORAL_CANDIDATO', 'NR_TITULO_ELEITORAL'],
    'nome': ['NM_CANDIDATO'],
    'nome_urna': ['NM_URNA_CANDIDATO'],
    'nome_social': ['NM_SOCIAL_CANDIDATO'],
    'data_nascimento': ['DT_NASCIMENTO'],
    'genero': ['DS_GENERO'],
    'cor_raca': ['DS_COR_RACA'],
    'grau_instrucao': ['DS_GRAU_INSTRUCAO'],
    'estado_civil': ['DS_ESTADO_CIVIL'],
    'nacionalidade': ['DS_NACIONALIDADE'],
    'uf_nascimento': ['SG_UF_NASCIMENTO'],
    'municipio_nascimento': ['NM_MUNICIPIO_NASCIMENTO'],
    'ocupacao': ['DS_OCUPACAO'],
    'email': ['NM_EMAIL'],
    'partido_sigla': ['SG_PARTIDO'],
    'partido_numero': ['NR_PARTIDO'],
    'cargo': ['DS_CARGO'],
    'uf': ['SG_UF'],
    'situacao_candidatura': ['DS_SITUACAO_CANDIDATURA'],
    'situacao_turno': ['DS_SIT_TOT_TURNO'],
    'reeleicao': ['ST_REELEICAO'],
    'ano_eleicao': ['ANO_ELEICAO'],
}

# Valores que representam NULL nos CSVs do TSE
VALORES_NULOS = {'#NULO#', '#NE#', '#NULO', '#NE', '', '-1', '-3'}


def normalizar_cpf(cpf_raw, sq_candidato=None):
    """Remove formatação e valida CPF. Usa SQ_CANDIDATO como fallback."""
    if cpf_raw:
        cpf = cpf_raw.replace('.', '').replace('-', '').replace(' ', '').strip()
        # CPF válido: 11 dígitos, não todos iguais, não zeros
        if cpf.isdigit() and len(cpf) >= 10:
            # Padronizar para 11 dígitos (CPFs antigos podem ter 10)
            cpf = cpf.zfill(11)
            if cpf != '00000000000' and len(set(cpf)) > 1:
                return cpf
    # Fallback: usar SQ_CANDIDATO como identificador sintético
    if sq_candidato:
        sq = sq_candidato.strip()
        if sq and sq not in VALORES_NULOS and len(sq) > 3:
            return f"SQ{sq}"
    return None


def converter_data(data_str):
    """Converte DD/MM/YYYY para YYYY-MM-DD, validando a data"""
    if not data_str or data_str in VALORES_NULOS:
        return None
    try:
        parts = data_str.strip().split('/')
        if len(parts) == 3:
            dia, mes, ano = parts
            if len(ano) == 4 and dia.isdigit() and mes.isdigit() and ano.isdigit():
                d, m, a = int(dia), int(mes), int(ano)
                # Rejeitar datas inválidas
                if m < 1 or m > 12 or d < 1 or d > 31 or a < 1900 or a > 2025:
                    return None
                return f"{ano}-{mes.zfill(2)}-{dia.zfill(2)}"
    except Exception:
        pass
    return None


def limpar_valor(valor):
    """Limpa um valor do CSV: remove aspas, espaços, trata nulos"""
    if valor is None:
        return None
    val = valor.strip().strip('"')
    if val in VALORES_NULOS:
        return None
    return val


def encontrar_coluna(headers, nomes_possiveis):
    """Encontra o nome de coluna correto dentre as variações possíveis"""
    for nome in nomes_possiveis:
        if nome in headers:
            return nome
    return None


def download_com_retry(url, max_retries=3, timeout=600):
    """Faz download com retry e backoff exponencial"""
    for tentativa in range(max_retries):
        try:
            resp = requests.get(url, timeout=timeout, stream=True)
            if resp.status_code == 200:
                return resp.content
            elif resp.status_code == 404:
                print(f"    404 - Arquivo não encontrado")
                return None
            else:
                print(f"    Status {resp.status_code}, tentativa {tentativa + 1}/{max_retries}")
        except requests.exceptions.Timeout:
            print(f"    Timeout, tentativa {tentativa + 1}/{max_retries}")
        except requests.exceptions.ConnectionError:
            print(f"    Erro de conexão, tentativa {tentativa + 1}/{max_retries}")
        except Exception as e:
            print(f"    Erro: {e}, tentativa {tentativa + 1}/{max_retries}")

        if tentativa < max_retries - 1:
            wait = 2 ** (tentativa + 1) * 5
            print(f"    Aguardando {wait}s antes de retry...")
            time.sleep(wait)

    return None


def processar_ano(ano, politicos):
    """
    Baixa o ZIP de candidatos de um ano e atualiza o dict de políticos.
    Processa de forma incremental para economia de memória.
    """
    url = f"{TSE_CDN}/consulta_cand_{ano}.zip"
    print(f"\n  Baixando {url}...")

    content = download_com_retry(url)
    if content is None:
        print(f"  FALHA ao baixar dados de {ano}")
        return 0

    tamanho_mb = len(content) / (1024 * 1024)
    print(f"  Download OK: {tamanho_mb:.1f} MB")

    try:
        z = zipfile.ZipFile(io.BytesIO(content))
    except zipfile.BadZipFile:
        print(f"  ERRO: ZIP corrompido para {ano}")
        return 0

    del content  # Libera memória do download

    arquivos = z.namelist()

    # Encontrar arquivos de candidatos (CSV ou TXT)
    cand_files = [
        f for f in arquivos
        if ('consulta_cand' in f.lower() or 'candidato' in f.lower())
        and (f.endswith('.csv') or f.endswith('.txt'))
        and not f.startswith('__MACOSX')
        and not f.startswith('.')
    ]

    if not cand_files:
        print(f"  AVISO: Nenhum arquivo de candidatos encontrado para {ano}")
        print(f"  Arquivos no ZIP: {arquivos[:10]}")
        z.close()
        return 0

    # Preferir o arquivo BR (agregado nacional)
    br_files = [f for f in cand_files if '_BR.' in f or '_BRASIL.' in f.upper()]
    if br_files:
        cand_files = br_files
        print(f"  Usando arquivo nacional: {cand_files[0]}")
    else:
        print(f"  Processando {len(cand_files)} arquivos estaduais")

    total_registros = 0
    cpfs_invalidos = 0

    for filename in cand_files:
        try:
            with z.open(filename) as f:
                raw = f.read()

            # Tentar decodificar com latin1 (padrão TSE)
            try:
                text = raw.decode('latin1')
            except UnicodeDecodeError:
                try:
                    text = raw.decode('utf-8')
                except UnicodeDecodeError:
                    text = raw.decode('cp1252')

            del raw  # Libera bytes

            # Remover BOM se presente
            if text.startswith('\ufeff'):
                text = text[1:]

            reader = csv.DictReader(io.StringIO(text), delimiter=';', quotechar='"')

            if not reader.fieldnames:
                continue

            # Limpar headers
            headers_limpos = [
                h.strip().strip('"').replace('\ufeff', '')
                for h in reader.fieldnames
            ]
            reader.fieldnames = headers_limpos

            # Construir mapa de colunas para este arquivo
            col_map = {}
            for target, possiveis in COLUNAS_MAPA.items():
                found = encontrar_coluna(headers_limpos, possiveis)
                if found:
                    col_map[target] = found

            if 'nome' not in col_map:
                print(f"    AVISO: Coluna de nome não encontrada em {filename}")
                continue

            for row in reader:
                # Extrair campos usando o mapa de colunas
                reg = {}
                for target, source_col in col_map.items():
                    reg[target] = limpar_valor(row.get(source_col))

                nome = reg.get('nome')
                if not nome:
                    continue

                # Normalizar CPF (com fallback para SQ_CANDIDATO)
                cpf = normalizar_cpf(reg.get('cpf'), reg.get('sq_candidato'))
                if not cpf:
                    cpfs_invalidos += 1
                    continue

                ano_eleicao = int(reg.get('ano_eleicao') or ano)
                total_registros += 1

                # Atualizar dicionário de políticos
                if cpf in politicos:
                    pol = politicos[cpf]
                    existing_ano = pol.get('ultimo_ano_eleicao', 0)

                    pol['total_candidaturas'] = pol.get('total_candidaturas', 1) + 1

                    # Acumular anos, cargos e partidos
                    pol['_anos'].add(ano_eleicao)
                    if reg.get('cargo'):
                        pol['_cargos'].add(reg['cargo'])
                    if reg.get('partido_sigla'):
                        pol['_partidos'].add(reg['partido_sigla'])

                    # Atualizar com dados mais recentes
                    if ano_eleicao >= existing_ano:
                        for campo in ['nome', 'nome_urna', 'nome_social',
                                      'genero', 'cor_raca', 'grau_instrucao',
                                      'estado_civil', 'nacionalidade', 'ocupacao',
                                      'email']:
                            if reg.get(campo):
                                pol[campo] = reg[campo]

                        pol['ultimo_ano_eleicao'] = ano_eleicao
                        pol['ultimo_partido_sigla'] = reg.get('partido_sigla')
                        pol['ultimo_partido_numero'] = reg.get('partido_numero')
                        pol['ultimo_cargo'] = reg.get('cargo')
                        pol['ultimo_uf'] = reg.get('uf')
                        pol['ultima_situacao_candidatura'] = reg.get('situacao_candidatura')
                        pol['ultimo_resultado'] = reg.get('situacao_turno')
                        pol['reeleicao'] = reg.get('reeleicao')

                        # Dados pessoais: manter se já preenchidos ou atualizar
                        if reg.get('data_nascimento') and not pol.get('data_nascimento'):
                            pol['data_nascimento'] = reg['data_nascimento']
                        if reg.get('titulo_eleitoral') and not pol.get('titulo_eleitoral'):
                            pol['titulo_eleitoral'] = reg['titulo_eleitoral']
                        if reg.get('uf_nascimento') and not pol.get('uf_nascimento'):
                            pol['uf_nascimento'] = reg['uf_nascimento']
                        if reg.get('municipio_nascimento') and not pol.get('municipio_nascimento'):
                            pol['municipio_nascimento'] = reg['municipio_nascimento']
                else:
                    # Novo político
                    politicos[cpf] = {
                        'cpf': cpf,
                        'titulo_eleitoral': reg.get('titulo_eleitoral'),
                        'nome': nome,
                        'nome_urna': reg.get('nome_urna'),
                        'nome_social': reg.get('nome_social'),
                        'data_nascimento': reg.get('data_nascimento'),
                        'genero': reg.get('genero'),
                        'cor_raca': reg.get('cor_raca'),
                        'grau_instrucao': reg.get('grau_instrucao'),
                        'estado_civil': reg.get('estado_civil'),
                        'nacionalidade': reg.get('nacionalidade'),
                        'uf_nascimento': reg.get('uf_nascimento'),
                        'municipio_nascimento': reg.get('municipio_nascimento'),
                        'ocupacao': reg.get('ocupacao'),
                        'email': reg.get('email'),
                        'ultimo_partido_sigla': reg.get('partido_sigla'),
                        'ultimo_partido_numero': reg.get('partido_numero'),
                        'ultimo_ano_eleicao': ano_eleicao,
                        'ultimo_cargo': reg.get('cargo'),
                        'ultimo_uf': reg.get('uf'),
                        'ultima_situacao_candidatura': reg.get('situacao_candidatura'),
                        'ultimo_resultado': reg.get('situacao_turno'),
                        'reeleicao': reg.get('reeleicao'),
                        'total_candidaturas': 1,
                        '_anos': {ano_eleicao},
                        '_cargos': {reg['cargo']} if reg.get('cargo') else set(),
                        '_partidos': {reg['partido_sigla']} if reg.get('partido_sigla') else set(),
                    }

            del text  # Libera string

        except Exception as e:
            print(f"    Erro em {filename}: {e}")

    z.close()

    print(f"  {ano}: {total_registros} candidaturas processadas, "
          f"{cpfs_invalidos} CPFs inválidos ignorados, "
          f"{len(politicos)} políticos únicos acumulados")

    return total_registros


def detectar_colunas_tabela():
    """Detecta quais colunas existem na tabela dim_politicos"""
    print("Detectando colunas da tabela dim_politicos...")
    try:
        r = supabase.table("dim_politicos").select("*").limit(1).execute()
        if r.data:
            colunas = set(r.data[0].keys())
        else:
            # Tabela vazia - inserir registro de teste para descobrir colunas
            try:
                supabase.table("dim_politicos").insert(
                    {"cpf": "__TESTE__", "nome_completo": "TESTE"}
                ).execute()
                r2 = supabase.table("dim_politicos").select("*").limit(1).execute()
                colunas = set(r2.data[0].keys()) if r2.data else set()
                supabase.table("dim_politicos").delete().eq("cpf", "__TESTE__").execute()
            except Exception:
                try:
                    supabase.table("dim_politicos").insert(
                        {"cpf": "__TESTE__", "nome_completo": "TESTE"}
                    ).execute()
                    r2 = supabase.table("dim_politicos").select("*").limit(1).execute()
                    colunas = set(r2.data[0].keys()) if r2.data else set()
                    supabase.table("dim_politicos").delete().eq("cpf", "__TESTE__").execute()
                except Exception:
                    colunas = set()

        # Remover colunas auto-gerenciadas
        colunas.discard('id')
        colunas.discard('criado_em')
        colunas.discard('atualizado_em')
        colunas.discard('created_at')
        colunas.discard('updated_at')

        print(f"  Colunas encontradas: {sorted(colunas)}")
        return colunas
    except Exception as e:
        print(f"  Erro ao detectar colunas: {e}")
        return set()


def finalizar_registros(politicos, colunas_tabela):
    """Prepara registros para inserção no Supabase, mapeando para as colunas existentes"""
    print(f"\nFinalizando {len(politicos)} registros...")

    # Mapeamento: nome interno -> nome na tabela
    # Suporta ambos schemas (original e novo)
    MAPA_COLUNAS = {
        'cpf': 'cpf',
        'nome': 'nome_completo',
        'nome_urna': 'nome_urna',
        'nome_social': 'nome_social',
        'data_nascimento': 'data_nascimento',
        'genero': 'sexo',
        'titulo_eleitoral': 'titulo_eleitoral',
        'cor_raca': 'cor_raca',
        'grau_instrucao': 'grau_instrucao',
        'estado_civil': 'estado_civil',
        'nacionalidade': 'nacionalidade',
        'uf_nascimento': 'uf_nascimento',
        'municipio_nascimento': 'municipio_nascimento',
        'ocupacao': 'ocupacao',
        'email': 'email',
        'ultimo_partido_sigla': 'ultimo_partido_sigla',
        'ultimo_partido_numero': 'ultimo_partido_numero',
        'ultimo_ano_eleicao': 'ultimo_ano_eleicao',
        'ultimo_cargo': 'ultimo_cargo',
        'ultimo_uf': 'ultimo_uf',
        'ultima_situacao_candidatura': 'ultima_situacao_candidatura',
        'ultimo_resultado': 'ultimo_resultado',
        'reeleicao': 'reeleicao',
        'total_candidaturas': 'total_candidaturas',
        'anos_eleicoes': 'anos_eleicoes',
        'cargos_disputados': 'cargos_disputados',
        'partidos_historico': 'partidos_historico',
    }

    # Filtrar apenas colunas que existem na tabela
    colunas_validas = {
        k: v for k, v in MAPA_COLUNAS.items()
        if v in colunas_tabela
    }

    colunas_ignoradas = {
        k: v for k, v in MAPA_COLUNAS.items()
        if v not in colunas_tabela and k != 'cpf'
    }

    if colunas_ignoradas:
        print(f"\n  AVISO: {len(colunas_ignoradas)} campos sem coluna correspondente na tabela:")
        for k, v in sorted(colunas_ignoradas.items()):
            print(f"    {k} -> {v} (não existe)")
        print("\n  Execute o seguinte SQL no Supabase SQL Editor para adicionar as colunas:")
        print("  " + "-" * 50)
        alter_parts = []
        type_map = {
            'ultimo_partido_numero': 'INTEGER',
            'ultimo_ano_eleicao': 'INTEGER',
            'total_candidaturas': 'INTEGER DEFAULT 1',
        }
        for k, v in sorted(colunas_ignoradas.items()):
            col_type = type_map.get(k, 'TEXT')
            alter_parts.append(f"ADD COLUMN IF NOT EXISTS {v} {col_type}")
        sql = "ALTER TABLE dim_politicos\n  " + ",\n  ".join(alter_parts) + ";"
        print(f"  {sql}")
        print("  " + "-" * 50)

    print(f"\n  Colunas que serão inseridas: {sorted(colunas_validas.values())}")

    registros = []
    for cpf, pol in politicos.items():
        # Converter sets para strings ordenadas
        anos = sorted(pol.pop('_anos', set()))
        cargos = sorted(pol.pop('_cargos', set()))
        partidos = sorted(pol.pop('_partidos', set()))

        pol['anos_eleicoes'] = ','.join(str(a) for a in anos)
        pol['cargos_disputados'] = ','.join(cargos)
        pol['partidos_historico'] = ','.join(partidos)

        # Converter data de nascimento para formato ISO
        pol['data_nascimento'] = converter_data(pol.get('data_nascimento'))

        # Converter partido_numero para inteiro
        try:
            pn = pol.get('ultimo_partido_numero')
            pol['ultimo_partido_numero'] = int(pn) if pn else None
        except (ValueError, TypeError):
            pol['ultimo_partido_numero'] = None

        # Truncar email se muito longo
        if pol.get('email') and len(pol['email']) > 255:
            pol['email'] = pol['email'][:255]

        # Converter gênero para formato da coluna (CHAR(1) se 'sexo')
        if 'sexo' in colunas_tabela and pol.get('genero'):
            genero_map = {
                'MASCULINO': 'M', 'FEMININO': 'F',
                'NÃO INFORMADO': None, 'NAO INFORMADO': None,
            }
            pol['genero'] = genero_map.get(pol['genero'], pol['genero'][:1] if pol['genero'] else None)

        # Montar registro com apenas colunas válidas
        record = {}
        for internal_key, table_col in colunas_validas.items():
            val = pol.get(internal_key)
            if val is not None:
                record[table_col] = val
        # CPF é sempre obrigatório - validar tamanho para a coluna
        # CPFs reais têm 11 dígitos; SQ_CANDIDATO começa com "SQ" e pode ser > 11
        if len(cpf) > 11:
            # Coluna cpf pode ser VARCHAR(11) - pular registros com ID sintético
            continue
        record['cpf'] = cpf

        registros.append(record)

    return registros


def inserir_politicos(registros):
    """Insere políticos no Supabase em batches"""
    total = len(registros)
    print(f"\nInserindo {total:,} políticos no Supabase...")

    batch_size = 1000
    inseridos = 0
    erros = 0
    ultimo_progresso = 0

    for i in range(0, total, batch_size):
        batch = registros[i:i + batch_size]
        try:
            supabase.table("dim_politicos").upsert(
                batch, on_conflict="cpf"
            ).execute()
            inseridos += len(batch)
        except Exception as e:
            # Dividir batch em mini-batches para isolar o erro
            mini_size = 50
            for j in range(0, len(batch), mini_size):
                mini = batch[j:j + mini_size]
                try:
                    supabase.table("dim_politicos").upsert(
                        mini, on_conflict="cpf"
                    ).execute()
                    inseridos += len(mini)
                except Exception as e2:
                    # Tentar um por um
                    for record in mini:
                        try:
                            supabase.table("dim_politicos").upsert(
                                record, on_conflict="cpf"
                            ).execute()
                            inseridos += 1
                        except Exception as e3:
                            erros += 1
                            if erros <= 5:
                                print(f"\n  Erro (CPF {record.get('cpf', '?')}): {e3}")

        # Progresso a cada 5%
        progresso = min(inseridos + erros, total)
        pct = (progresso / total) * 100
        if pct >= ultimo_progresso + 5 or progresso == total:
            print(f"  Progresso: {progresso:,}/{total:,} ({pct:.1f}%) - "
                  f"OK: {inseridos:,}, Erros: {erros:,}")
            ultimo_progresso = int(pct // 5) * 5

    print(f"\n  Concluído: {inseridos:,} inseridos, {erros:,} erros")
    return inseridos


CACHE_FILE = os.path.join(os.path.dirname(__file__), ".cache_politicos.json")


def salvar_cache(politicos, total_candidaturas):
    """Salva dados processados em cache local para evitar re-download"""
    print(f"\nSalvando cache em {CACHE_FILE}...")
    # Converter sets para listas para serialização JSON
    data = {}
    for cpf, pol in politicos.items():
        p = dict(pol)
        p['_anos'] = sorted(p.get('_anos', set()))
        p['_cargos'] = sorted(p.get('_cargos', set()))
        p['_partidos'] = sorted(p.get('_partidos', set()))
        data[cpf] = p
    with open(CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump({'total_candidaturas': total_candidaturas, 'politicos': data}, f)
    print(f"  Cache salvo: {len(data):,} políticos")


def carregar_cache():
    """Carrega cache se existir"""
    if not os.path.exists(CACHE_FILE):
        return None, 0
    print(f"Cache encontrado: {CACHE_FILE}")
    with open(CACHE_FILE, 'r', encoding='utf-8') as f:
        raw = json.load(f)
    politicos = {}
    for cpf, pol in raw['politicos'].items():
        pol['_anos'] = set(pol.get('_anos', []))
        pol['_cargos'] = set(pol.get('_cargos', []))
        pol['_partidos'] = set(pol.get('_partidos', []))
        politicos[cpf] = pol
    total = raw.get('total_candidaturas', 0)
    print(f"  {len(politicos):,} políticos carregados do cache")
    return politicos, total


if __name__ == "__main__":
    print("=" * 60)
    print("ETL TSE - Dimensão Políticos")
    print("Fonte: Dados Abertos TSE")
    print(f"Eleições: {ANOS_ELEICOES[0]} a {ANOS_ELEICOES[-1]} "
          f"({len(ANOS_ELEICOES)} anos)")
    print("=" * 60)

    inicio = time.time()

    # Tentar carregar do cache primeiro
    politicos, total_candidaturas = carregar_cache()

    if politicos:
        print("Usando dados do cache (pular download)")
        anos_processados = len(ANOS_ELEICOES)
    else:
        # Dicionário para acumular políticos únicos (chave = CPF)
        politicos = {}
        total_candidaturas = 0
        anos_processados = 0

        # Processar do mais antigo ao mais recente
        # (dados mais recentes sobrescrevem os antigos)
        for ano in sorted(ANOS_ELEICOES):
            print(f"\n{'─' * 50}")
            print(f"ELEIÇÃO {ano}")
            print(f"{'─' * 50}")

            registros = processar_ano(ano, politicos)
            total_candidaturas += registros
            anos_processados += 1

            # Pausa entre downloads para não sobrecarregar o servidor do TSE
            if ano != ANOS_ELEICOES[-1]:
                time.sleep(2)

        # Salvar cache para re-execuções futuras
        salvar_cache(politicos, total_candidaturas)

    # Resumo pré-inserção
    print(f"\n{'=' * 60}")
    print("RESUMO DO PROCESSAMENTO")
    print(f"{'=' * 60}")
    print(f"Anos processados:     {anos_processados}/{len(ANOS_ELEICOES)}")
    print(f"Total candidaturas:   {total_candidaturas:,}")
    print(f"Políticos únicos:     {len(politicos):,}")

    # Estatísticas por gênero
    generos = {}
    for pol in politicos.values():
        g = pol.get('genero', 'NÃO INFORMADO') or 'NÃO INFORMADO'
        generos[g] = generos.get(g, 0) + 1
    print(f"\nPor gênero:")
    for g, count in sorted(generos.items(), key=lambda x: -x[1]):
        print(f"  {g}: {count:,}")

    # Estatísticas por último cargo
    cargos = {}
    for pol in politicos.values():
        c = pol.get('ultimo_cargo', 'NÃO INFORMADO') or 'NÃO INFORMADO'
        cargos[c] = cargos.get(c, 0) + 1
    print(f"\nPor último cargo disputado:")
    for c, count in sorted(cargos.items(), key=lambda x: -x[1])[:10]:
        print(f"  {c}: {count:,}")

    # Detectar colunas da tabela e preparar registros
    colunas_tabela = detectar_colunas_tabela()

    if politicos and colunas_tabela:
        registros = finalizar_registros(politicos, colunas_tabela)
        inseridos = inserir_politicos(registros)
    elif not colunas_tabela:
        print("ERRO: Não foi possível detectar as colunas da tabela dim_politicos")

    duracao = time.time() - inicio
    minutos = int(duracao // 60)
    segundos = int(duracao % 60)

    print(f"\n{'=' * 60}")
    print(f"ETL TSE concluído em {minutos}min {segundos}s")
    print(f"Total de políticos na dim_politicos: {len(politicos):,}")
    print(f"{'=' * 60}")
