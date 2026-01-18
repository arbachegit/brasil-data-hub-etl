#!/usr/bin/env python3
"""
ETL para carregar dados geograficos do IBGE no Supabase
Fonte: API IBGE Localidades
"""
import os
import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

IBGE_API = "https://servicodados.ibge.gov.br/api/v1/localidades"

def carregar_regioes():
    """Carrega as 5 regioes do Brasil"""
    print("Carregando regioes...")
    resp = requests.get(f"{IBGE_API}/regioes")
    regioes = resp.json()
    
    for r in regioes:
        data = {
            "codigo_ibge": str(r["id"]),
            "nome": r["nome"],
            "sigla": r["sigla"]
        }
        try:
            supabase.table("geo_regioes").upsert(data, on_conflict="codigo_ibge").execute()
            print(f"  Regiao: {r['nome']}")
        except Exception as e:
            print(f"  Erro regiao {r['nome']}: {e}")
    
    print(f"Total: {len(regioes)} regioes")

def carregar_estados():
    """Carrega os 27 estados (26 + DF)"""
    print("\nCarregando estados...")
    resp = requests.get(f"{IBGE_API}/estados")
    estados = resp.json()
    
    regioes_db = supabase.table("geo_regioes").select("id, codigo_ibge").execute()
    regiao_map = {r["codigo_ibge"]: r["id"] for r in regioes_db.data}
    
    for e in estados:
        regiao_id = regiao_map.get(str(e["regiao"]["id"]))
        data = {
            "codigo_ibge": str(e["id"]),
            "nome": e["nome"],
            "sigla": e["sigla"],
            "regiao_id": regiao_id
        }
        try:
            supabase.table("geo_estados").upsert(data, on_conflict="codigo_ibge").execute()
            print(f"  Estado: {e['sigla']} - {e['nome']}")
        except Exception as e2:
            print(f"  Erro estado {e['nome']}: {e2}")
    
    print(f"Total: {len(estados)} estados")

def carregar_municipios():
    """Carrega todos os municipios (~5570)"""
    print("\nCarregando municipios por estado...")
    
    estados_db = supabase.table("geo_estados").select("id, codigo_ibge, sigla").execute()
    
    total_municipios = 0
    
    for estado in estados_db.data:
        uf_code = estado["codigo_ibge"]
        estado_id = estado["id"]
        uf_sigla = estado["sigla"]
        
        resp = requests.get(f"{IBGE_API}/estados/{uf_code}/municipios")
        municipios = resp.json()
        
        batch_size = 100
        for i in range(0, len(municipios), batch_size):
            batch = municipios[i:i+batch_size]
            records = []
            
            for m in batch:
                records.append({
                    "codigo_ibge": str(m["id"]),
                    "nome": m["nome"],
                    "estado_id": estado_id
                })
            
            try:
                supabase.table("geo_municipios").upsert(records, on_conflict="codigo_ibge").execute()
            except Exception as e:
                print(f"  Erro {uf_sigla} batch {i}: {e}")
        
        total_municipios += len(municipios)
        print(f"  {uf_sigla}: {len(municipios)} municipios")
    
    print(f"Total: {total_municipios} municipios")

if __name__ == "__main__":
    print("="*50)
    print("ETL IBGE - Base Geografica")
    print("="*50)
    
    carregar_regioes()
    carregar_estados()
    carregar_municipios()
    
    print("\n" + "="*50)
    print("ETL IBGE concluido!")
    print("="*50)
