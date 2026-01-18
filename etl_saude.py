#!/usr/bin/env python3
"""
ETL para carregar dados de saude do CNES/DATASUS no Supabase
Usa a API do CNES para estabelecimentos por municipio
"""
import os
import requests
import time
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# API CNES - Dados abertos
CNES_API = "https://apidadosabertos.saude.gov.br/cnes"

def carregar_estabelecimentos_amostra():
    """Carrega uma amostra de estabelecimentos de saude"""
    print("Carregando estabelecimentos de saude...")
    
    # Buscar municipios com codigo IBGE
    municipios = supabase.table("geo_municipios").select("id, codigo_ibge, nome").limit(50).execute()
    
    # Buscar tipos de estabelecimento do banco
    tipos_db = supabase.table("saude_tipo_estabelecimento").select("id, codigo").execute()
    tipo_map = {t["codigo"]: t["id"] for t in tipos_db.data}
    
    total_inseridos = 0
    
    for mun in municipios.data:
        try:
            # API CNES por municipio
            url = f"{CNES_API}/estabelecimentos?codigo_municipio={mun['codigo_ibge']}&limit=20"
            resp = requests.get(url, timeout=30)
            
            if resp.status_code != 200:
                continue
                
            dados = resp.json()
            estabs = dados.get("estabelecimentos", [])
            
            for est in estabs:
                tipo_id = tipo_map.get(str(est.get("codigo_tipo_unidade")))
                
                record = {
                    "codigo_cnes": str(est.get("codigo_cnes", "")),
                    "nome": est.get("nome_fantasia") or est.get("razao_social", ""),
                    "municipio_id": mun["id"],
                    "tipo_estabelecimento_id": tipo_id,
                    "endereco": est.get("endereco"),
                    "bairro": est.get("bairro"),
                    "cep": est.get("cep"),
                    "telefone": est.get("telefone"),
                    "latitude": est.get("latitude"),
                    "longitude": est.get("longitude"),
                    "atende_sus": est.get("atende_sus", False)
                }
                
                try:
                    supabase.table("saude_estabelecimentos").upsert(
                        record, on_conflict="codigo_cnes"
                    ).execute()
                    total_inseridos += 1
                except Exception as e:
                    pass
            
            print(f"  {mun['nome']}: {len(estabs)} estabelecimentos")
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"  Erro {mun['nome']}: {e}")
    
    print(f"\nTotal inseridos: {total_inseridos}")

def carregar_indicadores_saude():
    """Carrega indicadores de saude por municipio"""
    print("\nCarregando indicadores de saude por municipio...")
    
    # Contar estabelecimentos por municipio
    municipios = supabase.table("geo_municipios").select("id, codigo_ibge").execute()
    
    for mun in municipios.data[:100]:  # Amostra
        # Contar estabelecimentos deste municipio
        count = supabase.table("saude_estabelecimentos").select(
            "id", count="exact"
        ).eq("municipio_id", mun["id"]).execute()
        
        if count.count and count.count > 0:
            record = {
                "municipio_id": mun["id"],
                "ano_referencia": 2024,
                "total_estabelecimentos": count.count,
                "fonte": "CNES"
            }
            
            try:
                supabase.table("saude_indicadores_municipio").upsert(
                    record, on_conflict="municipio_id,ano_referencia"
                ).execute()
            except:
                pass
    
    print("Indicadores carregados!")

if __name__ == "__main__":
    print("="*50)
    print("ETL SAUDE - CNES/DATASUS")
    print("="*50)
    
    carregar_estabelecimentos_amostra()
    carregar_indicadores_saude()
    
    print("\n" + "="*50)
    print("ETL SAUDE concluido!")
    print("="*50)
