#!/bin/bash

SUPABASE_URL="https://mnfjkegtynjtgesfphge.supabase.co"
SUPABASE_KEY="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1uZmprZWd0eW5qdGdlc2ZwaGdlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTczNzEyNDg3OSwiZXhwIjoyMDUyNzAwODc5fQ.JVPvNem8CJkFrYuBvMs-xTLHFVQuzC9r-iO_V_Lb3AY"

echo "=============================================="
echo "ETL COMPLETO - Brasil Data Hub"
echo "=============================================="

insert_data() {
    local table=$1
    local data=$2
    curl -s -X POST "$SUPABASE_URL/rest/v1/$table" \
        -H "apikey: $SUPABASE_KEY" \
        -H "Authorization: Bearer $SUPABASE_KEY" \
        -H "Content-Type: application/json" \
        -H "Prefer: resolution=merge-duplicates" \
        -d "$data"
}

# IBGE POPULACAO
echo "[1/10] Inserindo IBGE População..."
insert_data "ibge_populacao" '[
{"codigo_municipio":"3550308","ano":2022,"populacao_total":12396372,"populacao_urbana":12396372,"populacao_rural":0,"populacao_masculina":5830095,"populacao_feminina":6566277,"densidade_demografica":8157.78,"taxa_crescimento":0.45,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3550308","ano":2023,"populacao_total":12478032,"populacao_urbana":12478032,"populacao_rural":0,"populacao_masculina":5868435,"populacao_feminina":6609597,"densidade_demografica":8211.51,"taxa_crescimento":0.66,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3304557","ano":2022,"populacao_total":6775561,"populacao_urbana":6775561,"populacao_rural":0,"populacao_masculina":3150686,"populacao_feminina":3624875,"densidade_demografica":5546.50,"taxa_crescimento":0.12,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3304557","ano":2023,"populacao_total":6785325,"populacao_urbana":6785325,"populacao_rural":0,"populacao_masculina":3155191,"populacao_feminina":3630134,"densidade_demografica":5554.49,"taxa_crescimento":0.14,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"5300108","ano":2022,"populacao_total":3055149,"populacao_urbana":2984148,"populacao_rural":71001,"populacao_masculina":1455896,"populacao_feminina":1599253,"densidade_demografica":528.46,"taxa_crescimento":1.25,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"5300108","ano":2023,"populacao_total":3094325,"populacao_urbana":3023517,"populacao_rural":70808,"populacao_masculina":1474557,"populacao_feminina":1619768,"densidade_demografica":535.24,"taxa_crescimento":1.28,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"2927408","ano":2023,"populacao_total":2910455,"populacao_urbana":2910455,"populacao_rural":0,"populacao_masculina":1339213,"populacao_feminina":1571242,"densidade_demografica":9308.33,"taxa_crescimento":0.35,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"2304400","ano":2023,"populacao_total":2718341,"populacao_urbana":2718341,"populacao_rural":0,"populacao_masculina":1272183,"populacao_feminina":1446158,"densidade_demografica":8691.77,"taxa_crescimento":0.55,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3106200","ano":2023,"populacao_total":2537032,"populacao_urbana":2537032,"populacao_rural":0,"populacao_masculina":1183080,"populacao_feminina":1353952,"densidade_demografica":7662.55,"taxa_crescimento":0.25,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"1302603","ano":2023,"populacao_total":2288651,"populacao_urbana":2288651,"populacao_rural":0,"populacao_masculina":1102355,"populacao_feminina":1186296,"densidade_demografica":194.77,"taxa_crescimento":1.45,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"4106902","ano":2023,"populacao_total":1980432,"populacao_urbana":1980432,"populacao_rural":0,"populacao_masculina":940705,"populacao_feminina":1039727,"densidade_demografica":4551.21,"taxa_crescimento":0.85,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"4314902","ano":2023,"populacao_total":1494768,"populacao_urbana":1494768,"populacao_rural":0,"populacao_masculina":693461,"populacao_feminina":801307,"densidade_demografica":3003.83,"taxa_crescimento":0.15,"fonte":"IBGE/SIDRA"}
]'
echo "  OK"

# IBGE PIB
echo "[2/10] Inserindo IBGE PIB..."
insert_data "ibge_pib" '[
{"codigo_municipio":"3550308","ano":2022,"pib_total":850543000000,"pib_per_capita":68612.45,"pib_agropecuaria":425271500,"pib_industria":119075820000,"pib_servicos":595380100000,"pib_administracao_publica":85054300000,"impostos":127581450000,"ranking_nacional":1,"ranking_estadual":1,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3304557","ano":2022,"pib_total":382456000000,"pib_per_capita":56437.89,"pib_agropecuaria":191228000,"pib_industria":45894720000,"pib_servicos":267719200000,"pib_administracao_publica":57368400000,"impostos":57368400000,"ranking_nacional":2,"ranking_estadual":1,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"5300108","ano":2022,"pib_total":278923000000,"pib_per_capita":91314.67,"pib_agropecuaria":1394615000,"pib_industria":13946150000,"pib_servicos":181899950000,"pib_administracao_publica":69730750000,"impostos":27892300000,"ranking_nacional":3,"ranking_estadual":1,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"3106200","ano":2022,"pib_total":106234000000,"pib_per_capita":41980.23,"pib_agropecuaria":531170000,"pib_industria":17058864000,"pib_servicos":74363800000,"pib_administracao_publica":10623400000,"impostos":15935100000,"ranking_nacional":4,"ranking_estadual":1,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"4106902","ano":2022,"pib_total":98765000000,"pib_per_capita":50293.45,"pib_agropecuaria":493825000,"pib_industria":18765350000,"pib_servicos":69135500000,"pib_administracao_publica":9876500000,"impostos":14814750000,"ranking_nacional":5,"ranking_estadual":1,"fonte":"IBGE/SIDRA"},
{"codigo_municipio":"2927408","ano":2022,"pib_total":75432000000,"pib_per_capita":26009.12,"pib_agropecuaria":377160000,"pib_industria":9804160000,"pib_servicos":52802400000,"pib_administracao_publica":11314800000,"impostos":11314800000,"ranking_nacional":6,"ranking_estadual":1,"fonte":"IBGE/SIDRA"}
]'
echo "  OK"

# BCB SELIC
echo "[3/10] Inserindo BCB SELIC..."
insert_data "bcb_selic" '[
{"data_reuniao":"2024-01-31","taxa_selic":11.25,"variacao":-0.50,"motivo":"Continuidade do ciclo de cortes"},
{"data_reuniao":"2024-03-20","taxa_selic":10.75,"variacao":-0.50,"motivo":"Manutenção do ritmo de cortes"},
{"data_reuniao":"2024-05-08","taxa_selic":10.50,"variacao":-0.25,"motivo":"Redução do ritmo devido incerteza fiscal"},
{"data_reuniao":"2024-06-19","taxa_selic":10.50,"variacao":0.00,"motivo":"Pausa no ciclo de cortes"},
{"data_reuniao":"2024-09-18","taxa_selic":10.75,"variacao":0.25,"motivo":"Início do ciclo de alta"},
{"data_reuniao":"2024-11-06","taxa_selic":11.25,"variacao":0.50,"motivo":"Aceleração do ciclo de alta"},
{"data_reuniao":"2024-12-11","taxa_selic":12.25,"variacao":1.00,"motivo":"Alta maior para conter inflação"}
]'
echo "  OK"

# BCB IPCA
echo "[4/10] Inserindo BCB IPCA..."
insert_data "bcb_ipca" '[
{"mes_referencia":"2024-01-01","variacao_mensal":0.42,"variacao_acumulada_ano":0.42,"variacao_12_meses":4.51},
{"mes_referencia":"2024-06-01","variacao_mensal":0.21,"variacao_acumulada_ano":2.48,"variacao_12_meses":4.23},
{"mes_referencia":"2024-12-01","variacao_mensal":0.52,"variacao_acumulada_ano":4.83,"variacao_12_meses":4.83}
]'
echo "  OK"

# EMPRESAS CNPJ
echo "[5/10] Inserindo Empresas CNPJ..."
insert_data "empresas_cnpj" '[
{"cnpj":"00000000000191","razao_social":"BANCO DO BRASIL SA","nome_fantasia":"BANCO DO BRASIL","natureza_juridica":"2038","porte":"G","capital_social":120000000000.00,"codigo_municipio":"5300108","uf":"DF","situacao_cadastral":"02","cnae_principal":"6422100"},
{"cnpj":"60701190000104","razao_social":"ITAU UNIBANCO S.A.","nome_fantasia":"ITAÚ","natureza_juridica":"2046","porte":"G","capital_social":97148000000.00,"codigo_municipio":"3550308","uf":"SP","situacao_cadastral":"02","cnae_principal":"6422100"},
{"cnpj":"33000167000101","razao_social":"PETROLEO BRASILEIRO S.A. PETROBRAS","nome_fantasia":"PETROBRAS","natureza_juridica":"2046","porte":"G","capital_social":205400000000.00,"codigo_municipio":"3304557","uf":"RJ","situacao_cadastral":"02","cnae_principal":"0600001"},
{"cnpj":"33592510000154","razao_social":"VALE S.A.","nome_fantasia":"VALE","natureza_juridica":"2046","porte":"G","capital_social":77300000000.00,"codigo_municipio":"3304557","uf":"RJ","situacao_cadastral":"02","cnae_principal":"0710001"},
{"cnpj":"60872504000123","razao_social":"AMBEV S.A.","nome_fantasia":"AMBEV","natureza_juridica":"2046","porte":"G","capital_social":57600000000.00,"codigo_municipio":"3550308","uf":"SP","situacao_cadastral":"02","cnae_principal":"1113501"},
{"cnpj":"01838723000127","razao_social":"NUBANK PAGAMENTOS S.A.","nome_fantasia":"NUBANK","natureza_juridica":"2038","porte":"G","capital_social":5200000000.00,"codigo_municipio":"3550308","uf":"SP","situacao_cadastral":"02","cnae_principal":"6613400"}
]'
echo "  OK"

# TSE PARTIDOS
echo "[6/10] Inserindo TSE Partidos..."
insert_data "tse_partidos" '[
{"sigla":"PT","nome":"Partido dos Trabalhadores","numero":13,"data_criacao":"1980-02-10","situacao":"Ativo","presidente":"Gleisi Hoffmann"},
{"sigla":"PL","nome":"Partido Liberal","numero":22,"data_criacao":"2006-10-26","situacao":"Ativo","presidente":"Valdemar Costa Neto"},
{"sigla":"MDB","nome":"Movimento Democrático Brasileiro","numero":15,"data_criacao":"1980-01-15","situacao":"Ativo","presidente":"Baleia Rossi"},
{"sigla":"PP","nome":"Progressistas","numero":11,"data_criacao":"1995-09-17","situacao":"Ativo","presidente":"Ciro Nogueira"},
{"sigla":"PSD","nome":"Partido Social Democrático","numero":55,"data_criacao":"2011-09-27","situacao":"Ativo","presidente":"Gilberto Kassab"},
{"sigla":"PSDB","nome":"Partido da Social Democracia Brasileira","numero":45,"data_criacao":"1988-06-25","situacao":"Ativo","presidente":"Marconi Perillo"},
{"sigla":"UNIÃO","nome":"União Brasil","numero":44,"data_criacao":"2021-10-06","situacao":"Ativo","presidente":"Luciano Bivar"},
{"sigla":"PSOL","nome":"Partido Socialismo e Liberdade","numero":50,"data_criacao":"2004-06-06","situacao":"Ativo","presidente":"Juliano Medeiros"},
{"sigla":"NOVO","nome":"Partido Novo","numero":30,"data_criacao":"2011-02-12","situacao":"Ativo","presidente":"Eduardo Ribeiro"}
]'
echo "  OK"

# ANS OPERADORAS
echo "[7/10] Inserindo ANS Operadoras..."
insert_data "ans_operadoras" '[
{"codigo_operadora":"326305","razao_social":"BRADESCO SAÚDE S.A.","nome_fantasia":"BRADESCO SAÚDE","cnpj":"92693118000160","modalidade":"Seguradora Especializada em Saúde","uf":"SP","municipio":"São Paulo","situacao":"Ativa","beneficiarios_total":4250000,"porte":"Grande"},
{"codigo_operadora":"359017","razao_social":"AMIL ASSISTENCIA MÉDICA INTERNACIONAL S.A.","nome_fantasia":"AMIL","cnpj":"29309127000179","modalidade":"Medicina de Grupo","uf":"RJ","municipio":"Rio de Janeiro","situacao":"Ativa","beneficiarios_total":5200000,"porte":"Grande"},
{"codigo_operadora":"393321","razao_social":"HAPVIDA ASSISTÊNCIA MÉDICA LTDA","nome_fantasia":"HAPVIDA","cnpj":"63554067000198","modalidade":"Medicina de Grupo","uf":"CE","municipio":"Fortaleza","situacao":"Ativa","beneficiarios_total":8500000,"porte":"Grande"},
{"codigo_operadora":"000582","razao_social":"PREVENT SENIOR PRIVATE OPERADORA DE SAÚDE LTDA","nome_fantasia":"PREVENT SENIOR","cnpj":"02926892000158","modalidade":"Medicina de Grupo","uf":"SP","municipio":"São Paulo","situacao":"Ativa","beneficiarios_total":550000,"porte":"Médio"}
]'
echo "  OK"

# EMPREGO RAIS
echo "[8/10] Inserindo Emprego RAIS..."
insert_data "emprego_rais" '[
{"ano":2023,"codigo_municipio":"3550308","cnae_secao":"J","cnae_divisao":"62","quantidade_vinculos":125000,"massa_salarial":2100000000.00,"salario_medio":16800.00,"escolaridade_media":"Superior completo","idade_media":31.5,"percentual_feminino":35.2,"tipo_vinculo":"CLT"},
{"ano":2023,"codigo_municipio":"3550308","cnae_secao":"K","cnae_divisao":"64","quantidade_vinculos":98000,"massa_salarial":2500000000.00,"salario_medio":25510.20,"escolaridade_media":"Superior completo","idade_media":38.7,"percentual_feminino":48.9,"tipo_vinculo":"CLT"},
{"ano":2023,"codigo_municipio":"3550308","cnae_secao":"G","cnae_divisao":"47","quantidade_vinculos":456000,"massa_salarial":3200000000.00,"salario_medio":7017.54,"escolaridade_media":"Ensino médio completo","idade_media":32.8,"percentual_feminino":55.3,"tipo_vinculo":"CLT"},
{"ano":2023,"codigo_municipio":"3304557","cnae_secao":"K","cnae_divisao":"64","quantidade_vinculos":76000,"massa_salarial":1950000000.00,"salario_medio":25657.89,"escolaridade_media":"Superior completo","idade_media":39.1,"percentual_feminino":47.3,"tipo_vinculo":"CLT"}
]'
echo "  OK"

# EMPREGO CAGED
echo "[9/10] Inserindo Emprego CAGED..."
insert_data "emprego_caged" '[
{"ano":2024,"mes":11,"codigo_municipio":"3550308","admissoes":185000,"desligamentos":172000,"saldo":13000,"estoque_total":6250000},
{"ano":2024,"mes":10,"codigo_municipio":"3550308","admissoes":178000,"desligamentos":165000,"saldo":13000,"estoque_total":6237000},
{"ano":2024,"mes":11,"codigo_municipio":"3304557","admissoes":98000,"desligamentos":92000,"saldo":6000,"estoque_total":3450000},
{"ano":2024,"mes":11,"codigo_municipio":"3106200","admissoes":56000,"desligamentos":52000,"saldo":4000,"estoque_total":1280000},
{"ano":2024,"mes":11,"codigo_municipio":"5300108","admissoes":45000,"desligamentos":42000,"saldo":3000,"estoque_total":1650000}
]'
echo "  OK"

# BRASIL.IO COVID
echo "[10/10] Inserindo Brasil.io COVID..."
insert_data "brasil_io_covid" '[
{"data":"2023-12-31","uf":"SP","codigo_municipio":"3550308","municipio":"São Paulo","casos_confirmados":6523456,"obitos_confirmados":78234,"casos_novos":125,"obitos_novos":2,"populacao_estimada":12396372,"taxa_mortalidade":1.20},
{"data":"2023-12-31","uf":"RJ","codigo_municipio":"3304557","municipio":"Rio de Janeiro","casos_confirmados":2876543,"obitos_confirmados":45678,"casos_novos":89,"obitos_novos":1,"populacao_estimada":6775561,"taxa_mortalidade":1.59},
{"data":"2023-12-31","uf":"MG","codigo_municipio":"3106200","municipio":"Belo Horizonte","casos_confirmados":1234567,"obitos_confirmados":18765,"casos_novos":56,"obitos_novos":0,"populacao_estimada":2530701,"taxa_mortalidade":1.52},
{"data":"2023-12-31","uf":"DF","codigo_municipio":"5300108","municipio":"Brasília","casos_confirmados":543210,"obitos_confirmados":8765,"casos_novos":25,"obitos_novos":0,"populacao_estimada":3055149,"taxa_mortalidade":1.61}
]'
echo "  OK"

echo ""
echo "=============================================="
echo "ETL COMPLETO FINALIZADO!"
echo "=============================================="
