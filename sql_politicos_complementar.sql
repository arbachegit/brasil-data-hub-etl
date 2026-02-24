-- ============================================================
-- SQL: Tabelas complementares de dados políticos
-- Executar no Supabase SQL Editor
-- ============================================================

-- 1. Adicionar colunas de votos a fato_politicos_mandatos
ALTER TABLE fato_politicos_mandatos
  ADD COLUMN IF NOT EXISTS votos_nominais BIGINT,
  ADD COLUMN IF NOT EXISTS percentual_votos NUMERIC(8,4);

-- 2. Bens declarados por candidato
CREATE TABLE IF NOT EXISTS fato_bens_candidato (
  id BIGSERIAL PRIMARY KEY,
  politico_id BIGINT NOT NULL REFERENCES dim_politicos(id),
  ano_eleicao INTEGER NOT NULL,
  ordem INTEGER NOT NULL DEFAULT 0,
  tipo_bem TEXT,
  descricao TEXT,
  valor NUMERIC(15,2),
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(politico_id, ano_eleicao, ordem)
);

CREATE INDEX IF NOT EXISTS idx_bens_politico ON fato_bens_candidato(politico_id);
CREATE INDEX IF NOT EXISTS idx_bens_ano ON fato_bens_candidato(ano_eleicao);

-- 3. Receitas de campanha
CREATE TABLE IF NOT EXISTS fato_receitas_campanha (
  id BIGSERIAL PRIMARY KEY,
  politico_id BIGINT NOT NULL REFERENCES dim_politicos(id),
  ano_eleicao INTEGER NOT NULL,
  tipo_receita TEXT,
  fonte_recurso TEXT,
  valor NUMERIC(15,2),
  cpf_cnpj_doador TEXT,
  nome_doador TEXT,
  sequencial TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(politico_id, ano_eleicao, sequencial)
);

CREATE INDEX IF NOT EXISTS idx_receitas_politico ON fato_receitas_campanha(politico_id);
CREATE INDEX IF NOT EXISTS idx_receitas_ano ON fato_receitas_campanha(ano_eleicao);

-- 4. Votos legislativos (Câmara + Senado)
CREATE TABLE IF NOT EXISTS fato_votos_legislativos (
  id BIGSERIAL PRIMARY KEY,
  politico_id BIGINT NOT NULL REFERENCES dim_politicos(id),
  data_votacao DATE NOT NULL,
  votacao_id TEXT NOT NULL,
  voto TEXT NOT NULL,
  proposicao TEXT,
  descricao_votacao TEXT,
  fonte TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(politico_id, votacao_id)
);

CREATE INDEX IF NOT EXISTS idx_votos_politico ON fato_votos_legislativos(politico_id);
CREATE INDEX IF NOT EXISTS idx_votos_data ON fato_votos_legislativos(data_votacao);

-- 5. Emendas parlamentares
CREATE TABLE IF NOT EXISTS fato_emendas_parlamentares (
  id BIGSERIAL PRIMARY KEY,
  politico_id BIGINT REFERENCES dim_politicos(id),
  autor TEXT NOT NULL,
  ano INTEGER NOT NULL,
  codigo_emenda TEXT NOT NULL,
  tipo_emenda TEXT,
  valor_empenhado NUMERIC(15,2),
  valor_liquidado NUMERIC(15,2),
  valor_pago NUMERIC(15,2),
  localidade TEXT,
  funcao TEXT,
  subfuncao TEXT,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(codigo_emenda, ano)
);

CREATE INDEX IF NOT EXISTS idx_emendas_politico ON fato_emendas_parlamentares(politico_id);
CREATE INDEX IF NOT EXISTS idx_emendas_ano ON fato_emendas_parlamentares(ano);
