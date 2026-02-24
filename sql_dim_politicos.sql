-- ============================================================
-- dim_politicos - Tabela dimensão de políticos brasileiros
-- Fonte: TSE Dados Abertos (todas as eleições 1982-2024)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_politicos (
    id BIGSERIAL PRIMARY KEY,
    cpf TEXT UNIQUE NOT NULL,
    titulo_eleitoral TEXT,
    nome TEXT NOT NULL,
    nome_urna TEXT,
    nome_social TEXT,
    data_nascimento DATE,
    genero TEXT,
    cor_raca TEXT,
    grau_instrucao TEXT,
    estado_civil TEXT,
    nacionalidade TEXT,
    uf_nascimento TEXT,
    municipio_nascimento TEXT,
    ocupacao TEXT,
    email TEXT,
    ultimo_partido_sigla TEXT,
    ultimo_partido_numero INTEGER,
    ultimo_ano_eleicao INTEGER,
    ultimo_cargo TEXT,
    ultimo_uf TEXT,
    ultima_situacao_candidatura TEXT,
    ultimo_resultado TEXT,
    reeleicao TEXT,
    total_candidaturas INTEGER DEFAULT 1,
    anos_eleicoes TEXT,
    cargos_disputados TEXT,
    partidos_historico TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Indices para consultas frequentes
CREATE INDEX IF NOT EXISTS idx_dim_politicos_nome ON dim_politicos(nome);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_partido ON dim_politicos(ultimo_partido_sigla);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_uf ON dim_politicos(ultimo_uf);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_cargo ON dim_politicos(ultimo_cargo);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_ano ON dim_politicos(ultimo_ano_eleicao);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_genero ON dim_politicos(genero);
CREATE INDEX IF NOT EXISTS idx_dim_politicos_cor_raca ON dim_politicos(cor_raca);
