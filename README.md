# BrasilDataHub

ETL repository for Brazilian public-data ingestion into Supabase.

## Scope

BrasilDataHub collects, normalizes, enriches, and validates public datasets from sources such as IBGE, BCB, TSE, legislative APIs, and municipal indicator datasets.

Covered domains include:

- geography, population, PIB, and census/SIDRA data
- macroeconomic series such as IPCA, SELIC, and USD exchange rates
- political candidates, mandates, assets, campaign revenue, affiliations, election results, votes, and parliamentary amendments
- municipal indicators for health, education, sanitation, mortality, employment, finance, and IDH
- econometric projections and gap-filling routines for municipal time series

## Runtime

Most scripts are standalone Python ETL jobs that use `SUPABASE_URL` and `SUPABASE_KEY` from the environment.

```bash
export SUPABASE_URL="https://..."
export SUPABASE_KEY="..."
python etl_ibge.py
```

SQL schema helpers live in `sql_*.sql`.
