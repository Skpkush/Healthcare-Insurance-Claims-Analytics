# Azure Cost Log — Healthcare V2

## Session: [12.05.2026]

## Resources deployed
- rg-healthcare-v2 (Central India)
- Storage account with healthcare-claims-raw container (250 MB CSVs)
- PostgreSQL Flexible Server B1ms (1 vCore, 2GB RAM, 32 GiB storage)
- Data Factory df-healthcare-v2-skp
- One pipeline pl_load_stg_train (executed successfully)

## Pipeline execution
- Train.csv (5,410 rows) → stg_train (Azure PSQL): SUCCESS

## Cleanup
Resource group DELETED at time after delete
Cost verified at zero within 24h: [yes]

## Artifacts preserved in repo
- 5 screenshots in azure/screenshots/
- 5 ADF JSON exports in azure/adf/
- Architecture diagram in docs/
- This cost log