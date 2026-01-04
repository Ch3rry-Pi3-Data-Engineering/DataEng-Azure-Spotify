# Azure Spotify Data Engineering (IaC)

Terraform-first infrastructure for a Spotify data engineering project using the medallion architecture (bronze/silver/gold).

## Quick Start
1) Install prerequisites:
   - Azure CLI (az)
   - Terraform (>= 1.5)
   - Python 3.10+

2) Authenticate to Azure:
```powershell
az login
az account show
```

3) Deploy infrastructure:
```powershell
python scripts\deploy.py
```

For SQL deployments, Entra admin login defaults to the current Azure CLI user if omitted. Password and public IP are auto-generated/detected if omitted:
```powershell
$env:SQL_ADMIN_LOGIN = "sqladmin"
$env:AZUREAD_ADMIN_LOGIN = "your.name@domain.com"
```
Auto-generated values are written to `terraform/03_sql_database/terraform.tfvars` (gitignored).

## Project Structure
- terraform/01_resource_group: Azure resource group
- terraform/02_storage_account: ADLS Gen2 storage account + medallion containers
- terraform/03_sql_database: Azure SQL Server + dev database
- terraform/04_data_factory: Azure Data Factory v2
- terraform/05_adf_linked_services: ADF linked services (SQL + ADLS Gen2)
- terraform/06_adf_pipeline_incremental: ADF datasets + incremental ingestion pipeline
- scripts/: Deploy/destroy helpers (auto-writes terraform.tfvars)
- guides/setup.md: Detailed setup guide
- data_scripts/: SQL/scripts for data loading

## Deploy/Destroy Options
Deploy specific stacks:
```powershell
python scripts\deploy.py --rg-only
python scripts\deploy.py --storage-only
python scripts\deploy.py --sql-only
python scripts\deploy.py --datafactory-only
python scripts\deploy.py --adf-links-only
python scripts\deploy.py --adf-pipeline-only
python scripts\deploy.py --sql-only --sql-init
python scripts\deploy.py --skip-sql-init
python scripts\deploy.py --skip-adf-git-check
```
`--sql-init` runs `data_scripts/spotify_initial_load.sql` using `sqlcmd`.
Full deploys run the SQL init step unless `--skip-sql-init` is provided.
The ADF step prompts you to confirm GitHub linking unless `--skip-adf-git-check` is provided.

Install `sqlcmd`:
```powershell
winget install Microsoft.Sqlcmd
```

```powershell
choco install sqlcmd -y
```

```bash
brew install sqlcmd
```

```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft-prod.list >/dev/null
sudo apt-get update
sudo apt-get install -y sqlcmd
```

```bash
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
curl -sSL https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/microsoft-prod.repo
sudo yum install -y sqlcmd
```

## Git Integrations
- Azure Data Factory and Azure Databricks Git linking is manual. See `guides/setup.md` for detailed steps.

## ADF Linked Services
Linked services for SQL and ADLS Gen2 are created via Terraform in `terraform/05_adf_linked_services`.

## ADF Incremental Pipeline
Datasets and the incremental ingestion pipeline are created via Terraform in `terraform/06_adf_pipeline_incremental`.

## ADLS Seed Files
The storage module uploads `data_scripts/cdc.json` and `data_scripts/empty.json` into `bronze/cdc`.

Destroy:
```powershell
python scripts\destroy.py
```

## Guide
See guides/setup.md for detailed instructions.
