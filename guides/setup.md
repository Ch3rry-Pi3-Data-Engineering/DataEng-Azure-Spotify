# Project Setup Guide

This project provisions Azure resources using Terraform and includes helper scripts.

## Prerequisites
- Azure CLI (az) installed and authenticated
- Terraform installed (>= 1.5)
- Python 3.10+ (for running the helper scripts)

## Azure CLI
Check your Azure CLI and login status:

```powershell
az --version
az login
az account show
```

If you need to switch subscriptions:

```powershell
az account list --output table
az account set --subscription "<subscription-id-or-name>"
az account show
```

## Terraform Setup
Check if Terraform is installed and on PATH:

```powershell
terraform version
```

Install or update Terraform on Windows:

```powershell
winget install HashiCorp.Terraform
```

```powershell
choco install terraform -y
```

After installing, re-open PowerShell and re-run terraform version.

## Project Structure
- terraform/01_resource_group: Azure resource group
- terraform/02_storage_account: ADLS Gen2 storage account + medallion containers
- terraform/03_sql_database: Azure SQL Server + dev database
- terraform/04_data_factory: Azure Data Factory v2
- terraform/05_adf_linked_services: ADF linked services (SQL + ADLS Gen2)
- terraform/06_adf_pipeline_incremental: ADF datasets + incremental ingestion pipeline
- scripts/: Helper scripts to deploy/destroy Terraform resources

## Configure Terraform
The deploy script writes terraform.tfvars files automatically.
If you want different defaults, edit DEFAULTS in scripts/deploy.py before running.

For the SQL stack, Entra admin login defaults to the current Azure CLI user if omitted. The SQL password and client IP are auto-generated/detected if omitted:
```powershell
$env:SQL_ADMIN_LOGIN = "sqladmin"
$env:AZUREAD_ADMIN_LOGIN = "your.name@domain.com"
```

Optional overrides:
```powershell
$env:SQL_ADMIN_PASSWORD = "UseAStr0ng!Passw0rd"
$env:SQL_CLIENT_IP = "<your-public-ip>"
```

## Deploy Resources
From the repo root or scripts folder, run:

```powershell
python scripts\deploy.py
```

Optional flags:

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

## Azure Data Factory GitHub Linking (Manual)
Terraform does not configure ADF Git integration. After ADF is created, link it in ADF Studio:

1) Open ADF Studio for your factory.
2) Go to Manage (wrench icon) -> Git configuration -> Configure.
3) Select GitHub and set (replace with your own org/repo as needed; values below are examples):
   - Repository type: GitHub
   - GitHub account: Ch3rry-Pi3-Data-Engineering (example)
   - Repository name: DataEng-Azure-Spotify (example)
   - Collaboration branch: main
   - Publish branch: adf_publish
   - Root folder: /
   - Publish (from ADF Studio): Enabled
   - Custom comment: Enabled
4) Save and publish.

### OAuth App Restrictions Bootstrap (GitHub Orgs)
If your GitHub org enforces OAuth app restrictions, you must allow a first-time OAuth handshake.

One-time bootstrap flow:
1) GitHub org settings -> Third-party access -> OAuth app policy -> temporarily disable "Restrict third-party application access".
2) In ADF Studio, connect the repo (this registers the AzureDataFactory OAuth app).
3) Re-enable "Restrict third-party application access".
4) GitHub user settings -> Applications -> Authorized OAuth Apps -> AzureDataFactory -> grant org access.

Notes:
- OAuth apps do not appear for approval until they authenticate once.
- With restrictions enabled, that first authentication is blocked.
- This is expected GitHub behavior.

## Azure Databricks GitHub Linking (Manual)
Use a GitHub personal access token (PAT) and link it in Databricks:

1) Create a GitHub PAT:
   - GitHub -> Settings -> Developer settings -> Personal access tokens.
   - Use scopes: repo (private) or public_repo (public only).
   - If your org requires SSO, authorize the token for the org.
2) In Databricks, open your workspace.
3) Click your user profile -> Settings -> Git Integration.
4) Add Git provider = GitHub and paste the PAT.
5) In Repos, click "Add Repo", paste the repo URL, and create.

## Azure Data Factory Linked Services (Terraform)
The SQL and ADLS Gen2 linked services are created by Terraform in `terraform/05_adf_linked_services`.

Defaults:
- Linked service name: `lsqldb-spotify-dev` (override via DEFAULTS in `scripts/deploy.py`).
- Server/database: pulled from SQL module outputs.
- Authentication: SQL auth using the SQL admin login/password from `terraform/03_sql_database/terraform.tfvars`.
- Subscription: uses the current Azure CLI context (default subscription).
- ADLS Gen2 linked service name: `lsadls-spotify` (override via DEFAULTS in `scripts/deploy.py`).
- ADLS Gen2 auth: account key from `terraform/02_storage_account` outputs.

If you want a different linked service name or SQL login, update `scripts/deploy.py` or the generated `terraform/05_adf_linked_services/terraform.tfvars`.

### Import Terraform Resources into Git Mode
Terraform creates ADF resources (linked services, datasets, pipelines) in Live mode. To bring them into Git mode:
1) Switch to Git mode (top bar, branch selector next to the GitHub icon).
2) Click "Import resources" (top bar).
3) Select the linked service(s), datasets, and pipelines from Live mode and import.
4) Publish to `adf_publish`.

Note: `adf_publish` is an auto-generated branch created by ADF when you publish. It is not meant to be merged into `main` and will be recreated if deleted.

## Seed the SQL Database
To run `data_scripts/spotify_initial_load.sql` against the new database:
```powershell
python scripts\deploy.py --sql-only --sql-init
```

This step uses `sqlcmd`. Install it if needed:
```powershell
winget install Microsoft.Sqlcmd
```

```powershell
choco install sqlcmd -y
```

macOS:
```bash
brew install sqlcmd
```

Ubuntu/Debian:
```bash
curl -sSL https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc >/dev/null
curl -sSL https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/microsoft-prod.list >/dev/null
sudo apt-get update
sudo apt-get install -y sqlcmd
```

RHEL/CentOS:
```bash
sudo rpm --import https://packages.microsoft.com/keys/microsoft.asc
curl -sSL https://packages.microsoft.com/config/rhel/9/prod.repo | sudo tee /etc/yum.repos.d/microsoft-prod.repo
sudo yum install -y sqlcmd
```

If you did not set `SQL_ADMIN_PASSWORD`, read it from `terraform/03_sql_database/terraform.tfvars`.

## Destroy Resources
To tear down resources:

```powershell
python scripts\destroy.py
```

Optional flags:

```powershell
python scripts\destroy.py --rg-only
python scripts\destroy.py --storage-only
python scripts\destroy.py --sql-only
python scripts\destroy.py --datafactory-only
python scripts\destroy.py --adf-links-only
python scripts\destroy.py --adf-pipeline-only
```

## Notes
- Storage account names must be 3-24 characters and lowercase letters/numbers.
- The storage account is created with hierarchical namespace enabled (ADLS Gen2).
- The deploy script creates bronze, silver, and gold containers for the medallion architecture.
- The storage module uploads data_scripts/cdc.json and data_scripts/empty.json into bronze/cdc.
- Storage replication defaults to LRS (locally redundant storage).
- SQL admin credentials are read from env vars, or auto-generated and saved to terraform/03_sql_database/terraform.tfvars.
- The SQL server allows Azure services and the detected or provided client IP address.
- Full deploys run SQL init unless --skip-sql-init is provided.
- The deploy script prompts for ADF Git linking unless --skip-adf-git-check is provided.
- terraform.tfvars files are generated and are gitignored.
