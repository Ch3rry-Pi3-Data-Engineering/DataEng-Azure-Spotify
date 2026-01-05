import argparse
import json
import os
import secrets
import shutil
import string
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path

DEFAULTS = {
    "resource_group_name_prefix": "rg-spotify",
    "location": "eastus2",
    "storage_account_name_prefix": "stspotify",
    "storage_account_replication_type": "LRS",
    "data_factory_name_prefix": "adf-spotify",
    "sql_server_name_prefix": "sql-spotify",
    "sql_admin_login": "sqladmin",
    "sql_database_name": "spotify-dev",
    "sql_database_sku_name": "GP_S_Gen5_1",
    "sql_max_size_gb": 1,
    "sql_min_capacity": 0.5,
    "sql_auto_pause_delay_in_minutes": 60,
    "sql_public_network_access_enabled": True,
    "sql_zone_redundant": False,
    "adf_sql_linked_service_name": "lsqldb-spotify-dev",
    "adf_adls_linked_service_name": "lsadls-spotify",
    "adf_pipeline_name": "incremental_ingestion",
    "adf_cdc_dataset_name": "ds_spotify_cdc_json",
    "adf_sql_dataset_name": "ds_spotify_sql_source",
    "adf_sink_dataset_name": "ds_spotify_bronze_parquet",
    "adf_lookup_container": "bronze",
    "adf_lookup_folder": "@{item().table}_cdc",
    "adf_lookup_file": "cdc.json",
    "adf_sink_container": "bronze",
    "adf_sink_folder": "@item().table",
    "adf_sink_file": "@concat(item().table,'_',variables('current'))",
}

SQLCMD_FALLBACK_PATHS = [
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\sqlcmd.exe",
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\sqlcmd.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\sqlcmd.exe",
    r"C:\Program Files (x86)\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\sqlcmd.exe",
]

DEFAULT_CDC_FOLDERS = [
    "DimArtist_cdc",
    "DimDate_cdc",
    "DimTrack_cdc",
    "DimUser_cdc",
    "FactStream_cdc",
]
EXTRA_SEED_FOLDERS = [
    "FactStream",
]

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    subprocess.check_call(cmd)

def run_capture(cmd):
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.check_output(cmd, text=True).strip()

def run_optional(cmd):
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.run(cmd)

def run_sensitive(cmd, redacted_indices):
    display_cmd = cmd[:]
    for index in redacted_indices:
        if 0 <= index < len(display_cmd):
            display_cmd[index] = "***"
    print(f"\n$ {' '.join(display_cmd)}")
    subprocess.check_call(cmd)

def require_adf_git_linked(skip_check):
    if skip_check:
        return
    if not sys.stdin.isatty():
        raise RuntimeError(
            "ADF Git check requires an interactive terminal. Re-run with --skip-adf-git-check to bypass."
        )
    response = input("Have you linked Azure Data Factory to GitHub? Type 'yes' to continue: ").strip().lower()
    if response not in ("y", "yes"):
        print("Link GitHub in ADF Studio (Manage -> Git configuration), then re-run the deploy.")
        sys.exit(1)

def get_azuread_admin_login():
    env_value = os.environ.get("AZUREAD_ADMIN_LOGIN")
    if env_value:
        return env_value
    az_path = shutil.which("az")
    if not az_path:
        raise RuntimeError("AZUREAD_ADMIN_LOGIN is not set and Azure CLI (az) was not found.")
    try:
        value = run_capture([az_path, "account", "show", "--query", "user.name", "-o", "tsv"])
    except subprocess.CalledProcessError as exc:
        raise RuntimeError("Failed to read Azure CLI account. Set AZUREAD_ADMIN_LOGIN and retry.") from exc
    if not value:
        raise RuntimeError("Azure CLI did not return a user name. Set AZUREAD_ADMIN_LOGIN and retry.")
    return value

def get_env_required(key):
    value = os.environ.get(key)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value

def get_env_optional(key):
    value = os.environ.get(key)
    return value if value not in (None, "") else None

def read_tfvars_value(path, key):
    if not path.exists():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        current_key, value = stripped.split("=", 1)
        if current_key.strip() != key:
            continue
        value = value.strip()
        if value == "null":
            return None
        if value.startswith('"') and value.endswith('"'):
            return value[1:-1].replace('\\"', '"')
        return value
    return None

def get_tfvar_or_default(path, key, default):
    value = read_tfvars_value(path, key)
    return default if value is None else value

def generate_password(length=20):
    symbols = "!@#$%^&*_-+=?"
    alphabet = string.ascii_letters + string.digits + symbols
    while True:
        password = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(char.islower() for char in password)
            and any(char.isupper() for char in password)
            and any(char.isdigit() for char in password)
            and any(char in symbols for char in password)
        ):
            return password

def detect_public_ip():
    try:
        with urllib.request.urlopen("https://api.ipify.org", timeout=10) as response:
            value = response.read().decode("utf-8").strip()
            return value if value else None
    except (urllib.error.URLError, TimeoutError):
        return None

def get_sql_admin_password(sql_dir, allow_generate=True):
    env_password = os.environ.get("SQL_ADMIN_PASSWORD")
    if env_password:
        return env_password, False
    existing = read_tfvars_value(sql_dir / "terraform.tfvars", "sql_admin_password")
    if existing:
        return existing, False
    if allow_generate:
        return generate_password(), True
    raise RuntimeError("SQL admin password not found. Set SQL_ADMIN_PASSWORD or deploy SQL first.")

def get_sql_admin_login(sql_dir):
    env_login = os.environ.get("SQL_ADMIN_LOGIN")
    if env_login:
        return env_login
    existing = read_tfvars_value(sql_dir / "terraform.tfvars", "sql_admin_login")
    if existing:
        return existing
    return DEFAULTS["sql_admin_login"]

def get_sql_client_ip(sql_dir):
    env_ip = os.environ.get("SQL_CLIENT_IP")
    if env_ip:
        return env_ip, False
    existing = read_tfvars_value(sql_dir / "terraform.tfvars", "client_ip_address")
    if existing:
        return existing, False
    detected = detect_public_ip()
    if detected:
        return detected, True
    raise RuntimeError("Could not detect public IP. Set SQL_CLIENT_IP before deploying SQL.")

def find_sqlcmd():
    sqlcmd_path = shutil.which("sqlcmd")
    if sqlcmd_path:
        return sqlcmd_path
    for path in SQLCMD_FALLBACK_PATHS:
        if Path(path).exists():
            return path
    return None

def hcl_value(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    escaped = str(value).replace('"', '\\"')
    return f'"{escaped}"'

def write_tfvars(path, items):
    lines = [f"{key} = {hcl_value(value)}" for key, value in items]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

def get_output(tf_dir, output_name):
    return run_capture(["terraform", f"-chdir={tf_dir}", "output", "-raw", output_name])

def get_output_optional(tf_dir, output_name):
    try:
        return get_output(tf_dir, output_name)
    except subprocess.CalledProcessError:
        return None

def get_state_addresses(tf_dir):
    try:
        output = run_capture(["terraform", f"-chdir={tf_dir}", "state", "list"])
    except subprocess.CalledProcessError:
        return set()
    return {line.strip() for line in output.splitlines() if line.strip()}

def write_rg_tfvars(rg_dir):
    items = [
        ("resource_group_name", None),
        ("resource_group_name_prefix", DEFAULTS["resource_group_name_prefix"]),
        ("location", DEFAULTS["location"]),
    ]
    write_tfvars(rg_dir / "terraform.tfvars", items)

def write_storage_tfvars(storage_dir, rg_name):
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("storage_account_name_prefix", DEFAULTS["storage_account_name_prefix"]),
        ("account_replication_type", DEFAULTS["storage_account_replication_type"]),
    ]
    write_tfvars(storage_dir / "terraform.tfvars", items)

def write_data_factory_tfvars(data_factory_dir, rg_name):
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("data_factory_name_prefix", DEFAULTS["data_factory_name_prefix"]),
    ]
    write_tfvars(data_factory_dir / "terraform.tfvars", items)

def write_sql_tfvars(sql_dir, rg_name):
    admin_login = os.environ.get("SQL_ADMIN_LOGIN", DEFAULTS["sql_admin_login"])
    admin_password, generated_password = get_sql_admin_password(sql_dir, allow_generate=True)
    azuread_admin_login = get_azuread_admin_login()
    azuread_admin_object_id = get_env_optional("AZUREAD_ADMIN_OBJECT_ID")
    client_ip_address, detected_ip = get_sql_client_ip(sql_dir)
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("sql_server_name_prefix", DEFAULTS["sql_server_name_prefix"]),
        ("sql_admin_login", admin_login),
        ("sql_admin_password", admin_password),
        ("azuread_admin_login", azuread_admin_login),
        ("azuread_admin_object_id", azuread_admin_object_id),
        ("client_ip_address", client_ip_address),
        ("database_name", DEFAULTS["sql_database_name"]),
        ("database_sku_name", DEFAULTS["sql_database_sku_name"]),
        ("max_size_gb", DEFAULTS["sql_max_size_gb"]),
        ("min_capacity", DEFAULTS["sql_min_capacity"]),
        ("auto_pause_delay_in_minutes", DEFAULTS["sql_auto_pause_delay_in_minutes"]),
        ("public_network_access_enabled", DEFAULTS["sql_public_network_access_enabled"]),
        ("zone_redundant", DEFAULTS["sql_zone_redundant"]),
    ]
    write_tfvars(sql_dir / "terraform.tfvars", items)
    if generated_password:
        print("Generated SQL admin password and stored it in terraform/03_sql_database/terraform.tfvars")
    if detected_ip:
        print(f"Detected public IP {client_ip_address} and stored it in terraform/03_sql_database/terraform.tfvars")
    return admin_login, admin_password

def write_adf_linked_services_tfvars(
    linked_services_dir,
    data_factory_id,
    sql_server_fqdn,
    sql_database_name,
    sql_username,
    sql_password,
    storage_dfs_endpoint,
    storage_account_key,
):
    items = [
        ("data_factory_id", data_factory_id),
        ("sql_linked_service_name", DEFAULTS["adf_sql_linked_service_name"]),
        ("sql_server_fqdn", sql_server_fqdn),
        ("sql_database_name", sql_database_name),
        ("sql_username", sql_username),
        ("sql_password", sql_password),
        ("adls_linked_service_name", DEFAULTS["adf_adls_linked_service_name"]),
        ("storage_dfs_endpoint", storage_dfs_endpoint),
        ("storage_account_key", storage_account_key),
    ]
    write_tfvars(linked_services_dir / "terraform.tfvars", items)

def write_adf_pipeline_tfvars(
    pipeline_dir,
    data_factory_id,
    sql_linked_service_name,
    adls_linked_service_name,
):
    items = [
        ("data_factory_id", data_factory_id),
        ("sql_linked_service_name", sql_linked_service_name),
        ("adls_linked_service_name", adls_linked_service_name),
        ("pipeline_name", DEFAULTS["adf_pipeline_name"]),
        ("cdc_dataset_name", DEFAULTS["adf_cdc_dataset_name"]),
        ("sql_dataset_name", DEFAULTS["adf_sql_dataset_name"]),
        ("sink_dataset_name", DEFAULTS["adf_sink_dataset_name"]),
        ("lookup_container", DEFAULTS["adf_lookup_container"]),
        ("lookup_folder", DEFAULTS["adf_lookup_folder"]),
        ("lookup_file", DEFAULTS["adf_lookup_file"]),
        ("sink_container", DEFAULTS["adf_sink_container"]),
        ("sink_folder", DEFAULTS["adf_sink_folder"]),
        ("sink_file", DEFAULTS["adf_sink_file"]),
    ]
    write_tfvars(pipeline_dir / "terraform.tfvars", items)

def maybe_reset_pipeline_for_dataset_rename(pipeline_dir):
    desired_cdc = get_tfvar_or_default(
        pipeline_dir / "terraform.tfvars",
        "cdc_dataset_name",
        DEFAULTS["adf_cdc_dataset_name"],
    )
    desired_sql = get_tfvar_or_default(
        pipeline_dir / "terraform.tfvars",
        "sql_dataset_name",
        DEFAULTS["adf_sql_dataset_name"],
    )
    desired_sink = get_tfvar_or_default(
        pipeline_dir / "terraform.tfvars",
        "sink_dataset_name",
        DEFAULTS["adf_sink_dataset_name"],
    )
    current_cdc = get_output_optional(pipeline_dir, "cdc_dataset_name")
    current_sql = get_output_optional(pipeline_dir, "sql_dataset_name")
    current_sink = get_output_optional(pipeline_dir, "sink_dataset_name")
    if not any([current_cdc, current_sql, current_sink]):
        return
    if (current_cdc, current_sql, current_sink) == (desired_cdc, desired_sql, desired_sink):
        return
    state = get_state_addresses(pipeline_dir)
    if "azurerm_data_factory_pipeline.incremental" not in state:
        return
    print("Dataset names changed; removing pipeline to unblock dataset replacement.")
    run(["terraform", f"-chdir={pipeline_dir}", "destroy", "-auto-approve", "-target=azurerm_data_factory_pipeline.incremental"])

def run_sql_script(sql_dir, admin_login, admin_password, script_path):
    if not script_path.exists():
        raise FileNotFoundError(f"Missing SQL script: {script_path}")
    sqlcmd_path = find_sqlcmd()
    if sqlcmd_path is None:
        raise FileNotFoundError(
            "sqlcmd not found. Install Microsoft sqlcmd or re-run with --skip-sql-init."
        )
    server_fqdn = get_output(sql_dir, "sql_server_fqdn")
    database_name = get_output(sql_dir, "sql_database_name")
    cmd = [
        sqlcmd_path,
        "-b",
        "-S",
        server_fqdn,
        "-d",
        database_name,
        "-U",
        admin_login,
        "-P",
        admin_password,
        "-i",
        str(script_path),
    ]
    password_index = cmd.index("-P") + 1
    run_sensitive(cmd, redacted_indices=[password_index])

def ensure_storage_seed_blobs(storage_dir):
    repo_root = storage_dir.parent.parent
    cdc_folders = get_cdc_folders(repo_root)
    account_name = get_output_optional(storage_dir, "storage_account_name")
    if not account_name:
        return
    state = get_state_addresses(storage_dir)
    imports = {}
    for folder in cdc_folders:
        seed_key = f"{folder}/cdc.json"
        empty_key = f"{folder}/empty.json"
        imports[f'azurerm_storage_blob.cdc_seed["{seed_key}"]'] = (
            f"https://{account_name}.blob.core.windows.net/bronze/{seed_key}"
        )
        imports[f'azurerm_storage_blob.cdc_empty["{empty_key}"]'] = (
            f"https://{account_name}.blob.core.windows.net/bronze/{empty_key}"
        )
    for address, resource_id in imports.items():
        if address in state:
            continue
        result = run_optional(
            ["terraform", f"-chdir={storage_dir}", "import", address, resource_id]
        )
        if result.returncode != 0:
            print(f"Skipping import for {address}. It may not exist yet.")

def get_cdc_folders(repo_root):
    loop_input_paths = [
        repo_root / "data_scripts" / "loop_input.json",
        repo_root / "data_scripts" / "loop_input.txt",
    ]
    data = None
    for path in loop_input_paths:
        if path.exists():
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
                break
            except json.JSONDecodeError:
                data = None
    folders = set()
    if isinstance(data, list):
        folders.update(
            f"{entry.get('table')}_cdc"
            for entry in data
            if isinstance(entry, dict) and entry.get("table")
        )
    if not folders:
        folders.update(DEFAULT_CDC_FOLDERS)
    folders.update(EXTRA_SEED_FOLDERS)
    return sorted(folders)

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Deploy Terraform stacks for the Spotify data platform.")
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--rg-only", action="store_true", help="Deploy only the resource group stack")
        group.add_argument("--storage-only", action="store_true", help="Deploy only the storage account stack")
        group.add_argument("--sql-only", action="store_true", help="Deploy only the SQL server + database stack")
        group.add_argument("--datafactory-only", action="store_true", help="Deploy only the data factory stack")
        group.add_argument("--adf-links-only", action="store_true", help="Deploy only the ADF linked services stack")
        group.add_argument("--adf-pipeline-only", action="store_true", help="Deploy only the ADF pipeline stack")
        parser.add_argument("--sql-init", action="store_true", help="Run the SQL init script after SQL deploy")
        parser.add_argument("--skip-sql-init", action="store_true", help="Skip SQL init on full deploy")
        parser.add_argument("--skip-adf-git-check", action="store_true", help="Skip manual ADF Git prompt")
        args = parser.parse_args()

        repo_root = Path(__file__).resolve().parent.parent
        rg_dir = repo_root / "terraform" / "01_resource_group"
        storage_dir = repo_root / "terraform" / "02_storage_account"
        sql_dir = repo_root / "terraform" / "03_sql_database"
        data_factory_dir = repo_root / "terraform" / "04_data_factory"
        linked_services_dir = repo_root / "terraform" / "05_adf_linked_services"
        pipeline_dir = repo_root / "terraform" / "06_adf_pipeline_incremental"
        full_deploy = not (
            args.rg_only
            or args.storage_only
            or args.sql_only
            or args.datafactory_only
            or args.adf_links_only
            or args.adf_pipeline_only
        )
        run_sql_init = args.sql_init or (full_deploy and not args.skip_sql_init)

        if args.rg_only:
            write_rg_tfvars(rg_dir)
            run(["terraform", f"-chdir={rg_dir}", "init"])
            run(["terraform", f"-chdir={rg_dir}", "apply", "-auto-approve"])
            sys.exit(0)

        if args.storage_only:
            run(["terraform", f"-chdir={rg_dir}", "init"])
            rg_name = get_output(rg_dir, "resource_group_name")
            write_storage_tfvars(storage_dir, rg_name)
            run(["terraform", f"-chdir={storage_dir}", "init"])
            ensure_storage_seed_blobs(storage_dir)
            run(["terraform", f"-chdir={storage_dir}", "apply", "-auto-approve"])
            sys.exit(0)

        if args.sql_only:
            run(["terraform", f"-chdir={rg_dir}", "init"])
            rg_name = get_output(rg_dir, "resource_group_name")
            sql_admin_login, sql_admin_password = write_sql_tfvars(sql_dir, rg_name)
            run(["terraform", f"-chdir={sql_dir}", "init"])
            run(["terraform", f"-chdir={sql_dir}", "apply", "-auto-approve"])
            if run_sql_init:
                script_path = repo_root / "data_scripts" / "spotify_initial_load.sql"
                run_sql_script(sql_dir, sql_admin_login, sql_admin_password, script_path)
            sys.exit(0)

        if args.datafactory_only:
            run(["terraform", f"-chdir={rg_dir}", "init"])
            rg_name = get_output(rg_dir, "resource_group_name")
            write_data_factory_tfvars(data_factory_dir, rg_name)
            run(["terraform", f"-chdir={data_factory_dir}", "init"])
            run(["terraform", f"-chdir={data_factory_dir}", "apply", "-auto-approve"])
            require_adf_git_linked(args.skip_adf_git_check)
            sys.exit(0)

        if args.adf_links_only:
            data_factory_id = get_output(data_factory_dir, "data_factory_id")
            sql_server_fqdn = get_output(sql_dir, "sql_server_fqdn")
            sql_database_name = get_output(sql_dir, "sql_database_name")
            sql_username = get_sql_admin_login(sql_dir)
            sql_password, _ = get_sql_admin_password(sql_dir, allow_generate=False)
            storage_dfs_endpoint = get_output(storage_dir, "primary_dfs_endpoint")
            storage_account_key = get_output(storage_dir, "storage_account_primary_access_key")
            write_adf_linked_services_tfvars(
                linked_services_dir,
                data_factory_id,
                sql_server_fqdn,
                sql_database_name,
                sql_username,
                sql_password,
                storage_dfs_endpoint,
                storage_account_key,
            )
            run(["terraform", f"-chdir={linked_services_dir}", "init"])
            run(["terraform", f"-chdir={linked_services_dir}", "apply", "-auto-approve"])
            sys.exit(0)

        if args.adf_pipeline_only:
            data_factory_id = get_output(data_factory_dir, "data_factory_id")
            sql_linked_service_name = get_output(linked_services_dir, "sql_linked_service_name")
            adls_linked_service_name = get_output(linked_services_dir, "adls_linked_service_name")
            write_adf_pipeline_tfvars(
                pipeline_dir,
                data_factory_id,
                sql_linked_service_name,
                adls_linked_service_name,
            )
            run(["terraform", f"-chdir={pipeline_dir}", "init"])
            maybe_reset_pipeline_for_dataset_rename(pipeline_dir)
            run(["terraform", f"-chdir={pipeline_dir}", "apply", "-auto-approve"])
            sys.exit(0)

        write_rg_tfvars(rg_dir)
        run(["terraform", f"-chdir={rg_dir}", "init"])
        run(["terraform", f"-chdir={rg_dir}", "apply", "-auto-approve"])
        rg_name = get_output(rg_dir, "resource_group_name")

        write_storage_tfvars(storage_dir, rg_name)
        run(["terraform", f"-chdir={storage_dir}", "init"])
        ensure_storage_seed_blobs(storage_dir)
        run(["terraform", f"-chdir={storage_dir}", "apply", "-auto-approve"])

        sql_admin_login, sql_admin_password = write_sql_tfvars(sql_dir, rg_name)
        run(["terraform", f"-chdir={sql_dir}", "init"])
        run(["terraform", f"-chdir={sql_dir}", "apply", "-auto-approve"])
        if run_sql_init:
            script_path = repo_root / "data_scripts" / "spotify_initial_load.sql"
            run_sql_script(sql_dir, sql_admin_login, sql_admin_password, script_path)

        write_data_factory_tfvars(data_factory_dir, rg_name)
        run(["terraform", f"-chdir={data_factory_dir}", "init"])
        run(["terraform", f"-chdir={data_factory_dir}", "apply", "-auto-approve"])
        require_adf_git_linked(args.skip_adf_git_check)

        data_factory_id = get_output(data_factory_dir, "data_factory_id")
        sql_server_fqdn = get_output(sql_dir, "sql_server_fqdn")
        sql_database_name = get_output(sql_dir, "sql_database_name")
        storage_dfs_endpoint = get_output(storage_dir, "primary_dfs_endpoint")
        storage_account_key = get_output(storage_dir, "storage_account_primary_access_key")
        write_adf_linked_services_tfvars(
            linked_services_dir,
            data_factory_id,
            sql_server_fqdn,
            sql_database_name,
            sql_admin_login,
            sql_admin_password,
            storage_dfs_endpoint,
            storage_account_key,
        )
        run(["terraform", f"-chdir={linked_services_dir}", "init"])
        run(["terraform", f"-chdir={linked_services_dir}", "apply", "-auto-approve"])

        sql_linked_service_name = get_output(linked_services_dir, "sql_linked_service_name")
        adls_linked_service_name = get_output(linked_services_dir, "adls_linked_service_name")
        write_adf_pipeline_tfvars(
            pipeline_dir,
            data_factory_id,
            sql_linked_service_name,
            adls_linked_service_name,
        )
        run(["terraform", f"-chdir={pipeline_dir}", "init"])
        maybe_reset_pipeline_for_dataset_rename(pipeline_dir)
        run(["terraform", f"-chdir={pipeline_dir}", "apply", "-auto-approve"])
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {exc}")
        sys.exit(exc.returncode)
