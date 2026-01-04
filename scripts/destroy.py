import argparse
import subprocess
import sys
from pathlib import Path

DEFAULTS = {
    "adf_sql_linked_service_name": "lsqldb-spotify-dev",
    "adf_adls_linked_service_name": "lsadls-spotify",
    "adf_pipeline_name": "incremental_ingestion",
    "adf_cdc_dataset_name": "ds_spotify_cdc_json",
    "adf_sql_dataset_name": "ds_spotify_sql_source",
    "adf_sink_dataset_name": "ds_spotify_bronze_parquet",
    "adf_lookup_container": "bronze",
    "adf_lookup_folder": "cdc",
    "adf_lookup_file": "cdc.json",
    "adf_sink_container": "bronze",
    "adf_sink_folder": "Users",
    "adf_sink_file": "@{concat(pipeline().parameters.table,'_',variables('current'))}.parquet",
}

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    subprocess.check_call(cmd)

def run_capture(cmd):
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.check_output(cmd, text=True).strip()

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

def get_output(tf_dir, output_name):
    return run_capture(["terraform", f"-chdir={tf_dir}", "output", "-raw", output_name])

def prepare_adf_linked_services_tfvars(
    linked_services_dir,
    data_factory_dir,
    sql_dir,
    storage_dir,
):
    sql_tfvars = sql_dir / "terraform.tfvars"
    sql_username = read_tfvars_value(sql_tfvars, "sql_admin_login") or "sqladmin"
    sql_password = read_tfvars_value(sql_tfvars, "sql_admin_password")
    if not sql_password:
        raise RuntimeError("SQL admin password not found in terraform/03_sql_database/terraform.tfvars.")

    sql_link_name = read_tfvars_value(
        linked_services_dir / "terraform.tfvars", "sql_linked_service_name"
    ) or DEFAULTS["adf_sql_linked_service_name"]
    adls_link_name = read_tfvars_value(
        linked_services_dir / "terraform.tfvars", "adls_linked_service_name"
    ) or DEFAULTS["adf_adls_linked_service_name"]

    data_factory_id = get_output(data_factory_dir, "data_factory_id")
    sql_server_fqdn = get_output(sql_dir, "sql_server_fqdn")
    sql_database_name = get_output(sql_dir, "sql_database_name")
    storage_dfs_endpoint = get_output(storage_dir, "primary_dfs_endpoint")
    storage_account_key = get_output(storage_dir, "storage_account_primary_access_key")

    items = [
        ("data_factory_id", data_factory_id),
        ("sql_linked_service_name", sql_link_name),
        ("sql_server_fqdn", sql_server_fqdn),
        ("sql_database_name", sql_database_name),
        ("sql_username", sql_username),
        ("sql_password", sql_password),
        ("adls_linked_service_name", adls_link_name),
        ("storage_dfs_endpoint", storage_dfs_endpoint),
        ("storage_account_key", storage_account_key),
    ]
    write_tfvars(linked_services_dir / "terraform.tfvars", items)

def prepare_adf_pipeline_tfvars(
    pipeline_dir,
    data_factory_dir,
    linked_services_dir,
):
    pipeline_tfvars = pipeline_dir / "terraform.tfvars"
    pipeline_name = get_tfvar_or_default(
        pipeline_tfvars, "pipeline_name", DEFAULTS["adf_pipeline_name"]
    )
    cdc_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "cdc_dataset_name", DEFAULTS["adf_cdc_dataset_name"]
    )
    sql_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "sql_dataset_name", DEFAULTS["adf_sql_dataset_name"]
    )
    sink_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "sink_dataset_name", DEFAULTS["adf_sink_dataset_name"]
    )
    lookup_container = get_tfvar_or_default(
        pipeline_tfvars, "lookup_container", DEFAULTS["adf_lookup_container"]
    )
    lookup_folder = get_tfvar_or_default(
        pipeline_tfvars, "lookup_folder", DEFAULTS["adf_lookup_folder"]
    )
    lookup_file = get_tfvar_or_default(
        pipeline_tfvars, "lookup_file", DEFAULTS["adf_lookup_file"]
    )
    sink_container = get_tfvar_or_default(
        pipeline_tfvars, "sink_container", DEFAULTS["adf_sink_container"]
    )
    sink_folder = get_tfvar_or_default(
        pipeline_tfvars, "sink_folder", DEFAULTS["adf_sink_folder"]
    )
    sink_file = get_tfvar_or_default(
        pipeline_tfvars, "sink_file", DEFAULTS["adf_sink_file"]
    )

    data_factory_id = get_output(data_factory_dir, "data_factory_id")
    sql_linked_service_name = get_output(linked_services_dir, "sql_linked_service_name")
    adls_linked_service_name = get_output(linked_services_dir, "adls_linked_service_name")

    items = [
        ("data_factory_id", data_factory_id),
        ("sql_linked_service_name", sql_linked_service_name),
        ("adls_linked_service_name", adls_linked_service_name),
        ("pipeline_name", pipeline_name),
        ("cdc_dataset_name", cdc_dataset_name),
        ("sql_dataset_name", sql_dataset_name),
        ("sink_dataset_name", sink_dataset_name),
        ("lookup_container", lookup_container),
        ("lookup_folder", lookup_folder),
        ("lookup_file", lookup_file),
        ("sink_container", sink_container),
        ("sink_folder", sink_folder),
        ("sink_file", sink_file),
    ]
    write_tfvars(pipeline_dir / "terraform.tfvars", items)
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Destroy Terraform stacks for the Spotify data platform.")
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--rg-only", action="store_true", help="Destroy only the resource group stack")
        group.add_argument("--storage-only", action="store_true", help="Destroy only the storage account stack")
        group.add_argument("--sql-only", action="store_true", help="Destroy only the SQL server + database stack")
        group.add_argument("--datafactory-only", action="store_true", help="Destroy only the data factory stack")
        group.add_argument("--adf-links-only", action="store_true", help="Destroy only the ADF linked services stack")
        group.add_argument("--adf-pipeline-only", action="store_true", help="Destroy only the ADF pipeline stack")
        args = parser.parse_args()

        repo_root = Path(__file__).resolve().parent.parent
        rg_dir = repo_root / "terraform" / "01_resource_group"
        storage_dir = repo_root / "terraform" / "02_storage_account"
        sql_dir = repo_root / "terraform" / "03_sql_database"
        data_factory_dir = repo_root / "terraform" / "04_data_factory"
        linked_services_dir = repo_root / "terraform" / "05_adf_linked_services"
        pipeline_dir = repo_root / "terraform" / "06_adf_pipeline_incremental"

        if args.rg_only:
            tf_dirs = [rg_dir]
        elif args.storage_only:
            tf_dirs = [storage_dir]
        elif args.sql_only:
            tf_dirs = [sql_dir]
        elif args.datafactory_only:
            tf_dirs = [data_factory_dir]
        elif args.adf_links_only:
            prepare_adf_linked_services_tfvars(
                linked_services_dir,
                data_factory_dir,
                sql_dir,
                storage_dir,
            )
            tf_dirs = [linked_services_dir]
        elif args.adf_pipeline_only:
            prepare_adf_pipeline_tfvars(
                pipeline_dir,
                data_factory_dir,
                linked_services_dir,
            )
            tf_dirs = [pipeline_dir]
        else:
            prepare_adf_pipeline_tfvars(
                pipeline_dir,
                data_factory_dir,
                linked_services_dir,
            )
            prepare_adf_linked_services_tfvars(
                linked_services_dir,
                data_factory_dir,
                sql_dir,
                storage_dir,
            )
            tf_dirs = [
                pipeline_dir,
                linked_services_dir,
                data_factory_dir,
                sql_dir,
                storage_dir,
                rg_dir,
            ]

        for tf_dir in tf_dirs:
            if not tf_dir.exists():
                raise FileNotFoundError(f"Missing Terraform dir: {tf_dir}")
            run(["terraform", f"-chdir={tf_dir}", "destroy", "-auto-approve"])
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {exc}")
        sys.exit(exc.returncode)
