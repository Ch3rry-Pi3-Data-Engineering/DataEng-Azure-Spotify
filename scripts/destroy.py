import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path

DEFAULTS = {
    "adf_sql_linked_service_name": "lsqldb-spotify-dev",
    "adf_adls_linked_service_name": "lsadls-spotify",
    "adf_pipeline_arm_name": "incremental_ingestion_arm",
    "adf_cdc_dataset_arm_name": "ds_spotify_cdc_json_arm",
    "adf_sql_dataset_arm_name": "ds_spotify_sql_source_arm",
    "adf_sink_dataset_arm_name": "ds_spotify_bronze_parquet_arm",
    "adf_lookup_container": "bronze",
    "adf_lookup_folder": "@{item().table}_cdc",
    "adf_lookup_file": "cdc.json",
    "adf_sink_container": "bronze",
    "adf_sink_folder": "@item().table",
    "adf_sink_file": "@concat(item().table,'_',variables('current'))",
    "location": "eastus2",
    "monitoring_workspace_name_prefix": "law-spotify",
    "monitoring_action_group_name_prefix": "ag-spotify",
    "monitoring_action_group_short_name": "agspot",
    "monitoring_alert_name_prefix": "alert-spotify",
    "monitoring_email_to": "the_rfc@hotmail.co.uk",
    "monitoring_alert_frequency": 5,
    "monitoring_alert_window": 5,
    "monitoring_alert_failure_severity": 2,
    "monitoring_alert_success_severity": 3,
    "databricks_workspace_name_prefix": "dbw-spotify",
    "databricks_managed_rg_name_prefix": "rg-databricks-spotify",
    "databricks_sku": "premium",
    "databricks_access_connector_name_prefix": "dbc-spotify-ac",
    "databricks_uc_catalog_name": "spotify",
    "databricks_uc_schema_name": "silver",
    "databricks_uc_storage_credential_name": "sc-spotify-mi",
    "databricks_uc_bronze_location_name": "bronze",
    "databricks_uc_silver_location_name": "silver",
    "databricks_uc_gold_schema_name": "gold",
    "databricks_uc_gold_location_name": "gold",
}

def run(cmd):
    print(f"\n$ {' '.join(cmd)}")
    subprocess.check_call(cmd)

def run_capture(cmd):
    print(f"\n$ {' '.join(cmd)}")
    return subprocess.check_output(cmd, text=True).strip()

def load_env_file(path):
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if stripped.startswith("export "):
            stripped = stripped[len("export "):].strip()
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value

def get_env_required(key):
    value = os.environ.get(key)
    if value is None or value == "":
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value

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

ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

def _strip_ansi(value):
    return ANSI_RE.sub("", value)

def _normalize_output(value):
    if not value:
        return None
    cleaned = _strip_ansi(value).strip()
    if not cleaned:
        return None
    if "No outputs found" in cleaned:
        return None
    return cleaned

def get_output(tf_dir, output_name):
    value = _normalize_output(
        run_capture(["terraform", f"-chdir={tf_dir}", "output", "-raw", output_name])
    )
    if value is None:
        raise RuntimeError(f"Output {output_name} not found in {tf_dir}")
    return value

def get_output_optional(tf_dir, output_name):
    try:
        return _normalize_output(
            run_capture(["terraform", f"-chdir={tf_dir}", "output", "-raw", output_name])
        )
    except subprocess.CalledProcessError:
        return None

def get_data_factory_id_from_state(data_factory_dir):
    state_path = data_factory_dir / "terraform.tfstate"
    if not state_path.exists():
        return None
    try:
        data = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    for resource in data.get("resources", []):
        if resource.get("type") != "azurerm_data_factory":
            continue
        if resource.get("name") != "main":
            continue
        for instance in resource.get("instances", []):
            attrs = instance.get("attributes", {})
            if isinstance(attrs, dict) and attrs.get("id"):
                return attrs["id"]
    return None

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

def prepare_adf_pipeline_arm_tfvars(
    pipeline_dir,
    data_factory_dir,
    linked_services_dir,
):
    pipeline_tfvars = pipeline_dir / "terraform.tfvars"
    pipeline_name = get_tfvar_or_default(
        pipeline_tfvars, "pipeline_name", DEFAULTS["adf_pipeline_arm_name"]
    )
    cdc_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "cdc_dataset_name", DEFAULTS["adf_cdc_dataset_arm_name"]
    )
    sql_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "sql_dataset_name", DEFAULTS["adf_sql_dataset_arm_name"]
    )
    sink_dataset_name = get_tfvar_or_default(
        pipeline_tfvars, "sink_dataset_name", DEFAULTS["adf_sink_dataset_arm_name"]
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

    data_factory_id = (
        get_output_optional(data_factory_dir, "data_factory_id")
        or read_tfvars_value(pipeline_tfvars, "data_factory_id")
        or get_data_factory_id_from_state(data_factory_dir)
    )
    sql_linked_service_name = (
        get_output_optional(linked_services_dir, "sql_linked_service_name")
        or read_tfvars_value(linked_services_dir / "terraform.tfvars", "sql_linked_service_name")
        or read_tfvars_value(pipeline_tfvars, "sql_linked_service_name")
        or DEFAULTS["adf_sql_linked_service_name"]
    )
    adls_linked_service_name = (
        get_output_optional(linked_services_dir, "adls_linked_service_name")
        or read_tfvars_value(linked_services_dir / "terraform.tfvars", "adls_linked_service_name")
        or read_tfvars_value(pipeline_tfvars, "adls_linked_service_name")
        or DEFAULTS["adf_adls_linked_service_name"]
    )

    if not data_factory_id:
        raise RuntimeError(
            "Missing Data Factory or linked service outputs for ARM pipeline destroy. "
            "Re-run the deploy or ensure terraform.tfvars exists for the ARM pipeline module."
        )

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

def prepare_monitoring_tfvars(monitoring_dir, rg_dir, data_factory_dir, pipeline_dir):
    rg_name = get_output_optional(rg_dir, "resource_group_name") or read_tfvars_value(
        rg_dir / "terraform.tfvars", "resource_group_name"
    )
    if not rg_name:
        raise RuntimeError("Resource group name not found for monitoring destroy.")
    data_factory_id = (
        get_output_optional(data_factory_dir, "data_factory_id")
        or get_data_factory_id_from_state(data_factory_dir)
    )
    if not data_factory_id:
        raise RuntimeError("Data Factory ID not found for monitoring destroy.")
    pipeline_name = (
        get_output_optional(pipeline_dir, "pipeline_name")
        or read_tfvars_value(pipeline_dir / "terraform.tfvars", "pipeline_name")
        or DEFAULTS["adf_pipeline_arm_name"]
    )
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("data_factory_id", data_factory_id),
        ("pipeline_name", pipeline_name),
        ("workspace_name_prefix", DEFAULTS["monitoring_workspace_name_prefix"]),
        ("action_group_name_prefix", DEFAULTS["monitoring_action_group_name_prefix"]),
        ("action_group_short_name", DEFAULTS["monitoring_action_group_short_name"]),
        ("alert_name_prefix", DEFAULTS["monitoring_alert_name_prefix"]),
        ("email_to", DEFAULTS["monitoring_email_to"]),
        ("alert_frequency_minutes", DEFAULTS["monitoring_alert_frequency"]),
        ("alert_window_minutes", DEFAULTS["monitoring_alert_window"]),
        ("alert_failure_severity", DEFAULTS["monitoring_alert_failure_severity"]),
        ("alert_success_severity", DEFAULTS["monitoring_alert_success_severity"]),
    ]
    write_tfvars(monitoring_dir / "terraform.tfvars", items)

def prepare_databricks_tfvars(databricks_dir, rg_dir):
    rg_name = get_output_optional(rg_dir, "resource_group_name") or read_tfvars_value(
        rg_dir / "terraform.tfvars", "resource_group_name"
    )
    if not rg_name:
        raise RuntimeError("Resource group name not found for Databricks destroy.")
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("workspace_name_prefix", DEFAULTS["databricks_workspace_name_prefix"]),
        ("managed_resource_group_name_prefix", DEFAULTS["databricks_managed_rg_name_prefix"]),
        ("sku", DEFAULTS["databricks_sku"]),
    ]
    write_tfvars(databricks_dir / "terraform.tfvars", items)

def prepare_databricks_access_connector_tfvars(access_connector_dir, rg_dir, storage_dir):
    rg_name = get_output_optional(rg_dir, "resource_group_name") or read_tfvars_value(
        rg_dir / "terraform.tfvars", "resource_group_name"
    )
    if not rg_name:
        raise RuntimeError("Resource group name not found for Databricks access connector destroy.")
    storage_account_id = get_output_optional(storage_dir, "storage_account_id")
    if not storage_account_id:
        raise RuntimeError("Storage account ID not found for Databricks access connector destroy.")
    items = [
        ("resource_group_name", rg_name),
        ("location", DEFAULTS["location"]),
        ("storage_account_id", storage_account_id),
        ("access_connector_name_prefix", DEFAULTS["databricks_access_connector_name_prefix"]),
    ]
    write_tfvars(access_connector_dir / "terraform.tfvars", items)

def prepare_databricks_uc_tfvars(uc_dir, databricks_dir, access_connector_dir, storage_dir):
    databricks_account_id = get_env_required("DATABRICKS_ACCOUNT_ID")
    databricks_client_id = get_env_required("DATABRICKS_CLIENT_ID")
    databricks_client_secret = get_env_required("DATABRICKS_CLIENT_SECRET")

    databricks_host = get_output_optional(databricks_dir, "databricks_workspace_url")
    databricks_workspace_resource_id = get_output_optional(databricks_dir, "databricks_workspace_id")
    access_connector_id = get_output_optional(access_connector_dir, "access_connector_id")
    storage_account_name = get_output_optional(storage_dir, "storage_account_name")

    missing = [
        name
        for name, value in (
            ("databricks_host", databricks_host),
            ("databricks_workspace_resource_id", databricks_workspace_resource_id),
            ("access_connector_id", access_connector_id),
            ("storage_account_name", storage_account_name),
        )
        if not value
    ]
    if missing:
        raise RuntimeError(
            f"Missing Terraform outputs for Databricks UC destroy: {', '.join(missing)}"
        )

    items = [
        ("databricks_host", databricks_host),
        ("databricks_account_id", databricks_account_id),
        ("databricks_client_id", databricks_client_id),
        ("databricks_client_secret", databricks_client_secret),
        ("databricks_workspace_resource_id", databricks_workspace_resource_id),
        ("access_connector_id", access_connector_id),
        ("storage_account_name", storage_account_name),
        ("catalog_name", DEFAULTS["databricks_uc_catalog_name"]),
        ("schema_name", DEFAULTS["databricks_uc_schema_name"]),
        ("gold_schema_name", DEFAULTS["databricks_uc_gold_schema_name"]),
        ("storage_credential_name", DEFAULTS["databricks_uc_storage_credential_name"]),
        ("bronze_location_name", DEFAULTS["databricks_uc_bronze_location_name"]),
        ("silver_location_name", DEFAULTS["databricks_uc_silver_location_name"]),
        ("gold_location_name", DEFAULTS["databricks_uc_gold_location_name"]),
    ]
    write_tfvars(uc_dir / "terraform.tfvars", items)
if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Destroy Terraform stacks for the Spotify data platform.")
        group = parser.add_mutually_exclusive_group()
        group.add_argument("--rg-only", action="store_true", help="Destroy only the resource group stack")
        group.add_argument("--storage-only", action="store_true", help="Destroy only the storage account stack")
        group.add_argument("--sql-only", action="store_true", help="Destroy only the SQL server + database stack")
        group.add_argument("--datafactory-only", action="store_true", help="Destroy only the data factory stack")
        group.add_argument("--adf-links-only", action="store_true", help="Destroy only the ADF linked services stack")
        group.add_argument(
            "--adf-pipeline-only",
            action="store_true",
            help="Destroy only the ADF pipeline stack (ARM/azapi)",
        )
        group.add_argument("--monitoring-only", action="store_true", help="Destroy only the monitoring stack")
        group.add_argument("--databricks-only", action="store_true", help="Destroy only the Databricks stack")
        group.add_argument(
            "--databricks-access-connector-only",
            action="store_true",
            help="Destroy only the Databricks access connector stack",
        )
        group.add_argument(
            "--databricks-uc-only",
            action="store_true",
            help="Destroy only the Databricks Unity Catalog stack",
        )
        args = parser.parse_args()

        repo_root = Path(__file__).resolve().parent.parent
        load_env_file(repo_root / ".env")
        rg_dir = repo_root / "terraform" / "01_resource_group"
        storage_dir = repo_root / "terraform" / "02_storage_account"
        sql_dir = repo_root / "terraform" / "03_sql_database"
        data_factory_dir = repo_root / "terraform" / "04_data_factory"
        linked_services_dir = repo_root / "terraform" / "05_adf_linked_services"
        pipeline_arm_dir = repo_root / "terraform" / "06_adf_pipeline_incremental_arm"
        monitoring_dir = repo_root / "terraform" / "07_monitoring"
        databricks_dir = repo_root / "terraform" / "08_databricks"
        access_connector_dir = repo_root / "terraform" / "09_databricks_access_connector"
        uc_dir = repo_root / "terraform" / "10_databricks_uc"

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
            prepare_adf_pipeline_arm_tfvars(
                pipeline_arm_dir,
                data_factory_dir,
                linked_services_dir,
            )
            tf_dirs = [pipeline_arm_dir]
        elif args.monitoring_only:
            prepare_monitoring_tfvars(monitoring_dir, rg_dir, data_factory_dir, pipeline_arm_dir)
            tf_dirs = [monitoring_dir]
        elif args.databricks_only:
            prepare_databricks_tfvars(databricks_dir, rg_dir)
            tf_dirs = [databricks_dir]
        elif args.databricks_access_connector_only:
            prepare_databricks_access_connector_tfvars(access_connector_dir, rg_dir, storage_dir)
            tf_dirs = [access_connector_dir]
        elif args.databricks_uc_only:
            prepare_databricks_uc_tfvars(uc_dir, databricks_dir, access_connector_dir, storage_dir)
            tf_dirs = [uc_dir]
        else:
            prepare_adf_pipeline_arm_tfvars(
                pipeline_arm_dir,
                data_factory_dir,
                linked_services_dir,
            )
            prepare_monitoring_tfvars(monitoring_dir, rg_dir, data_factory_dir, pipeline_arm_dir)
            prepare_databricks_tfvars(databricks_dir, rg_dir)
            prepare_databricks_access_connector_tfvars(access_connector_dir, rg_dir, storage_dir)
            prepare_databricks_uc_tfvars(uc_dir, databricks_dir, access_connector_dir, storage_dir)
            prepare_adf_linked_services_tfvars(
                linked_services_dir,
                data_factory_dir,
                sql_dir,
                storage_dir,
            )
            tf_dirs = [
                uc_dir,
                access_connector_dir,
                databricks_dir,
                monitoring_dir,
                pipeline_arm_dir,
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
