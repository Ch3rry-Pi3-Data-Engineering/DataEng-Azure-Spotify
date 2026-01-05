output "catalog_name" {
  value = databricks_catalog.main.name
}

output "catalog_storage_root" {
  value = databricks_catalog.main.storage_root
}

output "schema_name" {
  value = databricks_schema.silver.name
}

output "gold_schema_name" {
  value = databricks_schema.gold.name
}

output "storage_credential_name" {
  value = databricks_storage_credential.managed_identity.name
}

output "bronze_external_location_name" {
  value = databricks_external_location.bronze.name
}

output "silver_external_location_name" {
  value = databricks_external_location.silver.name
}

output "gold_external_location_name" {
  value = databricks_external_location.gold.name
}

output "bronze_url" {
  value = local.bronze_url
}

output "silver_url" {
  value = local.silver_url
}

output "gold_url" {
  value = local.gold_url
}
