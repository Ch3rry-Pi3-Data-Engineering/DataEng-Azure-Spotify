terraform {
  required_version = ">= 1.5"

  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.48"
    }
  }
}

provider "databricks" {
  host          = var.databricks_host
  account_id    = var.databricks_account_id
  client_id     = var.databricks_client_id
  client_secret = var.databricks_client_secret
}

locals {
  bronze_url = "abfss://${var.bronze_container}@${var.storage_account_name}.dfs.core.windows.net/"
  silver_url = "abfss://${var.silver_container}@${var.storage_account_name}.dfs.core.windows.net/"
  gold_url   = "abfss://${var.gold_container}@${var.storage_account_name}.dfs.core.windows.net/"
  catalog_storage_root = coalesce(var.catalog_storage_root, local.silver_url)
}

resource "databricks_catalog" "main" {
  name    = var.catalog_name
  comment = "Spotify data catalog"
  storage_root = local.catalog_storage_root

  depends_on = [databricks_external_location.silver]
}

resource "databricks_storage_credential" "managed_identity" {
  name = var.storage_credential_name

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "bronze" {
  name            = var.bronze_location_name
  url             = local.bronze_url
  credential_name = databricks_storage_credential.managed_identity.name
  comment         = "Bronze medallion data"
  force_destroy   = true
}

resource "databricks_external_location" "silver" {
  name            = var.silver_location_name
  url             = local.silver_url
  credential_name = databricks_storage_credential.managed_identity.name
  comment         = "Silver medallion data"
  force_destroy   = true
}

resource "databricks_external_location" "gold" {
  name            = var.gold_location_name
  url             = local.gold_url
  credential_name = databricks_storage_credential.managed_identity.name
  comment         = "Gold medallion data"
  force_destroy   = true
}

resource "databricks_schema" "silver" {
  name         = var.schema_name
  catalog_name = databricks_catalog.main.name
  storage_root = local.silver_url
  comment      = "Silver schema"
  force_destroy = true

  depends_on = [databricks_external_location.silver]
}

resource "databricks_schema" "gold" {
  name         = var.gold_schema_name
  catalog_name = databricks_catalog.main.name
  storage_root = local.gold_url
  comment      = "Gold schema"
  force_destroy = true

  depends_on = [databricks_external_location.gold]
}
