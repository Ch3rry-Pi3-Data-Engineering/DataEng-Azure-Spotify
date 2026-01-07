terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

data "azurerm_resource_group" "main" {
  name = var.resource_group_name
}

resource "random_pet" "storage" {
  length    = 2
  separator = ""
}

locals {
  storage_account_name = var.storage_account_name != null ? var.storage_account_name : substr("${var.storage_account_name_prefix}${random_pet.storage.id}", 0, 24)
  container_names      = toset(var.container_names)
  loop_input_path = fileexists("${path.module}/../../data_scripts/loop_input.json") ? "${path.module}/../../data_scripts/loop_input.json" : "${path.module}/../../data_scripts/loop_input.txt"
  loop_input = jsondecode(file(local.loop_input_path))
  cdc_folders = toset([for entry in local.loop_input : "${entry.table}_cdc"])
  extra_seed_folders = toset([])
  seed_folders = setunion(local.cdc_folders, local.extra_seed_folders)
  cdc_seed_files = {
    for folder in local.seed_folders :
    "${folder}/cdc.json" => "${path.module}/../../data_scripts/cdc.json"
  }
  cdc_empty_files = {
    for folder in local.seed_folders :
    "${folder}/empty.json" => "${path.module}/../../data_scripts/empty.json"
  }
  dimuser_dirs = toset([
    "DimUser",
    "DimUser/data",
    "DimUser/checkpoint",
  ])
}

resource "azurerm_storage_account" "main" {
  name                     = local.storage_account_name
  resource_group_name      = data.azurerm_resource_group.main.name
  location                 = coalesce(var.location, data.azurerm_resource_group.main.location)
  account_tier             = var.account_tier
  account_replication_type = var.account_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"
  tags                     = var.tags
}

resource "azurerm_storage_container" "medallion" {
  for_each              = local.container_names
  name                  = each.key
  storage_account_name  = azurerm_storage_account.main.name
  container_access_type = "private"
}

resource "azurerm_storage_blob" "cdc_seed" {
  for_each               = local.cdc_seed_files
  name                   = each.key
  storage_account_name   = azurerm_storage_account.main.name
  storage_container_name = azurerm_storage_container.medallion["bronze"].name
  type                   = "Block"
  source                 = each.value
  content_type           = "application/json"
}

resource "azurerm_storage_blob" "cdc_empty" {
  for_each               = local.cdc_empty_files
  name                   = each.key
  storage_account_name   = azurerm_storage_account.main.name
  storage_container_name = azurerm_storage_container.medallion["bronze"].name
  type                   = "Block"
  source                 = each.value
  content_type           = "application/json"
}

resource "azurerm_storage_data_lake_gen2_path" "silver_dimuser_dirs" {
  for_each           = local.dimuser_dirs
  path               = each.key
  filesystem_name    = azurerm_storage_container.medallion["silver"].name
  storage_account_id = azurerm_storage_account.main.id
  resource           = "directory"
}

