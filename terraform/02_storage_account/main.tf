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

resource "azurerm_storage_blob" "cdc_json" {
  name                   = "cdc/cdc.json"
  storage_account_name   = azurerm_storage_account.main.name
  storage_container_name = azurerm_storage_container.medallion["bronze"].name
  type                   = "Block"
  source                 = "${path.module}/../../data_scripts/cdc.json"
  content_type           = "application/json"
}

resource "azurerm_storage_blob" "cdc_empty_json" {
  name                   = "cdc/empty.json"
  storage_account_name   = azurerm_storage_account.main.name
  storage_container_name = azurerm_storage_container.medallion["bronze"].name
  type                   = "Block"
  source                 = "${path.module}/../../data_scripts/empty.json"
  content_type           = "application/json"
}

