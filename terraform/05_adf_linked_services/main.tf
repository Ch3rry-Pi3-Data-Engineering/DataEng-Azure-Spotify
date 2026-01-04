terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
  }
}

provider "azurerm" {
  features {}
}

locals {
  sql_connection_string = "Server=tcp:${var.sql_server_fqdn},1433;Initial Catalog=${var.sql_database_name};Persist Security Info=False;User ID=${var.sql_username};Password=${var.sql_password};MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
}

resource "azurerm_data_factory_linked_service_azure_sql_database" "sql" {
  name            = var.sql_linked_service_name
  data_factory_id = var.data_factory_id

  connection_string = local.sql_connection_string
  description       = var.description
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name            = var.adls_linked_service_name
  data_factory_id = var.data_factory_id

  url         = var.storage_dfs_endpoint
  storage_account_key = var.storage_account_key
  description = var.description
}
