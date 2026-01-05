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

resource "random_pet" "databricks" {
  length    = 2
  separator = "-"
}

locals {
  location = coalesce(var.location, data.azurerm_resource_group.main.location)

  workspace_name = var.workspace_name != null ? var.workspace_name : "${var.workspace_name_prefix}-${random_pet.databricks.id}"
  managed_rg_name = var.managed_resource_group_name != null ? var.managed_resource_group_name : "${var.managed_resource_group_name_prefix}-${random_pet.databricks.id}"
}

resource "azurerm_databricks_workspace" "main" {
  name                        = local.workspace_name
  resource_group_name         = data.azurerm_resource_group.main.name
  location                    = local.location
  sku                         = var.sku
  managed_resource_group_name = local.managed_rg_name
  tags                        = var.tags
}
