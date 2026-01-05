output "databricks_workspace_id" {
  value       = azurerm_databricks_workspace.main.id
  description = "ID of the Databricks workspace"
}

output "databricks_workspace_name" {
  value       = azurerm_databricks_workspace.main.name
  description = "Name of the Databricks workspace"
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.main.workspace_url
  description = "URL of the Databricks workspace"
}

output "databricks_managed_resource_group_name" {
  value       = azurerm_databricks_workspace.main.managed_resource_group_name
  description = "Managed resource group name for the Databricks workspace"
}
