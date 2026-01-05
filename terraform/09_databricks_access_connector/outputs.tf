output "access_connector_id" {
  value       = azurerm_databricks_access_connector.main.id
  description = "ID of the Databricks access connector"
}

output "access_connector_name" {
  value       = azurerm_databricks_access_connector.main.name
  description = "Name of the Databricks access connector"
}

output "access_connector_principal_id" {
  value       = azurerm_databricks_access_connector.main.identity[0].principal_id
  description = "Managed identity principal ID for the access connector"
}
