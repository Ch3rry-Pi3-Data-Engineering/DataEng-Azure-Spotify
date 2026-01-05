output "pipeline_name" {
  value = azapi_resource.pipeline.name
}

output "cdc_dataset_name" {
  value = azurerm_data_factory_dataset_json.cdc.name
}

output "sql_dataset_name" {
  value = azurerm_data_factory_dataset_azure_sql_table.sql.name
}

output "sink_dataset_name" {
  value = azurerm_data_factory_dataset_parquet.adls_sink.name
}
