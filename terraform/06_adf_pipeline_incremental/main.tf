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
  sql_reader_query = "SELECT * FROM @{pipeline().parameters.schema}.@{pipeline().parameters.table} WHERE @{pipeline().parameters.cdc_col} > '@{activity('last_cdc').output.value[0].cdc}'"

  sql_linked_service_id  = "${var.data_factory_id}/linkedservices/${var.sql_linked_service_name}"

  lookup_dataset_params = {
    container = var.lookup_container
    folder    = var.lookup_folder
    file      = var.lookup_file
  }

  sink_dataset_params = {
    container = var.sink_container
    folder    = var.sink_folder
    file      = var.sink_file
  }

  sql_dataset_params = {
    schema = "@{pipeline().parameters.schema}"
    table  = "@{pipeline().parameters.table}"
  }
}

resource "azurerm_data_factory_dataset_json" "cdc" {
  name                = var.cdc_dataset_name
  data_factory_id     = var.data_factory_id
  linked_service_name = var.adls_linked_service_name
  encoding            = "UTF-8"

  parameters = {
    container = "String"
    folder    = "String"
    file      = "String"
  }

  azure_blob_storage_location {
    container                = "@{dataset().container}"
    path                     = "@{dataset().folder}"
    filename                 = "@{dataset().file}"
    dynamic_container_enabled = true
    dynamic_path_enabled      = true
    dynamic_filename_enabled  = true
  }
}

resource "azurerm_data_factory_dataset_parquet" "adls_sink" {
  name                = var.sink_dataset_name
  data_factory_id     = var.data_factory_id
  linked_service_name = var.adls_linked_service_name
  compression_codec   = "snappy"

  parameters = {
    container = "String"
    folder    = "String"
    file      = "String"
  }

  azure_blob_fs_location {
    file_system               = "@{dataset().container}"
    path                      = "@{dataset().folder}"
    filename                  = "@{dataset().file}"
    dynamic_file_system_enabled = true
    dynamic_path_enabled        = true
    dynamic_filename_enabled    = true
  }
}

resource "azurerm_data_factory_dataset_azure_sql_table" "sql" {
  name                = var.sql_dataset_name
  data_factory_id     = var.data_factory_id
  linked_service_id   = local.sql_linked_service_id

  parameters = {
    schema = "String"
    table  = "String"
  }

  schema = "@{dataset().schema}"
  table  = "@{dataset().table}"
}

resource "azurerm_data_factory_pipeline" "incremental" {
  name            = var.pipeline_name
  data_factory_id = var.data_factory_id

  parameters = {
    schema  = "String"
    table   = "String"
    cdc_col = "String"
  }

  variables = {
    current = "String"
  }

  activities_json = jsonencode([
    {
      name = "last_cdc"
      type = "Lookup"
      typeProperties = {
        source = {
          type = "JsonSource"
        }
        dataset = {
          referenceName = azurerm_data_factory_dataset_json.cdc.name
          type          = "DatasetReference"
          parameters    = local.lookup_dataset_params
        }
        firstRowOnly = false
      }
    },
    {
      name = "current_time"
      type = "SetVariable"
      typeProperties = {
        variableName = "current"
        value        = "@utcNow()"
      }
    },
    {
      name = "sql_to_datalake"
      type = "Copy"
      dependsOn = [
        {
          activity             = "last_cdc"
          dependencyConditions = ["Succeeded"]
        },
        {
          activity             = "current_time"
          dependencyConditions = ["Succeeded"]
        }
      ]
      inputs = [
        {
          referenceName = azurerm_data_factory_dataset_azure_sql_table.sql.name
          type          = "DatasetReference"
          parameters    = local.sql_dataset_params
        }
      ]
      outputs = [
        {
          referenceName = azurerm_data_factory_dataset_parquet.adls_sink.name
          type          = "DatasetReference"
          parameters    = local.sink_dataset_params
        }
      ]
      typeProperties = {
        source = {
          type           = "SqlSource"
          sqlReaderQuery = local.sql_reader_query
        }
        sink = {
          type = "ParquetSink"
        }
      }
    }
  ])
}
