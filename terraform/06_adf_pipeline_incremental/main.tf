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
  sql_reader_query = "SELECT * FROM @{item().schema}.@{item().table} WHERE @{item().cdc_col} > '@{if(empty(item().from_date),activity('last_cdc').output.value[0].cdc,item().from_date)}'"
  max_cdc_query    = "SELECT MAX(@{item().cdc_col}) as cdc FROM @{item().schema}.@{item().table}"

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

  cdc_empty_dataset_params = {
    container = "bronze"
    folder    = "@{item().table}_cdc"
    file      = "empty.json"
  }

  cdc_latest_dataset_params = {
    container = "bronze"
    folder    = "@{item().table}_cdc"
    file      = "cdc.json"
  }

  sql_dataset_params = {
    schema = "@{item().schema}"
    table  = "@{item().table}"
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
    loop_input = "Array"
  }

  variables = {
    current = "String"
  }

  activities_json = jsonencode([
    {
      name = "for_each_table"
      type = "ForEach"
      typeProperties = {
        isSequential = true
        items = {
          type  = "Expression"
          value = "@pipeline().parameters.loop_input"
        }
        activities = [
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
          },
          {
            name = "if_incremental_data"
            type = "IfCondition"
            dependsOn = [
              {
                activity             = "sql_to_datalake"
                dependencyConditions = ["Succeeded"]
              }
            ]
            typeProperties = {
              expression = {
                type  = "Expression"
                value = "@greater(activity('sql_to_datalake').output.dataRead, 0)"
              }
              ifTrueActivities = [
                {
                  name = "max_cdc"
                  type = "Script"
                  linkedServiceName = {
                    referenceName = var.sql_linked_service_name
                    type          = "LinkedServiceReference"
                  }
                  typeProperties = {
                    scripts = [
                      {
                        type = "Query"
                        text = local.max_cdc_query
                      }
                    ]
                  }
                },
                {
                  name = "update_last_cdc"
                  type = "Copy"
                  dependsOn = [
                    {
                      activity             = "max_cdc"
                      dependencyConditions = ["Succeeded"]
                    }
                  ]
                  inputs = [
                    {
                      referenceName = azurerm_data_factory_dataset_json.cdc.name
                      type          = "DatasetReference"
                      parameters    = local.cdc_empty_dataset_params
                    }
                  ]
                  outputs = [
                    {
                      referenceName = azurerm_data_factory_dataset_json.cdc.name
                      type          = "DatasetReference"
                      parameters    = local.cdc_latest_dataset_params
                    }
                  ]
                  typeProperties = {
                    source = {
                      type = "JsonSource"
                      additionalColumns = [
                        {
                          name  = "cdc"
                          value = "@activity('max_cdc').output.resultSets[0].rows[0].cdc"
                        }
                      ]
                    }
                    sink = {
                      type = "JsonSink"
                    }
                  }
                }
              ]
              ifFalseActivities = [
                {
                  name = "delete_empty_file"
                  type = "Delete"
                  typeProperties = {
                    dataset = {
                      referenceName = azurerm_data_factory_dataset_parquet.adls_sink.name
                      type          = "DatasetReference"
                      parameters    = local.sink_dataset_params
                    }
                    enableLogging = true
                    logStorageSettings = {
                      linkedServiceName = {
                        referenceName = var.adls_linked_service_name
                        type          = "LinkedServiceReference"
                      }
                      path = ""
                    }
                  }
                }
              ]
            }
          }
        ]
      }
    }
  ])
}
