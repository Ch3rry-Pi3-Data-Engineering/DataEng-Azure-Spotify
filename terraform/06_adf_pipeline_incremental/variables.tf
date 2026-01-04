variable "data_factory_id" {
  type        = string
  description = "ID of the Azure Data Factory"
}

variable "sql_linked_service_name" {
  type        = string
  description = "ADF linked service name for the Azure SQL database"
}

variable "adls_linked_service_name" {
  type        = string
  description = "ADF linked service name for ADLS Gen2"
}

variable "pipeline_name" {
  type        = string
  description = "ADF pipeline name"
  default     = "incremental_ingestion"
}

variable "cdc_dataset_name" {
  type        = string
  description = "ADF dataset name for the CDC lookup JSON"
  default     = "ds_spotify_cdc_json"
}

variable "sql_dataset_name" {
  type        = string
  description = "ADF dataset name for the SQL source"
  default     = "ds_spotify_sql_source"
}

variable "sink_dataset_name" {
  type        = string
  description = "ADF dataset name for the ADLS sink"
  default     = "ds_spotify_bronze_parquet"
}

variable "lookup_container" {
  type        = string
  description = "ADLS container for the CDC lookup file"
  default     = "bronze"
}

variable "lookup_folder" {
  type        = string
  description = "ADLS folder for the CDC lookup file"
  default     = "cdc"
}

variable "lookup_file" {
  type        = string
  description = "CDC JSON file name"
  default     = "cdc.json"
}

variable "sink_container" {
  type        = string
  description = "ADLS container for the sink dataset"
  default     = "bronze"
}

variable "sink_folder" {
  type        = string
  description = "ADLS folder for the sink dataset"
  default     = "Users"
}

variable "sink_file" {
  type        = string
  description = "Sink file name"
  default     = "@{concat(pipeline().parameters.table,'_',variables('current'))}.parquet"
}
