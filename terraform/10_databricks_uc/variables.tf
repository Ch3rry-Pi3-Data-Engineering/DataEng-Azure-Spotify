variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL (https://adb-<id>.<region>.azuredatabricks.net)."
}

variable "databricks_account_id" {
  type        = string
  description = "Databricks account ID for OAuth authentication."
}

variable "databricks_client_id" {
  type        = string
  description = "Databricks OAuth client ID."
}

variable "databricks_client_secret" {
  type        = string
  description = "Databricks OAuth client secret."
  sensitive   = true
}

variable "databricks_workspace_resource_id" {
  type        = string
  description = "Optional: Databricks workspace ARM resource ID."
  default     = null
}

variable "access_connector_id" {
  type        = string
  description = "Azure Databricks access connector resource ID."
}

variable "storage_account_name" {
  type        = string
  description = "Storage account name for external locations."
}

variable "catalog_name" {
  type        = string
  description = "Unity Catalog catalog name."
}

variable "schema_name" {
  type        = string
  description = "Unity Catalog schema name."
}

variable "gold_schema_name" {
  type        = string
  description = "Unity Catalog gold schema name."
  default     = "gold"
}

variable "catalog_storage_root" {
  type        = string
  description = "Optional managed location for the catalog (abfss://...). Defaults to the silver container."
  default     = null
}

variable "storage_credential_name" {
  type        = string
  description = "Storage credential name."
}

variable "bronze_location_name" {
  type        = string
  description = "External location name for bronze container."
}

variable "silver_location_name" {
  type        = string
  description = "External location name for silver container."
}

variable "gold_location_name" {
  type        = string
  description = "External location name for gold container."
  default     = "gold"
}

variable "bronze_container" {
  type        = string
  description = "Bronze container name."
  default     = "bronze"
}

variable "silver_container" {
  type        = string
  description = "Silver container name."
  default     = "silver"
}

variable "gold_container" {
  type        = string
  description = "Gold container name."
  default     = "gold"
}
