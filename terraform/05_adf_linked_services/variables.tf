variable "data_factory_id" {
  type        = string
  description = "Data Factory ID that owns the linked service"
}

variable "sql_linked_service_name" {
  type        = string
  description = "Name of the Azure SQL Database linked service"
  default     = "lsqldb-spotify-dev"
}

variable "sql_server_fqdn" {
  type        = string
  description = "SQL Server FQDN (server.database.windows.net)"
}

variable "sql_database_name" {
  type        = string
  description = "SQL database name"
}

variable "sql_username" {
  type        = string
  description = "SQL authentication username"
}

variable "sql_password" {
  type        = string
  description = "SQL authentication password"
  sensitive   = true
}

variable "adls_linked_service_name" {
  type        = string
  description = "Name of the ADLS Gen2 linked service"
  default     = "lsadls-spotify"
}

variable "storage_dfs_endpoint" {
  type        = string
  description = "ADLS Gen2 DFS endpoint (https://<account>.dfs.core.windows.net)"
}

variable "storage_account_key" {
  type        = string
  description = "Storage account key for ADLS Gen2 linked service"
  sensitive   = true
}

variable "description" {
  type        = string
  description = "Linked service description"
  default     = "Linked services for Azure SQL Database and ADLS Gen2"
}
