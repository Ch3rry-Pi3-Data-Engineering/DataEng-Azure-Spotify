variable "resource_group_name" {
  type        = string
  description = "Resource group name for the Databricks workspace"
}

variable "location" {
  type        = string
  description = "Azure region for the Databricks workspace"
  default     = null
}

variable "workspace_name" {
  type        = string
  description = "Databricks workspace name (if null, uses workspace_name_prefix + random suffix)"
  default     = null
}

variable "workspace_name_prefix" {
  type        = string
  description = "Prefix used to build the workspace name when workspace_name is null"
  default     = "dbw-spotify"
}

variable "managed_resource_group_name" {
  type        = string
  description = "Managed resource group name (if null, uses managed_resource_group_name_prefix + random suffix)"
  default     = null
}

variable "managed_resource_group_name_prefix" {
  type        = string
  description = "Prefix used to build the managed resource group name when managed_resource_group_name is null"
  default     = "rg-databricks-spotify"
}

variable "sku" {
  type        = string
  description = "Databricks workspace SKU"
  default     = "premium"
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to the Databricks workspace"
  default     = {}
}
