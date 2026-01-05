variable "resource_group_name" {
  type        = string
  description = "Resource group name for the access connector"
}

variable "location" {
  type        = string
  description = "Azure region for the access connector (defaults to RG location if null)"
  default     = null
}

variable "storage_account_id" {
  type        = string
  description = "Storage account ID for role assignment scope"
}

variable "access_connector_name" {
  type        = string
  description = "Access connector name (if null, uses access_connector_name_prefix + random suffix)"
  default     = null
}

variable "access_connector_name_prefix" {
  type        = string
  description = "Prefix used to build the access connector name when access_connector_name is null"
  default     = "dbc-spotify-ac"
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to the access connector"
  default     = {}
}
