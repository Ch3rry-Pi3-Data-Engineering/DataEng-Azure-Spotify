variable "resource_group_name" {
  type        = string
  description = "Resource group name for monitoring resources"
}

variable "location" {
  type        = string
  description = "Azure region for monitoring resources"
  default     = null
}

variable "data_factory_id" {
  type        = string
  description = "ID of the Azure Data Factory to monitor"
}

variable "pipeline_name" {
  type        = string
  description = "ADF pipeline name to alert on"
  default     = "incremental_ingestion_arm"
}

variable "email_to" {
  type        = string
  description = "Email address for alert notifications"
  default     = "the_rfc@hotmail.co.uk"
}

variable "workspace_name" {
  type        = string
  description = "Log Analytics workspace name (if null, uses prefix + random suffix)"
  default     = null
}

variable "workspace_name_prefix" {
  type        = string
  description = "Prefix used to build the workspace name when workspace_name is null"
  default     = "law-spotify"
}

variable "workspace_sku" {
  type        = string
  description = "Log Analytics workspace SKU"
  default     = "PerGB2018"
}

variable "retention_in_days" {
  type        = number
  description = "Log Analytics retention in days"
  default     = 30
}

variable "action_group_name" {
  type        = string
  description = "Action group name (if null, uses prefix + random suffix)"
  default     = null
}

variable "action_group_name_prefix" {
  type        = string
  description = "Prefix used to build the action group name when action_group_name is null"
  default     = "ag-spotify"
}

variable "action_group_short_name" {
  type        = string
  description = "Short name for the action group (1-12 chars)"
  default     = "agspot"
}

variable "alert_name_prefix" {
  type        = string
  description = "Prefix used to build alert rule names"
  default     = "alert-spotify"
}

variable "alert_failure_name" {
  type        = string
  description = "Alert rule name for failures (if null, uses prefix + random suffix)"
  default     = null
}

variable "alert_success_name" {
  type        = string
  description = "Alert rule name for successes (if null, uses prefix + random suffix)"
  default     = null
}

variable "alert_frequency_minutes" {
  type        = number
  description = "How often the alert query runs (minutes)"
  default     = 5
}

variable "alert_window_minutes" {
  type        = number
  description = "Lookback window for alert query (minutes)"
  default     = 5
}

variable "alert_failure_severity" {
  type        = number
  description = "Severity for failure alert (0-4)"
  default     = 2
}

variable "alert_success_severity" {
  type        = number
  description = "Severity for success alert (0-4)"
  default     = 3
}

variable "tags" {
  type        = map(string)
  description = "Tags applied to monitoring resources"
  default     = {}
}
