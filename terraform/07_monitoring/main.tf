terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.100"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "azurerm" {
  features {}
}

data "azurerm_resource_group" "main" {
  name = var.resource_group_name
}

resource "random_pet" "monitoring" {
  length    = 2
  separator = "-"
}

locals {
  location = coalesce(var.location, data.azurerm_resource_group.main.location)

  workspace_name = var.workspace_name != null ? var.workspace_name : "${var.workspace_name_prefix}-${random_pet.monitoring.id}"
  action_group_name = var.action_group_name != null ? var.action_group_name : "${var.action_group_name_prefix}-${random_pet.monitoring.id}"

  alert_failure_name = var.alert_failure_name != null ? var.alert_failure_name : "${var.alert_name_prefix}-pipeline-failed-${random_pet.monitoring.id}"
  alert_success_name = var.alert_success_name != null ? var.alert_success_name : "${var.alert_name_prefix}-pipeline-succeeded-${random_pet.monitoring.id}"

  failure_query = <<-KQL
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.DATAFACTORY"
| where Category == "PipelineRuns"
| extend pipeline_name = tostring(column_ifexists("pipelineName_s", column_ifexists("PipelineName_s", "")))
| extend status = tostring(column_ifexists("Status_s", column_ifexists("status_s", "")))
| where pipeline_name == "${var.pipeline_name}"
| where status == "Failed"
KQL

  success_query = <<-KQL
AzureDiagnostics
| where ResourceProvider == "MICROSOFT.DATAFACTORY"
| where Category == "PipelineRuns"
| extend pipeline_name = tostring(column_ifexists("pipelineName_s", column_ifexists("PipelineName_s", "")))
| extend status = tostring(column_ifexists("Status_s", column_ifexists("status_s", "")))
| where pipeline_name == "${var.pipeline_name}"
| where status == "Succeeded"
KQL
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = local.workspace_name
  location            = local.location
  resource_group_name = data.azurerm_resource_group.main.name
  sku                 = var.workspace_sku
  retention_in_days   = var.retention_in_days
  tags                = var.tags
}

resource "azurerm_monitor_diagnostic_setting" "data_factory" {
  name                       = "adf-pipeline-logs"
  target_resource_id         = var.data_factory_id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  log_analytics_destination_type = "AzureDiagnostics"

  log {
    category = "PipelineRuns"
    enabled  = true
  }
}

resource "azurerm_monitor_action_group" "email" {
  name                = local.action_group_name
  resource_group_name = data.azurerm_resource_group.main.name
  short_name          = var.action_group_short_name

  email_receiver {
    name          = "pipeline-alerts"
    email_address = var.email_to
  }

  tags = var.tags
}

resource "azurerm_monitor_scheduled_query_rules_alert" "pipeline_failed" {
  name                = local.alert_failure_name
  location            = local.location
  resource_group_name = data.azurerm_resource_group.main.name
  data_source_id      = azurerm_log_analytics_workspace.main.id
  description         = "Alert when the pipeline fails in the last window."
  enabled             = true
  severity            = var.alert_failure_severity
  frequency           = var.alert_frequency_minutes
  time_window         = var.alert_window_minutes
  query               = local.failure_query

  action {
    action_group = [azurerm_monitor_action_group.email.id]
  }

  trigger {
    operator  = "GreaterThan"
    threshold = 0
  }
}

resource "azurerm_monitor_scheduled_query_rules_alert" "pipeline_succeeded" {
  name                = local.alert_success_name
  location            = local.location
  resource_group_name = data.azurerm_resource_group.main.name
  data_source_id      = azurerm_log_analytics_workspace.main.id
  description         = "Alert when the pipeline succeeds in the last window."
  enabled             = true
  severity            = var.alert_success_severity
  frequency           = var.alert_frequency_minutes
  time_window         = var.alert_window_minutes
  query               = local.success_query

  action {
    action_group = [azurerm_monitor_action_group.email.id]
  }

  trigger {
    operator  = "GreaterThan"
    threshold = 0
  }
}
