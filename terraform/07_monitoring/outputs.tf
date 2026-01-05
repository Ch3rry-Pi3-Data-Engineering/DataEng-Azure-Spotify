output "workspace_id" {
  value = azurerm_log_analytics_workspace.main.id
}

output "action_group_id" {
  value = azurerm_monitor_action_group.email.id
}

output "failure_alert_id" {
  value = azurerm_monitor_scheduled_query_rules_alert.pipeline_failed.id
}

output "success_alert_id" {
  value = azurerm_monitor_scheduled_query_rules_alert.pipeline_succeeded.id
}
