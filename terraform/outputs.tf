output "resource_group_name" {
  value       = azurerm_resource_group.rg.name
  description = "Azure resource group created for the workspace."
}

output "databricks_workspace_id" {
  value       = azurerm_databricks_workspace.dbw.id
  description = "Full Azure resource ID for the Databricks workspace. Paste into envs/<env>.tfvars for the Databricks provider."
}

output "databricks_workspace_url" {
  value       = azurerm_databricks_workspace.dbw.workspace_url
  description = "Databricks workspace host URL. Paste into envs/<env>.tfvars as databricks_workspace_url if you use PAT auth."
}
