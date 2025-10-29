output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}

output "databricks_workspace_id" {
  value = azurerm_databricks_workspace.dbw.id
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.dbw.workspace_url
  description = "Copy this into dev.tfvars as databricks_host when you enable the databricks provider."
}
