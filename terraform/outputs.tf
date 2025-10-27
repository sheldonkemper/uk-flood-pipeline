output "resource_group_name" {
  value = azurerm_resource_group.rg.name
}
output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.dbw.workspace_url
}
