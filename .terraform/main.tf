locals {
  rg_name = coalesce(var.resource_group, "rg-${var.location}-${var.project_name}-${var.environment}")
  ws_name = coalesce(var.workspace_name, "dbw-${var.project_name}-${var.environment}")
}

resource "azurerm_resource_group" "rg" {
  name     = local.rg_name
  location = var.location
  tags     = var.tags
}

resource "azurerm_databricks_workspace" "dbw" {
  name                        = local.ws_name
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "${local.rg_name}-dbricks"
  tags                        = var.tags
}
