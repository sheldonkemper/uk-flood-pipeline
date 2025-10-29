locals {
  rg_name         = "rg-uksouth-flood-dev"
  workspace_name  = "dbw-flood-dev"
  managed_rg_name = "rg-uksouth-flood-dev-dbricks" # informational
  default_tags = {
    project     = "flood-monitoring"
    environment = var.environment
    owner       = "sheldon"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = local.rg_name
  location = var.location
  tags     = local.default_tags
}

resource "azurerm_databricks_workspace" "dbw" {
  name                = local.workspace_name
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  sku                 = "premium"

  public_network_access_enabled     = true
  customer_managed_key_enabled      = false
  infrastructure_encryption_enabled = false

  tags = local.default_tags
}
