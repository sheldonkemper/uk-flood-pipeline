# ------------------------------------------------------------
# ADLS Gen2 storage account for Unity Catalog managed tables
# ------------------------------------------------------------
resource "azurerm_storage_account" "flood_uc" {
  name                     = "flooducuksouthdev"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  # Required for Unity Catalog (ADLS Gen2)
  is_hns_enabled                  = true
  allow_nested_items_to_be_public = false

  tags = local.default_tags
}

# Container must be 3–63 characters, lowercase letters, numbers, and hyphens only
resource "azurerm_storage_container" "uc" {
  name                  = "uc-flood-dev"
  storage_account_name  = azurerm_storage_account.flood_uc.name
  container_access_type = "private"
}

# ------------------------------------------------------------
# Reference existing Unity Catalog Access Connector
# ------------------------------------------------------------
data "azurerm_databricks_access_connector" "flood" {
  name                = "unity-catalog-access-connector"
  resource_group_name = "databricks-rg-rg-uksouth-flood-dev"
}

# ------------------------------------------------------------
# Grant Access Connector permissions to the storage account
# ------------------------------------------------------------
resource "azurerm_role_assignment" "uc_contrib" {
  scope                = azurerm_storage_account.flood_uc.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_databricks_access_connector.flood.identity[0].principal_id
}

# ------------------------------------------------------------
# Databricks resources for Unity Catalog integration
# ------------------------------------------------------------

# 1. Storage credential for UC (uses Access Connector identity)
resource "databricks_storage_credential" "flood_uc" {
  name = "flood_uc_cred"

  azure_managed_identity {
    access_connector_id = data.azurerm_databricks_access_connector.flood.id
  }

  comment = "Credential for ADLS Gen2 storage account flooducuksouthdev"
}

# 2. External location (maps UC credential to ADLS path)
resource "databricks_external_location" "flood_uc" {
  name            = "flood_uc_location"
  url             = "abfss://${azurerm_storage_container.uc.name}@${azurerm_storage_account.flood_uc.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.flood_uc.name
  comment         = "External location for Unity Catalog managed tables"

  depends_on = [
    azurerm_role_assignment.uc_contrib
  ]
}
