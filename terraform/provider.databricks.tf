provider "databricks" {
  host      = var.databricks_workspace_url
  token     = var.databricks_token
  auth_type = "pat"

  azure_client_id             = null
  azure_tenant_id             = null
  azure_workspace_resource_id = null
}
