# Current working Databricks provider via PAT (kept until OIDC verified)
provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}

# Future Databricks provider via Azure OIDC (do not enable yet)
# provider "databricks" {
#   host                        = var.databricks_workspace_url
#   azure_client_id             = var.azure_client_id
#   azure_tenant_id             = var.azure_tenant_id
#   azure_workspace_resource_id = var.databricks_workspace_id
# }
