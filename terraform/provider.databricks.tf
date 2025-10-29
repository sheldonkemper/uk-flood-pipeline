# Enable when you start creating Databricks-managed resources (e.g., DLT).
# Databricks provider via Azure AD (uses your `az login` context).
provider "databricks" {
  host  = var.databricks_workspace_url
  token = var.databricks_token
}
