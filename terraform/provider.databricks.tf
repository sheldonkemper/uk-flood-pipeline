# Enable when you start creating Databricks-managed resources (e.g., DLT).
# Using Azure CLI / OIDC auth (no PAT).
variable "databricks_host" {
  type        = string
  description = "Databricks workspace URL, e.g., https://adb-1234567890123456.19.azuredatabricks.net"
  default     = null
}

# Only configure when host is known (post-first-apply). Until then, you can comment this out.
provider "databricks" {
  host      = var.databricks_host
  auth_type = "azure-cli"
}
