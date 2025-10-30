# ======================================================
# Environment
# ======================================================
variable "environment" {
  type        = string
  description = "Environment name, e.g., dev or prod"
}

variable "location" {
  type        = string
  description = "Azure region for deployment"
  default     = "uksouth"
}

# ======================================================
# Azure / Databricks workspace context
# ======================================================
variable "resource_group_name" {
  type        = string
  description = "Azure resource group containing the Databricks workspace"
  default     = "rg-uksouth-flood-dev"
}

# Workspace URL (works for both PAT and OIDC)
variable "databricks_workspace_url" {
  type        = string
  description = "Workspace host URL (https://adb-xxx.azuredatabricks.net)"
}

# Full Azure resource ID of the Databricks workspace (required for OIDC)
variable "databricks_workspace_id" {
  type        = string
  default     = ""
  description = "Full Azure resource ID for the Databricks workspace"
}

# ======================================================
# OIDC for Databricks (future cutover)
# ======================================================
variable "azure_client_id" {
  type        = string
  default     = ""
  description = "Azure AD app client ID for OIDC auth to Databricks"
}

variable "azure_tenant_id" {
  type        = string
  default     = ""
  description = "Azure AD tenant ID for OIDC auth"
}

# ======================================================
# PAT authentication (temporary fallback)
# ======================================================
variable "databricks_token" {
  type        = string
  description = "Personal Access Token for Databricks authentication (temporary auth method)"
  default     = ""
}

# ======================================================
# Cluster configuration (single-user dev)
# ======================================================
variable "cluster_name" {
  type        = string
  description = "Name of the single-user development cluster"
  default     = "single-user-dev"
}

variable "spark_version" {
  type        = string
  description = "Databricks Runtime version used by the cluster (must match Databricks Connect)"
  default     = "13.3.x-scala2.12"
}

variable "node_type_id" {
  type        = string
  description = "Optional explicit node type; leave empty to auto-select the smallest local-disk VM"
  default     = ""
}

variable "num_workers" {
  type        = number
  description = "Number of worker nodes (0 = single-node cluster)"
  default     = 0
}

variable "autotermination_minutes" {
  type        = number
  description = "Cluster auto-termination time in minutes to save cost"
  default     = 20
}

variable "cluster_user_email" {
  type        = string
  description = "Workspace user who owns the SINGLE_USER cluster"
  default     = "sheldon.kemper@outlook.com"
}
