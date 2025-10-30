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

variable "databricks_workspace_url" {
  type        = string
  description = "Workspace host URL (https://adb-xxx.azuredatabricks.net)"
}

# ======================================================
# PAT authentication
# ======================================================
variable "databricks_token" {
  type        = string
  description = "Personal Access Token for Databricks authentication"
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
  description = "Databricks Runtime version used by the cluster"
  default     = "13.3.x-scala2.12"
}

variable "node_type_id" {
  type        = string
  description = "Optional explicit node type"
  default     = ""
}

variable "num_workers" {
  type        = number
  description = "Number of worker nodes"
  default     = 0
}

variable "autotermination_minutes" {
  type        = number
  description = "Cluster auto-termination time"
  default     = 10
}

variable "cluster_user_email" {
  type        = string
  description = "Workspace user for SINGLE_USER mode"
  default     = "sheldon.kemper@outlook.com"
}
