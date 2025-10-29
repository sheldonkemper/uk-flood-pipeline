terraform {
  required_version = ">= 1.6.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.117"
    }
    # The Databricks provider is optional for now since we don't create Databricks-managed resources yet.
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}
