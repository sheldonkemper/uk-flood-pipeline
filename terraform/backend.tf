terraform {
  backend "azurerm" {
    resource_group_name  = "rg-uksouth-flood-dev"
    storage_account_name = "tfstateuksouthdev"
    container_name       = "tfstate"
    key                  = "uk-flood-dev.terraform.tfstate"
  }
}
