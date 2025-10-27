variable "environment" {
  type = string
}

variable "location" {
  type    = string
  default = "uksouth"
}

variable "project_name" {
  type    = string
  default = "flood-monitoring"
}

variable "resource_group" {
  type    = string
  default = null
}

variable "workspace_name" {
  type    = string
  default = null
}

variable "tags" {
  type    = map(string)
  default = {}
}

variable "databricks_host" {
  type = string 
}

variable "databricks_token" {
  type = string, 
  sensitive = true 
}

