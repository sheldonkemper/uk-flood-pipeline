variable "environment" {
  type        = string
  description = "Environment name, e.g., dev or prod"
}

variable "location" {
  type        = string
  description = "Azure region, e.g., uksouth"
  default     = "uksouth"
}
