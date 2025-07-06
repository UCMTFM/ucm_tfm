variable "name" {
  type = string
}

variable "prefix" {
  type = string
}

variable "sku"{
  type = string
  default = "trial"
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "tags" {
  type = map(string)
}

variable "azure_client_id" {
  description = "Client ID for the Azure AD application"
  type        = string
}