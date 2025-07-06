variable "name" {
  type = string
}

variable "prefix" {
  type = string
}

variable "sku"{
  type = string
  default = "standard"
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

variable "member_ids" {
  type        = list(string)
  description = "List of object IDs of users who will be assigned the Key Vault Secrets Officer role"
}

variable "lakehouse_stg_account_key" {
  type        = string
  description = "Primary access key for the storage account, used to create a Key Vault secret"
  sensitive   = true
}

variable "landing_stg_account_key" {
  type        = string
  description = "Primary access key for the storage account, used to create a Key Vault secret"
  sensitive   = true
}

variable "access_connector_id" {
  type        = string
  description = "ID of the Databricks Access Connector"
}