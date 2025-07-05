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

variable "stg_account_access_key" {
  type        = string
  description = "Primary access key for the storage account, used to create a Key Vault secret"
  sensitive   = true
}