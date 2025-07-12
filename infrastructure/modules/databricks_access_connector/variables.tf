variable "prefix" {
  type = string
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

variable "storage_account_id" {
  type        = string
  description = "The resource ID of the Storage Account"
}