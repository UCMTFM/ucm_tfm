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

variable "container_name" {
  type = string
}

variable "lakehouse_storage_account_name" {
  type = string
}

variable "admin_group_name" {
  type = string
}