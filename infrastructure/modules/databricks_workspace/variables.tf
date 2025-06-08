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
