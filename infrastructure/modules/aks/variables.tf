variable "name" {
  type = string
}

variable "prefix" {
  type = string
}

variable "sku_tier" {
  type    = string
  default = "Free"
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
