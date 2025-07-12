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

variable "agent_vm_size" {
  type    = string
  default = "Standard_DS2_v3"
}

variable "temporary_name_for_rotation" {
  type    = string
  default = "tmpnodepoolforvmresizing"
}

variable "agents_count" {
  type    = number
  default = null
}

variable "enable_autoscaling" {
  type    = bool
  default = true
}

variable "agents_max_count" {
  type    = number
  default = 2
}

variable "agents_min_count" {
  type    = number
  default = 1
}

variable "auto_scaler_profile_enabled" {
  type    = bool
  default = true
}
