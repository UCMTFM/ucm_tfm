variable "databricks_host" {
  type = string
}

variable "workspace_resource_id" {
  type = string
}

variable "prefix" {
  type = string
}

variable "access_connector_id" {
  type = string
}

variable "lakehouse_external_layers" {
  type    = list(string)
  default = ["bronze", "silver", "gold"]
}

variable "lakehouse_storage_account_name" {
  type = string
}

# variable "key_vault_id" {
#   type = string
# }

# variable "key_vault_uri" {
#   type = string
# }