variable "project" {
  description = "Unique identifier for resources"
  type        = string
}

variable "location" {
  description = "Azure region to deploy resources"
  type        = string
  default     = "North EU"
}

variable "tags" {
  description = "Common base tags for resources"
  type        = map(string)
  default     = { environment = "development", created_by = "terraform" }
}

variable "group_members" {
  description = "List of user principal names to include in the AAD group"
  type        = list(string)
}

variable "azure_client_id" {
  type = string
  sensitive = true
  description = "Azure Client ID for the service principal"
}

variable "lakehouse_directories" {
  description = "List of directories to be created in the lakhouse Storage Account"
  type        = list(string)
}

variable "landing_directories" {
  description = "List of directories to be created in the landing Storage Account"
  type        = list(string)
}