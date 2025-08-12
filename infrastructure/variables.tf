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
  type        = string
  sensitive   = true
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

variable "databricks_cluster_user" {
  type        = string
  description = "User name for the single user of the Databricks cluster"
}

variable "databricks_location" {
  type        = string
  description = "Location for the Databricks workspace"
}

variable "git_repo_https_url" {
  type        = string
  description = "Git repository URL for the project"
}
