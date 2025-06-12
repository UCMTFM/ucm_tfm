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
