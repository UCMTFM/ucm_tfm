variable "cluster_name" {
  type        = string
  description = "Name of the Databricks cluster"
}

variable "spark_version" {
  type        = string
  description = "Spark version for the Databricks cluster"
}

variable "node_type_id" {
  type        = string
  description = "Node type ID for the Databricks cluster"
}

variable "idle_minutes" {
  type        = number
  description = "Number of idle minutes before the cluster is terminated"
}

variable "user_email" {
  type        = string
  description = "Email of the user who will own the cluster"
}

variable "workspace_url" {
  type        = string
  description = "URL of the Databricks workspace"
}