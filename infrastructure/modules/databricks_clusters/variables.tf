variable "prefix" {
  type        = string
  description = "Name of the project, used as a prefix for resources"
}

variable "spark_version" {
  type        = string
  description = "Spark version for the Databricks cluster."
}

variable "node_type_id" {
  type        = string
  description = "Node type ID for the Databricks cluster"
}

variable "idle_minutes" {
  type        = number
  description = "Number of idle minutes before the cluster is terminated"
}

variable "num_workers" {
  type        = number
  description = "Number of workers for the Databricks cluster"
}

variable "databricks_workspace_id" {
  type        = string
  description = "Resource ID of the Databricks workspace"
}

variable "databricks_cluster_user" {
  type        = string
  description = "User name for the single user of the Databricks cluster"  
}