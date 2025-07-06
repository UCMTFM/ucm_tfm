variable "prefix" {
  type        = string
  description = "Name of the project, used as a prefix for resources"
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

variable "num_workers" {
  type        = number
  description = "Number of workers for the Databricks cluster"
}

variable "workspace_url" {
  type        = string
  description = "URL of the Databricks workspace"
}