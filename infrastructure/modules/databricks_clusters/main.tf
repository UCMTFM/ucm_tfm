terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }
}

provider "databricks" {
  host = var.workspace_url
}

resource "databricks_cluster" "personal_camilo" {
  cluster_name            = var.cluster_name
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = var.idle_minutes
  num_workers             = 0
  single_user_name        = var.user_email

  custom_tags = {
    ResourceClass = "SingleNode"
  }
}