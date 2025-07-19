terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }
}

provider "databricks" {
  azure_workspace_resource_id = var.databricks_workspace_id
}

resource "databricks_cluster" "shared_compute_cluster" {
  cluster_name            = "cluster-${var.prefix}"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  autotermination_minutes = var.idle_minutes
  # data_security_mode      = "SINGLE_USER"
  num_workers             = var.num_workers
  # single_user_name        = var.databricks_cluster_user

  spark_conf = {
    "spark.databricks.cluster.profile" = "singleNode"
    "spark.master"                     = "local[*]"
  }

  lifecycle {
    create_before_destroy = false
    prevent_destroy       = false
  }
}