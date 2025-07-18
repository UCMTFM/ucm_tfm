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

locals {
  node_type_hash = substr(md5(var.node_type_id), 0, 6)
}

data "databricks_cluster_policy" "shared_compute" {
  name = "Shared Compute"
}

resource "databricks_cluster" "shared_compute_cluster" {
  cluster_name            = "cluster-${var.prefix}-${local.node_type_hash}"
  spark_version           = var.spark_version
  node_type_id            = var.node_type_id
  policy_id               = data.databricks_cluster_policy.shared_compute.id

  autotermination_minutes = var.idle_minutes

  autoscale {
    min_workers = var.num_workers
    max_workers = var.num_workers
  }

  data_security_mode      = "USER_ISOLATION"

  lifecycle {
    create_before_destroy = false
    prevent_destroy       = false
  }
}