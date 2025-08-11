terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.86.0"
    }
  }
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "${var.prefix}_shared_autoscaling_cluster"
  spark_version           = var.spark_version != null ? var.spark_version : data.databricks_spark_version.latest_lts.id
  node_type_id            = var.node_type_id != null ? var.node_type_id : data.databricks_node_type.smallest.id
  autotermination_minutes = var.idle_minutes
  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }
}

# resource "databricks_cluster" "single_node" {
#   cluster_name            = "Single Node"
#   spark_version           = data.databricks_spark_version.latest_lts.id
#   node_type_id            = data.databricks_node_type.smallest.id
#   autotermination_minutes = 20
#   is_single_node          = true
#   kind                    = "CLASSIC_PREVIEW"
# }
