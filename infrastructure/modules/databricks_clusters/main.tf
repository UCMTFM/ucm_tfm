terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.86.0"
    }
  }
}

# resource "databricks_cluster_policy" "single_node_single_user" {
#   name = "Single Node Single User Policy"
#
#   definition = jsonencode({
#     "num_workers": {
#       "type": "fixed",
#       "value": "0"
#     },
#     "spark_conf.spark.databricks.cluster.profile": {
#       "type": "fixed",
#       "value": "singleNode"
#     },
#     "spark_conf.spark.master": {
#       "type": "fixed",
#       "value": "local[*]"
#     },
#     "data_security_mode": {
#       "type": "fixed",
#       "value": "SINGLE_USER"
#     },
#     "single_user_name": {
#       "type": "fixed",
#       "value": var.databricks_cluster_user
#     },
#     "autotermination_minutes": {
#       "type": "fixed",
#       "value": tostring(var.idle_minutes)
#     }
#   })
# }
#
# resource "databricks_cluster" "shared_compute_cluster" {
#   provider      = databricks
#   cluster_name  = "single-node-${var.prefix}"
#   spark_version = var.spark_version
#   node_type_id  = var.node_type_id
#   policy_id     = databricks_cluster_policy.single_node_single_user.id
# }

data "databricks_node_type" "smallest" {
  local_disk = true
}

data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

resource "databricks_cluster" "single_node" {
  cluster_name            = "Single Node"
  spark_version           = data.databricks_spark_version.latest_lts.id
  node_type_id            = data.databricks_node_type.smallest.id
  autotermination_minutes = 20
  is_single_node          = true
  kind                    = "CLASSIC_PREVIEW"
}
