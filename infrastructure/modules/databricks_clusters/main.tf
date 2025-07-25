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
#   cluster_name  = "single-node-${var.prefix}"
#   spark_version = var.spark_version
#   node_type_id  = var.node_type_id
#   policy_id     = databricks_cluster_policy.single_node_single_user.id
# }
