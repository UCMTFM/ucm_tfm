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

resource "databricks_cluster_policy" "single_node_policy" {
  name = "Single Node Policy"

  definition = jsonencode({
    "num_workers" : {
      "type" : "fixed",
      "value" : "0"
    },
    "spark_conf.spark.databricks.cluster.profile" : {
      "type" : "fixed",
      "value" : "singleNode"
    },
    "spark_conf.spark.master" : {
      "type" : "fixed",
      "value" : "local[*]"
    },
    "data_security_mode" : {
      "type" : "fixed",
      "value" : "SINGLE_USER"
    },
    "single_user_name" : {
      "type" : "fixed",
      "value" : var.databricks_cluster_user
    }
  })
}

resource "databricks_cluster" "shared_compute_cluster" {
  cluster_name  = "cluster-${var.prefix}"
  policy_id     = databricks_cluster_policy.single_node_policy.id
  spark_version = var.spark_version
  node_type_id  = var.node_type_id
  autotermination_minutes = var.idle_minutes
}