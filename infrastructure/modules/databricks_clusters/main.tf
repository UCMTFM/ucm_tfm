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
  ml                = true
}

resource "databricks_cluster" "shared_autoscaling" {
  cluster_name            = "${var.prefix}_shared_autoscaling_cluster"
  spark_version           = var.spark_version != null ? var.spark_version : data.databricks_spark_version.latest_lts.id
  node_type_id            = var.node_type_id != null ? var.node_type_id : data.databricks_node_type.smallest.id
  autotermination_minutes = var.idle_minutes
  data_security_mode      = "USER_ISOLATION"

  autoscale {
    min_workers = var.min_workers
    max_workers = var.max_workers
  }

  spark_conf = {
    "spark.sql.catalog.spark_catalog" = "org.apache.spark.sql.execution.datasources.v2.unity.UnityCatalogSparkSessionExtension"
  }
}

resource "databricks_repo" "databricks_notebooks" {
  url    = var.git_repo_https_url
  branch = "main"
  path   = "/Repos/ucm_tfm/databricks_notebooks"

  sparse_checkout {
    patterns = ["databricks_notebooks/"]
  }
}
