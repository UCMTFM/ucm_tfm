terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.86.0"
    }
  }
}

locals {
  src_root = "${path.module}/config_files"
  files    = fileset(local.src_root, "**")
}

resource "databricks_storage_credential" "access_connector_credential" {
  provider = databricks
  name     = "dac-${var.prefix}"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }

  comment = "Credential linked to Access Connector"
}

resource "databricks_external_location" "lakehouse_layers" {
  provider        = databricks
  for_each        = toset(var.lakehouse_external_layers)
  name            = "external_location_${each.key}"
  url             = "abfss://${var.container_name}@${var.lakehouse_storage_account_name}.dfs.core.windows.net/${each.key}"
  credential_name = databricks_storage_credential.access_connector_credential.name
  comment         = "External location for the ${each.key} layer of the Lakehouse"

  depends_on = [
    databricks_storage_credential.access_connector_credential
  ]
}

resource "databricks_grants" "lakehouse_layers_grants" {
  provider          = databricks
  for_each          = toset(var.lakehouse_external_layers)
  external_location = databricks_external_location.lakehouse_layers[each.key].id

  grant {
    principal  = "account users"
    privileges = ["READ_FILES", "WRITE_FILES", "CREATE_EXTERNAL_TABLE", "CREATE_MANAGED_STORAGE"]
  }
}

resource "databricks_grants" "catalog_grants" {
  for_each = toset(var.users)

  catalog = "adbucmappinnovalakehouse"

  grant {
    principal  = each.value
    privileges = ["USE_CATALOG"]
  }
}

resource "databricks_grants" "schema_grants" {
  for_each = toset(var.users)

  schema = "adbucmappinnovalakehouse.silver"

  grant {
    principal  = each.value
    privileges = ["USE_SCHEMA", "CREATE", "MODIFY", "DELETE"]
  }

  grant {
    principal  = each.value
    privileges = ["SELECT"]
  }
}

resource "databricks_secret_scope" "keyvault_scope" {
  name = "akv-${var.prefix}"

  keyvault_metadata {
    dns_name    = var.key_vault_uri
    resource_id = var.key_vault_id
  }
}

resource "databricks_workspace_conf" "dbfs_browser" {
  custom_config = {
    "enableDbfsFileBrowser" = "true"
  }
}

resource "databricks_dbfs_file" "upload_to_filestore" {
  for_each = { for f in local.files : f => f }
  path     = "/FileStore/config/${each.key}"
  source   = "${local.src_root}/${each.value}"
}
