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

resource "databricks_storage_credential" "access_connector_credential" {
    name = "dac-${var.prefix}"

    azure_managed_identity {
      access_connector_id = var.access_connector_id
    }

    comment = "Credential linked to Access Connector"
}

resource "databricks_external_location" "lakehouse_layers" {
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
  for_each          = toset(var.lakehouse_external_layers)
  external_location = databricks_external_location.lakehouse_layers[each.key].id

  grant {
    principal  = "account users"
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

# resource "databricks_secret_scope" "keyvault_scope" {
#   name = "akv-${var.prefix}"

#   keyvault_metadata {
#     dns_name    = var.key_vault_uri
#     resource_id = var.key_vault_id
#   }
# }