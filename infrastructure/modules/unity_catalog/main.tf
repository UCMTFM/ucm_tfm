terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }
}

provider "databricks" {
  alias                       = "default"
  auth_type                   = "azure-client-secret"
  azure_client_id             = var.azure_client_id
  azure_client_secret         = var.azure_client_secret
  azure_tenant_id             = var.azure_tenant_id
  azure_workspace_resource_id = var.workspace_resource_id
}

data "databricks_current_metastore" "this" {}

data "azuread_service_principal" "this" {
  application_id = var.azure_client_id
}

resource "databricks_grants" "metastore_admin" {
  metastore_id = data.databricks_current_metastore.this.id
  grant {
    principal  = data.azuread_service_principal.this.object_id
    privileges = [
      "USE_CATALOG",
      "CREATE_CATALOG",
      "CREATE_EXTERNAL_LOCATION",
      "CREATE_STORAGE_CREDENTIAL",
      "CREATE_SHARE",
      "CREATE_PROVIDER",
      "CREATE_RECIPIENT",
      "MANAGE"
    ]
  }
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
    url             = "abfss://${each.key}@${var.lakehouse_storage_account_name}.dfs.core.windows.net/"
    credential_name = databricks_storage_credential.access_connector_credential.name
    comment         = "External location for the ${each.key} layer of the Lakehouse"

    depends_on = [
        databricks_storage_credential.access_connector_credential
    ]
}

# resource "databricks_secret_scope" "keyvault_scope" {
#   name = "akv-${var.prefix}"

#   keyvault_metadata {
#     dns_name    = var.key_vault_uri
#     resource_id = var.key_vault_id
#   }
# }