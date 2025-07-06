terraform {
  required_providers {
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }
}

resource "azurerm_databricks_access_connector" "databricks_connector" {
  name                = "adb-access-connector-${var.prefix}"
  resource_group_name = var.resource_group_name
  location            = var.location
  tags                = var.tags

  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_role_assignment" "access_connector_blob_contributor" {
  principal_id         = azurerm_databricks_access_connector.databricks_connector.identity[0].principal_id
  role_definition_name = "Storage Blob Data Contributor"
  scope                = var.storage_account_id
}