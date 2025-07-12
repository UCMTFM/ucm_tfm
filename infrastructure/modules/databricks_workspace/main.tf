resource "azurerm_databricks_workspace" "this" {
  name                          = "adb${var.prefix}${var.name}"
  resource_group_name           = var.resource_group_name
  location                      = var.location
  sku                           = var.sku
  managed_resource_group_name   = "rg-databricks-mrg-${var.prefix}"
  tags                          = var.tags
  
  custom_parameters {
    no_public_ip = false
  }
}

resource "databricks_metastore" "this" {
  name          = "lakehouse"
  storage_root  = "abfss://${var.container_name}@${var.lakehouse_storage_account_name}.dfs.core.windows.net/"
  owner         = var.admin_group_name
  force_destroy = true
}