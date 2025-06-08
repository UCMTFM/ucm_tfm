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

