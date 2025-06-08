resource "azurerm_storage_account" "this" {
  name                     = "sta${var.prefix}${var.name}"
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true
  tags                     = var.tags
}
