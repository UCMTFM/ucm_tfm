resource "azurerm_resource_group" "this" {
  name     = "rg-${var.prefix}-${var.name}"
  location = var.location
  tags     = var.tags
}
