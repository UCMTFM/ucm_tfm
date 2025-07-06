module "aks" {
  source  = "Azure/aks/azurerm"
  version = "10.1.1"

  cluster_name        = "aks${var.prefix}${var.name}"
  location            = var.location
  resource_group_name = var.resource_group_name
  tags                = var.tags
  sku_tier            = var.sku_tier
}
