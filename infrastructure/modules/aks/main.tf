module "aks" {
  source  = "Azure/aks/azurerm"
  version = "10.1.1"

  cluster_name = "aks${var.prefix}${var.name}"
  prefix       = var.prefix
  location     = var.location

  agents_size                 = var.agent_vm_size
  temporary_name_for_rotation = var.temporary_name_for_rotation
  enable_auto_scaling         = var.enable_autoscaling
  agents_max_count            = var.agents_max_count
  agents_min_count            = var.agents_min_count
  agents_count                = var.enable_autoscaling ? null : var.agents_count

  resource_group_name         = var.resource_group_name
  tags                        = var.tags
  sku_tier                    = var.sku_tier
  auto_scaler_profile_enabled = var.auto_scaler_profile_enabled
}
