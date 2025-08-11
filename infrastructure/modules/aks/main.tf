resource "azurerm_kubernetes_cluster" "aks" {
  name                = "aks${var.prefix}${var.name}"
  location            = var.location
  resource_group_name = var.resource_group_name
  dns_prefix          = var.dns_prefix != null ? var.dns_prefix : "aks${var.prefix}${var.name}"

  default_node_pool {
    name       = "default"
    node_count = var.agents_count
    vm_size    = var.agent_vm_size
  }

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}
