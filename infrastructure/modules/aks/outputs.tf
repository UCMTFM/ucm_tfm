output "cluster_name" {
<<<<<<< HEAD
  value = azurerm_kubernetes_cluster.aks.name
}

output "host" {
  value = azurerm_kubernetes_cluster.aks.kube_config[0].host
}

output "client_certificate" {
  value     = azurerm_kubernetes_cluster.aks.kube_config[0].client_certificate
  sensitive = true
}

output "client_key" {
  value     = azurerm_kubernetes_cluster.aks.kube_config[0].client_key
  sensitive = true
}

output "cluster_ca_certificate" {
  value     = azurerm_kubernetes_cluster.aks.kube_config[0].cluster_ca_certificate
  sensitive = true
=======
  value = module.aks.aks_name
}

output "host" {
  value = module.aks.host
}

output "kube_config" {
  value = module.aks.kube_config_raw
}

output "client_certificate" {
  value = module.aks.client_certificate
}

output "client_key" {
  value = module.aks.client_key
}

output "cluster_ca_certificate" {
  value = module.aks.cluster_ca_certificate
>>>>>>> parent of d49ea89 (Change AKS module definition)
}
