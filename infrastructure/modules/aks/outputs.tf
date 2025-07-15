output "cluster_name" {
  value = azuread_kubernetes_cluster.aks.name
}

output "host" {
  value = azuread_kubernetes_cluster.aks.kube_config[0].host
}

output "client_certificate" {
  value     = azuread_kubernetes_cluster.aks.kube_config[0].client_certificate
  sensitive = true
}

output "client_key" {
  value     = azuread_kubernetes_cluster.aks.kube_config[0].client_key
  sensitive = true
}

output "cluster_ca_certificate" {
  value     = azuread_kubernetes_cluster.aks.kube_config[0].cluster_ca_certificate
  sensitive = true
}
