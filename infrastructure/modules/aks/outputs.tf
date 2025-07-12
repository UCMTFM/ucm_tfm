output "cluster_name" {
  value = module.aks.aks_name
}

output "host" {
  value = module.aks.admin_host
}

output "kube_config" {
  value = module.aks.kube_config_raw
}

output "client_certificate" {
  value = module.aks.admin_client_certificate
}

output "client_key" {
  value = module.aks.admin_client_key
}

output "cluster_ca_certificate" {
  value = module.aks.admin_cluster_ca_certificate
}
