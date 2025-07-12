output "cluster_name" {
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
}
