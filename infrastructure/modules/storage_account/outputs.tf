output "id" {
  value = azurerm_storage_account.this.id
}

output "account_name" {
  value = azurerm_storage_account.this.name
}

output "dfs_endpoint" {
  description = "The primary DFS endpoint (https://<account>.dfs.core.windows.net) for the storage account"
  value       = azurerm_storage_account.this.primary_dfs_endpoint
}

output "primary_access_key" {
  description = "Primary access key for the storage account"
  value       = azurerm_storage_account.this.primary_access_key
  sensitive   = true
}
