output "id" {
  value = azurerm_databricks_access_connector.databricks_connector.id
}

output "guid" {
  value = azurerm_databricks_access_connector.databricks_connector.identity[0].principal_id
}