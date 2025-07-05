data "azurerm_client_config" "current" {}

data "azuread_service_principal" "databricks" {
  display_name = "AzureDatabricks"
}

resource "azurerm_key_vault" "kv" {
  name                        = "akv${var.prefix}${var.name}"
  location                    = var.location
  resource_group_name         = var.resource_group_name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = var.sku
  purge_protection_enabled    = true
  soft_delete_retention_days  = 30
}

resource "azurerm_role_assignment" "secrets_officer_members" {
  for_each             = toset(var.member_ids)
  principal_id         = each.key
  role_definition_name = "Key Vault Secrets Officer"
  scope                = azurerm_key_vault.kv.id
}

resource "azurerm_role_assignment" "secrets_officer_databricks" {
  principal_id         = data.azuread_service_principal.databricks.object_id
  role_definition_name = "Key Vault Secrets Officer"
  scope                = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "my_secret" {
  name         = "stg-account-access-key"
  value        = var.stg_account_access_key
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "text/plain"
}