data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {
  name                        = "akv${var.prefix}"
  location                    = var.location
  resource_group_name         = var.resource_group_name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = var.sku
  purge_protection_enabled    = true
  soft_delete_retention_days  = 30
}

resource "azurerm_key_vault_access_policy" "terraform" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = data.azurerm_client_config.current.object_id

  secret_permissions = [
    "Get", "List", "Set"
  ]
}

resource "azurerm_role_assignment" "secrets_officer_members" {
  for_each             = toset(var.member_ids)
  principal_id         = each.key
  role_definition_name = "Key Vault Secrets Officer"
  scope                = azurerm_key_vault.kv.id
}

resource "azurerm_role_assignment" "secrets_officer_databricks" {
  principal_id         = var.access_connector_id
  role_definition_name = "Key Vault Secrets Officer"
  scope                = azurerm_key_vault.kv.id
}

resource "azurerm_key_vault_secret" "lakehouse_secret" {
  name         = "lakehouse-stg-account-access-key"
  value        = var.lakehouse_stg_account_key
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "text/plain"
}

resource "azurerm_key_vault_secret" "landing_secret" {
  name         = "landing-stg-account-access-key"
  value        = var.landing_stg_account_key
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "text/plain"
}