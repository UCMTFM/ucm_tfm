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
    "Get",
    "List",
    "Set",
    "Delete",
    "Purge",
    "Recover"
  ]
}

resource "azurerm_key_vault_access_policy" "members" {
  for_each    = toset(var.member_ids)
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = each.key

  secret_permissions = ["List", "Get", "Set", "Delete"]
}

# Para Databricks (si quieres que lea secretos desde el KV):
resource "azurerm_key_vault_access_policy" "databricks" {
  key_vault_id = azurerm_key_vault.kv.id
  tenant_id    = data.azurerm_client_config.current.tenant_id
  object_id    = var.access_connector_guid

  secret_permissions = ["Get", "List"]
}

resource "azurerm_key_vault_secret" "lakehouse_secret" {
  name         = "lakehouse-stg-account-access-key"
  value        = var.lakehouse_stg_account_key
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "text/plain"

  depends_on = [azurerm_key_vault_access_policy.terraform]
}

resource "azurerm_key_vault_secret" "landing_secret" {
  name         = "landing-stg-account-access-key"
  value        = var.landing_stg_account_key
  key_vault_id = azurerm_key_vault.kv.id
  content_type = "text/plain"

  depends_on = [azurerm_key_vault_access_policy.terraform]
}