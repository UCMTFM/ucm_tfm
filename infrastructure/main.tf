locals {
  tags = merge(
    var.tags,
    {
      project = var.project
    }
  )
}


# Data LakeHouse Resource Group

module "resource_group" {
  source   = "./modules/resource_group"
  name     = "lakehouse"
  prefix   = var.project
  location = var.location
  tags     = local.tags
}

resource "azuread_group" "admins" {
  display_name     = "adg${var.project}"
  security_enabled = true
}

data "azuread_user" "members" {
  for_each            = toset(var.group_members)
  user_principal_name = each.key
}

data "azuread_service_principal" "github_app" {
  client_id = var.azure_client_id
}

resource "azuread_group_member" "members" {
  for_each         = data.azuread_user.members
  group_object_id  = azuread_group.admins.object_id
  member_object_id = each.value.object_id
}

resource "azuread_group_member" "sp_member" {
  group_object_id  = azuread_group.admins.object_id
  member_object_id = data.azuread_service_principal.github_app.object_id
}

resource "azurerm_role_assignment" "aad_group_rg_contributor" {
  scope                = module.resource_group.id
  role_definition_name = "Contributor"
  principal_id         = azuread_group.admins.object_id
}

# Landing Storage

module "landing_storage" {
  source              = "./modules/storage_account"
  name                = "landing"
  prefix              = var.project
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "landing" {
  name               = "landing"
  storage_account_id = module.landing_storage.id
}

# LakeHouse Storage

module "lakehouse_storage" {
  source              = "./modules/storage_account"
  name                = "lakehouse"
  prefix              = var.project
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "lakehouse" {
  name               = "lakehouse"
  storage_account_id = module.lakehouse_storage.id
}

resource "azurerm_storage_data_lake_gen2_path" "bronze" {
  path               = "bronze"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
  storage_account_id = module.lakehouse_storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "silver" {
  path               = "silver"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
  storage_account_id = module.lakehouse_storage.id
  resource           = "directory"
}

resource "azurerm_storage_data_lake_gen2_path" "gold" {
  path               = "gold"
  filesystem_name    = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
  storage_account_id = module.lakehouse_storage.id
  resource           = "directory"
}

# Databricks Workspace

module "databricks_workspace" {
  source              = "./modules/databricks_workspace"
  prefix              = var.project
  name                = "lakehouse"
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
}

# Databricks Access Connector

module "databricks_access_connector" {
  source              = "./modules/databricks_access_connector"
  prefix              = var.project
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location
  tags                = local.tags
  storage_account_id  = module.lakehouse_storage.id
}

# Unity Catalog

module "unity_catalog" {
  source                         = "./modules/unity_catalog"
  databricks_workspace_id        = module.databricks_workspace.id
  prefix                         = var.project
  access_connector_id            = module.databricks_access_connector.id
  lakehouse_external_layers      = ["bronze", "silver", "gold"]
  lakehouse_storage_account_name = module.lakehouse_storage.account_name
  container_name                 = "lakehouse"
  admin_group_name               = azuread_group.admins.display_name
}

# Databricks Clusters

module "shared_compute" {
  source                  = "./modules/databricks_clusters"
  prefix                  = var.project
  spark_version           = "16.4.x-scala2.12"
  node_type_id            = "Standard_DS3_v2"
  idle_minutes            = 15
  num_workers             = 1
  databricks_workspace_id = module.databricks_workspace.id
}

# Azure k8s Cluster

module "aks" {
  source = "./modules/aks"
  name   = "lakehousecluster"
  prefix = var.project

  location            = module.resource_group.location
  resource_group_name = module.resource_group.name

  tags = local.tags
}

provider "helm" {
  kubernetes = {
    host                   = module.aks.host
    client_certificate     = base64decode(module.aks.client_certificate)
    client_key             = base64decode(module.aks.client_key)
    cluster_ca_certificate = base64decode(module.aks.cluster_ca_certificate)
  }
}

resource "helm_release" "airflow" {
  depends_on       = [module.aks]
  name             = "airflow-server"
  create_namespace = true
  namespace        = "airflow"
  repository       = "https://airflow.apache.org"
  chart            = "airflow"
  version          = "1.17.0"
  wait             = false
  timeout          = 300

  values = [file("${path.root}/helm_charts/airflow.yaml")]
}
