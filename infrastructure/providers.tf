terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "3.117.1"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "3.4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.30.0"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "statfmmbeterraformstate"
    container_name       = "tfstate"
    key                  = "terraform.tfstate"
  }
}

provider "azurerm" {
  features {}
}

provider "databricks" {
  alias = "default"
}

