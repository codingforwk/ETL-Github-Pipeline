terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0" 
    }
  }
}
# Configure Azure provider
provider "azurerm" {
  features {}  # REQUIRED empty features block
}
# Create a resource group
resource "azurerm_resource_group" "github_data" {
  name     = "github-pipeline-rg"  # Follow naming conventions
  location = "westus"             # Choose your region
}

# ADLS Gen2 Storage Account
resource "azurerm_storage_account" "github_storage" {
  name                     = "githubdatastoreyoussef2"  # Must be GLOBALLY unique
  resource_group_name      = azurerm_resource_group.github_data.name
  location                 = azurerm_resource_group.github_data.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  is_hns_enabled           = true  # REQUIRED for ADLS Gen2
}

# Raw data container
resource "azurerm_storage_data_lake_gen2_filesystem" "raw" {
  name               = "raw-events"
  storage_account_id = azurerm_storage_account.github_storage.id
}

# Processed data container
resource "azurerm_storage_data_lake_gen2_filesystem" "processed" {
  name               = "processed-events"
  storage_account_id = azurerm_storage_account.github_storage.id
}

# Synapse Workspace
resource "azurerm_synapse_workspace" "synapse" {
  name                                 = "github-synapse-ya"  # Globally unique name
  resource_group_name                  = azurerm_resource_group.github_data.name
  location                             = azurerm_resource_group.github_data.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.raw.id

  sql_administrator_login          = "synapseadmin"
  sql_administrator_login_password = "Ya-ssword1"  # Change this in production!

  identity {
    type = "SystemAssigned"
  }
}

# Allow Synapse to access storage
resource "azurerm_role_assignment" "synapse_storage" {
  scope                = azurerm_storage_account.github_storage.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_synapse_workspace.synapse.identity[0].principal_id
}

# Dedicated SQL Pool (Optional but recommended for BI queries)
resource "azurerm_synapse_sql_pool" "sql_pool" {
  name                 = "githubpool"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  sku_name             = "DW100c"  
  create_mode          = "Default"
}

# Firewall rule to allow access from Codespaces/your IP
resource "azurerm_synapse_firewall_rule" "allow_codespaces" {
  name                 = "allow-codespaces"
  synapse_workspace_id = azurerm_synapse_workspace.synapse.id
  start_ip_address     = "20.61.126.210"  # Get this via: curl ifconfig.me
  end_ip_address       = "20.61.126.210"
}