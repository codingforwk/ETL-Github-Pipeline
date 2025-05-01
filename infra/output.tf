# infra/outputs.tf
output "storage_account_name" {
  value = azurerm_storage_account.github_storage.name
}

output "storage_account_key" {
  value     = azurerm_storage_account.github_storage.primary_access_key
  sensitive = true
}

output "synapse_sql_endpoint" {
  value = azurerm_synapse_workspace.synapse.connectivity_endpoints.sql
}