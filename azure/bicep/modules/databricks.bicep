// Azure Databricks workspace and an Access Connector (managed identity) that
// Databricks uses to reach ADLS Gen2 via Unity Catalog storage credentials.

@description('Databricks workspace name.')
param workspaceName string

@description('Access Connector name.')
param accessConnectorName string

@description('Azure region.')
param location string

@description('Managed resource group name for the workspace.')
param managedResourceGroupName string

@description('Resource ID of the storage account the connector may access.')
param storageAccountId string

@description('Resource tags.')
param tags object = {}

var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'

resource accessConnector 'Microsoft.Databricks/accessConnectors@2023-05-01' = {
  name: accessConnectorName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
}

resource workspace 'Microsoft.Databricks/workspaces@2023-05-01' = {
  name: workspaceName
  location: location
  tags: tags
  sku: {
    name: 'premium'
  }
  properties: {
    managedResourceGroupId: subscriptionResourceId('Microsoft.Resources/resourceGroups', managedResourceGroupName)
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: last(split(storageAccountId, '/'))
}

resource connectorStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccountId, accessConnector.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: accessConnector.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output workspaceId string = workspace.id
output workspaceName string = workspace.name
output workspaceUrl string = workspace.properties.workspaceUrl
output accessConnectorId string = accessConnector.id
