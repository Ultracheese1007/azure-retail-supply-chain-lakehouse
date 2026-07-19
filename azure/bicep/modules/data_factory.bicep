// Azure Data Factory with a system-assigned managed identity, granted
// Storage Blob Data Contributor on the storage account so pipelines can write
// to the landing zone without any stored secret.

@description('Data Factory name.')
param dataFactoryName string

@description('Azure region.')
param location string

@description('Resource ID of the storage account to grant access to.')
param storageAccountId string

@description('Resource ID of the Databricks workspace the factory triggers jobs on.')
param databricksWorkspaceId string

@description('Resource tags.')
param tags object = {}

// Storage Blob Data Contributor built-in role.
var storageBlobDataContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
// Contributor built-in role (workspace-level; lets ADF invoke the Databricks Job).
var contributorRoleId = 'b24988ac-6180-42a0-ab88-20f7382dd24c'

resource dataFactory 'Microsoft.DataFactory/factories@2018-06-01' = {
  name: dataFactoryName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: last(split(storageAccountId, '/'))
}

resource adfStorageRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storageAccountId, dataFactory.id, storageBlobDataContributorRoleId)
  scope: storageAccount
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', storageBlobDataContributorRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

resource databricksWorkspace 'Microsoft.Databricks/workspaces@2023-05-01' existing = {
  name: last(split(databricksWorkspaceId, '/'))
}

// Contributor on the Databricks workspace so the factory's managed identity can
// invoke the Databricks Job. Job-level CAN MANAGE RUN is granted inside the
// workspace as a documented post-deployment step (see the deployment guide),
// because workspace job ACLs are not expressible in Bicep.
resource adfDatabricksRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(databricksWorkspaceId, dataFactory.id, contributorRoleId)
  scope: databricksWorkspace
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', contributorRoleId)
    principalId: dataFactory.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output dataFactoryId string = dataFactory.id
output dataFactoryName string = dataFactory.name
output dataFactoryPrincipalId string = dataFactory.identity.principalId
