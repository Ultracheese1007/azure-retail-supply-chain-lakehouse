// ADLS Gen2-enabled storage account with the landing container.
// Hierarchical namespace (isHnsEnabled) is what makes a Blob account an ADLS Gen2 account.

@description('Globally unique storage account name.')
param storageAccountName string

@description('Azure region.')
param location string

@description('Landing container (file system) name.')
param landingContainerName string = 'landing'

@description('Resource tags.')
param tags object = {}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storageAccount
  name: 'default'
}

resource landingContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: landingContainerName
  properties: {
    publicAccess: 'None'
  }
}

output storageAccountId string = storageAccount.id
output storageAccountName string = storageAccount.name
output dfsEndpoint string = storageAccount.properties.primaryEndpoints.dfs
