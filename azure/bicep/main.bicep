// Retail lakehouse Azure infrastructure entry point.
//
// Provisions the deployment-ready target for the pipeline: an ADLS Gen2 storage
// account (landing zone), a Data Factory with a managed identity that can write
// to it, and an Azure Databricks workspace with an Access Connector for
// Unity Catalog storage access.
//
// This template is compiled and checked by GitHub Actions before it is treated as
// statically validated. It has not been deployed: no Azure subscription is
// provisioned for this portfolio project.

targetScope = 'resourceGroup'

@description('Short project name used to derive resource names.')
param projectName string = 'retaillh'

@description('Deployment environment (e.g. dev).')
param environment string = 'dev'

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Globally unique storage account name (3-24 lowercase alphanumerics).')
param storageAccountName string = toLower('${projectName}${environment}st')

@description('Landing container name.')
param landingContainerName string = 'landing'

@description('Resource tags applied to every resource.')
param tags object = {
  project: 'retail-lakehouse'
  environment: environment
}

var dataFactoryName = '${projectName}-${environment}-adf'
var databricksWorkspaceName = '${projectName}-${environment}-dbw'
var databricksAccessConnectorName = '${projectName}-${environment}-dbc'
var databricksManagedRgName = '${projectName}-${environment}-dbw-managed'

module storage 'modules/storage.bicep' = {
  name: 'storage'
  params: {
    storageAccountName: storageAccountName
    location: location
    landingContainerName: landingContainerName
    tags: tags
  }
}

module databricks 'modules/databricks.bicep' = {
  name: 'databricks'
  params: {
    workspaceName: databricksWorkspaceName
    accessConnectorName: databricksAccessConnectorName
    location: location
    managedResourceGroupName: databricksManagedRgName
    storageAccountId: storage.outputs.storageAccountId
    tags: tags
  }
}

module dataFactory 'modules/data_factory.bicep' = {
  name: 'dataFactory'
  params: {
    dataFactoryName: dataFactoryName
    location: location
    storageAccountId: storage.outputs.storageAccountId
    databricksWorkspaceId: databricks.outputs.workspaceId
    tags: tags
  }
}

output storageAccountName string = storage.outputs.storageAccountName
output landingDfsEndpoint string = storage.outputs.dfsEndpoint
output dataFactoryName string = dataFactory.outputs.dataFactoryName
output dataFactoryPrincipalId string = dataFactory.outputs.dataFactoryPrincipalId
output databricksWorkspaceUrl string = databricks.outputs.workspaceUrl
output databricksWorkspaceId string = databricks.outputs.workspaceId
