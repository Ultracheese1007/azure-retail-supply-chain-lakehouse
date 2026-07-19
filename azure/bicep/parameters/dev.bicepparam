using '../main.bicep'

param projectName = 'retaillh'
param environment = 'dev'
param landingContainerName = 'landing'
param tags = {
  project: 'retail-lakehouse'
  environment: 'dev'
  owner: 'data-engineering'
}
