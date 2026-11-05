targetScope = 'resourceGroup'

@description('Use a reviewed Azure region; no region is silently selected by this template.')
param location string

@minLength(3)
@maxLength(8)
param prefix string = 'gaprices'

@description('The GitHub Pages origin only, without the repository path.')
param pagesOrigin string

@description('Microsoft Entra object ID of the SQL administrator user or group.')
param administratorObjectId string

@description('Display name of that Microsoft Entra administrator.')
param administratorName string

@description('Public IPv4 address permitted to bootstrap the SQL database. No broad client range is opened.')
param sqlClientIp string

@description('Explicitly decide whether Azure services may reach SQL; required for the pipeline Script activity without private networking. Authentication is still required, including from other Azure tenants.')
param allowAzureServices bool

@description('A supported Synapse Spark runtime in the selected region.')
param sparkVersion string = '3.5'

var suffix = uniqueString(resourceGroup().id)
var blobRole = subscriptionResourceId('Microsoft.Authorization/roleDefinitions', 'ba92f5b4-2d11-453d-a403-e96b0029c9fe')

resource privateStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: '${prefix}raw${suffix}'
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: false
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
    isHnsEnabled: true
  }
}

resource privateService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: privateStorage
  name: 'default'
}

resource privateContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: privateService
  name: 'private'
  properties: { publicAccess: 'None' }
}

resource workspaceContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: privateService
  name: 'workspace'
  properties: { publicAccess: 'None' }
}

resource workspace 'Microsoft.Synapse/workspaces@2021-06-01' = {
  name: '${prefix}-${suffix}'
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    azureADOnlyAuthentication: true
    defaultDataLakeStorage: {
      accountUrl: privateStorage.properties.primaryEndpoints.dfs
      filesystem: workspaceContainer.name
      resourceId: privateStorage.id
    }
    publicNetworkAccess: 'Enabled'
  }
}

resource administrator 'Microsoft.Synapse/workspaces/administrators@2021-06-01' = {
  parent: workspace
  name: 'activeDirectory'
  properties: {
    administratorType: 'ActiveDirectory'
    login: administratorName
    sid: administratorObjectId
    tenantId: tenant().tenantId
  }
}

resource sqlClientAccess 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = {
  parent: workspace
  name: 'SqlBootstrapClient'
  properties: { startIpAddress: sqlClientIp, endIpAddress: sqlClientIp }
}

resource azureServicesAccess 'Microsoft.Synapse/workspaces/firewallRules@2021-06-01' = if (allowAzureServices) {
  parent: workspace
  name: 'AllowAllWindowsAzureIps'
  properties: { startIpAddress: '0.0.0.0', endIpAddress: '0.0.0.0' }
}

resource spark 'Microsoft.Synapse/workspaces/bigDataPools@2021-06-01' = {
  parent: workspace
  name: 'etl'
  location: location
  properties: {
    nodeCount: 3
    nodeSize: 'Small'
    nodeSizeFamily: 'MemoryOptimized'
    sparkVersion: sparkVersion
    autoScale: { enabled: false }
    autoPause: { enabled: true, delayInMinutes: 5 }
    sessionLevelPackagesEnabled: false
    libraryRequirements: {
      filename: 'requirements.txt'
      content: loadTextContent('../synapse/requirements.txt')
    }
  }
  dependsOn: [workspaceAccess, privateAccess, publicAccess]
}

resource publicStorage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: '${prefix}web${suffix}'
  location: location
  kind: 'StorageV2'
  sku: { name: 'Standard_LRS' }
  properties: {
    accessTier: 'Hot'
    allowBlobPublicAccess: true
    allowSharedKeyAccess: false
    defaultToOAuthAuthentication: true
    minimumTlsVersion: 'TLS1_2'
    supportsHttpsTrafficOnly: true
    publicNetworkAccess: 'Enabled'
  }
}

resource publicService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: publicStorage
  name: 'default'
  properties: {
    cors: {
      corsRules: [
        {
          allowedOrigins: [pagesOrigin]
          allowedMethods: ['GET', 'HEAD', 'OPTIONS']
          allowedHeaders: []
          exposedHeaders: ['ETag', 'Content-Length']
          maxAgeInSeconds: 3600
        }
      ]
    }
  }
}

resource publicContainer 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: publicService
  name: 'lookup'
  properties: { publicAccess: 'Blob' }
}

resource privateAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: privateContainer
  name: guid(privateContainer.id, workspace.id, blobRole)
  properties: {
    principalId: workspace.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobRole
  }
}

resource publicAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: publicContainer
  name: guid(publicContainer.id, workspace.id, blobRole)
  properties: {
    principalId: workspace.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobRole
  }
}

resource workspaceAccess 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  scope: workspaceContainer
  name: guid(workspaceContainer.id, workspace.id, blobRole)
  properties: {
    principalId: workspace.identity.principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: blobRole
  }
}

// Firewall rules and schedule activation require an explicit deployment decision.
output privateAccountUrl string = privateStorage.properties.primaryEndpoints.blob
output servingAccountUrl string = publicStorage.properties.primaryEndpoints.blob
output publicDataBaseUrl string = '${publicStorage.properties.primaryEndpoints.blob}lookup/'
output workspaceName string = workspace.name
output privateAccountName string = privateStorage.name
output servingAccountName string = publicStorage.name
output sqlEndpoint string = '${workspace.name}-ondemand.sql.azuresynapse.net'
output workspacePrincipalId string = workspace.identity.principalId
output sparkPool string = spark.name
