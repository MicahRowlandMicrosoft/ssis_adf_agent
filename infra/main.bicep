// ============================================================================
//  Test infrastructure for SSIS -> ADF conversion deployed to MCPTest factory.
//
//  Provisions:
//    * Storage account + blob container (sink for the Flat File Destination)
//    * Azure SQL logical server + serverless database (source for the OLE DB
//      Source) with Microsoft Entra-only auth
//    * Firewall rule allowing Azure services (incl. ADF) to connect
//    * RBAC: ADF managed identity gets Storage Blob Data Contributor
//
//  After deploy, run the companion deploy.ps1 to:
//    * Seed dbo.activity with ~300 dummy rows
//    * Add the ADF managed identity as a SQL user (db_datareader)
//    * Print the connection strings to paste into the two linked services
// ============================================================================

@description('Azure region for all resources.')
param location string = resourceGroup().location

@description('Short prefix used to name resources. Must be 3-11 lowercase alphanumeric.')
@minLength(3)
@maxLength(11)
param namePrefix string = 'ssisadftest'

@description('Object ID of the Microsoft Entra principal that will be the SQL admin (your user OID).')
param sqlAdminObjectId string

@description('Display name of the Microsoft Entra principal that will be the SQL admin.')
param sqlAdminLogin string

@description('Principal ID of the ADF factory managed identity.')
param adfPrincipalId string

@description('Name of the blob container the Flat File Destination dataset writes to.')
param containerName string = 'todo-container'

// ---------------------------------------------------------------------------
//  Storage
// ---------------------------------------------------------------------------
var storageName = toLower('${namePrefix}sa${uniqueString(resourceGroup().id)}')

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: take(storageName, 24)
  location: location
  sku: { name: 'Standard_LRS' }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
    supportsHttpsTrafficOnly: true
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  parent: storage
  name: 'default'
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  parent: blobService
  name: containerName
  properties: { publicAccess: 'None' }
}

// Storage Blob Data Contributor for the ADF MI
var blobContributorRoleId = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
resource adfBlobRole 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(storage.id, adfPrincipalId, blobContributorRoleId)
  scope: storage
  properties: {
    principalId: adfPrincipalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId(
      'Microsoft.Authorization/roleDefinitions',
      blobContributorRoleId
    )
  }
}

// ---------------------------------------------------------------------------
//  Azure SQL (Entra-only auth, serverless GP_S_Gen5_1)
// ---------------------------------------------------------------------------
var sqlServerName = toLower('${namePrefix}-sql-${uniqueString(resourceGroup().id)}')
var sqlDbName = 'addstest'

resource sqlServer 'Microsoft.Sql/servers@2023-08-01-preview' = {
  name: sqlServerName
  location: location
  identity: { type: 'SystemAssigned' }
  properties: {
    minimalTlsVersion: '1.2'
    publicNetworkAccess: 'Enabled'
    administrators: {
      administratorType: 'ActiveDirectory'
      principalType: 'User'
      tenantId: subscription().tenantId
      sid: sqlAdminObjectId
      login: sqlAdminLogin
      azureADOnlyAuthentication: true
    }
  }
}

resource sqlAllowAzure 'Microsoft.Sql/servers/firewallRules@2023-08-01-preview' = {
  parent: sqlServer
  name: 'AllowAllAzureServices'
  properties: {
    startIpAddress: '0.0.0.0'
    endIpAddress: '0.0.0.0'
  }
}

resource sqlDb 'Microsoft.Sql/servers/databases@2023-08-01-preview' = {
  parent: sqlServer
  name: sqlDbName
  location: location
  sku: {
    name: 'GP_S_Gen5_1'
    tier: 'GeneralPurpose'
    family: 'Gen5'
    capacity: 1
  }
  properties: {
    autoPauseDelay: 60
    minCapacity: json('0.5')
    maxSizeBytes: 2147483648  // 2 GB
    zoneRedundant: false
  }
}

// ---------------------------------------------------------------------------
//  Outputs
// ---------------------------------------------------------------------------
output storageAccountName string = storage.name
output storageBlobEndpoint string = storage.properties.primaryEndpoints.blob
output storageContainerName string = container.name
output sqlServerFqdn string = sqlServer.properties.fullyQualifiedDomainName
output sqlDatabaseName string = sqlDb.name
output blobLinkedServiceConnectionString string = format(
  'DefaultEndpointsProtocol=https;AccountName={0};EndpointSuffix={1};',
  storage.name,
  environment().suffixes.storage
)
output sqlConnectionString string = format(
  'Server=tcp:{0},1433;Database={1};Authentication=Active Directory Default;',
  sqlServer.properties.fullyQualifiedDomainName,
  sqlDb.name
)
