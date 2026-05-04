// Azure Container Apps deployment for Strategic Task Management
//
// Creates:
//   - Container Apps Environment (shared log analytics workspace)
//   - App container (Streamlit dashboard)
//   - Sync container (background ingestion worker)
//   - Managed Identity for Key Vault access
//
// Prerequisites:
//   - Azure Database for PostgreSQL Flexible Server (created separately)
//   - Azure Container Registry with built images pushed
//   - Azure Key Vault holding secrets (see parameter descriptions)
//
// Deploy:
//   az deployment group create \
//     --resource-group <rg> \
//     --template-file azure/container-apps.bicep \
//     --parameters @azure/parameters.json

@description('Azure region for all resources')
param location string = resourceGroup().location

@description('Short environment name used as a resource name prefix (e.g. stm-prod)')
param environmentName string

@description('Container registry login server (e.g. myregistry.azurecr.io)')
param containerRegistryServer string

@description('App image tag (e.g. myregistry.azurecr.io/stm-app:latest)')
param appImageTag string

@description('Sync image tag (e.g. myregistry.azurecr.io/stm-sync:latest)')
param syncImageTag string

@description('PostgreSQL connection string — reference a Key Vault secret in production')
@secure()
param databaseUrl string

@description('Azure AD tenant ID that users must belong to')
param allowedTenantId string = ''

@description('Comma-separated Azure AD group object IDs that receive the viewer role')
param viewerGroupIds string = ''

@description('Comma-separated Azure AD group object IDs that receive the admin role')
param adminGroupIds string = ''

@description('Trusted proxy shared secret for App Service Easy Auth validation')
@secure()
param trustedProxySecret string

@description('Microsoft Graph tenant ID for SharePoint/OneDrive source access')
param graphTenantId string = ''

@description('Microsoft Graph client ID (app registration)')
param graphClientId string = ''

@description('Microsoft Graph client secret')
@secure()
param graphClientSecret string = ''

@description('Task source config for the sync worker, usually a Graph/SharePoint JSON source config')
param syncSourceConfig string

@description('Whether the sync worker may initialize schema objects. Use true only for first deployment/bootstrap.')
@allowed([
  'true'
  'false'
])
param bootstrapSchema string = 'false'

// --- Log Analytics Workspace --------------------------------------------------

resource logWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: '${environmentName}-logs'
  location: location
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// --- Container Apps Environment -----------------------------------------------

resource containerAppsEnv 'Microsoft.App/managedEnvironments@2023-05-01' = {
  name: '${environmentName}-env'
  location: location
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logWorkspace.properties.customerId
        sharedKey: logWorkspace.listKeys().primarySharedKey
      }
    }
  }
}

// --- App Container (Streamlit dashboard) --------------------------------------

resource appContainer 'Microsoft.App/containerApps@2023-05-01' = {
  name: '${environmentName}-app'
  location: location
  properties: {
    managedEnvironmentId: containerAppsEnv.id
    configuration: {
      // Expose externally via HTTPS; Azure App Service Easy Auth is configured
      // on the Container App ingress, so AUTH_MODE=app_service is correct here.
      ingress: {
        external: true
        targetPort: 8501
        transport: 'http'
        allowInsecure: false
      }
      registries: [
        {
          server: containerRegistryServer
          identity: 'system'
        }
      ]
      secrets: [
        { name: 'database-url', value: databaseUrl }
        { name: 'trusted-proxy-secret', value: trustedProxySecret }
        { name: 'graph-client-secret', value: graphClientSecret }
      ]
    }
    template: {
      containers: [
        {
          name: 'app'
          image: appImageTag
          resources: { cpu: json('0.5'), memory: '1Gi' }
          env: [
            { name: 'ENVIRONMENT', value: 'production' }
            { name: 'AUTH_MODE', value: 'app_service' }
            { name: 'AUTH_REQUIRED', value: 'true' }
            { name: 'AUTH_REQUIRE_EXPLICIT_ACCESS', value: 'true' }
            { name: 'AUTH_DEFAULT_ROLE', value: '' }
            { name: 'AUTH_USE_DATABASE_ROLES', value: 'true' }
            { name: 'AUTH_AUDIT_TO_DATABASE', value: 'true' }
            { name: 'AUTH_ALLOWED_TENANT_IDS', value: allowedTenantId }
            { name: 'AUTH_VIEWER_GROUP_IDS', value: viewerGroupIds }
            { name: 'AUTH_ADMIN_GROUP_IDS', value: adminGroupIds }
            { name: 'DATABASE_URL', secretRef: 'database-url' }
            { name: 'TASKS_SOURCE', secretRef: 'database-url' }
            { name: 'APP_TRUSTED_PROXY_SECRET', secretRef: 'trusted-proxy-secret' }
            { name: 'GRAPH_TENANT_ID', value: graphTenantId }
            { name: 'GRAPH_CLIENT_ID', value: graphClientId }
            { name: 'GRAPH_CLIENT_SECRET', secretRef: 'graph-client-secret' }
            { name: 'DB_BOOTSTRAP_SCHEMA', value: 'false' }
            { name: 'APP_REFRESH_MS', value: '60000' }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 2
      }
    }
  }
  identity: { type: 'SystemAssigned' }
}

// --- Sync Container (background ingestion worker) -----------------------------

resource syncContainer 'Microsoft.App/containerApps@2023-05-01' = {
  name: '${environmentName}-sync'
  location: location
  properties: {
    managedEnvironmentId: containerAppsEnv.id
    configuration: {
      // No ingress -- the sync worker is internal only.
      registries: [
        {
          server: containerRegistryServer
          identity: 'system'
        }
      ]
      secrets: [
        { name: 'database-url', value: databaseUrl }
        { name: 'graph-client-secret', value: graphClientSecret }
      ]
    }
    template: {
      containers: [
        {
          name: 'sync'
          image: syncImageTag
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: [
            { name: 'ENVIRONMENT', value: 'production' }
            { name: 'DATABASE_URL', secretRef: 'database-url' }
            { name: 'DB_BOOTSTRAP_SCHEMA', value: bootstrapSchema }
            { name: 'SYNC_SOURCE_CONFIG', value: syncSourceConfig }
            { name: 'TASK_SOURCE_ROOT', value: '/app/data' }
            { name: 'SYNC_POLL_SECONDS', value: '30' }
            { name: 'SYNC_REFRESH_SECONDS', value: '1800' }
            { name: 'GRAPH_TENANT_ID', value: graphTenantId }
            { name: 'GRAPH_CLIENT_ID', value: graphClientId }
            { name: 'GRAPH_CLIENT_SECRET', secretRef: 'graph-client-secret' }
          ]
        }
      ]
      scale: {
        minReplicas: 1
        maxReplicas: 1
      }
    }
  }
  identity: { type: 'SystemAssigned' }
}

// --- Outputs ------------------------------------------------------------------

output appFqdn string = appContainer.properties.configuration.ingress.fqdn
output appPrincipalId string = appContainer.identity.principalId
output syncPrincipalId string = syncContainer.identity.principalId
