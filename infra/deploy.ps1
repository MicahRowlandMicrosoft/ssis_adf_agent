<#
.SYNOPSIS
    Provision test infrastructure for the deployed MCPTest ADF factory and
    seed dbo.activity with 300 rows of dummy data.

.DESCRIPTION
    1. Reads the current az login user and the ADF factory's managed-identity
       principal ID.
    2. Deploys infra/main.bicep into the target resource group.
    3. Adds the ADF MI as a SQL contained user with db_datareader.
    4. Runs infra/seed_activity.sql against the new database.
    5. Prints the connection strings to paste into the two linked services
       deployed by the SSIS->ADF agent.

    Idempotent - safe to re-run.

.EXAMPLE
    pwsh ./infra/deploy.ps1 `
        -SubscriptionId 564fde6a-18b1-425a-a184-ea80343143e4 `
        -ResourceGroup  rg-mcp-ssis-to-adf-test `
        -FactoryName    MCPTest
#>
param(
    [Parameter(Mandatory = $true)] [string] $SubscriptionId,
    [Parameter(Mandatory = $true)] [string] $ResourceGroup,
    [Parameter(Mandatory = $true)] [string] $FactoryName,
    [string] $Location      = 'westus2',
    [string] $NamePrefix    = 'ssisadftest',
    [string] $ContainerName = 'todo-container'
)

$ErrorActionPreference = 'Stop'
$here = Split-Path -Parent $MyInvocation.MyCommand.Definition

# ---------------------------------------------------------------------------
# Pre-flight: verify CLI tooling is present
# ---------------------------------------------------------------------------
foreach ($cmd in @('az', 'sqlcmd')) {
    if (-not (Get-Command $cmd -ErrorAction SilentlyContinue)) {
        throw "$cmd not found in PATH. Install Azure CLI and sqlcmd (mssql-tools)."
    }
}

# ---------------------------------------------------------------------------
# Ensure a Bicep CLI is available and tell az to use it from PATH.
# This avoids the 'az bicep install' auto-download which frequently fails
# with SSL: UNEXPECTED_EOF_WHILE_READING behind corporate TLS proxies.
# ---------------------------------------------------------------------------
function Ensure-Bicep {
    if (Get-Command bicep -ErrorAction SilentlyContinue) {
        Write-Host "Found standalone bicep: $((Get-Command bicep).Source)" -ForegroundColor DarkGray
    }
    elseif (Get-Command winget -ErrorAction SilentlyContinue) {
        Write-Host "Installing Bicep CLI via winget ..." -ForegroundColor Yellow
        winget install -e --id Microsoft.Bicep --accept-source-agreements --accept-package-agreements
        if ($LASTEXITCODE -ne 0) {
            throw "winget install Microsoft.Bicep failed. Install manually from https://github.com/Azure/bicep/releases"
        }
        $wingetLinks = Join-Path $env:LOCALAPPDATA 'Microsoft\WinGet\Links'
        if (Test-Path (Join-Path $wingetLinks 'bicep.exe')) {
            $env:PATH = "$wingetLinks;$env:PATH"
        }
        if (-not (Get-Command bicep -ErrorAction SilentlyContinue)) {
            throw "Bicep installed via winget but not on PATH. Open a new shell and re-run."
        }
    }
    else {
        throw "No standalone bicep found and winget unavailable. Install Bicep from https://github.com/Azure/bicep/releases (bicep-win-x64.exe -> rename to bicep.exe on PATH)."
    }

    az config set bicep.use_binary_from_path=true --only-show-errors | Out-Null
}

Ensure-Bicep

az account set --subscription $SubscriptionId | Out-Null
if ($LASTEXITCODE -ne 0) { throw "az account set failed." }

# ---------------------------------------------------------------------------
# Resolve current user (becomes SQL admin) and ADF factory MI
# ---------------------------------------------------------------------------
$me = az ad signed-in-user show -o json | ConvertFrom-Json
$adminObjectId = $me.id
$adminLogin    = $me.userPrincipalName
Write-Host "SQL admin will be: $adminLogin ($adminObjectId)" -ForegroundColor Cyan

$adf = az datafactory show `
    --resource-group $ResourceGroup `
    --name $FactoryName `
    -o json | ConvertFrom-Json
$adfPrincipalId = $adf.identity.principalId
if (-not $adfPrincipalId) {
    throw "ADF factory $FactoryName has no system-assigned managed identity."
}
Write-Host "ADF MI principal: $adfPrincipalId" -ForegroundColor Cyan

# ---------------------------------------------------------------------------
# Deploy main.bicep
# ---------------------------------------------------------------------------
Write-Host "`nDeploying infra/main.bicep ..." -ForegroundColor Yellow
$depJson = az deployment group create `
    --resource-group $ResourceGroup `
    --template-file (Join-Path $here 'main.bicep') `
    --parameters `
        location=$Location `
        namePrefix=$NamePrefix `
        sqlAdminObjectId=$adminObjectId `
        sqlAdminLogin=$adminLogin `
        adfPrincipalId=$adfPrincipalId `
        containerName=$ContainerName `
    -o json
if ($LASTEXITCODE -ne 0 -or -not $depJson) {
    throw "Bicep deployment failed. See error output above."
}
$dep = $depJson | ConvertFrom-Json

$out = $dep.properties.outputs
$storageName     = $out.storageAccountName.value
$blobEndpoint    = $out.storageBlobEndpoint.value
$sqlServerFqdn   = $out.sqlServerFqdn.value
$sqlDatabaseName = $out.sqlDatabaseName.value
$blobConn        = $out.blobLinkedServiceConnectionString.value
$sqlConn         = $out.sqlConnectionString.value

foreach ($pair in @(
    @{Name='storageAccountName'; Value=$storageName},
    @{Name='sqlServerFqdn';      Value=$sqlServerFqdn},
    @{Name='sqlDatabaseName';    Value=$sqlDatabaseName}
)) {
    if (-not $pair.Value) {
        throw "Deployment succeeded but output '$($pair.Name)' is empty. Inspect the deployment in the portal."
    }
}

Write-Host "`nProvisioned:" -ForegroundColor Green
Write-Host "  Storage account : $storageName"
Write-Host "  Blob container  : $ContainerName"
Write-Host "  SQL server      : $sqlServerFqdn"
Write-Host "  SQL database    : $sqlDatabaseName"

# ---------------------------------------------------------------------------
# Add ADF MI as SQL user (must be done over T-SQL; ARM cannot do this)
# ---------------------------------------------------------------------------
Write-Host "`nGranting ADF MI db_datareader on $sqlDatabaseName ..." -ForegroundColor Yellow
$adfUserSql = @"
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$FactoryName')
BEGIN
    CREATE USER [$FactoryName] FROM EXTERNAL PROVIDER;
END
ALTER ROLE db_datareader ADD MEMBER [$FactoryName];
"@
$adfUserSql | sqlcmd -S $sqlServerFqdn -d $sqlDatabaseName -G -b
if ($LASTEXITCODE -ne 0) { throw "sqlcmd (grant ADF MI) failed." }

# ---------------------------------------------------------------------------
# Seed dbo.activity
# ---------------------------------------------------------------------------
Write-Host "`nSeeding dbo.activity ..." -ForegroundColor Yellow
sqlcmd -S $sqlServerFqdn -d $sqlDatabaseName -G -b -i (Join-Path $here 'seed_activity.sql')
if ($LASTEXITCODE -ne 0) { throw "sqlcmd (seed_activity.sql) failed." }

# ---------------------------------------------------------------------------
# Print linked-service updates the user must paste into ADF Studio
# ---------------------------------------------------------------------------
Write-Host "`n========== Linked service updates ==========" -ForegroundColor Green
Write-Host @"

LS_83BC83D9-0964-4E95-AD1A-D90C7CFACFA6 (AzureBlobStorage)
  Recommended: switch authentication to 'Managed Identity' and set the
  storage account name to:
      $storageName
  Or use this connection string (then add an account key from the portal):
      $blobConn

LS_D0EC7691-66FF-4ECD-8873-1BDEBB324C92 (SqlServer)
  Change 'type' from SqlServer to AzureSqlDatabase, remove connectVia,
  set authentication to 'Managed Identity', and set:
      Server   : $sqlServerFqdn
      Database : $sqlDatabaseName
  Or use this connection string:
      $sqlConn

Dataset DS_Flat_File_Destination - update typeProperties.location.container
to:  $ContainerName  (already matches default).

After updating both linked services, click 'Test connection' on each, then
debug pipeline PL_Parent_1x_DataFlowTask__3x_FileSystemTask with a small
configItems array.
"@
