param(
    [Parameter(Mandatory)][string]$Workspace,
    [Parameter(Mandatory)][string]$PrivateAccount,
    [Parameter(Mandatory)][string]$ServingAccount,
    [string]$Pool = 'etl',
    [string]$Database = 'prices'
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$output = Join-Path $root '.local\synapse'
python (Join-Path $PSScriptRoot 'build_synapse.py') --workspace $Workspace `
    --private-account $PrivateAccount --serving-account $ServingAccount `
    --pool $Pool --database $Database --output $output
if ($LASTEXITCODE -ne 0) { throw 'Synapse artifact build failed.' }
$bundle = Get-Content -Raw (Join-Path $output 'bundle.json') | ConvertFrom-Json
az storage blob upload --account-name $PrivateAccount --container-name workspace `
    --name $bundle.blob --file (Join-Path $output 'etl.zip') --auth-mode login `
    --overwrite false --only-show-errors --output none
if ($LASTEXITCODE -ne 0) {
    # Existing immutable bundle is acceptable only if its bytes match.
    $existing = Join-Path $output 'existing.zip'
    az storage blob download --account-name $PrivateAccount --container-name workspace `
        --name $bundle.blob --file $existing --auth-mode login --overwrite --only-show-errors --output none
    if ($LASTEXITCODE -ne 0) { throw 'Cannot upload or verify the code bundle.' }
    try {
        if ((Get-FileHash -LiteralPath $existing -Algorithm SHA256).Hash.ToLowerInvariant() -ne $bundle.sha256) {
            throw 'Immutable code bundle checksum conflict.'
        }
    } finally { Remove-Item -LiteralPath $existing }
}
az synapse notebook import --workspace-name $Workspace --name GeorgiaPrices `
    --file "@$(Join-Path $output 'GeorgiaPrices.ipynb')" --spark-pool-name $Pool `
    --executor-count 2 --executor-size Small --only-show-errors --output none
if ($LASTEXITCODE -ne 0) { throw 'Notebook import failed.' }
az synapse linked-service create --workspace-name $Workspace --name ServerlessPrices `
    --file "@$(Join-Path $output 'ServerlessPrices.json')" --only-show-errors --output none
if ($LASTEXITCODE -ne 0) { throw 'Serverless SQL linked service creation failed.' }
az synapse pipeline create --workspace-name $Workspace --name GeorgiaMonthly `
    --file "@$(Join-Path $output 'GeorgiaMonthly.json')" --only-show-errors --output none
if ($LASTEXITCODE -ne 0) { throw 'Pipeline creation failed.' }
Write-Output "Artifacts published. Run $output\bootstrap.sql as the Entra SQL administrator."
Write-Output 'No pipeline or schedule was started. Complete a manual smoke run before importing/enabling the schedule.'
