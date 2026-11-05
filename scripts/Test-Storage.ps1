$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$wslc = (Get-Command wslc.exe -ErrorAction Stop).Source
$network = 'georgia-pricer-a5e015e3-test'
$container = 'georgia-pricer-a5e015e3-azurite'
$local = Join-Path $root '.local'
New-Item -ItemType Directory -Force -Path $local | Out-Null
$serverEnv = Join-Path $local 'azurite.env'
$clientEnv = Join-Path $local 'storage-tests.env'
$key = [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(32))
Set-Content -LiteralPath $serverEnv -Value "AZURITE_ACCOUNTS=localtest:$key"
Set-Content -LiteralPath $clientEnv -Value "AZURITE_CONNECTION_STRING=DefaultEndpointsProtocol=http;AccountName=localtest;AccountKey=$key;BlobEndpoint=http://${container}:10000/localtest;"
$networkCreated = $false
$containerStarted = $false
try {
    & $wslc network create $network
    if ($LASTEXITCODE -ne 0) { throw 'Could not create isolated test network.' }
    $networkCreated = $true
    & $wslc run --rm --detach --name $container --network $network --env-file $serverEnv `
        mcr.microsoft.com/azure-storage/azurite:3.35.0 azurite-blob --blobHost 0.0.0.0 --skipApiVersionCheck
    if ($LASTEXITCODE -ne 0) { throw 'Could not start Azurite.' }
    $containerStarted = $true
    & $wslc run --rm --cpus 1 --memory 2G --network $network --env-file $clientEnv `
        --mount "type=bind,source=$root,target=/app" `
        georgia-pricer:local test
    if ($LASTEXITCODE -ne 0) { throw 'Storage integration tests failed.' }
} finally {
    if ($containerStarted) { & $wslc stop $container | Out-Null }
    if ($networkCreated) { & $wslc network remove $network | Out-Null }
    Remove-Item -LiteralPath $serverEnv, $clientEnv -Force
}
