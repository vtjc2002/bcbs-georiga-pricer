param(
    [ValidateSet('Test', 'Pilot', 'Run')][string]$Mode = 'Test',
    [string]$Index,
    [string]$Filename,
    [string]$Volume,
    [double]$Cpus = 1,
    [string]$Memory = '2G',
    [ValidateRange(1, 1024)][double]$MaxOutputGiB = 3,
    [switch]$Offline
)
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
$wslc = (Get-Command wslc.exe -ErrorAction Stop).Source
& $wslc info
if ($LASTEXITCODE -ne 0) { throw 'wslc runtime is not ready.' }
$dockerfile = if ($Offline) { 'etl\Dockerfile.local' } else { 'etl\Dockerfile' }
& $wslc build --file (Join-Path $root $dockerfile) --tag georgia-pricer:local $root
if ($LASTEXITCODE -ne 0) { throw 'Container image build failed.' }
if ($Mode -eq 'Test') {
    & $wslc run --rm --cpus $Cpus --memory $Memory georgia-pricer:local test
} else {
    $output = Join-Path $root '.local'
    New-Item -ItemType Directory -Force -Path $output | Out-Null
    $mount = if ($Volume) { "type=volume,source=$Volume,target=/output" }
        else { "type=bind,source=$output,target=/output" }
    $arguments = @(
        'run', '--rm', '--cpus', "$Cpus", '--memory', $Memory,
        '--mount', $mount
    )
    if ($Index) {
        $resolvedIndex = (Resolve-Path -LiteralPath $Index).Path
        $arguments += @('--mount', "type=bind,source=$resolvedIndex,target=/input/index.json,readonly")
    }
    $arguments += @('georgia-pricer:local', 'run', '--output', '/output', '--max-output-gib', "$MaxOutputGiB")
    if ($Index) { $arguments += @('--index', '/input/index.json') }
    if ($Mode -eq 'Pilot') {
        $arguments += '--pilot'
        if ($Filename) { $arguments += @('--filename', $Filename) }
        else { $arguments += @('--max-files', '1') }
    }
    & $wslc @arguments
}
if ($LASTEXITCODE -ne 0) { throw 'Local container validation failed.' }
