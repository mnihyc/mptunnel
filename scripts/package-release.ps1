param(
    [string]$Target = "",
    [string]$Profile = "release",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

if ($Target -eq "") {
    $Target = (rustc -vV | Select-String "^host:" | ForEach-Object { $_.ToString().Split(" ")[1] })
}

$Binary = "mptunnel.exe"
$TargetDir = Join-Path "target" (Join-Path $Target $Profile)

if (-not $NoBuild) {
    cargo build --profile $Profile --target $Target --bin mptunnel
}

$Metadata = cargo metadata --no-deps --format-version 1 | ConvertFrom-Json
$Version = $Metadata.packages[0].version
$Package = "mptunnel-$Version-$Target"
$DistDir = "dist"
$Stage = Join-Path $DistDir $Package

if (Test-Path $Stage) {
    Remove-Item -Recurse -Force $Stage
}
New-Item -ItemType Directory -Force $Stage | Out-Null
Copy-Item (Join-Path $TargetDir $Binary) $Stage
Copy-Item README.md $Stage
Copy-Item LICENSE $Stage
Copy-Item -Recurse docs $Stage

New-Item -ItemType Directory -Force $DistDir | Out-Null
$Archive = Join-Path $DistDir "$Package.zip"
if (Test-Path $Archive) {
    Remove-Item -Force $Archive
}
Compress-Archive -Path $Stage -DestinationPath $Archive

$Hash = Get-FileHash -Algorithm SHA256 $Archive
"$($Hash.Hash.ToLowerInvariant())  $Archive" | Set-Content "$Archive.sha256"
Write-Output $Archive
