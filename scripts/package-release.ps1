param(
    [string]$Target = "",
    [string]$Profile = "release",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"

$WintunVersion = "0.14.1"
$WintunSha256 = "07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51"
$WintunUrl = "https://www.wintun.net/builds/wintun-$WintunVersion.zip"

if ($Target -eq "") {
    $Target = (rustc -vV | Select-String "^host:" | ForEach-Object { $_.ToString().Split(" ")[1] })
}

if ($Target -match "^x86_64-.*-windows-") {
    $WintunArchitecture = "amd64"
} elseif ($Target -match "^aarch64-.*-windows-") {
    $WintunArchitecture = "arm64"
} elseif ($Target -match "windows") {
    throw "Unsupported Windows target architecture: $Target"
} else {
    throw "package-release.ps1 only supports Windows target triples: $Target"
}

function Get-WintunArchive {
    $CacheDir = Join-Path "target" "release-dependencies"
    $Archive = Join-Path $CacheDir "wintun-$WintunVersion.zip"
    New-Item -ItemType Directory -Force $CacheDir | Out-Null

    if (Test-Path $Archive) {
        $CachedHash = (Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLowerInvariant()
        if ($CachedHash -eq $WintunSha256) {
            return $Archive
        }
        Remove-Item -Force $Archive
    }

    $Download = "$Archive.download-$PID"
    Remove-Item -Force -ErrorAction SilentlyContinue $Download
    $null = Invoke-WebRequest -Uri $WintunUrl -OutFile $Download
    $DownloadedHash = (Get-FileHash -Algorithm SHA256 $Download).Hash.ToLowerInvariant()
    if ($DownloadedHash -ne $WintunSha256) {
        Remove-Item -Force $Download
        throw "Wintun $WintunVersion checksum verification failed"
    }
    Move-Item -Force $Download $Archive
    return $Archive
}

function Copy-WintunPackageFiles([string]$Archive, [string]$Architecture, [string]$Destination) {
    $ResolvedArchive = (Resolve-Path $Archive).Path
    $Zip = [System.IO.Compression.ZipFile]::OpenRead($ResolvedArchive)
    try {
        $Dll = $Zip.GetEntry("wintun/bin/$Architecture/wintun.dll")
        $License = $Zip.GetEntry("wintun/LICENSE.txt")
        if ($null -eq $Dll -or $null -eq $License) {
            throw "Wintun $WintunVersion archive is missing required package files"
        }
        [System.IO.Compression.ZipFileExtensions]::ExtractToFile(
            $Dll,
            (Join-Path $Destination "wintun.dll"),
            $true
        )
        [System.IO.Compression.ZipFileExtensions]::ExtractToFile(
            $License,
            (Join-Path $Destination "WINTUN-LICENSE.txt"),
            $true
        )
    } finally {
        $Zip.Dispose()
    }
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
$WintunArchive = Get-WintunArchive
Copy-WintunPackageFiles $WintunArchive $WintunArchitecture $Stage

New-Item -ItemType Directory -Force $DistDir | Out-Null
$Archive = Join-Path $DistDir "$Package.zip"
if (Test-Path $Archive) {
    Remove-Item -Force $Archive
}
Compress-Archive -Path $Stage -DestinationPath $Archive

$Hash = Get-FileHash -Algorithm SHA256 $Archive
"$($Hash.Hash.ToLowerInvariant())  $Archive" | Set-Content "$Archive.sha256"
Write-Output $Archive
