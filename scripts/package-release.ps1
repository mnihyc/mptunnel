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
    if ($LASTEXITCODE -ne 0) {
        throw "rustc failed to report the host target"
    }
}
if ($Target -notmatch '^[A-Za-z0-9][A-Za-z0-9_.+-]*$') {
    throw "Invalid target triple: $Target"
}
if ($Profile -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') {
    throw "Invalid Cargo profile: $Profile"
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

$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$OriginalLocation = Get-Location

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

try {
    Set-Location $RepoRoot

    if (-not $NoBuild) {
        cargo build --locked --profile $Profile --target $Target --bin mptunnel
        if ($LASTEXITCODE -ne 0) {
            throw "cargo build failed for $Target"
        }
    }

    $MetadataJson = cargo metadata --locked --no-deps --format-version 1
    if ($LASTEXITCODE -ne 0) {
        throw "cargo metadata failed"
    }
    $Metadata = $MetadataJson | ConvertFrom-Json
    $Packages = @($Metadata.packages | Where-Object { $_.name -eq "mptunnel" })
    if ($Packages.Count -ne 1) {
        throw "cargo metadata did not contain exactly one mptunnel package"
    }
    $Version = $Packages[0].version
    if ($Version -notmatch '^[A-Za-z0-9][A-Za-z0-9.+-]*$') {
        throw "Cargo returned an unsafe package version: $Version"
    }

    $ProfileDir = $Profile
    if ($Profile -eq "dev") {
        $ProfileDir = "debug"
    }
    $TargetDir = Join-Path $Metadata.target_directory (Join-Path $Target $ProfileDir)
    $Binary = "mptunnel.exe"
    $BinaryPath = Join-Path $TargetDir $Binary
    if (-not (Test-Path -PathType Leaf $BinaryPath) -or (Get-Item $BinaryPath).Length -eq 0) {
        throw "Built binary is missing or empty: $BinaryPath"
    }

    $Package = "mptunnel-$Version-$Target"
    $DistDir = "dist"
    $Stage = Join-Path $DistDir $Package
    $ReleaseFiles = @("README.md", "RFC.md", "LICENSE", "SECURITY.md", "CONTRIBUTING.md", "config.toml")
    $ReleaseDocs = @("docs/ARCHITECTURE.md", "docs/OPERATIONS.md", "docs/PERFORMANCE.md")
    $ReleaseExamples = @("examples/client.toml", "examples/server.toml")
    $ReleaseAssets = @("docs/assets/dashboard.png")
    foreach ($ReleaseFile in $ReleaseFiles + $ReleaseDocs + $ReleaseExamples + $ReleaseAssets) {
        if (-not (Test-Path -PathType Leaf $ReleaseFile)) {
            throw "Required release file is missing: $ReleaseFile"
        }
    }

    if (Test-Path $Stage) {
        Remove-Item -Recurse -Force $Stage
    }
    $StageDocs = Join-Path $Stage "docs"
    $StageAssets = Join-Path $StageDocs "assets"
    $StageExamples = Join-Path $Stage "examples"
    New-Item -ItemType Directory -Force $StageAssets | Out-Null
    New-Item -ItemType Directory -Force $StageExamples | Out-Null
    Copy-Item $BinaryPath $Stage
    Copy-Item $ReleaseFiles $Stage
    Copy-Item $ReleaseDocs $StageDocs
    Copy-Item $ReleaseExamples $StageExamples
    Copy-Item $ReleaseAssets $StageAssets

    $WintunArchive = Get-WintunArchive
    Copy-WintunPackageFiles $WintunArchive $WintunArchitecture $Stage

    New-Item -ItemType Directory -Force $DistDir | Out-Null
    $Archive = Join-Path $DistDir "$Package.zip"
    $Checksum = "$Archive.sha256"
    Remove-Item -Force -ErrorAction SilentlyContinue -Path @($Archive, $Checksum)
    Compress-Archive -Path $Stage -DestinationPath $Archive

    $Hash = (Get-FileHash -Algorithm SHA256 $Archive).Hash.ToLowerInvariant()
    $ArchiveName = Split-Path -Leaf $Archive
    "$Hash  $ArchiveName" | Set-Content -Encoding ascii $Checksum
    Write-Output $Archive
} finally {
    Set-Location $OriginalLocation
}
