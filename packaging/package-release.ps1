param(
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Target,
    [string]$Profile = "release",
    [switch]$NoBuild
)

$ErrorActionPreference = "Stop"
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

$WintunVersion = "0.14.1"
$WintunSha256 = "07c256185d6ee3652e09fa55c0b673e2624b565e02c4b9091c79ca7d2f24ef51"
$WintunUrl = "https://www.wintun.net/builds/wintun-$WintunVersion.zip"

if ($Profile -notmatch '^[A-Za-z0-9][A-Za-z0-9_.-]*$') {
    throw "Invalid Cargo profile: $Profile"
}

$ReleaseContract = Join-Path $RepoRoot "packaging/tools/release_contract.py"
$ContractJson = python -B $ReleaseContract target --target $Target
if ($LASTEXITCODE -ne 0) {
    throw "Unsupported release target: $Target"
}
$Contract = $ContractJson | ConvertFrom-Json
if ($Contract.os -ne "windows") {
    throw "package-release.ps1 only supports normalized Windows release targets: $Target"
}

if ($Target -eq "x86_64-pc-windows-msvc") {
    $WintunArchitecture = "amd64"
} elseif ($Target -eq "aarch64-pc-windows-msvc") {
    $WintunArchitecture = "arm64"
} else {
    throw "Unsupported normalized Windows release target: $Target"
}

$OriginalLocation = Get-Location
$OriginalRustFlags = $env:RUSTFLAGS
$OriginalPythonPycachePrefix = $env:PYTHONPYCACHEPREFIX
$OriginalTemp = $env:TEMP
$OriginalTmp = $env:TMP

function Get-WintunArchive {
    $CacheDir = Join-Path ".tmp/release" "dependencies"
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
    New-Item -ItemType Directory -Force ".tmp/release", ".tmp/python-cache", ".tmp/system" | Out-Null
    $env:PYTHONPYCACHEPREFIX = (Resolve-Path ".tmp/python-cache").Path
    $Scratch = (Resolve-Path ".tmp/system").Path
    $env:TEMP = $Scratch
    $env:TMP = $Scratch

    if (-not $NoBuild) {
        if ([string]::IsNullOrWhiteSpace($env:RUSTFLAGS)) {
            $env:RUSTFLAGS = "-C target-feature=+crt-static"
        } elseif (-not $env:RUSTFLAGS.Contains("+crt-static")) {
            $env:RUSTFLAGS = "$env:RUSTFLAGS -C target-feature=+crt-static"
        }
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
    $MptunnelPackages = @($Metadata.packages | Where-Object { $_.name -eq "mptunnel" })
    if ($MptunnelPackages.Count -ne 1) {
        throw "cargo metadata did not contain exactly one mptunnel package"
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

    $Package = $Contract.package
    $DistDir = ".tmp/release/dist"
    $Stage = Join-Path $DistDir $Package
    $ReleaseFiles = @("packaging/README.md", "LICENSE")
    $ReleaseExamples = @("examples/client.toml", "examples/server.toml")
    foreach ($ReleaseFile in $ReleaseFiles + $ReleaseExamples) {
        if (-not (Test-Path -PathType Leaf $ReleaseFile)) {
            throw "Required release file is missing: $ReleaseFile"
        }
    }

    if (Test-Path $Stage) {
        Remove-Item -Recurse -Force $Stage
    }
    $StageExamples = Join-Path $Stage "examples"
    New-Item -ItemType Directory -Force $StageExamples | Out-Null
    Copy-Item $BinaryPath $Stage
    Copy-Item "packaging/README.md" (Join-Path $Stage "README.md")
    Copy-Item "LICENSE" $Stage
    Copy-Item $ReleaseExamples $StageExamples

    $WintunArchive = Get-WintunArchive
    Copy-WintunPackageFiles $WintunArchive $WintunArchitecture $Stage

    New-Item -ItemType Directory -Force $DistDir | Out-Null
    $Archive = Join-Path $DistDir $Contract.archive_name
    Remove-Item -Force -ErrorAction SilentlyContinue $Archive
    python -B packaging/tools/build_release_archive.py `
        --stage $Stage `
        --archive $Archive `
        --target $Target
    if ($LASTEXITCODE -ne 0) {
        throw "Deterministic release archive construction failed"
    }
    Remove-Item -Recurse -Force $Stage
    Write-Output $Archive
} finally {
    $env:RUSTFLAGS = $OriginalRustFlags
    $env:PYTHONPYCACHEPREFIX = $OriginalPythonPycachePrefix
    $env:TEMP = $OriginalTemp
    $env:TMP = $OriginalTmp
    Set-Location $OriginalLocation
}
