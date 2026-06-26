param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("client", "server")]
    [string]$Mode,

    [string]$Name = "mptunnel",
    [string]$BinaryPath = "C:\Program Files\mptunnel\mptunnel.exe",
    [string]$Arguments = "",
    [string]$DisplayName = "mptunnel"
)

$ErrorActionPreference = "Stop"

$principal = New-Object Security.Principal.WindowsPrincipal([Security.Principal.WindowsIdentity]::GetCurrent())
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw "Administrator rights are required to install the Windows service"
}

$serviceArgs = "--service-mode --supervise $Mode"
if ($Arguments -ne "") {
    $serviceArgs = "$serviceArgs $Arguments"
}
$quotedBinary = '"' + $BinaryPath + '"'
$binaryName = "$quotedBinary $serviceArgs"

if (Get-Service -Name $Name -ErrorAction SilentlyContinue) {
    Stop-Service -Name $Name -ErrorAction SilentlyContinue
    sc.exe delete $Name | Out-Null
}

New-Service `
    -Name $Name `
    -BinaryPathName $binaryName `
    -DisplayName $DisplayName `
    -StartupType Automatic | Out-Null

sc.exe failure $Name reset= 60 actions= restart/5000/restart/15000/restart/30000 | Out-Null
Write-Output "Installed service $Name"
