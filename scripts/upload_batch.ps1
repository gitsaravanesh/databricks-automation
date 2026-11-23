<#
Upload one sample batch to a landing volume (Windows PowerShell).

Usage, from the repo root:
    .\scripts\upload_batch.ps1 -Batch 1 -Catalog <catalog>                       # dev
    .\scripts\upload_batch.ps1 -Batch 1 -Catalog <catalog> -Schema test_bronze   # test
    .\scripts\upload_batch.ps1 -Batch 1 -Catalog <catalog> -Schema bronze        # prod
Add -CliProfile <name> to use a specific Databricks CLI profile.
#>
param(
    [Parameter(Mandatory = $true)][ValidateRange(1, 3)][int]$Batch,
    [Parameter(Mandatory = $true)][string]$Catalog,
    [string]$Schema = "dev_bronze",
    [string]$CliProfile = ""
)

$ErrorActionPreference = "Stop"
$dest = "dbfs:/Volumes/$Catalog/$Schema/landing/incoming"
$src = Get-ChildItem -Directory "sample_data" | Where-Object { $_.Name -like ("batch_{0:D2}_*" -f $Batch) } |
    Select-Object -First 1
if (-not $src) { throw "No sample_data folder for batch $Batch. Run from the repo root." }

foreach ($table in Get-ChildItem -Directory $src.FullName) {
    $cliArgs = @("fs", "cp", "-r", "--overwrite", $table.FullName, "$dest/$($table.Name)")
    if ($CliProfile) { $cliArgs += @("--profile", $CliProfile) }
    Write-Host "Uploading $($src.Name)/$($table.Name) -> $dest/$($table.Name)"
    & databricks @cliArgs
    if ($LASTEXITCODE -ne 0) { throw "Upload failed for $($table.Name)" }
}
Write-Host "Batch $Batch uploaded to $dest"
