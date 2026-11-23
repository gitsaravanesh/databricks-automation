<#
Upload one sample batch to the landing volume (Windows PowerShell).

Usage, from the repo root:
    .\scripts\upload_batch.ps1 -Batch 1
    .\scripts\upload_batch.ps1 -Batch 2 -CliProfile free
    .\scripts\upload_batch.ps1 -Batch 1 -Dest "dbfs:/Volumes/workspace/raja_bronze/landing/incoming"
#>
param(
    [Parameter(Mandatory = $true)][ValidateRange(1, 3)][int]$Batch,
    [string]$Dest = "dbfs:/Volumes/workspace/dev_bronze/landing/incoming",
    [string]$CliProfile = ""
)

$ErrorActionPreference = "Stop"
$src = Get-ChildItem -Directory "sample_data" | Where-Object { $_.Name -like ("batch_{0:D2}_*" -f $Batch) } |
    Select-Object -First 1
if (-not $src) { throw "No sample_data folder for batch $Batch. Run from the repo root." }

foreach ($table in Get-ChildItem -Directory $src.FullName) {
    $cliArgs = @("fs", "cp", "-r", "--overwrite", $table.FullName, "$Dest/$($table.Name)")
    if ($CliProfile) { $cliArgs += @("--profile", $CliProfile) }
    Write-Host "Uploading $($src.Name)/$($table.Name) -> $Dest/$($table.Name)"
    & databricks @cliArgs
    if ($LASTEXITCODE -ne 0) { throw "Upload failed for $($table.Name)" }
}
Write-Host "Batch $Batch uploaded. Now run: databricks bundle run -t dev bronze_silver_job"
