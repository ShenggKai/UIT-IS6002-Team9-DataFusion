# Chay demo tren Windows PowerShell.
# Khong doi execution policy cua may. Dung lai khi lenh con loi.
param(
    [int]$Rows = 5000000,
    [int]$Runs = 7,
    [int[]]$Parallelism = @(1, 2),
    [string]$Data = "",
    [string]$Out = ""
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $Data) { $Data = Join-Path $Root "data" }
if (-not $Out) { $Out = Join-Path $Root "results-rerun" }

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $Python)) {
    Write-Error "Chua co $Python. Tao virtualenv theo README, roi chay lai script nay."
}
if ($Python -like "*WindowsApps*") {
    Write-Error "Interpreter dang tro vao WindowsApps stub. Hay dung .venv\Scripts\python.exe."
}

function Invoke-Step([string[]]$StepArgs) {
    & $Python @StepArgs
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }
}

$ParArgs = @("--parallelism") + ($Parallelism | ForEach-Object { "$_" })
Invoke-Step @(
    (Join-Path $Root "src\gen_data.py"),
    "--rows", "$Rows", "--seed", "42", "--out", $Data
)
Invoke-Step @(
    (Join-Path $Root "src\run_experiments.py"),
    "--data", $Data, "--out", $Out, "--runs", "$Runs", "--seed", "42"
) + $ParArgs
Invoke-Step @((Join-Path $Root "src\make_charts.py"), $Out)
Write-Host "Xong. Ket qua nam o $Out"
