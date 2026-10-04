param(
    [Parameter(Mandatory = $true)][string]$Directory,
    [Parameter(Mandatory = $true)][string]$ReadControlDirectory,
    [Parameter(Mandatory = $true)][string]$Scope,
    [Parameter(Mandatory = $true)][string]$CredentialReference,
    [Parameter(Mandatory = $true)][string]$Config,
    [Parameter(Mandatory = $true)][string]$Units,
    [Parameter(Mandatory = $true)][string]$MaxSlippage,
    [Parameter(Mandatory = $true)][string]$QuoteOutput,
    [Parameter(Mandatory = $true)][string]$ResultOutput,
    [string]$ValuationTolerance,
    [string]$HistoryOutput,
    [string]$DashboardOutput,
    [int]$DoctorIntervalSeconds = 0,
    [string]$Ledger,
    [string]$Hypothesis,
    [Parameter(Mandatory = $true)][string]$Attestation,
    [switch]$PlanOnly
)
$ErrorActionPreference = 'Stop'
$workspace = Split-Path -Parent $PSScriptRoot
$python = Join-Path $workspace '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Run uv sync --frozen --all-groups in the repository before installing tasks.'
}
$arguments = @(
    '-m', 'trading.live_tasks', 'plan',
    '--directory', (Resolve-Path -LiteralPath $Directory).Path,
    '--read-control-directory', (Resolve-Path -LiteralPath $ReadControlDirectory).Path,
    '--scope', $Scope,
    '--credential-reference', $CredentialReference,
    '--config', (Resolve-Path -LiteralPath $Config).Path,
    '--units', $Units,
    '--max-slippage', $MaxSlippage,
    '--quote-output', $QuoteOutput,
    '--result-output', $ResultOutput
)
if ($ValuationTolerance) { $arguments += @('--valuation-tolerance', $ValuationTolerance) }
if ($DoctorIntervalSeconds -gt 0) { $arguments += @('--doctor-interval-seconds', $DoctorIntervalSeconds) }
if ($DashboardOutput) { $arguments += @('--dashboard-output', $DashboardOutput) }
if ($HistoryOutput) { $arguments += @('--history-output', $HistoryOutput) }
if ($Ledger) { $arguments += @('--ledger', (Resolve-Path -LiteralPath $Ledger).Path) }
if ($Hypothesis) { $arguments += @('--hypothesis', $Hypothesis) }
$arguments += @('--attestation', (Resolve-Path -LiteralPath $Attestation).Path)
Push-Location $workspace
try {
    $planText = & $python @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Live cycle plan failed; inspect the printed reason.' }
    $plan = ($planText -join "`n") | ConvertFrom-Json
} finally {
    Pop-Location
}
if ($PlanOnly) {
    # Python already emitted ASCII JSON; preserve escaped paths across code pages.
    $planText
    return
}
$principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
$currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
# Only this journal's own task can be updated. Never overwrite an unrelated task.
foreach ($spec in $plan.tasks) {
    $existing = Get-ScheduledTask -TaskPath '\' -TaskName $spec.name -ErrorAction SilentlyContinue
    $existingSid = $null
    if ($existing) {
        try {
            if ($existing.Principal.UserId -match '^S-\d(-\d+)+$') {
                $existingSid = ([Security.Principal.SecurityIdentifier]::new($existing.Principal.UserId)).Value
            } else {
                $account = [Security.Principal.NTAccount]::new($existing.Principal.UserId)
                $existingSid = $account.Translate([Security.Principal.SecurityIdentifier]).Value
            }
        } catch {
            throw "Cannot verify the scheduled task owner for $($spec.name)."
        }
    }
    if ($existing -and (
        $existing.Description -ne $spec.description -or
        $existingSid -ne $currentSid
    )) {
        throw "An unrelated scheduled task already uses the name $($spec.name)."
    }
}
$now = Get-Date
$start = $now.Date.AddHours($now.Hour + 1).AddMinutes($plan.start_minute)
foreach ($spec in $plan.tasks) {
    $action = New-ScheduledTaskAction -Execute $spec.executable -Argument $spec.arguments `
        -WorkingDirectory $spec.working_directory
    # A task with its own interval (the doctor) starts within a minute; the cycle keeps hh:01.
    if ($spec.PSObject.Properties.Name -contains 'interval_seconds') {
        $hourly = New-ScheduledTaskTrigger -Once -At ($now.AddMinutes(1)) `
            -RepetitionInterval ([TimeSpan]::FromSeconds($spec.interval_seconds))
    } else {
        $hourly = New-ScheduledTaskTrigger -Once -At $start `
            -RepetitionInterval ([TimeSpan]::FromSeconds($plan.interval_seconds))
    }
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::FromSeconds($spec.execution_limit_seconds))
    Register-ScheduledTask -TaskPath '\' -TaskName $spec.name -Action $action -Trigger @($hourly) `
        -Principal $principal -Settings $settings -Description $spec.description -Force | Out-Null
}
$plan | ConvertTo-Json -Depth 5
