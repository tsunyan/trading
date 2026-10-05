param(
    [Parameter(Mandatory = $true)][string]$Directory,
    [int]$IntervalSeconds = 60,
    [int]$SyncDurationSeconds = 0,
    [switch]$PlanOnly
)
$ErrorActionPreference = 'Stop'
$workspace = Split-Path -Parent $PSScriptRoot
$python = Join-Path $workspace '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Run uv sync --frozen --all-groups in the repository before installing tasks.'
}
$resolvedDirectory = (Resolve-Path -LiteralPath $Directory).Path
Push-Location $workspace
try {
    $planArguments = @('-m', 'trading.private_tasks', 'plan', '--directory', $resolvedDirectory,
        '--interval-seconds', $IntervalSeconds)
    if ($SyncDurationSeconds -gt 0) {
        $planArguments += @('--sync-duration-seconds', $SyncDurationSeconds)
    }
    $planText = & $python @planArguments
    if ($LASTEXITCODE -ne 0) { throw 'Private watchdog plan failed; inspect local saved state.' }
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
. (Join-Path $PSScriptRoot 'task-owner.ps1')
$currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
# Only the bound monitor's own task can be updated. Never overwrite an unrelated task.
foreach ($spec in $plan.tasks) {
    $existing = Get-ScheduledTask -TaskPath '\' -TaskName $spec.name -ErrorAction SilentlyContinue
    Assert-OwnScheduledTask -Existing $existing -Name $spec.name -Description $spec.description `
        -CurrentSid $currentSid
}
foreach ($spec in $plan.tasks) {
    $action = New-ScheduledTaskAction -Execute $spec.executable -Argument $spec.arguments `
        -WorkingDirectory $spec.working_directory
    $periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
        -RepetitionInterval ([TimeSpan]::FromSeconds($plan.interval_seconds))
    $logon = New-ScheduledTaskTrigger -AtLogOn -User $principal.UserId
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::FromSeconds($spec.execution_limit_seconds))
    Register-ScheduledTask -TaskPath '\' -TaskName $spec.name -Action $action -Trigger @($periodic, $logon) `
        -Principal $principal -Settings $settings -Description $spec.description -Force | Out-Null
}
$plan | ConvertTo-Json -Depth 5
