param(
    [Parameter(Mandatory = $true)][string]$Directory,
    [string]$Policy,
    [switch]$PlanOnly
)
$ErrorActionPreference = 'Stop'
$workspace = Split-Path -Parent $PSScriptRoot
$python = Join-Path $workspace '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw 'Run uv sync --frozen --all-groups in the repository before installing tasks.'
}
$resolvedDirectory = (Resolve-Path -LiteralPath $Directory).Path
$planArgs = @('-m', 'trading.windows_tasks', 'plan', '--directory', $resolvedDirectory)
if ($Policy) {
    $planArgs += @('--policy', (Resolve-Path -LiteralPath $Policy).Path)
}
Push-Location $workspace
try {
    $planText = & $python @planArgs
    if ($LASTEXITCODE -ne 0) { throw ($planText -join "`n") }
    $plan = ($planText -join "`n") | ConvertFrom-Json
} finally {
    Pop-Location
}
if ($PlanOnly) {
    # Python already emitted ASCII JSON; preserve escaped paths across code pages.
    $planText
    return
}
. (Join-Path $PSScriptRoot 'task-owner.ps1')
$currentSid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
# Check both names before registering either, so an unrelated or another user's task is
# never overwritten.
foreach ($spec in $plan.tasks) {
    $existing = Get-ScheduledTask -TaskPath '\' -TaskName $spec.name -ErrorAction SilentlyContinue
    Assert-OwnScheduledTask -Existing $existing -Name $spec.name -Description $spec.description `
        -CurrentSid $currentSid
}
$principal = New-ScheduledTaskPrincipal -UserId ([Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited
foreach ($spec in $plan.tasks) {
    $action = New-ScheduledTaskAction -Execute $spec.executable -Argument $spec.arguments `
        -WorkingDirectory $spec.working_directory
    $offset = if ($spec.name.EndsWith('-Watchdog')) { 2 } else { 1 }
    $periodic = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes($offset) `
        -RepetitionInterval ([TimeSpan]::FromSeconds($plan.interval_seconds))
    $logon = New-ScheduledTaskTrigger -AtLogOn -User $principal.UserId
    $settings = New-ScheduledTaskSettingsSet -MultipleInstances IgnoreNew -StartWhenAvailable `
        -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
        -ExecutionTimeLimit ([TimeSpan]::FromSeconds($spec.execution_limit_seconds))
    Register-ScheduledTask -TaskPath '\' -TaskName $spec.name -Action $action -Trigger @($periodic, $logon) `
        -Principal $principal -Settings $settings -Description $spec.description -Force | Out-Null
}
$plan | ConvertTo-Json -Depth 5
