# Shared by the task installers: only update a task this user registered for this plan.

function Get-TaskOwnerSid([string]$UserId) {
    # Task Scheduler may normalize DOMAIN\user to a short name or a SID.
    if ($UserId -match '^S-\d(-\d+)+$') {
        return ([Security.Principal.SecurityIdentifier]::new($UserId)).Value
    }
    $account = [Security.Principal.NTAccount]::new($UserId)
    return $account.Translate([Security.Principal.SecurityIdentifier]).Value
}

function Assert-OwnScheduledTask {
    param($Existing, [string]$Name, [string]$Description, [string]$CurrentSid)
    if (-not $Existing) { return }
    try {
        $existingSid = Get-TaskOwnerSid $Existing.Principal.UserId
    } catch {
        throw "Cannot verify the scheduled task owner for $Name."
    }
    if ($Existing.Description -ne $Description -or $existingSid -ne $CurrentSid) {
        throw "An unrelated scheduled task already uses the name $Name."
    }
}
