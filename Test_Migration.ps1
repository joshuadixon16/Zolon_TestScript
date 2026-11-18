#Requires -Version 7.0

<#
.SYNOPSIS
  Test harness for Migration_Script.ps1. Gives PASS / FAIL / WARN / SKIP feedback in three phases.

.DESCRIPTION
  Phase 1  Offline   No infrastructure needed. Parses the script, unit-tests the LSN conversion,
                     DMS id generation and JSON payloads. Always runs.
  Phase 2  Preflight Read-only checks against the real environment (SQL source/target, AWS, S3, DMS,
                     backup share). Only a tiny temp object is written to S3, then deleted.
                     Runs when the connection parameters are supplied.
  Phase 3  Pilot     -RunPilot: runs the real migration for ONE database (-PilotDatabase), then
                     reports the outcome per step. -CleanupPilot removes what the pilot created
                     (DMS task + endpoints, restored RDS database, S3 object) after asking you to confirm.

  Exit code: 0 = no FAIL, 1 = at least one FAIL.

.EXAMPLE
  # Phase 1 only (no credentials needed)
  ./Test_Migration.ps1

  # Phase 1 + 2
  ./Test_Migration.ps1 -SourceInstance sql01 -SourceCredential (Get-Credential) `
     -TargetEndpoint mydb.abc.us-east-2.rds.amazonaws.com -TargetCredential (Get-Credential) `
     -BackupDir 'D:\Backups' -S3Bucket my-migration-bucket -Region us-east-2 `
     -DmsReplicationInstanceArn arn:aws:dms:us-east-2:111122223333:rep:XXXX

  # Phase 1 + 2 + 3 with a small pilot database
  ./Test_Migration.ps1 <same as above> -RunPilot -PilotDatabase SalesDb
#>
[CmdletBinding()]
param(
    [string]$ScriptPath = (Join-Path $PSScriptRoot 'Migration_Script.ps1'),

    [string]$SourceInstance,
    [pscredential]$SourceCredential,
    [int]$SourcePort = 1433,
    [string]$TargetEndpoint,
    [pscredential]$TargetCredential,
    [int]$TargetPort = 1433,
    [string]$BackupDir,
    [string]$BackupDirLocal,
    [string]$S3Bucket,
    [string]$S3Prefix = 'sqlmigration',
    [string]$Region,
    [string]$AwsProfile,
    [string]$DmsReplicationInstanceArn,
    [switch]$TrustServerCertificate,
    [ValidateSet('none', 'require')][string]$DmsSslMode = 'none',

    [switch]$RunPilot,
    [string]$PilotDatabase,
    [int]$PilotMonitorMinutes = 10,
    [switch]$CleanupPilot
)

$ErrorActionPreference = 'Stop'
$script:Results = [System.Collections.Generic.List[object]]::new()

function Add-Result([string]$Phase, [string]$Name, [string]$Status, [string]$Detail = '') {
    $script:Results.Add([pscustomobject]@{ Phase = $Phase; Check = $Name; Status = $Status; Detail = $Detail })
    $color = @{ PASS = 'Green'; FAIL = 'Red'; WARN = 'Yellow'; SKIP = 'DarkGray' }[$Status]
    Write-Host ('  [{0}] {1}{2}' -f $Status, $Name, $(if ($Detail) { " - $Detail" } else { '' })) -ForegroundColor $color
}

# Runs a check; a throw is FAIL, a returned string is the PASS detail. Return 'WARN: text' to warn.
function Test-Check([string]$Phase, [string]$Name, [scriptblock]$Body) {
    try {
        $r = & $Body
        if ($r -is [string] -and $r.StartsWith('WARN:')) { Add-Result $Phase $Name 'WARN' $r.Substring(5).Trim() }
        else { Add-Result $Phase $Name 'PASS' ([string]$r) }
    }
    catch { Add-Result $Phase $Name 'FAIL' $_.Exception.Message }
}

function Assert-Equal($Actual, $Expected, [string]$What) {
    if ($Actual -ne $Expected) { throw "$What : expected '$Expected' but got '$Actual'" }
}

#region ---------- Phase 1: offline ----------
Write-Host "`n=== Phase 1: offline checks ===" -ForegroundColor Cyan

$tokens = $null; $parseErrors = $null
$ast = $null
Test-Check 'Offline' 'Script file exists' {
    if (-not (Test-Path $ScriptPath)) { throw "Not found: $ScriptPath" }
    $ScriptPath
}
Test-Check 'Offline' 'Script parses without syntax errors' {
    $script:ast = [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path $ScriptPath), [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count) { throw (($parseErrors | ForEach-Object { "line $($_.Extent.StartLineNumber): $($_.Message)" }) -join '; ') }
    "$($script:ast.FindAll({ $args[0] -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $true).Count) functions"
}

# Load selected functions from the script via the AST, without running the migration itself.
function Import-ScriptFunction([string[]]$Names) {
    foreach ($n in $Names) {
        $fn = $script:ast.Find({ param($a) $a -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $a.Name -eq $n }, $true)
        if (-not $fn) { throw "Function $n not found in script" }
        Invoke-Expression ($fn.Extent.Text -replace '^function\s+', 'function script:')
    }
}

if ($script:ast) {
    Test-Check 'Offline' 'No leftover markdown-link artifacts in code' {
        $hit = Select-String -Path $ScriptPath -Pattern '\]\(http|\[file://' -SimpleMatch:$false
        if ($hit) { throw "Suspicious text at line $($hit[0].LineNumber)" }
        'clean'
    }

    Test-Check 'Offline' 'SqlLit / SqlId escape quotes and brackets' {
        Import-ScriptFunction 'SqlLit', 'SqlId'
        Assert-Equal (SqlLit "O'Brien") "N'O''Brien'" 'SqlLit'
        Assert-Equal (SqlId 'a]b') '[a]]b]' 'SqlId'
        'ok'
    }

    Test-Check 'Offline' 'LSN decimal -> DMS hex conversion' {
        Import-ScriptFunction 'Invoke-CdcPositionDb'
        # 0x00000025:0x00000138:0x0001 = seq 37, offset 312, slot 1
        $script:State = @{ Databases = @{ T = @{ Backup = @{ LastLsn = '37000000031200001' }; Cdc = @{} } } }
        Invoke-CdcPositionDb 'T'
        Assert-Equal $script:State.Databases.T.Cdc.StartPosition '00000025:00000138:0001' 'StartPosition (small)'

        # Large values: seq 4,000,000 / offset 9,999,999,999 would overflow offset digits, use realistic maxima
        $dec = [bigint]4000000 * [bigint]::Pow(10, 15) + [bigint]123456789 * [bigint]::Pow(10, 5) + 65535
        $script:State = @{ Databases = @{ T = @{ Backup = @{ LastLsn = $dec.ToString() }; Cdc = @{} } } }
        Invoke-CdcPositionDb 'T'
        $exp = '{0:x8}:{1:x8}:{2:x4}' -f 4000000, 123456789, 65535
        Assert-Equal $script:State.Databases.T.Cdc.StartPosition $exp 'StartPosition (large)'
    }

    Test-Check 'Offline' 'Invalid LSN is rejected' {
        $script:State = @{ Databases = @{ T = @{ Backup = @{ LastLsn = '5' }; Cdc = @{} } } }
        try { Invoke-CdcPositionDb 'T'; throw 'no error raised' }
        catch { if ($_.Exception.Message -notmatch 'Unparseable LSN') { throw } }
        'ok'
    }

    Test-Check 'Offline' 'DMS identifiers are valid, stable and unique' {
        Import-ScriptFunction 'New-DmsId'
        $a = New-DmsId 'mig-task' 'Sales DB_2024'
        Assert-Equal $a (New-DmsId 'mig-task' 'Sales DB_2024') 'stability'
        if ($a -notmatch '^[a-z][a-z0-9-]*$') { throw "invalid chars: $a" }
        if ($a.Length -gt 255) { throw 'too long' }
        if ($a -match '--') { throw "double hyphen not allowed by DMS: $a" }
        if ((New-DmsId 'mig-task' 'Sales-DB-2024') -eq $a) { throw 'collision between similar names' }
        $a
    }

    Test-Check 'Offline' 'Table mappings + task settings are valid JSON' {
        $tm = $script:ast.Find({ param($a) $a -is [System.Management.Automation.Language.AssignmentStatementAst] -and $a.Left.Extent.Text -eq '$TableMappings' }, $true)
        $ts = $script:ast.Find({ param($a) $a -is [System.Management.Automation.Language.AssignmentStatementAst] -and $a.Left.Extent.Text -eq '$TaskSettings' }, $true)
        if (-not $tm -or -not $ts) { throw 'definitions not found' }
        Invoke-Expression $tm.Extent.Text; Invoke-Expression $ts.Extent.Text
        $m = $TableMappings | ConvertFrom-Json
        Assert-Equal $m.rules.Count 2 'rule count'
        Assert-Equal $m.rules[1].'rule-action' 'exclude' 'cdc exclusion'
        $null = $TaskSettings | ConvertFrom-Json
        'ok'
    }

    Test-Check 'Offline' 'Test-S3Object logic (size + SHA-256)' {
        Import-ScriptFunction 'Test-S3Object'
        $hex = ('AB' * 32)
        $b64 = [Convert]::ToBase64String([Convert]::FromHexString($hex))
        $b = @{ SizeBytes = 100; Sha256 = $hex }
        if (-not (Test-S3Object ([pscustomobject]@{ ContentLength = 100; ChecksumSHA256 = $b64 }) $b)) { throw 'matching object rejected' }
        if (Test-S3Object ([pscustomobject]@{ ContentLength = 99; ChecksumSHA256 = $b64 }) $b) { throw 'wrong size accepted' }
        if (Test-S3Object ([pscustomobject]@{ ContentLength = 100; ChecksumSHA256 = 'AAAA' }) $b) { throw 'wrong checksum accepted' }
        if (-not (Test-S3Object ([pscustomobject]@{ ContentLength = 100; ChecksumSHA256 = 'AAAA-3' }) $b)) { throw 'multipart composite rejected' }
        if (Test-S3Object $null $b) { throw 'null accepted' }
        'ok'
    }

    Test-Check 'Offline' 'State file survives a JSON round trip' {
        Import-ScriptFunction 'Save-State', 'New-DbRecord'
        $script:StateFile = Join-Path ([IO.Path]::GetTempPath()) "mig-test-state-$([guid]::NewGuid().ToString('N')).json"
        try {
            $script:State = @{ Databases = @{} }
            $d = New-DbRecord ([pscustomobject]@{ name = 'X' })
            $d.Backup = @{ LastLsn = '37000000031200001'; Verified = $true }
            $d.Restore = @{ TaskId = 7; StartedUtc = (Get-Date).ToUniversalTime().ToString('o') }
            $script:State.Databases.X = $d
            Save-State
            $back = Get-Content $script:StateFile -Raw | ConvertFrom-Json -AsHashtable
            Assert-Equal $back.Databases.X.Backup.LastLsn '37000000031200001' 'LSN kept as string'
            $dur = [int]((Get-Date).ToUniversalTime() - ([datetime]$back.Databases.X.Restore.StartedUtc).ToUniversalTime()).TotalSeconds
            if ([math]::Abs($dur) -gt 5) { throw "timestamp drifted by ${dur}s after reload" }
            'ok'
        }
        finally { Remove-Item $script:StateFile -Force -ErrorAction SilentlyContinue }
    }
}
else { Add-Result 'Offline' 'Unit tests' 'SKIP' 'script did not parse' }
#endregion

#region ---------- Phase 2: preflight ----------
Write-Host "`n=== Phase 2: environment preflight (read-only) ===" -ForegroundColor Cyan

$haveEnv = $SourceInstance -and $SourceCredential -and $TargetEndpoint -and $TargetCredential -and $BackupDir -and $S3Bucket -and $Region -and $DmsReplicationInstanceArn
if (-not $haveEnv) {
    Add-Result 'Preflight' 'Environment checks' 'SKIP' 'connection parameters not supplied'
}
else {
    if (-not $BackupDirLocal) { $BackupDirLocal = $BackupDir }
    $SourceHost = ($SourceInstance -split '\\')[0]
    $aws = @('--region', $Region); if ($AwsProfile) { $aws += @('--profile', $AwsProfile) }

    function T-Src([string]$q) { Invoke-Sqlcmd -ServerInstance "$SourceHost,$SourcePort" -Credential $SourceCredential -Query $q -TrustServerCertificate:$TrustServerCertificate.IsPresent -QueryTimeout 60 }
    function T-Tgt([string]$q) { Invoke-Sqlcmd -ServerInstance "$TargetEndpoint,$TargetPort" -Credential $TargetCredential -Query $q -TrustServerCertificate:$TrustServerCertificate.IsPresent -QueryTimeout 60 }
    function T-Aws([string[]]$a) {
        $out = & aws @aws @a 2>&1
        if ($LASTEXITCODE -ne 0) { throw "aws $($a[0..1] -join ' ') failed: $($out -join ' ')" }
        $out
    }

    Test-Check 'Preflight' 'Tools: aws CLI v2' {
        if (-not (Get-Command aws -ErrorAction SilentlyContinue)) { throw 'aws not on PATH' }
        $v = (& aws --version 2>&1) -join ' '
        if ($v -notmatch 'aws-cli/2') { throw "AWS CLI v2 required, found: $v" }
        $v
    }
    Test-Check 'Preflight' 'Tools: SqlServer module (Invoke-Sqlcmd)' {
        $m = Get-Module -ListAvailable SqlServer | Sort-Object Version -Descending | Select-Object -First 1
        if (-not $m) { throw 'SqlServer module not installed (Install-Module SqlServer)' }
        if ($m.Version.Major -lt 22) { throw "v$($m.Version) found; v22+ is required for -TrustServerCertificate" }
        "v$($m.Version)"
    }
    Test-Check 'Preflight' 'AWS credentials / identity' {
        $id = (T-Aws @('sts', 'get-caller-identity', '--output', 'json')) -join '' | ConvertFrom-Json
        $id.Arn
    }
    Test-Check 'Preflight' 'S3 bucket reachable' { T-Aws @('s3api', 'head-bucket', '--bucket', $S3Bucket) | Out-Null; $S3Bucket }
    Test-Check 'Preflight' 'S3 write / SHA-256 checksum / delete round trip' {
        $tmp = New-TemporaryFile
        $key = "$($S3Prefix.Trim('/'))/_preflight-$([guid]::NewGuid().ToString('N')).txt"
        try {
            'preflight' | Set-Content $tmp
            T-Aws @('s3', 'cp', $tmp.FullName, "s3://$S3Bucket/$key", '--checksum-algorithm', 'SHA256', '--only-show-errors') | Out-Null
            $h = (T-Aws @('s3api', 'head-object', '--bucket', $S3Bucket, '--key', $key, '--checksum-mode', 'ENABLED', '--output', 'json')) -join '' | ConvertFrom-Json
            if (-not $h.ChecksumSHA256) { throw 'object stored without a SHA-256 checksum' }
            'ok'
        }
        finally {
            Remove-Item $tmp -Force -ErrorAction SilentlyContinue
            try { T-Aws @('s3', 'rm', "s3://$S3Bucket/$key", '--only-show-errors') | Out-Null } catch { }
        }
    }
    Test-Check 'Preflight' 'Backup share visible to this host + writable' {
        if (-not (Test-Path $BackupDirLocal)) { throw "Not reachable: $BackupDirLocal" }
        $p = Join-Path $BackupDirLocal "_preflight-$([guid]::NewGuid().ToString('N')).tmp"
        'x' | Set-Content $p; Remove-Item $p -Force
        $drive = (Get-Item $BackupDirLocal).PSDrive
        if ($drive -and $null -ne $drive.Free) { "free space: {0:N1} GB" -f ($drive.Free / 1GB) } else { 'writable' }
    }
    Test-Check 'Preflight' 'Source SQL Server login' {
        $r = T-Src 'SELECT @@SERVERNAME AS n, SERVERPROPERTY(''ProductVersion'') AS v, IS_SRVROLEMEMBER(''sysadmin'') AS sa'
        "$($r.n) v$($r.v)" + $(if (-not $r.sa) { ' (not sysadmin; confirm BACKUP + msdb read rights)' } else { '' })
    }
    Test-Check 'Preflight' 'Source: SQL Server can write to BackupDir' {
        $f = ($BackupDir.TrimEnd('\', '/')) + '\_preflight_' + [guid]::NewGuid().ToString('N') + '.bak'
        T-Src "BACKUP DATABASE master TO DISK = N'$f' WITH COPY_ONLY, COMPRESSION, FORMAT, INIT" | Out-Null
        $loc = Join-Path $BackupDirLocal (Split-Path $f -Leaf)
        $seen = Test-Path $loc
        Remove-Item $loc -Force -ErrorAction SilentlyContinue
        if (-not $seen) { throw "SQL Server wrote the file but this host cannot see it at $loc (check BackupDir vs BackupDirLocal)" }
        'ok'
    }
    Test-Check 'Preflight' 'Source databases: recovery model + CDC' {
        $rows = @(T-Src "SELECT name, recovery_model_desc AS rm, is_cdc_enabled AS cdc FROM sys.databases WHERE database_id > 4 AND state_desc = 'ONLINE'")
        $notFull = @($rows | Where-Object rm -ne 'FULL')
        $noCdc = @($rows | Where-Object { -not $_.cdc })
        $msg = "$($rows.Count) user DBs"
        if ($notFull) { throw "$($notFull.Count) not in FULL recovery: $(($notFull.name | Select-Object -First 10) -join ', ')" }
        if ($noCdc) { return "WARN: $msg; MS-CDC not enabled on $($noCdc.Count) (needs MS-Replication or enabling CDC)" }
        $msg
    }
    Test-Check 'Preflight' 'Target RDS SQL Server login' {
        $r = T-Tgt 'SELECT @@VERSION AS v'
        ($r.v -split "`n")[0]
    }
    Test-Check 'Preflight' 'Target: SQLSERVER_BACKUP_RESTORE option enabled' {
        $o = (T-Tgt "SELECT OBJECT_ID('msdb.dbo.rds_restore_database') AS id").id
        if ($o -is [System.DBNull]) { throw 'msdb.dbo.rds_restore_database missing; add the option to the RDS option group' }
        'ok'
    }
    Test-Check 'Preflight' 'DMS replication instance available' {
        $ri = (T-Aws @('dms', 'describe-replication-instances', '--filters', "Name=replication-instance-arn,Values=$DmsReplicationInstanceArn", '--output', 'json')) -join '' | ConvertFrom-Json
        $i = $ri.ReplicationInstances[0]
        if ($i.ReplicationInstanceStatus -ne 'available') { throw "status is $($i.ReplicationInstanceStatus)" }
        "$($i.ReplicationInstanceIdentifier) ($($i.ReplicationInstanceClass))"
    }
    Test-Check 'Preflight' 'DMS describe-* behaviour for missing objects' {
        # The main script relies on this raising ResourceNotFound (handled by Invoke-AwsIfExists).
        $out = & aws @aws dms describe-replication-tasks --filters 'Name=replication-task-id,Values=mig-test-does-not-exist' --output json 2>&1
        if ($LASTEXITCODE -eq 0) { 'returns empty list (fine)' }
        elseif (($out -join ' ') -match 'ResourceNotFound') { 'raises ResourceNotFoundFault (handled)' }
        else { throw "unexpected error: $($out -join ' ')" }
    }
}
#endregion

#region ---------- Phase 3: pilot ----------
Write-Host "`n=== Phase 3: pilot migration ===" -ForegroundColor Cyan
$stateFile = $null
if (-not $RunPilot) { Add-Result 'Pilot' 'Pilot migration' 'SKIP' 'use -RunPilot -PilotDatabase <name>' }
elseif (-not $haveEnv -or -not $PilotDatabase) { Add-Result 'Pilot' 'Pilot migration' 'FAIL' '-RunPilot needs all connection parameters and -PilotDatabase' }
elseif (@($script:Results | Where-Object { $_.Phase -ne 'Pilot' -and $_.Status -eq 'FAIL' }).Count) {
    Add-Result 'Pilot' 'Pilot migration' 'SKIP' 'earlier FAIL results; fix those first'
}
else {
    $stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
    $stateFile = Join-Path $PSScriptRoot "pilot-state-$stamp.json"
    $reportDir = Join-Path $PSScriptRoot "pilot-reports-$stamp"
    $migArgs = @{
        SourceInstance = $SourceInstance; SourceCredential = $SourceCredential; SourcePort = $SourcePort
        TargetEndpoint = $TargetEndpoint; TargetCredential = $TargetCredential; TargetPort = $TargetPort
        BackupDir = $BackupDir; BackupDirLocal = $BackupDirLocal; S3Bucket = $S3Bucket; S3Prefix = "$S3Prefix-pilot"
        Region = $Region; DmsReplicationInstanceArn = $DmsReplicationInstanceArn
        OnlyDatabases = @($PilotDatabase); StateFile = $stateFile; ReportDir = $reportDir
        MonitorMinutes = $PilotMonitorMinutes; DmsSslMode = $DmsSslMode
    }
    if ($AwsProfile) { $migArgs.AwsProfile = $AwsProfile }
    if ($TrustServerCertificate) { $migArgs.TrustServerCertificate = $true }

    Write-Host "  Running pilot for '$PilotDatabase' (state: $stateFile)"
    $pilotWatch = [Diagnostics.Stopwatch]::StartNew()
    try { & pwsh -NoProfile -File $ScriptPath @migArgs; $code = $LASTEXITCODE }
    catch { $code = -1; Write-Warning $_.Exception.Message }
    $pilotWatch.Stop()
    Add-Result 'Pilot' 'Migration script exit code' $(if ($code -eq 0) { 'PASS' } else { 'FAIL' }) "exit $code after $([int]$pilotWatch.Elapsed.TotalMinutes) min"

    if (Test-Path $stateFile) {
        $st = Get-Content $stateFile -Raw | ConvertFrom-Json -AsHashtable
        $d = $st.Databases[$PilotDatabase]
        if ($d) {
            foreach ($step in 'Discover', 'Backup', 'Upload', 'Restore', 'CdcPosition', 'Dms') {
                $s = $d.Steps[$step]
                Add-Result 'Pilot' "Step $step" $(if ($s -eq 'DONE') { 'PASS' } else { 'FAIL' }) $(if ($s -ne 'DONE' -and $d.LastErrorStep -eq $step) { $d.LastError } else { $s })
            }
            if ($d.Backup.SizeBytes) { Add-Result 'Pilot' 'Backup size' 'PASS' ('{0:N1} MB, {1}s' -f ($d.Backup.SizeBytes / 1MB), $d.Backup.DurationSec) }
            if ($d.Restore.DurationSec) { Add-Result 'Pilot' 'Restore duration' 'PASS' ('{0:N1} min' -f ($d.Restore.DurationSec / 60)) }
            if ($d.Cdc.StartPosition) { Add-Result 'Pilot' 'CDC start position' 'PASS' $d.Cdc.StartPosition }
            if ($d.Dms.Status) {
                $lat = $d.Dms.LatencyTargetSec
                $ok = $d.Dms.Status -eq 'running'
                Add-Result 'Pilot' 'DMS task running' $(if ($ok) { 'PASS' } else { 'FAIL' }) "status $($d.Dms.Status); $($d.Dms.LastFailure)"
                if ($ok) { Add-Result 'Pilot' 'CDC latency' $(if ($null -ne $lat -and $lat -le 300) { 'PASS' } else { 'WARN' }) "target latency: $(if ($null -eq $lat) { 'no datapoint yet' } else { "${lat}s" })" }
            }
            # Post-restore sanity: database ONLINE and object counts comparable
            Test-Check 'Pilot' 'Target database ONLINE' {
                $r = Invoke-Sqlcmd -ServerInstance "$TargetEndpoint,$TargetPort" -Credential $TargetCredential -TrustServerCertificate:$TrustServerCertificate.IsPresent `
                    -Query "SELECT state_desc FROM sys.databases WHERE name = N'$($PilotDatabase.Replace("'", "''"))'"
                if (-not $r) { throw 'database not found on target' }
                if ($r.state_desc -ne 'ONLINE') { throw "state is $($r.state_desc)" }
                'ONLINE'
            }
            Test-Check 'Pilot' 'Table count matches source' {
                $q = "SELECT COUNT(*) AS c FROM [$($PilotDatabase.Replace(']', ']]'))].sys.tables"
                $s = (Invoke-Sqlcmd -ServerInstance "$SourceHost,$SourcePort" -Credential $SourceCredential -TrustServerCertificate:$TrustServerCertificate.IsPresent -Query $q).c
                $t = (Invoke-Sqlcmd -ServerInstance "$TargetEndpoint,$TargetPort" -Credential $TargetCredential -TrustServerCertificate:$TrustServerCertificate.IsPresent -Query $q).c
                Assert-Equal $t $s 'table count'
                "$t tables"
            }
        }
        else { Add-Result 'Pilot' 'State file' 'FAIL' "no record for $PilotDatabase" }
    }
    else { Add-Result 'Pilot' 'State file' 'FAIL' 'not written' }

    if ($CleanupPilot -and $d) {
        Write-Host "`n  Cleanup will DELETE: DMS task + 2 endpoints for '$PilotDatabase', the RESTORED database '$PilotDatabase' on $TargetEndpoint, and the S3 pilot object." -ForegroundColor Yellow
        if ((Read-Host "  Type the database name to confirm") -ceq $PilotDatabase) {
            $aws = @('--region', $Region); if ($AwsProfile) { $aws += @('--profile', $AwsProfile) }
            if ($d.Dms.TaskArn) {
                & aws @aws dms stop-replication-task --replication-task-arn $d.Dms.TaskArn 2>&1 | Out-Null
                for ($i = 0; $i -lt 30; $i++) {
                    $s = (& aws @aws dms describe-replication-tasks --filters "Name=replication-task-arn,Values=$($d.Dms.TaskArn)" --query 'ReplicationTasks[0].Status' --output text 2>&1)
                    if ($s -in 'stopped', 'ready', 'failed') { break }; Start-Sleep -Seconds 10
                }
                & aws @aws dms delete-replication-task --replication-task-arn $d.Dms.TaskArn 2>&1 | Out-Null
                Start-Sleep -Seconds 20
            }
            foreach ($arn in $d.Dms.SourceEndpointArn, $d.Dms.TargetEndpointArn) { if ($arn) { & aws @aws dms delete-endpoint --endpoint-arn $arn 2>&1 | Out-Null } }
            Invoke-Sqlcmd -ServerInstance "$TargetEndpoint,$TargetPort" -Credential $TargetCredential -TrustServerCertificate:$TrustServerCertificate.IsPresent `
                -Query "EXEC msdb.dbo.rds_drop_database @db_name = N'$($PilotDatabase.Replace("'", "''"))'" | Out-Null
            if ($d.Upload.Key) { & aws @aws s3 rm "s3://$S3Bucket/$($d.Upload.Key)" --only-show-errors 2>&1 | Out-Null }
            Add-Result 'Pilot' 'Cleanup' 'PASS' 'pilot artifacts removed (local .bak left in place)'
        }
        else { Add-Result 'Pilot' 'Cleanup' 'SKIP' 'confirmation not given' }
    }
}
#endregion

#region ---------- summary ----------
Write-Host "`n=== Summary ===" -ForegroundColor Cyan
$script:Results | Group-Object Status | Sort-Object Name | ForEach-Object { Write-Host ('  {0,-5} {1}' -f $_.Name, $_.Count) }
$reportPath = Join-Path $PSScriptRoot ("test-results-{0}.csv" -f (Get-Date -Format 'yyyyMMdd-HHmmss'))
$script:Results | Export-Csv $reportPath -NoTypeInformation
Write-Host "  Results: $reportPath"
$fails = @($script:Results | Where-Object Status -eq 'FAIL')
if ($fails) {
    Write-Host "`n  Failures:" -ForegroundColor Red
    $fails | ForEach-Object { Write-Host ("   - [{0}] {1}: {2}" -f $_.Phase, $_.Check, $_.Detail) -ForegroundColor Red }
}
exit ([int]($fails.Count -gt 0))
#endregion
