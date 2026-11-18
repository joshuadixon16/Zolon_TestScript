#Requires -Version 7.0

<#

.SYNOPSIS

  Migrates many SQL Server databases to RDS for SQL Server using native backup/restore,

  then keeps them in sync with DMS CDC-only tasks. Idempotent and resumable.

 

.DESCRIPTION

  Steps (per database, tracked in a JSON state file):

    1 Discover  2 Backup (COPY_ONLY, COMPRESSION, CHECKSUM + VERIFYONLY + SHA-256)

    3 Upload to S3   4 Restore on RDS (rds_restore_database)

    5 CDC start position (from the backup's last_lsn)   6 DMS endpoints + CDC task + monitoring

    7 Re-run the script to resume anything not DONE; a report is written on every run.

 

  Prerequisites

    - PowerShell 7+, SqlServer module (v22+ for -TrustServerCertificate), AWS CLI v2

    - RDS option group with SQLSERVER_BACKUP_RESTORE (IAM role with S3 read on the bucket)

    - Source: FULL recovery model; MS-CDC or MS-Replication set up per DMS requirements

    - Source log retained from the backup LSN until DMS catches up

    - Existing DMS replication instance that can reach both source and target

    - SQL authentication credentials for source (DMS needs them) and target

 

.EXAMPLE

  # Pilot one database first

  ./Migration_Script.ps1 -SourceInstance sql01 -SourceCredential (Get-Credential) `

     -TargetEndpoint mydb.abc.us-east-2.rds.amazonaws.com -TargetCredential (Get-Credential) `

     -BackupDir 'D:\Backups' -S3Bucket my-migration-bucket -Region us-east-2 `

     -DmsReplicationInstanceArn arn:aws:dms:us-east-2:111122223333:rep:XXXX -OnlyDatabases SalesDb

 

  # Full run / resume: same command without -OnlyDatabases

#>

[CmdletBinding()]

param(

    [Parameter(Mandatory)][string]$SourceInstance,

    [Parameter(Mandatory)][pscredential]$SourceCredential,

    [int]$SourcePort = 1433,

    [Parameter(Mandatory)][string]$TargetEndpoint,

    [Parameter(Mandatory)][pscredential]$TargetCredential,

    [int]$TargetPort = 1433,

    [Parameter(Mandatory)][string]$BackupDir,          # path as seen by the SOURCE SQL Server

    [string]$BackupDirLocal,                           # same folder as seen by this host (defaults to BackupDir)

    [Parameter(Mandatory)][string]$S3Bucket,

    [string]$S3Prefix = 'sqlmigration',

    [Parameter(Mandatory)][string]$Region,

    [string]$AwsProfile,

    [Parameter(Mandatory)][string]$DmsReplicationInstanceArn,

    [int]$ExpectedDatabaseCount = 50,

    [switch]$AllowCountMismatch,

    [string[]]$ExcludeDatabases = @(),

    [string[]]$OnlyDatabases = @(),

    [int]$MaxParallelRestores = 3,

    [string]$StateFile = './migration-state.json',

    [string]$ReportDir = './reports',

    [int]$MonitorMinutes = 30,

    [int]$LatencyThresholdSec = 300,

    [switch]$DeleteLocalAfterUpload,

    [switch]$TrustServerCertificate,

    [ValidateSet('none', 'require')][string]$DmsSslMode = 'none'

)

 

$ErrorActionPreference = 'Stop'

if (-not $BackupDirLocal) { $BackupDirLocal = $BackupDir }

$AwsBase = @('--region', $Region)

if ($AwsProfile) { $AwsBase += @('--profile', $AwsProfile) }

$SourceHost = ($SourceInstance -split '\\')[0]   # host only; the port is always passed explicitly
$StepOrder = @('Discover', 'Backup', 'Upload', 'Restore', 'CdcPosition', 'Dms')

 

#region ---------- helpers ----------

function Log([string]$Message, [string]$Level = 'INFO') {

    Write-Host ('{0:u} [{1}] {2}' -f (Get-Date).ToUniversalTime(), $Level, $Message)

}

function SqlLit([string]$s) { "N'" + $s.Replace("'", "''") + "'" }

function SqlId([string]$s) { '[' + $s.Replace(']', ']]') + ']' }

function NowUtc { (Get-Date).ToUniversalTime().ToString('o') }

 

function Invoke-Src([string]$Query) {

    Invoke-Sqlcmd -ServerInstance "$SourceHost,$SourcePort" -Credential $SourceCredential -Query $Query `

    -QueryTimeout 0 -TrustServerCertificate:$TrustServerCertificate.IsPresent

}

function Invoke-Tgt([string]$Query) {

    Invoke-Sqlcmd -ServerInstance "$TargetEndpoint,$TargetPort" -Credential $TargetCredential -Query $Query `

    -QueryTimeout 0 -TrustServerCertificate:$TrustServerCertificate.IsPresent

}

 

function Invoke-Aws {

    param([Parameter(Mandatory)][string[]]$CliArgs, [switch]$NoJson)

    $all = @($AwsBase) + $CliArgs

    if (-not $NoJson) { $all += @('--output', 'json') }

    $out = & aws @all 2>&1

    $code = $LASTEXITCODE

    $err = ($out | Where-Object { $_ -is [System.Management.Automation.ErrorRecord] } | ForEach-Object { $_.ToString() }) -join "`n"

    $txt = ($out | Where-Object { $_ -isnot [System.Management.Automation.ErrorRecord] }) -join "`n"

    if ($code -ne 0) { throw "aws $($CliArgs[0..1] -join ' ') failed (exit $code): $err $txt" }

    if ($NoJson -or [string]::IsNullOrWhiteSpace($txt)) { return $null }

    $txt | ConvertFrom-Json

}

 

# DMS describe-* calls raise ResourceNotFoundFault when a filter matches nothing; treat that as "absent".
function Invoke-AwsIfExists([string[]]$CliArgs) {
    try { Invoke-Aws $CliArgs }
    catch { if ($_.Exception.Message -match 'ResourceNotFound') { $null } else { throw } }
}

# Secrets go through a temp file, not the command line (command lines are visible in process lists).

function Invoke-AwsJson([string[]]$Cmd, [hashtable]$Body) {

    $f = New-TemporaryFile

    try {

        $Body | ConvertTo-Json -Depth 8 | Set-Content -Path $f -Encoding utf8NoBOM

        Invoke-Aws ($Cmd + @('--cli-input-json', "file://$($f.FullName)"))

    }

    finally { Remove-Item $f -Force -ErrorAction SilentlyContinue }

}

#endregion

 

#region ---------- state ----------

if (Test-Path $StateFile) { $State = Get-Content $StateFile -Raw | ConvertFrom-Json -AsHashtable }

else { $State = @{ Databases = @{} } }

 

function Save-State {

    $tmp = "$StateFile.tmp"

    $State | ConvertTo-Json -Depth 10 | Set-Content -Path $tmp -Encoding utf8NoBOM

    Move-Item $tmp $StateFile -Force

}

 

function New-DbRecord($row) {

    @{

        Name = $row.name; SizeMb = 0; RecoveryModel = ''; Collation = ''; CdcEnabled = $false

        Steps = @{ Discover = 'PENDING'; Backup = 'PENDING'; Upload = 'PENDING'; Restore = 'PENDING'; CdcPosition = 'PENDING'; Dms = 'PENDING' }

        Backup = @{}; Upload = @{}; Restore = @{}; Cdc = @{}; Dms = @{}

        LastError = $null; LastErrorStep = $null

    }

}

 

function Set-Step([string]$n, [string]$step, [string]$status, [string]$err = $null) {

    $d = $State.Databases[$n]

    $d.Steps[$step] = $status

    if ($status -eq 'FAILED') { $d.LastError = $err; $d.LastErrorStep = $step }

    elseif ($d.LastErrorStep -eq $step) { $d.LastError = $null; $d.LastErrorStep = $null }

    Save-State

}

 

function Test-Ready($d, [string]$step) {

    $i = [array]::IndexOf($StepOrder, $step)

    for ($k = 0; $k -lt $i; $k++) { if ($d.Steps[$StepOrder[$k]] -ne 'DONE') { return $false } }

    $true

}

 

# Runs one idempotent action per database. Each action re-verifies its artifacts, so

# "DONE" is never trusted blindly and a rerun is cheap when nothing changed.

function Invoke-PerDb([string]$Step, [scriptblock]$Action) {

    Log "=== $Step ==="

    foreach ($n in $Targets) {

        $d = $State.Databases[$n]

        if (-not (Test-Ready $d $Step)) { continue }

        try {

            Set-Step $n $Step 'RUNNING'

            & $Action $n

            Set-Step $n $Step 'DONE'

        }

        catch {

            Set-Step $n $Step 'FAILED' $_.Exception.Message

            Write-Warning "[$n] $Step failed: $($_.Exception.Message)"

        }

    }

}

#endregion

 

#region ---------- preflight + step 1: discover ----------

function Test-Preflight {

    foreach ($c in 'aws', 'Invoke-Sqlcmd') { if (-not (Get-Command $c -ErrorAction SilentlyContinue)) { throw "Required command not found: $c" } }

    if (-not (Test-Path $BackupDirLocal)) { throw "BackupDirLocal not reachable from this host: $BackupDirLocal" }

    Invoke-Aws @('s3api', 'head-bucket', '--bucket', $S3Bucket) | Out-Null

    $o = (Invoke-Tgt "SELECT OBJECT_ID('msdb.dbo.rds_restore_database') AS id").id

    if ($o -is [System.DBNull]) { throw 'rds_restore_database not found: enable the SQLSERVER_BACKUP_RESTORE option on the RDS instance.' }

    $ri = Invoke-Aws @('dms', 'describe-replication-instances', '--filters', "Name=replication-instance-arn,Values=$DmsReplicationInstanceArn")

    if (-not $ri.ReplicationInstances) { throw 'DMS replication instance not found.' }

    New-Item -ItemType Directory -Path $ReportDir -Force | Out-Null

}

 

function Step-Discover {

    Log '=== Discover ==='

    $rows = @(Invoke-Src @"

SELECT d.name, d.recovery_model_desc AS recovery_model, d.collation_name, d.is_cdc_enabled,

       CAST(SUM(CAST(mf.size AS bigint)) * 8.0 / 1024 AS decimal(18,1)) AS size_mb

FROM sys.databases d JOIN sys.master_files mf ON mf.database_id = d.database_id

WHERE d.database_id > 4 AND d.state_desc = 'ONLINE'

GROUP BY d.name, d.recovery_model_desc, d.collation_name, d.is_cdc_enabled

ORDER BY d.name

"@) | Where-Object { $_.name -notin $ExcludeDatabases }

 

    if ($rows.Count -ne $ExpectedDatabaseCount -and -not $AllowCountMismatch -and -not $OnlyDatabases) {

        throw "Expected $ExpectedDatabaseCount databases but found $($rows.Count). Use -AllowCountMismatch or -ExcludeDatabases if this is intended."

    }

    foreach ($r in $rows) {

        if (-not $State.Databases.ContainsKey($r.name)) { $State.Databases[$r.name] = New-DbRecord $r }

        $d = $State.Databases[$r.name]

        $d.SizeMb = [double]$r.size_mb

        $d.RecoveryModel = [string]$r.recovery_model

        $d.Collation = [string]$r.collation_name

        $d.CdcEnabled = [bool]$r.is_cdc_enabled

        if ($d.RecoveryModel -ne 'FULL') {

            Set-Step $r.name 'Discover' 'FAILED' "Recovery model is $($d.RecoveryModel); FULL is required for CDC."

        }

        else { Set-Step $r.name 'Discover' 'DONE' }

    }

    Log "Discovered $($rows.Count) databases."

}

#endregion

 

#region ---------- step 2: backup ----------

function Invoke-BackupDb([string]$n) {

    $d = $State.Databases[$n]; $b = $d.Backup

    $file = ($n -replace '[^\w\.\-]', '_') + '_full.bak'

    $srvPath = $BackupDir.TrimEnd('\', '/') + '\' + $file

    $locPath = Join-Path $BackupDirLocal $file

 

    if ($b.Verified -and (Test-Path $locPath) -and (Get-Item $locPath).Length -eq $b.SizeBytes) { return }

    if ($b.Verified -and $d.Steps.Upload -eq 'DONE' -and $d.Upload.Verified) { return }   # local copy already removed

 

    Log "Backup: $n"

    $sw = [Diagnostics.Stopwatch]::StartNew()

    # COPY_ONLY so the source's own differential base / backup chain is untouched.

    Invoke-Src "BACKUP DATABASE $(SqlId $n) TO DISK = $(SqlLit $srvPath) WITH COPY_ONLY, COMPRESSION, CHECKSUM, FORMAT, INIT, STOP_ON_ERROR" | Out-Null

    Invoke-Src "RESTORE VERIFYONLY FROM DISK = $(SqlLit $srvPath) WITH CHECKSUM" | Out-Null

 

    $row = Invoke-Src @"

SELECT TOP 1 CAST(bs.first_lsn AS varchar(30)) AS first_lsn, CAST(bs.last_lsn AS varchar(30)) AS last_lsn,

       CAST(bs.checkpoint_lsn AS varchar(30)) AS checkpoint_lsn

FROM msdb.dbo.backupset bs JOIN msdb.dbo.backupmediafamily mf ON mf.media_set_id = bs.media_set_id

WHERE bs.database_name = $(SqlLit $n) AND mf.physical_device_name = $(SqlLit $srvPath) AND bs.type = 'D'

ORDER BY bs.backup_set_id DESC

"@

    if (-not $row) { throw 'Backup set not found in msdb.' }

    if (-not (Test-Path $locPath)) { throw "Backup file is not visible to this host at $locPath" }

 

    $hash = (Get-FileHash $locPath -Algorithm SHA256).Hash

    $d.Backup = @{

        LocalPath = $locPath; ServerPath = $srvPath; SizeBytes = (Get-Item $locPath).Length; Sha256 = $hash

        FirstLsn = [string]$row.first_lsn; LastLsn = [string]$row.last_lsn; CheckpointLsn = [string]$row.checkpoint_lsn

        DurationSec = [int]$sw.Elapsed.TotalSeconds; FinishedUtc = NowUtc; Verified = $true

    }

}

#endregion

 

#region ---------- step 3: upload ----------

function Get-S3Head([string]$key) {

    try { Invoke-Aws @('s3api', 'head-object', '--bucket', $S3Bucket, '--key', $key, '--checksum-mode', 'ENABLED') }

    catch { if ($_.Exception.Message -match '404|Not Found') { $null } else { throw } }

}

 

# Size must match; for single-part objects the SHA-256 must match too. Multipart objects carry a

# composite checksum (suffix -N): there the CLI has already verified each part during upload.

function Test-S3Object($head, $b) {

    if (-not $head) { return $false }

    if ([int64]$head.ContentLength -ne [int64]$b.SizeBytes) { return $false }

    $cs = $head.ChecksumSHA256

    if ($cs -and $cs -notmatch '-\d+$') { return $cs -eq [Convert]::ToBase64String([Convert]::FromHexString($b.Sha256)) }

    $true

}

 

function Invoke-UploadDb([string]$n) {

    $d = $State.Databases[$n]; $b = $d.Backup

    $key = "$($S3Prefix.Trim('/'))/$(Split-Path $b.LocalPath -Leaf)"

    $ok = Test-S3Object (Get-S3Head $key) $b

    if (-not $ok) {

        if (-not (Test-Path $b.LocalPath)) { throw 'S3 copy is missing/invalid and the local backup is gone. Set Backup to PENDING in the state file to redo it.' }

        Log "Upload: $n"

        Invoke-Aws -NoJson @('s3', 'cp', $b.LocalPath, "s3://$S3Bucket/$key", '--checksum-algorithm', 'SHA256', '--only-show-errors') | Out-Null

        if (-not (Test-S3Object (Get-S3Head $key) $b)) { throw 'S3 object failed verification after upload.' }

    }

    $d.Upload = @{ Key = $key; Verified = $true; VerifiedUtc = NowUtc }

    if ($DeleteLocalAfterUpload -and (Test-Path $b.LocalPath)) { Remove-Item $b.LocalPath -Force }

}

#endregion

 

#region ---------- step 4: restore on RDS ----------

function Get-TgtDbState([string]$n) {

    $r = Invoke-Tgt "SELECT state_desc FROM sys.databases WHERE name = $(SqlLit $n)"

    if ($r) { [string]$r.state_desc } else { $null }

}

 

function Update-RestoreStatus([string]$n) {

    $r = $State.Databases[$n].Restore

    $t = Invoke-Tgt "EXEC msdb.dbo.rds_task_status @task_id = $($r.TaskId)"

    if (-not $t) { return }

    $r.TaskLifecycle = [string]$t.lifecycle

    $r.PercentComplete = [string]$t.'% complete'

    $r.TaskInfo = [string]$t.task_info

    if ($r.TaskLifecycle -in 'SUCCESS', 'ERROR', 'CANCELLED' -and -not $r.FinishedUtc) {

        $r.FinishedUtc = NowUtc

        $r.DurationSec = [int]((Get-Date).ToUniversalTime() - ([datetime]$r.StartedUtc).ToUniversalTime()).TotalSeconds

    }

}

 

# Returns 'DONE' or 'INFLIGHT'; throws on anything unsafe.

function Start-Restore([string]$n) {

    $d = $State.Databases[$n]; $r = $d.Restore

    if ($r.TaskId) { Update-RestoreStatus $n }

    $st = Get-TgtDbState $n

 

    if ($r.TaskLifecycle -eq 'SUCCESS' -and $st -eq 'ONLINE') { return 'DONE' }

    if ($r.TaskLifecycle -in 'CREATED', 'IN_PROGRESS') { return 'INFLIGHT' }

    if ($st) {

        # Only clean up a half-restored DB that one of OUR failed tasks left behind.

        if ($r.TaskId -and $r.TaskLifecycle -in 'ERROR', 'CANCELLED' -and $st -ne 'ONLINE') {

            Log "Dropping partial restore of $n (state $st) before retry"

            Invoke-Tgt "EXEC msdb.dbo.rds_drop_database @db_name = $(SqlLit $n)" | Out-Null

        }

        else { throw "Database already exists on the target (state $st) and was not produced by a successful restore task here; refusing to overwrite." }

    }

    $arn = "arn:aws:s3:::$S3Bucket/$($d.Upload.Key)"

    $res = Invoke-Tgt "EXEC msdb.dbo.rds_restore_database @restore_db_name = $(SqlLit $n), @s3_arn_to_restore_from = $(SqlLit $arn)"

    $d.Restore = @{ TaskId = [int]$res.task_id; TaskLifecycle = 'CREATED'; StartedUtc = NowUtc; FinishedUtc = $null }

    Log "Restore submitted: $n (task $($d.Restore.TaskId))"

    'INFLIGHT'

}

 

function Step-Restore {

    Log '=== Restore ==='

    $queue = [System.Collections.Generic.Queue[string]]::new()

    $inflight = [System.Collections.Generic.List[string]]::new()

    foreach ($n in $Targets) { if (Test-Ready $State.Databases[$n] 'Restore') { $queue.Enqueue($n) } }

 

    while ($queue.Count -gt 0 -or $inflight.Count -gt 0) {

        while ($queue.Count -gt 0 -and $inflight.Count -lt $MaxParallelRestores) {

            $n = $queue.Dequeue()

            try {

                if ((Start-Restore $n) -eq 'DONE') { Set-Step $n 'Restore' 'DONE' }

                else { Set-Step $n 'Restore' 'RUNNING'; $inflight.Add($n) }

            }

            catch { Set-Step $n 'Restore' 'FAILED' $_.Exception.Message; Write-Warning "[$n] Restore failed: $($_.Exception.Message)" }

        }

        if ($inflight.Count -eq 0) { continue }

        Start-Sleep -Seconds 30

        foreach ($n in @($inflight)) {

            try {

                Update-RestoreStatus $n

                $r = $State.Databases[$n].Restore

                switch ($r.TaskLifecycle) {

                    'SUCCESS' {

                        if ((Get-TgtDbState $n) -eq 'ONLINE') { Set-Step $n 'Restore' 'DONE' }

                        else { Set-Step $n 'Restore' 'FAILED' 'Restore task succeeded but database is not ONLINE.' }

                        $inflight.Remove($n) | Out-Null

                    }

                    { $_ -in 'ERROR', 'CANCELLED' } {

                        Set-Step $n 'Restore' 'FAILED' "Restore $($r.TaskLifecycle): $($r.TaskInfo)"

                        $inflight.Remove($n) | Out-Null

                    }

                    default { Log "Restore $n : $($r.TaskLifecycle) $($r.PercentComplete)%"; Save-State }

                }

            }

            catch { Set-Step $n 'Restore' 'FAILED' $_.Exception.Message; $inflight.Remove($n) | Out-Null }

        }

    }

}

#endregion

 

#region ---------- step 5: CDC start position ----------

# backupset.last_lsn is numeric(25,0): <VLF seq><block offset, 10 digits><slot, 5 digits>.

# DMS for SQL Server takes the hex form xxxxxxxx:xxxxxxxx:xxxx.

function Invoke-CdcPositionDb([string]$n) {

    $d = $State.Databases[$n]

    $bi = [bigint]::Parse($d.Backup.LastLsn)

    $seq = [long][bigint]::Divide($bi, [bigint]::Pow(10, 15))

    $off = [long][bigint]::Remainder([bigint]::Divide($bi, [bigint]::Pow(10, 5)), [bigint]::Pow(10, 10))

    $slot = [long][bigint]::Remainder($bi, [bigint]::Pow(10, 5))

    if ($seq -le 0) { throw "Unparseable LSN '$($d.Backup.LastLsn)'" }

    $d.Cdc = @{ StartPosition = ('{0:x8}:{1:x8}:{2:x4}' -f $seq, $off, $slot); SourceLsn = $d.Backup.LastLsn }

}

#endregion

 

#region ---------- step 6: DMS ----------

function New-DmsId([string]$prefix, [string]$n) {

    $s = ($n.ToLower() -replace '[^a-z0-9]+', '-').Trim('-'); if (-not $s) { $s = 'db' }

    $h = [BitConverter]::ToString([Security.Cryptography.SHA1]::HashData([Text.Encoding]::UTF8.GetBytes($n))).Replace('-', '').Substring(0, 6).ToLower()

    "$prefix-$s-$h"

}

 

function Initialize-DmsEndpoint([string]$id, [string]$type, [string]$server, [int]$port, [string]$db, [pscredential]$cred) {

    $ex = Invoke-AwsIfExists @('dms', 'describe-endpoints', '--filters', "Name=endpoint-id,Values=$id")

    if ($ex.Endpoints.Count -gt 0) { return $ex.Endpoints[0].EndpointArn }

    $res = Invoke-AwsJson @('dms', 'create-endpoint') @{

        EndpointIdentifier = $id; EndpointType = $type; EngineName = 'sqlserver'

        ServerName = $server; Port = $port; DatabaseName = $db; SslMode = $DmsSslMode

        Username = $cred.UserName; Password = $cred.GetNetworkCredential().Password

    }

    $res.Endpoint.EndpointArn

}

 

function Confirm-DmsConnection([string]$endpointArn) {

    $f = @('dms', 'describe-connections', '--filters', "Name=endpoint-arn,Values=$endpointArn", "Name=replication-instance-arn,Values=$DmsReplicationInstanceArn")

    $c = (Invoke-AwsIfExists $f).Connections | Select-Object -First 1

    if ($c.Status -eq 'successful') { return }

    $retest = [bool]($c -and $c.Status -eq 'failed'); $tries = 0   # a re-test can briefly still report the old 'failed'
    if (-not $c -or $c.Status -ne 'testing') {

        Invoke-Aws @('dms', 'test-connection', '--replication-instance-arn', $DmsReplicationInstanceArn, '--endpoint-arn', $endpointArn) | Out-Null

    }

    $deadline = (Get-Date).AddMinutes(5)

    do {

        Start-Sleep -Seconds 10; $tries++

        $c = (Invoke-AwsIfExists $f).Connections | Select-Object -First 1

        if ($c.Status -eq 'successful') { return }

        if ($c.Status -eq 'failed' -and -not ($retest -and $tries -lt 3)) { throw "DMS endpoint connection test failed: $($c.LastFailureMessage)" }

    } while ((Get-Date) -lt $deadline)

    throw 'DMS endpoint connection test timed out.'

}

 

function Get-DmsTask([string]$filterName, [string]$value) {

    (Invoke-AwsIfExists @('dms', 'describe-replication-tasks', '--filters', "Name=$filterName,Values=$value", '--without-settings')).ReplicationTasks | Select-Object -First 1

}

 

$TableMappings = @{ rules = @(

        @{ 'rule-type' = 'selection'; 'rule-id' = '1'; 'rule-name' = 'include-all'; 'object-locator' = @{ 'schema-name' = '%'; 'table-name' = '%' }; 'rule-action' = 'include' }

        @{ 'rule-type' = 'selection'; 'rule-id' = '2'; 'rule-name' = 'exclude-cdc'; 'object-locator' = @{ 'schema-name' = 'cdc'; 'table-name' = '%' }; 'rule-action' = 'exclude' }

    ) 
} | ConvertTo-Json -Depth 6 -Compress

$TaskSettings = @{ FullLoadSettings = @{ TargetTablePrepMode = 'DO_NOTHING' }; Logging = @{ EnableLogging = $true } } | ConvertTo-Json -Depth 4 -Compress

 

function Invoke-DmsDb([string]$n) {

    $d = $State.Databases[$n]

    $srcId = New-DmsId 'mig-src' $n; $tgtId = New-DmsId 'mig-tgt' $n; $taskId = New-DmsId 'mig-task' $n

 

    $srcArn = Initialize-DmsEndpoint $srcId 'source' $SourceHost $SourcePort $n $SourceCredential

    $tgtArn = Initialize-DmsEndpoint $tgtId 'target' $TargetEndpoint $TargetPort $n $TargetCredential

    Confirm-DmsConnection $srcArn

    Confirm-DmsConnection $tgtArn

 

    $t = Get-DmsTask 'replication-task-id' $taskId

    if (-not $t) {

        Log "DMS: creating CDC task for $n at $($d.Cdc.StartPosition)"

        Invoke-AwsJson @('dms', 'create-replication-task') @{

            ReplicationTaskIdentifier = $taskId; SourceEndpointArn = $srcArn; TargetEndpointArn = $tgtArn

            ReplicationInstanceArn = $DmsReplicationInstanceArn; MigrationType = 'cdc'

            TableMappings = $TableMappings; ReplicationTaskSettings = $TaskSettings

            CdcStartPosition = $d.Cdc.StartPosition

        } | Out-Null

        $deadline = (Get-Date).AddMinutes(5)

        do { Start-Sleep -Seconds 5; $t = Get-DmsTask 'replication-task-id' $taskId } while ($t.Status -ne 'ready' -and (Get-Date) -lt $deadline)

        if ($t.Status -ne 'ready') { throw "New DMS task did not reach 'ready' (status: $($t.Status))." }

    }

    switch ($t.Status) {

        'ready' { Invoke-Aws @('dms', 'start-replication-task', '--replication-task-arn', $t.ReplicationTaskArn, '--start-replication-task-type', 'start-replication') | Out-Null }

        { $_ -in 'stopped', 'failed' } {

            Log "DMS: resuming $n (was $($t.Status))"

            Invoke-Aws @('dms', 'start-replication-task', '--replication-task-arn', $t.ReplicationTaskArn, '--start-replication-task-type', $(if ($t.ReplicationTaskStartDate) { 'resume-processing' } else { 'start-replication' })) | Out-Null

        }

        default { }   # starting / running / modifying: nothing to do

    }

    $d.Dms = @{

        TaskId = $taskId; TaskArn = $t.ReplicationTaskArn; SourceEndpointArn = $srcArn; TargetEndpointArn = $tgtArn

        StartPosition = $d.Cdc.StartPosition; Status = $t.Status; LatencySourceSec = $null; LatencyTargetSec = $null; LastFailure = $null

    }

}

 

# CloudWatch dimensions: instance = replication instance identifier, task = task resource ID (last ARN segment).

function Get-DmsLatency([string]$instId, [string]$taskResourceId, [string]$metric) {

    $end = (Get-Date).ToUniversalTime(); $start = $end.AddMinutes(-10)

    $r = Invoke-Aws @('cloudwatch', 'get-metric-statistics', '--namespace', 'AWS/DMS', '--metric-name', $metric,

        '--dimensions', "Name=ReplicationInstanceIdentifier,Value=$instId", "Name=ReplicationTaskIdentifier,Value=$taskResourceId",

        '--start-time', ($start.ToString('s') + 'Z'), '--end-time', ($end.ToString('s') + 'Z'), '--period', '60', '--statistics', 'Maximum')

    $p = $r.Datapoints | Sort-Object Timestamp | Select-Object -Last 1

    if ($p) { [int]$p.Maximum } else { $null }

}

 

function Step-Monitor {

    Log '=== Monitor DMS ==='

    $instId = (Invoke-Aws @('dms', 'describe-replication-instances', '--filters', "Name=replication-instance-arn,Values=$DmsReplicationInstanceArn")).ReplicationInstances[0].ReplicationInstanceIdentifier

    $deadline = (Get-Date).AddMinutes($MonitorMinutes)

    do {

        $pending = 0

        foreach ($n in $Targets) {

            $d = $State.Databases[$n]

            if ($d.Steps.Dms -ne 'DONE') { continue }

            try {

                $t = Get-DmsTask 'replication-task-arn' $d.Dms.TaskArn

                $d.Dms.Status = $t.Status; $d.Dms.LastFailure = $t.LastFailureMessage

                $rid = $d.Dms.TaskArn.Split(':')[-1]

                $d.Dms.LatencySourceSec = Get-DmsLatency $instId $rid 'CDCLatencySource'

                $d.Dms.LatencyTargetSec = Get-DmsLatency $instId $rid 'CDCLatencyTarget'

                if ($t.Status -in 'failed', 'stopped') {

                    Set-Step $n 'Dms' 'FAILED' "DMS task $($t.Status): $($t.LastFailureMessage)"

                    Write-Warning "[$n] DMS task $($t.Status): $($t.LastFailureMessage)"

                }

                elseif ($t.Status -ne 'running' -or $null -eq $d.Dms.LatencyTargetSec -or $d.Dms.LatencyTargetSec -gt $LatencyThresholdSec) { $pending++ }

            }

            catch { Write-Warning "[$n] monitor error: $($_.Exception.Message)"; $pending++ }

        }

        Save-State

        Log "Monitor: $pending task(s) not yet caught up (threshold ${LatencyThresholdSec}s)"

        if ($pending -eq 0) { break }

        if ((Get-Date) -lt $deadline) { Start-Sleep -Seconds 60 }

    } while ((Get-Date) -lt $deadline)

}

#endregion

 

#region ---------- step 7: report ----------

function Write-Report {

    $rows = foreach ($n in $Targets) {

        $d = $State.Databases[$n]

        $failed = ($d.Steps.Values -contains 'FAILED')

        $allDone = -not ($d.Steps.Values | Where-Object { $_ -ne 'DONE' })

        $caughtUp = $allDone -and $d.Dms.Status -eq 'running' -and $null -ne $d.Dms.LatencyTargetSec -and $d.Dms.LatencyTargetSec -le $LatencyThresholdSec

        [pscustomobject]@{

            Database = $n

            Overall = if ($failed) { 'FAILED' } elseif ($caughtUp) { 'CAUGHT_UP' } elseif ($allDone) { 'REPLICATING' } else { 'PENDING' }

            SizeMB = $d.SizeMb

            BackupMB = if ($d.Backup.SizeBytes) { [math]::Round($d.Backup.SizeBytes / 1MB, 1) } else { $null }

            Sha256 = $d.Backup.Sha256

            BackupLastLsn = $d.Backup.LastLsn

            CdcStartPosition = $d.Cdc.StartPosition

            RestoreMin = if ($d.Restore.DurationSec) { [math]::Round($d.Restore.DurationSec / 60, 1) } else { $null }

            Discover = $d.Steps.Discover; Backup = $d.Steps.Backup; Upload = $d.Steps.Upload

            Restore = $d.Steps.Restore; CdcPosition = $d.Steps.CdcPosition; Dms = $d.Steps.Dms

            DmsStatus = $d.Dms.Status; LatencyTargetSec = $d.Dms.LatencyTargetSec

            CdcEnabledOnSource = $d.CdcEnabled

            LastErrorStep = $d.LastErrorStep; LastError = $d.LastError

        }

    }

    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd-HHmmss')

    $csv = Join-Path $ReportDir "migration-report-$stamp.csv"

    $html = Join-Path $ReportDir "migration-report-$stamp.html"

    $rows | Export-Csv $csv -NoTypeInformation

    $rows | ConvertTo-Html -Title 'SQL Server to RDS migration report' | Set-Content $html -Encoding utf8NoBOM

 

    Write-Host "`n===== Migration summary ====="

    $rows | Group-Object Overall | Sort-Object Name | ForEach-Object { Write-Host ("{0,-12} {1}" -f $_.Name, $_.Count) }

    $bad = @($rows | Where-Object Overall -eq 'FAILED')

    if ($bad) {

        Write-Host "`nFailed databases:"

        $bad | Format-Table Database, LastErrorStep, LastError -AutoSize -Wrap | Out-Host

    }

    $noCdc = @($rows | Where-Object { -not $_.CdcEnabledOnSource })

    if ($noCdc) { Write-Host "`nNote: MS-CDC is not enabled on $($noCdc.Count) source database(s); confirm MS-Replication or enable CDC for DMS." }

    Write-Host "`nReports: $csv`n         $html`nState:   $StateFile"

    return $bad.Count

}

#endregion

 

#region ---------- main ----------

$failedCount = 0

try {

    Test-Preflight

    Step-Discover

    $Targets = @($State.Databases.Keys | Where-Object { (-not $OnlyDatabases) -or $_ -in $OnlyDatabases } | Sort-Object)

    if (-not $Targets) { throw 'No databases selected.' }

 

    Invoke-PerDb 'Backup' ${function:Invoke-BackupDb}

    Invoke-PerDb 'Upload' ${function:Invoke-UploadDb}

    Step-Restore

    Invoke-PerDb 'CdcPosition' ${function:Invoke-CdcPositionDb}

    Invoke-PerDb 'Dms' ${function:Invoke-DmsDb}

    Step-Monitor

}

finally {

    Save-State

    if ($Targets) { $failedCount = Write-Report }

}

exit ([int]($failedCount -gt 0))

#endregion