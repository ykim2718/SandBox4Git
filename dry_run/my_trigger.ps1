# __version__ = "0.0.15"

# example/dry_run/my_trigger.ps1 — verify the dry-run flow (my_flow.py) at three stages.
#   -Mode local : run my_flow.py offline here (--run-on local: no Prefect server, no MLflow) - fastest sanity check.
#   -Mode serve : flow.serve() — a serve process on THIS machine runs my_flow.py (no work pool).
#   -Mode pool  : trigger the real pipeline deployment; pipeline.py (on a work-pool worker) git-fetches
#                 the repo and runs my_flow.py in a container - verifies the full code+data delivery path.
#
#   .\my_trigger.ps1
#   .\my_trigger.ps1 -Mode serve
#   .\my_trigger.ps1 -Mode pool -PrefectBlock <block> -GitRepo https://github.com/<u>/<repo>.git -GitCommit <sha> [-Payload <file.py>]

param(
    [ValidateSet("local", "serve", "pool")]
    [string]$Mode = "local",
    [string]$PrefectApiUrl = "http://localhost:4200/api",
    [string]$PrefectDeployment = "pipeline/pipelineflow-low",  # pool: registered deployment (work pool)
    [string]$Submitter = "",        # who launched it - dashboard label (all modes)
    [string]$PrefectBlock = "",  # pool: Credentials block name for MinIO creds (e.g. yrocket)
    [string]$GitCommit = "dryrun",  # pool: git_commit_hash (local runs offline, ignores it)
    [string]$DataFolder = "",  # local/serve; default <script>\data
    [string]$GitRepo = "",  # pool: repo pipeline.py fetches
    [string]$MinioKey = "electric_power_consumption/v0/powerconsumption.csv",  # pool: full OBJECT key (not a prefix)
    [string]$Payload = "my_flow.py"  # pool: payload script pipeline.py runs; must exist in the git repo + take --submitter/--data_folder
)

if (-not $Submitter) {
    $name = "yRocket"
    $now = Get-Date
    $tz  = [System.TimeZoneInfo]::Local
    $tzname = if ($tz.IsDaylightSavingTime($now)) {
        $tz.Id -replace 'Standard', 'Daylight'
    } else {
        $tz.Id
    }
    $tzAbbr = ($tzname -split '\s+' | ForEach-Object { $_[0] }) -join ''
    $Submitter = "{0}-{1:yyyyMMdd-HHmm}-{2}" -f $name, $now, $tzAbbr
}

$ErrorActionPreference = "Stop"
$here = Split-Path -Parent $MyInvocation.MyCommand.Path
if (-not $DataFolder) { $DataFolder = Join-Path $here "data" }

# Server Connection — serve/pool talk to this Prefect server (env var, current PowerShell only).
# local mode runs offline: my_flow.py --run-on local blanks PREFECT_API_URL inside the process.
$env:PREFECT_API_URL = $PrefectApiUrl
$env:PYTHONPATH = $here     # so `from my_flow import my_flow` resolves (serve mode)

switch ($Mode) {
    "local" {
        # offline in-process: my_flow.py --run-on local runs ephemerally with no Prefect server and no MLflow.
        python "$here\my_flow.py" --submitter $Submitter --data_folder $DataFolder --run-on local
    }
    "serve" {
        # serve mode: this process serves the deployment and runs my_flow.py (Ctrl+C to stop).
        # Trigger a run from another shell: prefect deployment run "my_flow/dry-run"
        python -c "from my_flow import my_flow; my_flow.serve(name='dry-run')"
    }
    "pool" {
        # work-pool mode: pipeline.py (on a worker) git-fetches <GitRepo>@<GitCommit> and runs my_flow.py,
        # downloading <MinioKey> to ./data - verifies the real code+data delivery path end to end.
        # submitter = dashboard label; prefect_block = Credentials block pipeline.py loads for MinIO creds.
        # -MinioKey is a single OBJECT key (pipeline.py downloads one file), not a catalog prefix.
        if (-not $GitRepo) {
            throw "pool mode needs -GitRepo (pipeline.py git-fetches that repo at -GitCommit and runs my_flow.py)."
        }
        if (-not $PrefectBlock) {
            throw "pool mode needs -PrefectBlock (Credentials block name pipeline.py loads, e.g. yrocket)."
        }
        prefect deployment run "$PrefectDeployment" `
            -p submitter=$Submitter -p prefect_block=$PrefectBlock -p git_repo=$GitRepo `
            -p git_commit_hash=$GitCommit -p minio_key=$MinioKey -p payload=$Payload
    }
}
