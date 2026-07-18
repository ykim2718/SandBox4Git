#!/usr/bin/env bash
# __version__ = "0.0.2"  # Semantic Versioning:  Version = Major.Minor.Patch  (bash port of my_trigger.ps1)
#
# example/dry_run/my_trigger.sh — verify the dry-run flow (my_flow.py) at three stages.
#   --mode local : run my_flow.py offline here (--run-on local: no Prefect server, no MLflow) - fastest sanity check.
#   --mode serve : flow.serve() — a serve process on THIS machine runs my_flow.py (no work pool).
#   --mode pool  : trigger the real pipeline deployment; pipeline.py (on a work-pool worker) git-fetches
#                  the repo and runs my_flow.py in a container - verifies the full code+data delivery path.
#
#   ./my_trigger.sh
#   ./my_trigger.sh --mode serve
#   ./my_trigger.sh --mode pool --prefect-block <block> --git-repo https://github.com/<u>/<repo>.git --git-commit <sha> [--payload <file.py>]
set -euo pipefail

MODE="local"
PREFECT_API_URL_ARG="http://localhost:4200/api"
PREFECT_DEPLOYMENT="pipeline/pipelineflow-low"   # pool: registered deployment (work pool)
SUBMITTER=""                                     # who launched it - dashboard label (all modes)
PREFECT_BLOCK=""                                 # pool: Credentials block name for MinIO creds (e.g. yrocket)
GIT_COMMIT="dryrun"                              # pool: git_commit_hash (local runs offline, ignores it)
DATA_FOLDER=""                                   # local/serve; default <script>/data
GIT_REPO=""                                      # pool: repo pipeline.py fetches
MINIO_KEY="electric_power_consumption/v0/powerconsumption.csv"   # pool: full OBJECT key (not a prefix)
PAYLOAD="my_flow.py"                             # pool: payload script pipeline.py runs; must exist in the git repo + take --submitter/--data_folder

while [ $# -gt 0 ]; do
    case "$1" in
        --mode)               MODE="$2"; shift 2 ;;
        --prefect-api-url)    PREFECT_API_URL_ARG="$2"; shift 2 ;;
        --prefect-deployment) PREFECT_DEPLOYMENT="$2"; shift 2 ;;
        --submitter)          SUBMITTER="$2"; shift 2 ;;
        --prefect-block)      PREFECT_BLOCK="$2"; shift 2 ;;
        --git-commit)         GIT_COMMIT="$2"; shift 2 ;;
        --data-folder)        DATA_FOLDER="$2"; shift 2 ;;
        --git-repo)           GIT_REPO="$2"; shift 2 ;;
        --minio-key)          MINIO_KEY="$2"; shift 2 ;;
        --payload)            PAYLOAD="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

# default submitter: yRocket-<yyyymmdd-HHMM>-<tz abbr>  (date +%Z already reflects DST)
if [ -z "$SUBMITTER" ]; then
    SUBMITTER="yRocket-$(date +%Y%m%d-%H%M)-$(date +%Z)"
fi

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[ -z "$DATA_FOLDER" ] && DATA_FOLDER="$here/data"

# serve/pool talk to this Prefect server (env var, this process only).
# local mode runs offline: my_flow.py --run-on local blanks PREFECT_API_URL inside the process.
export PREFECT_API_URL="$PREFECT_API_URL_ARG"
export PYTHONPATH="$here"     # so `from my_flow import my_flow` resolves (serve mode)

case "$MODE" in
    local)
        # offline in-process: my_flow.py --run-on local runs ephemerally with no Prefect server and no MLflow.
        python "$here/my_flow.py" --submitter "$SUBMITTER" --data_folder "$DATA_FOLDER" --run-on local
        ;;
    serve)
        # serve mode: this process serves the deployment and runs my_flow.py (Ctrl+C to stop).
        # Trigger a run from another shell: prefect deployment run "my_flow/dry-run"
        python -c "from my_flow import my_flow; my_flow.serve(name='dry-run')"
        ;;
    pool)
        # work-pool mode: pipeline.py (on a worker) git-fetches <git-repo>@<git-commit> and runs the payload,
        # downloading <minio-key> to ./data - verifies the real code+data delivery path end to end.
        # submitter = dashboard label; prefect-block = Credentials block pipeline.py loads for MinIO creds.
        # --minio-key is a single OBJECT key (pipeline.py downloads one file), not a catalog prefix.
        [ -n "$GIT_REPO" ]      || { echo "pool mode needs --git-repo (pipeline.py git-fetches that repo at --git-commit and runs the payload)." >&2; exit 1; }
        [ -n "$PREFECT_BLOCK" ] || { echo "pool mode needs --prefect-block (Credentials block name pipeline.py loads, e.g. yrocket)." >&2; exit 1; }
        prefect deployment run "$PREFECT_DEPLOYMENT" \
            -p submitter="$SUBMITTER" -p prefect_block="$PREFECT_BLOCK" -p git_repo="$GIT_REPO" \
            -p git_commit_hash="$GIT_COMMIT" -p minio_key="$MINIO_KEY" -p payload="$PAYLOAD"
        ;;
    *)
        echo "unknown --mode: $MODE (use local|serve|pool)" >&2; exit 2 ;;
esac
