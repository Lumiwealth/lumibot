import json, subprocess, sys
from pathlib import Path
run_id, stage, sha = sys.argv[1:4]
repo = sys.argv[4] if len(sys.argv) > 4 else "Lumiwealth/bot_manager"
root = Path("tmp/release-4.6.4")
metadata = json.loads(subprocess.check_output(["gh", "run", "view", run_id, "--repo", repo, "--json", "status,conclusion,createdAt,startedAt,updatedAt,jobs,url,headSha"], text=True))
(root / ("run-" + run_id + ".json")).write_text(json.dumps(metadata, indent=2) + "\n")
if metadata["status"] != "completed":
    raise SystemExit("Cannot import unfinished workflow")
ledger = root / "telemetry/events.ndjson"
cli = "/Users/robertgrzesik/Development/botspot_react/scripts/release_telemetry.mjs"
def record(component, status, at, classification="active"):
    args = ["node",cli,"record","--ledger",str(ledger),"--stage",stage,"--component",component,"--status",status,"--now",at,"--classification",classification,"--input-fingerprint",sha,"--artifact",str(root / ("run-"+run_id+".json")),"--run-url",metadata["url"]]
    if status == "failed": args.extend(["--reason-code","environment"])
    subprocess.run(args,check=True,stdout=subprocess.DEVNULL)
started = min(j["startedAt"] for j in metadata["jobs"] if j["startedAt"] != "0001-01-01T00:00:00Z")
record("github-queue-"+run_id,"started",metadata["createdAt"],"queue")
record("github-queue-"+run_id,"passed",started,"queue")
for job in metadata["jobs"]:
    if job["conclusion"] in ["skipped",""]: continue
    component = "github-"+str(job["databaseId"])+"-"+job["name"]
    record(component,"started",job["startedAt"])
    record(component,"passed" if job["conclusion"] == "success" else "failed",job["completedAt"])
print(json.dumps({"run":run_id,"conclusion":metadata["conclusion"],"jobs":len(metadata["jobs"])}))
