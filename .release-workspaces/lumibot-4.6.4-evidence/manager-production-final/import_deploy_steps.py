import subprocess
import sys
from datetime import datetime
from pathlib import Path

environment, run_id, sha = sys.argv[1:4]
stage = 'dev-deployment' if environment == 'dev' else 'production-deployment'
root = Path('tmp/release-4.6.4')
artifact = root / (environment + '-deploy.log')
lines = artifact.read_text().splitlines()
cli = '/Users/robertgrzesik/Development/botspot_react/scripts/release_telemetry.mjs'

def find(marker, start=0):
    for index, line in enumerate(lines[start:], start):
        if marker in line:
            stamp = line.split(' ', 1)[0]
            return index, datetime.fromisoformat(stamp.replace('Z', '+00:00')).isoformat()
    raise RuntimeError('Missing observed deployment marker: ' + marker)

def record(component, status, at, target_stage=stage):
    subprocess.run([
        'node', cli, 'record', '--ledger', str(root / 'telemetry/events.ndjson'),
        '--stage', target_stage, '--component', component, '--status', status,
        '--now', at, '--input-fingerprint', sha, '--artifact', str(artifact),
        '--run-url', 'https://github.com/Lumiwealth/bot_manager/actions/runs/' + run_id,
    ], check=True, stdout=subprocess.DEVNULL)

components = [
    ('terraform-plan-safety', 'Step 3: Terraform apply for ' + environment, 'Terraform plan safety check passed: no destructive actions.'),
    ('terraform-apply-observed', 'Terraform used the selected providers', 'Apply complete!'),
    ('runtime-release-reconciliation', 'Apply complete!', 'Infrastructure deployment done for environment: ' + environment + '!'),
    ('served-endpoint-readback', 'Running release manifest endpoint preflight...', 'Release manifest endpoint preflight passed.'),
]
if environment == 'dev':
    components += [
        ('deployed-integration-tests', 'Running integration tests on dev environment', 'Integration tests PASSED on dev environment.'),
        ('synthetic-live-placement-smoke', 'tests/integration/test_live_bot_smoke.py::test_live_paper_bot_smoke_uses_storage_safe_host', 'PASSED '),
    ]
for component, begin, end in components:
    index, start_at = find(begin)
    _, end_at = find(end, index)
    target = ('dev-readback' if environment == 'dev' else 'production-readback') if component == 'served-endpoint-readback' else stage
    record(component, 'started', start_at, target)
    record(component, 'passed', end_at, target)
print('Imported observed plan/apply, runtime, served readback and applicable integration stages.')
