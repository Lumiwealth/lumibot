import json
import subprocess
import sys
from pathlib import Path

import requests
from dotenv import dotenv_values

environment, release_id, manifest_sha = sys.argv[1:4]
if environment not in ('dev', 'prod'):
    raise SystemExit('Unsupported environment')
values = dotenv_values('.env')
base_url = values['TEST_' + environment.upper() + '_URL'].rstrip('/')
api_key = values[environment.upper() + '_API_KEY']
subprocess.run([
    '.venv/bin/python', 'scripts/preflight_release_endpoint.py',
    '--base-url', base_url, '--api-key', api_key,
    '--release-id', release_id, '--manifest-sha256', manifest_sha,
], check=True)
headers = {'X-API-Key': api_key}
response = requests.get(base_url + '/api/release_manifest', headers=headers, timeout=30)
if response.status_code == 404:
    response = requests.get(base_url + '/release_manifest', headers=headers, timeout=30)
response.raise_for_status()
payload = response.json()
assert payload['release_id'] == release_id
assert payload['manifest_sha256'] == manifest_sha
output = Path('tmp/release-4.6.4') / (environment + '-served-manifest.json')
output.write_text(json.dumps(payload, indent=2) + '\n')
print(json.dumps({'environment': environment, 'release_id': release_id, 'manifest_sha256': manifest_sha, 'artifact': str(output)}))
