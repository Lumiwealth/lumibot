import subprocess
from pathlib import Path

root = Path('tmp/release-4.6.4')
uv = ['uvx', '--from', 'uv==0.11.8', 'uv']
commands = [
    uv + ['run', '--locked', 'ruff', 'check', '.'],
    uv + ['run', '--locked', 'ruff', 'format', '--check', '.'],
    uv + ['run', '--locked', 'ty', 'check'],
    uv + ['export', '--locked', '--no-dev', '--no-emit-project', '--no-header', '--no-annotate', '--no-hashes', '--output-file', str(root / 'requirements-export.txt')],
    ['diff', '-u', 'requirements.txt', str(root / 'requirements-export.txt')],
    uv + ['--preview-features', 'audit', 'audit', '--frozen'],
    uv + ['build'],
    ['terraform', 'fmt', '-check', '-recursive', 'terraform'],
    ['terraform', '-chdir=terraform', 'validate'],
    ['terraform', '-chdir=terraform/modules/private_nat_router', 'test'],
    ['.venv/bin/python', 'scripts/build_runtime_bundle.py', '--env', 'dev', '--output', str(root / 'runtime-bundle.tar.zst'), '--metadata-output', str(root / 'runtime_bundle_manifest.json')],
]
with (root / 'static-qualified.log').open('w') as log:
    for command in commands:
        log.write('COMMAND ' + repr(command) + '\n')
        log.flush()
        subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT)
print('Complete static/package/Terraform gate passed; runtime bundle recorded.')
