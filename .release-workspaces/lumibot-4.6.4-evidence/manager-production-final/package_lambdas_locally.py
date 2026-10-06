import hashlib
import io
import json
import tempfile
import zipfile
from pathlib import Path

from build_lambda_assets import LambdaAssetsBuilder

root = Path('tmp/release-4.6.4').resolve()
tempfile.tempdir = str(root)
builder = object.__new__(LambdaAssetsBuilder)
builder.environment = 'dev'
rows = {}
for name in builder._function_suffixes():
    payload = builder.create_lambda_package(name)
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        assert archive.testzip() is None
        if name == 'bot-manager-api':
            assert archive.read('BotManager.py') == Path('BotManager.py').read_bytes()
    rows[name] = {'sha256': hashlib.sha256(payload).hexdigest(), 'bytes': len(payload)}
(root / 'lambda-local-packages.json').write_text(json.dumps(rows, indent=2) + '\n')
print(json.dumps({'packages': len(rows), 'uploads': 0, 'artifact': str(root / 'lambda-local-packages.json')}))
