from pathlib import Path

from dotenv import load_dotenv

# Load sensitive information such as API keys from .env files so that they are not stored in the repository
# but can still be accessed by the tests through os.environ
secrets_path = Path(__file__).parent.parent.parent / '.secrets'
if secrets_path.exists():
    for secret_file in secrets_path.glob('*.env'):
        load_dotenv(secret_file)
