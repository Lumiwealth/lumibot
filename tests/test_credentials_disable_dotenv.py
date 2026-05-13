import os
import sys
import inspect


def test_disable_dotenv_skips_recursive_scan(monkeypatch):
    """When LUMIBOT_DISABLE_DOTENV is set, importing credentials must not walk directories."""

    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")

    def _boom(*_args, **_kwargs):
        raise AssertionError("os.walk should not be called when LUMIBOT_DISABLE_DOTENV=1")

    monkeypatch.setattr(os, "walk", _boom)

    # credentials.py runs at import-time; force a clean import for this test.
    sys.modules.pop("lumibot.credentials", None)
    __import__("lumibot.credentials")


def test_find_and_load_dotenv_walks_upward_and_loads_local_override(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    monkeypatch.delenv("LUMIBOT_TEST_DOTENV_VALUE", raising=False)

    root = tmp_path / "repo"
    child = root / "nested" / "tests"
    child.mkdir(parents=True)
    (root / ".env").write_text("LUMIBOT_TEST_DOTENV_VALUE=base\n", encoding="utf-8")
    (root / ".env.local").write_text("LUMIBOT_TEST_DOTENV_VALUE=local\n", encoding="utf-8")

    sys.modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials").credentials

    assert credentials.find_and_load_dotenv(child)
    assert os.environ["LUMIBOT_TEST_DOTENV_VALUE"] == "local"


def test_find_and_load_dotenv_ignores_dotenv_directory(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    root = tmp_path / "repo"
    child = root / "nested"
    child.mkdir(parents=True)
    (root / ".env").mkdir()

    sys.modules.pop("lumibot.credentials", None)
    credentials = __import__("lumibot.credentials").credentials

    assert not credentials.find_and_load_dotenv(child)


def test_credentials_broker_exports_are_real_classes(monkeypatch):
    monkeypatch.setenv("LUMIBOT_DISABLE_DOTENV", "1")
    sys.modules.pop("lumibot.credentials", None)

    import lumibot.brokers as brokers
    import lumibot.credentials as credentials

    for name in sorted(credentials._BROKER_CLASS_NAMES):
        credential_class = getattr(credentials, name)
        broker_class = getattr(brokers, name)

        assert inspect.isclass(credential_class)
        assert credential_class is broker_class
