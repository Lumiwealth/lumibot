import os


OAUTH_REFRESH_MODE_ENV = "LUMIBOT_OAUTH_REFRESH_MODE"
OAUTH_REFRESH_MODE_AUTO = "auto"
OAUTH_REFRESH_MODE_EXTERNAL = "external"
OAUTH_REFRESH_MODES = {OAUTH_REFRESH_MODE_AUTO, OAUTH_REFRESH_MODE_EXTERNAL}


def get_oauth_refresh_mode(config=None) -> str:
    value = None
    if isinstance(config, dict):
        value = config.get(OAUTH_REFRESH_MODE_ENV)
    if value is None:
        value = os.environ.get(OAUTH_REFRESH_MODE_ENV)

    mode = str(value or OAUTH_REFRESH_MODE_AUTO).strip().lower()
    if mode not in OAUTH_REFRESH_MODES:
        allowed = ", ".join(sorted(OAUTH_REFRESH_MODES))
        raise ValueError(f"{OAUTH_REFRESH_MODE_ENV} must be one of: {allowed}")
    return mode


def is_external_oauth_refresh_mode(config=None) -> bool:
    return get_oauth_refresh_mode(config) == OAUTH_REFRESH_MODE_EXTERNAL
