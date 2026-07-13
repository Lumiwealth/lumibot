from __future__ import annotations

import os
import re
import tempfile
import uuid
from pathlib import Path
from typing import Callable, Protocol

IBEAM_CONTAINER_PORT = 4234
IBEAM_INPUTS_DIR = "/srv/inputs"
DEFAULT_IBEAM_TAG = "0.5.12"
DEFAULT_IBEAM_HOST_PORT = 4234

_SAFE_DOCKER_TAG = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SAFE_INSTANCE_ID = re.compile(r"[^A-Za-z0-9_.-]+")


class IbkrGatewayError(RuntimeError):
    """Raised when an IBKR REST gateway cannot be started safely."""


class IbkrGateway(Protocol):
    """Lifecycle boundary for a Client Portal REST transport."""

    @property
    def base_url(self) -> str: ...

    def start(self) -> None: ...

    def stop(self) -> None: ...


class ExternalIbkrGateway:
    """No-op lifecycle for an already managed gateway or future OAuth client."""

    def __init__(self, base_url: str):
        value = str(base_url or "").strip().rstrip("/")
        if not value:
            raise ValueError("IBKR external gateway requires a base URL")
        self._base_url = value

    @property
    def base_url(self) -> str:
        return self._base_url

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None


def _run_command(command: list[str], **kwargs):
    import subprocess

    return subprocess.run(command, **kwargs)


def _devnull():
    import subprocess

    return subprocess.DEVNULL


class IBeamGateway:
    """Own one isolated IBeam/Client Portal Gateway Docker container.

    IBKR credentials are forwarded by environment-variable name rather than
    embedded in Docker command arguments. Docker still stores container
    environment values, so this transport is suitable only for controlled
    local or internal proof-of-concept use.
    """

    def __init__(
        self,
        *,
        username: str,
        password: str,
        conf_text: str,
        host_port: int = DEFAULT_IBEAM_HOST_PORT,
        paper: bool = True,
        image_tag: str = DEFAULT_IBEAM_TAG,
        instance_id: str | None = None,
        runner: Callable[..., object] | None = None,
    ):
        if not str(username or "").strip() or not str(password or ""):
            raise ValueError("IBeam gateway requires IBKR username and password")

        try:
            parsed_port = int(host_port)
        except (TypeError, ValueError) as exc:
            raise ValueError("IBKR gateway port must be an integer") from exc
        if not 1 <= parsed_port <= 65535:
            raise ValueError("IBKR gateway port must be between 1 and 65535")

        tag = str(image_tag or "").strip()
        if not _SAFE_DOCKER_TAG.fullmatch(tag):
            raise ValueError("IBEAM_DOCKER_TAG contains unsupported characters")

        raw_instance_id = str(instance_id or uuid.uuid4().hex[:10]).strip()
        safe_instance_id = _SAFE_INSTANCE_ID.sub("-", raw_instance_id).strip("-._")
        if not safe_instance_id:
            raise ValueError("IBKR gateway instance id must contain letters or numbers")

        self.username = str(username)
        self.password = str(password)
        self.conf_text = str(conf_text)
        self.host_port = parsed_port
        self.paper = bool(paper)
        self.image_tag = tag
        self.instance_id = safe_instance_id[:48]
        self.container_name = f"lumibot-client-portal-{self.instance_id}"
        self.image = f"voyz/ibeam:{self.image_tag}"
        self._runner = runner or _run_command
        self._container_started = False
        self._conf_path: Path | None = None

    @property
    def base_url(self) -> str:
        return f"https://localhost:{self.host_port}/v1/api"

    def _run(self, command: list[str], **kwargs):
        return self._runner(command, **kwargs)

    @staticmethod
    def _returncode(result: object) -> int:
        return int(getattr(result, "returncode", 1))

    def _write_conf(self) -> Path:
        handle = tempfile.NamedTemporaryFile(
            delete=False,
            mode="w",
            suffix=".yaml",
            encoding="utf-8",
        )
        try:
            handle.write(self.conf_text)
            handle.flush()
        finally:
            handle.close()
        path = Path(handle.name)
        path.chmod(0o600)
        self._conf_path = path
        return path

    def _child_environment(self) -> dict[str, str]:
        environment = os.environ.copy()
        environment.update(
            {
                "IBEAM_ACCOUNT": self.username,
                "IBEAM_PASSWORD": self.password,
                "IBEAM_GATEWAY_BASE_URL": f"https://localhost:{IBEAM_CONTAINER_PORT}",
                "IBEAM_LOG_TO_FILE": "False",
                "IBEAM_REQUEST_RETRIES": "1",
                "IBEAM_PAGE_LOAD_TIMEOUT": "30",
                "IBEAM_INPUTS_DIR": IBEAM_INPUTS_DIR,
                "IBEAM_USE_PAPER_ACCOUNT": "true" if self.paper else "false",
            }
        )
        return environment

    def start(self) -> None:
        if self._container_started:
            return

        docker_version = self._run(
            ["docker", "--version"],
            stdout=_devnull(),
            stderr=_devnull(),
            check=False,
        )
        if self._returncode(docker_version) != 0:
            raise IbkrGatewayError("Docker is required to start IBeam")

        docker_status = self._run(
            ["docker", "ps"],
            stdout=_devnull(),
            stderr=_devnull(),
            check=False,
        )
        if self._returncode(docker_status) != 0:
            raise IbkrGatewayError("Docker daemon is not available")

        # A failed pull may still leave the requested versioned image in the local cache.
        self._run(
            ["docker", "pull", self.image],
            stdout=_devnull(),
            stderr=_devnull(),
            check=False,
        )

        conf_path = self._write_conf()
        command = [
            "docker",
            "run",
            "-d",
            "--name",
            self.container_name,
            "--restart",
            "no",
            "--label",
            "com.lumibot.ibkr-gateway=true",
            "--label",
            f"com.lumibot.ibkr-instance={self.instance_id}",
            "--env",
            "IBEAM_ACCOUNT",
            "--env",
            "IBEAM_PASSWORD",
            "--env",
            "IBEAM_GATEWAY_BASE_URL",
            "--env",
            "IBEAM_LOG_TO_FILE",
            "--env",
            "IBEAM_REQUEST_RETRIES",
            "--env",
            "IBEAM_PAGE_LOAD_TIMEOUT",
            "--env",
            "IBEAM_INPUTS_DIR",
            "--env",
            "IBEAM_USE_PAPER_ACCOUNT",
            "-p",
            f"127.0.0.1:{self.host_port}:{IBEAM_CONTAINER_PORT}",
            "-v",
            f"{conf_path}:{IBEAM_INPUTS_DIR}/conf.yaml:ro",
            self.image,
        ]
        try:
            result = self._run(
                command,
                env=self._child_environment(),
                stdout=_devnull(),
                stderr=_devnull(),
                text=True,
                check=False,
            )
        except Exception:
            self._remove_conf()
            raise
        if self._returncode(result) != 0:
            self._remove_conf()
            raise IbkrGatewayError(
                f"IBeam container failed to start for instance {self.instance_id}"
            )
        self._container_started = True

    def _remove_conf(self) -> None:
        if self._conf_path is None:
            return
        try:
            self._conf_path.unlink(missing_ok=True)
        finally:
            self._conf_path = None

    def stop(self) -> None:
        try:
            if self._container_started:
                self._run(
                    ["docker", "rm", "-f", self.container_name],
                    stdout=_devnull(),
                    stderr=_devnull(),
                    check=False,
                )
                self._container_started = False
        finally:
            self._remove_conf()
