import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest

from lumibot.data_sources.ibkr_gateway import (
    DEFAULT_IBEAM_TAG,
    IBeamGateway,
    IbkrGatewayError,
)


@dataclass
class _Result:
    returncode: int = 0


class _Runner:
    def __init__(self, returncodes=None):
        self.returncodes = list(returncodes or [])
        self.calls = []

    def __call__(self, command, **kwargs):
        self.calls.append((list(command), kwargs))
        return _Result(self.returncodes.pop(0) if self.returncodes else 0)


def _run_call(runner):
    return next(call for call in runner.calls if call[0][:3] == ["docker", "run", "-d"])


def test_ibeam_gateway_uses_isolated_local_binding_and_keeps_secrets_out_of_argv():
    runner = _Runner()
    gateway = IBeamGateway(
        username="paper-user-placeholder",
        password="paper-password-placeholder",
        conf_text="listenPort: 4234\n",
        host_port=43210,
        paper=True,
        instance_id="paper-smoke-1",
        runner=runner,
    )

    gateway.start()

    command, kwargs = _run_call(runner)
    command_text = " ".join(command)
    assert "paper-user-placeholder" not in command_text
    assert "paper-password-placeholder" not in command_text
    assert "127.0.0.1:43210:4234" in command
    assert any(value.endswith(":/srv/inputs/conf.yaml:ro") for value in command)
    assert gateway.container_name in command
    assert command[-1] == f"voyz/ibeam:{DEFAULT_IBEAM_TAG}"
    assert kwargs["env"]["IBEAM_ACCOUNT"] == "paper-user-placeholder"
    assert kwargs["env"]["IBEAM_PASSWORD"] == "paper-password-placeholder"
    assert kwargs["env"]["IBEAM_USE_PAPER_ACCOUNT"] == "true"
    assert gateway.base_url == "https://localhost:43210/v1/api"
    assert gateway._conf_path is not None
    assert stat.S_IMODE(gateway._conf_path.stat().st_mode) == 0o600

    conf_path = gateway._conf_path
    gateway.stop()

    assert not conf_path.exists()
    assert ["docker", "rm", "-f", gateway.container_name] in [call[0] for call in runner.calls]


def test_ibeam_gateway_start_is_idempotent():
    runner = _Runner()
    gateway = IBeamGateway(
        username="paper-user",
        password="paper-password",
        conf_text="listenPort: 4234\n",
        runner=runner,
    )

    gateway.start()
    gateway.start()

    run_calls = [call for call in runner.calls if call[0][:3] == ["docker", "run", "-d"]]
    assert len(run_calls) == 1
    gateway.stop()


def test_ibeam_gateway_removes_temporary_config_when_run_raises():
    class _FailingRunner(_Runner):
        def __call__(self, command, **kwargs):
            if command[:3] == ["docker", "run", "-d"]:
                conf_mount = command[command.index("-v") + 1]
                self.conf_path = Path(conf_mount.split(":", maxsplit=1)[0])
                raise OSError("docker connection closed")
            return super().__call__(command, **kwargs)

    runner = _FailingRunner()
    gateway = IBeamGateway(
        username="paper-user",
        password="paper-password",
        conf_text="listenPort: 4234\n",
        runner=runner,
    )

    with pytest.raises(OSError, match="docker connection closed"):
        gateway.start()

    assert not runner.conf_path.exists()
    assert gateway._conf_path is None


def test_ibeam_gateway_instances_use_distinct_names_and_configured_ports():
    first = IBeamGateway(
        username="user-one",
        password="password-one",
        conf_text="listenPort: 4234\n",
        host_port=43001,
        instance_id="deployment-one",
        runner=_Runner(),
    )
    second = IBeamGateway(
        username="user-two",
        password="password-two",
        conf_text="listenPort: 4234\n",
        host_port=43002,
        instance_id="deployment-two",
        runner=_Runner(),
    )

    assert first.container_name != second.container_name
    assert first.base_url != second.base_url


def test_ibeam_gateway_fails_closed_when_docker_is_missing():
    gateway = IBeamGateway(
        username="paper-user",
        password="paper-password",
        conf_text="listenPort: 4234\n",
        runner=_Runner(returncodes=[1]),
    )

    with pytest.raises(IbkrGatewayError, match="Docker is required"):
        gateway.start()


def test_ibeam_gateway_fails_closed_when_docker_probe_times_out():
    class _TimeoutRunner(_Runner):
        def __call__(self, command, **kwargs):
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    gateway = IBeamGateway(
        username="paper-user",
        password="paper-password",
        conf_text="listenPort: 4234\n",
        docker_probe_timeout=2,
        runner=_TimeoutRunner(),
    )

    with pytest.raises(IbkrGatewayError, match="checking for Docker after 2 seconds"):
        gateway.start()


def test_ibeam_gateway_bounds_probe_pull_run_and_stop_calls():
    runner = _Runner()
    gateway = IBeamGateway(
        username="paper-user",
        password="paper-password",
        conf_text="listenPort: 4234\n",
        docker_probe_timeout=3,
        docker_pull_timeout=45,
        runner=runner,
    )

    gateway.start()
    gateway.stop()

    timeouts = {tuple(command): kwargs["timeout"] for command, kwargs in runner.calls}
    assert timeouts[("docker", "--version")] == 3
    assert timeouts[("docker", "ps")] == 3
    assert timeouts[("docker", "pull", gateway.image)] == 45
    assert next(
        kwargs["timeout"]
        for command, kwargs in runner.calls
        if command[:3] == ["docker", "run", "-d"]
    ) == 3
    assert timeouts[("docker", "rm", "-f", gateway.container_name)] == 3


def test_ibeam_gateway_rejects_untrusted_image_reference_in_tag():
    with pytest.raises(ValueError, match="unsupported characters"):
        IBeamGateway(
            username="paper-user",
            password="paper-password",
            conf_text="listenPort: 4234\n",
            image_tag="0.5.12 malicious/image",
        )
