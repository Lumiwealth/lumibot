from pathlib import Path


def test_package_manifest_excludes_generated_python_bytecode():
    manifest = (Path(__file__).resolve().parents[1] / "MANIFEST.in").read_text(encoding="utf-8")

    assert "prune lumibot/resources/__pycache__" in manifest
    assert "global-exclude *.py[cod]" in manifest


def test_custom_wheel_build_cleans_stale_build_tree():
    setup_source = (Path(__file__).resolve().parents[1] / "setup.py").read_text(encoding="utf-8")

    assert "build_lib = Path(self.build_lib)" in setup_source
    assert "shutil.rmtree(build_lib)" in setup_source
