"""Removed ProjectX live flow semi-integration test.

Original test required real credentials; keeping it would cause CI instability.
If you need to run a live smoke test locally, resurrect the previous version
from git history (test_projectx_order_lifecycle_smoke) and provide PROJECTX_* env vars.
"""
import pytest
pytest.skip("Removed ProjectX live flow test (requires credentials)", allow_module_level=True)
