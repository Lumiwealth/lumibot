"""Reuse original published proof without fabricating native ledger rows."""

import json
import subprocess

import pytest

from scripts import qualify_agent_evals as qualification
from scripts import run_agent_evals as evals


SOURCE = "3540c7f6e2dcfd2e30ef2a253b66bb0fc076df3b"


def published():
    return json.loads(subprocess.check_output(
        ["git", "show", SOURCE + ":ci/agent-evals/qualification.json"],
        cwd=evals.REPO_ROOT,
    ))


def published_cases():
    """Keep the historical proof fixture bound to its actual case definitions."""
    receipt = published()
    cases = []
    for current in evals.load_cases(set(receipt["establishment"]["cases"])):
        path = current["_path"].relative_to(evals.REPO_ROOT).as_posix()
        historical = json.loads(subprocess.check_output(
            ["git", "show", f"{SOURCE}:{path}"], cwd=evals.REPO_ROOT,
        ))
        historical["_path"] = current["_path"]
        cases.append(historical)
    return cases


def test_published_establishment_keeps_original_calls_and_source_without_raw_rows():
    receipt = published()
    proof = qualification.build_establishment(
        [], [], published_cases(), source_ref=SOURCE, versions=receipt["sdk_versions"],
    )
    assert proof == receipt["establishment"]
    assert proof["source_commit"] != SOURCE  # Preserve actual historical provenance.
    assert len(proof["cases"]) == 29


def test_published_establishment_rejects_a_different_sdk_tuple():
    receipt = published()
    with pytest.raises(ValueError):
        qualification.build_establishment(
            [], [], published_cases(), source_ref=SOURCE,
            versions={**receipt["sdk_versions"], "google-adk": "unqualified"},
        )


def test_changed_case_cannot_inherit_published_establishment():
    receipt = published()
    cases = published_cases()
    selected = next(case for case in cases if case["id"] in receipt["establishment"]["cases"])
    selected["taskPrompt"] += "\nA materially different requested outcome."
    proof = qualification.build_establishment(
        [], [], cases, source_ref=SOURCE, versions=receipt["sdk_versions"],
    )
    assert selected["id"] not in proof["cases"]
    assert qualification.regression_required(selected, proof, [], "0" * 64) == evals.case_required_passes(selected)
