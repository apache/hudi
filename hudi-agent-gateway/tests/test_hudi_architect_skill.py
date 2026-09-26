# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Deterministic contracts for the Hudi Architect Flink PR1 foundation."""

from __future__ import annotations

import json
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

GATEWAY_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = GATEWAY_DIR.parent
SKILL_DIR = GATEWAY_DIR / "skills" / "hudi-architect"
REFERENCES_DIR = SKILL_DIR / "references"
CAPABILITY_MANIFEST = REFERENCES_DIR / "flink-1.20-hudi-1.2.0-capabilities.toml"
GATE_FIXTURE = (
    GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect" / "flink_pr1_scenarios.toml"
)
FLINK_VALIDATOR = SKILL_DIR / "validate_flink_capabilities.py"
PINNED_HUDI_REVISION = "f05c83f2b97732de7a558ff9b26959e1139c05f5"

FLINK_REFERENCES = (
    "flink-1.20-hudi-1.2.0-capabilities.md",
    "flink-1.20-hudi-1.2.0-capabilities.toml",
    "flink-question-flow.md",
    "flink-decision-overrides.md",
    "flink-warnings.md",
    "flink-config-templates.md",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _normalized(text: str) -> str:
    return " ".join(text.split())


def _load_toml(path: Path) -> dict:
    with path.open("rb") as toml_file:
        return tomllib.load(toml_file)


def _run_flink_validator(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(FLINK_VALIDATOR), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def test_flink_references_exist_and_are_linked_from_skill() -> None:
    skill = _read(SKILL_DIR / "SKILL.md")
    for filename in FLINK_REFERENCES:
        reference = REFERENCES_DIR / filename
        assert reference.is_file()
        assert "Licensed to the Apache Software Foundation" in _read(reference)
        assert f"`references/{filename}`" in skill


def test_engine_router_is_lazy_and_keeps_spark_on_shared_flow() -> None:
    skill = _read(SKILL_DIR / "SKILL.md")
    question_flow = _read(REFERENCES_DIR / "question-flow.md")

    assert "Load Flink references only after Flink is selected" in skill
    assert "Spark and HoodieStreamer requests must not load Flink warnings" in _normalized(skill)
    assert "Spark → proceed to Q1.2 and retain the existing shared flow" in question_flow
    assert "Flink → stop before Q1.2 and load `flink-question-flow.md`" in question_flow

    # Flink-only warning codes stay out of shared Spark references.
    assert "FLINK_" not in _read(REFERENCES_DIR / "warnings.md")
    assert "FLINK_" not in _read(REFERENCES_DIR / "config-templates.md")


def test_flink_baseline_is_fixed_and_pr1_does_not_claim_config_validated() -> None:
    capability = _read(REFERENCES_DIR / "flink-1.20-hudi-1.2.0-capabilities.md")
    manifest = _load_toml(CAPABILITY_MANIFEST)
    decisions = _read(REFERENCES_DIR / "flink-decision-overrides.md")
    output = _read(REFERENCES_DIR / "flink-config-templates.md")

    assert manifest["baseline_id"] == "hudi-1.2.0-flink-1.20"
    assert manifest["hudi"] == {
        "version": "1.2.0",
        "source_revision": PINNED_HUDI_REVISION,
        "release_ref": "release-1.2.0",
    }
    assert manifest["flink"] == {
        "compatibility_line": "1.20",
        "fixture_version": "1.20.1",
    }
    assert PINNED_HUDI_REVISION in capability
    assert "This status is unreachable in PR1" in capability
    assert "`CONFIG_VALIDATED` is defined" in decisions
    assert "`CONFIG_VALIDATED`." in output
    assert "CREATE TABLE" in output
    assert "Do not emit" in output


def test_flink_capability_manifest_validates_without_current_checkout_discovery() -> None:
    result = _run_flink_validator()

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "Validated Flink capability baseline hudi-1.2.0-flink-1.20"


def test_flink_capability_validation_emits_pinned_evidence() -> None:
    result = _run_flink_validator("--emit-evidence")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "baseline_id": "hudi-1.2.0-flink-1.20",
        "capability_manifest_schema": 1,
        "flink_fixture_version": "1.20.1",
        "hudi_source_revision": PINNED_HUDI_REVISION,
    }


def test_flink_capability_validation_rejects_option_absent_from_release_baseline() -> None:
    known = _run_flink_validator("--check-option", "write.operation")
    post_baseline = _run_flink_validator("--check-option", "hoodie.vector.columns")

    assert known.returncode == 0, known.stderr
    assert post_baseline.returncode == 1
    assert "not verified by baseline hudi-1.2.0-flink-1.20" in post_baseline.stderr


def test_flink_capability_validation_rejects_unverified_versions() -> None:
    matching = _run_flink_validator(
        "--hudi-version", "1.2.0", "--flink-version", "1.20.1"
    )
    other_hudi = _run_flink_validator("--hudi-version", "1.3.0")
    other_flink = _run_flink_validator("--flink-version", "1.21.0")

    assert matching.returncode == 0, matching.stderr
    assert other_hudi.returncode == 1
    assert "outside baseline 1.2.0" in other_hudi.stderr
    assert other_flink.returncode == 1
    assert "outside compatibility line 1.20" in other_flink.stderr


def test_flink_capability_validation_rejects_manifest_revision_drift(
    tmp_path: Path,
) -> None:
    drifted_manifest = tmp_path / "capabilities.toml"
    drifted_manifest.write_text(
        _read(CAPABILITY_MANIFEST).replace(PINNED_HUDI_REVISION, "f" * 40),
        encoding="utf-8",
    )

    result = _run_flink_validator("--manifest", str(drifted_manifest))

    assert result.returncode == 1
    assert "immutable Hudi 1.2.0 release commit" in result.stderr


def test_flink_capability_manifest_records_dannys_pr2_acceptance_contract() -> None:
    manifest = _load_toml(CAPABILITY_MANIFEST)
    effective_settings = {
        setting["key"]: setting["value"]
        for setting in manifest["required_effective_settings"]
    }
    deferred_checks = {
        check["id"]: check["target_pr"]
        for check in manifest["deferred_acceptance_checks"]
    }

    assert effective_settings == {
        "table.type": "COPY_ON_WRITE",
        "write.insert.cluster": False,
        "write.operation": "insert",
    }
    assert deferred_checks == {
        "FLINK_APPEND_MODE_CLUSTERING_ENABLED": "PR2",
        "FLINK_RECORD_KEY_FIELD_MISSING": "PR2",
        "FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY": "PR2",
    }


def test_pinned_source_hashes_match_when_release_object_is_available() -> None:
    object_check = subprocess.run(
        ["git", "cat-file", "-e", f"{PINNED_HUDI_REVISION}^{{commit}}"],
        cwd=REPO_ROOT,
        capture_output=True,
        check=False,
    )
    if object_check.returncode != 0:
        pytest.skip("pinned Hudi release commit is not available in this checkout")

    result = _run_flink_validator("--verify-source")
    assert result.returncode == 0, result.stderr


def test_pr1_scenarios_have_deterministic_status_and_are_non_executable() -> None:
    contract = _load_toml(GATE_FIXTURE)
    assert contract["status_precedence"] == ["INCOMPLETE", "REVIEW_REQUIRED", "BLOCKED"]
    precedence = {
        status: priority for priority, status in enumerate(contract["status_precedence"])
    }
    finding_status = contract["finding_status"]
    decisions = _read(REFERENCES_DIR / "flink-decision-overrides.md")

    for code in finding_status:
        assert f"`{code}`" in decisions

    for scenario in contract["scenarios"]:
        assert f"`{scenario['case_id']}`" in decisions
        statuses = [finding_status[code] for code in scenario["finding_codes"]]
        resolved_status = max(statuses, key=precedence.__getitem__)
        assert resolved_status == scenario["expected_final_status"], scenario["case_id"]
        assert scenario["executable_eligible"] is False


def test_combined_gate_fixture_preserves_all_reasons_and_blocked_wins() -> None:
    contract = _load_toml(GATE_FIXTURE)
    combined = next(
        scenario
        for scenario in contract["scenarios"]
        if scenario["case_id"] == "F11_COMBINED_GATES"
    )

    assert combined["finding_codes"] == [
        "FLINK_WRITER_MODEL_UNRESOLVED",
        "FLINK_PHYSICAL_SCHEMA_REQUIRED",
        "FLINK_REPLAY_IDEMPOTENCE_DEFERRED",
    ]
    assert combined["expected_final_status"] == "BLOCKED"
    assert combined["executable_eligible"] is False


def test_safety_gate_order_is_stable() -> None:
    flow = _read(REFERENCES_DIR / "flink-question-flow.md")
    headings = [
        "## F0 — Version baseline",
        "## F1 — New or existing table",
        "## F2 — Independent writers and table services",
        "## F3 — External catalog visibility",
        "## F4 — Physical schema availability",
        "## F5 — Mutation and record-key posture",
        "## F6 — Replay and backfill idempotence",
        "## PR1 completion",
    ]
    offsets = [flow.index(heading) for heading in headings]
    assert offsets == sorted(offsets)


def test_schema_pointer_does_not_satisfy_physical_schema_gate() -> None:
    flow = _read(REFERENCES_DIR / "flink-question-flow.md")
    warnings = _read(REFERENCES_DIR / "flink-warnings.md")

    schema_gate = flow.split("## F4 — Physical schema availability", 1)[1].split(
        "## F5 — Mutation and record-key posture", 1
    )[0]
    assert "schema URI" in schema_gate
    assert "concrete field names and types" in schema_gate
    assert "`FLINK_PHYSICAL_SCHEMA_REQUIRED`" in schema_gate
    assert "schema location whose contents cannot be read safely" in warnings


def test_redactor_removes_credentials_without_changing_normal_facts() -> None:
    evidence = """table.name=orders
	password = super-secret-password
	\"client_secret\": \"json-secret\",
	inline={\"table\":\"orders\",\"password\":\"inline-json-secret\"}
	endpoint=https://alice:uri-password@example.com/path?token=query-token&region=us
Authorization: Bearer bearer-token
command --api-key cli-secret --table orders
aws_access_key_id=AKIAABCDEFGHIJKLMNOP
-----BEGIN PRIVATE KEY-----
private-key-material
-----END PRIVATE KEY-----
"""
    result = subprocess.run(
        [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
        input=evidence,
        text=True,
        capture_output=True,
        check=True,
    )

    assert "table.name=orders" in result.stdout
    assert "region=us" in result.stdout
    assert result.stdout.count("<redacted>") >= 6
    for secret in (
        "super-secret-password",
        "json-secret",
        "inline-json-secret",
        "alice",
        "uri-password",
        "query-token",
        "bearer-token",
        "cli-secret",
        "AKIAABCDEFGHIJKLMNOP",
        "private-key-material",
    ):
        assert secret not in result.stdout
    assert "<redacted-private-key>" in result.stdout
