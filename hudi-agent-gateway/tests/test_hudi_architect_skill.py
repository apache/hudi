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

"""Deterministic contracts for the Hudi Architect Flink SQL path."""

from __future__ import annotations

import ast
import copy
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
FLINK_DESIGN_VALIDATOR = SKILL_DIR / "validate_flink_design.py"
FLINK_EXISTING_TABLE_VALIDATOR = SKILL_DIR / "validate_flink_existing_table.py"
PINNED_HUDI_REVISION = "f05c83f2b97732de7a558ff9b26959e1139c05f5"
PR2_FIXTURE_DIR = GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect" / "flink_pr2"
PR3_FIXTURE_DIR = GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect" / "flink_pr3"
PR4_FIXTURE_DIR = GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect" / "flink_pr4"
PR5_FIXTURE_DIR = GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect" / "flink_pr5"
ASF_LICENSE_MARKER = "Licensed to the Apache Software Foundation (ASF) under one"
PR2_JAVA_FIXTURE = (
    GATEWAY_DIR
    / "fixtures"
    / "hudi-architect-flink-1.20"
    / "src"
    / "test"
    / "java"
    / "org"
    / "apache"
    / "hudi"
    / "agent"
    / "architect"
    / "TestFlinkArchitectSqlFixtures.java"
)

FLINK_REFERENCES = (
    "flink-1.20-hudi-1.2.0-capabilities.md",
    "flink-1.20-hudi-1.2.0-capabilities.toml",
    "flink-question-flow.md",
    "flink-decision-overrides.md",
    "flink-warnings.md",
    "flink-config-templates.md",
    "flink-existing-table-reconciliation.md",
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _read_sql_golden(path: Path) -> str:
    license_header, separator, sql = _read(path).partition("*/")
    assert separator, f"missing block-comment license header: {path}"
    assert license_header.startswith("/*"), f"license header is not first: {path}"
    assert ASF_LICENSE_MARKER in license_header, f"missing ASF license header: {path}"
    return sql.lstrip("\r\n")


def _normalized(text: str) -> str:
    return " ".join(text.split())


def _load_toml(path: Path) -> dict:
    with path.open("rb") as toml_file:
        return tomllib.load(toml_file)


def _scenario_finding_codes(contract: dict, scenario: dict) -> list[str]:
    finding_codes = list(scenario.get("finding_codes", []))
    inputs = scenario.get("inputs", {})
    auto_key_contract = contract["auto_key_acceptance"]
    if (
        inputs.get("stable_business_key") is False
        and inputs.get("replay_behavior") in auto_key_contract["eligible_replay_answers"]
    ):
        answer = inputs["auto_key_acceptance"]
        transition = auto_key_contract[answer]
        if finding_code := transition.get("finding_code"):
            finding_codes.append(finding_code)

    return finding_codes


def _run_flink_validator(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(FLINK_VALIDATOR), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def _run_flink_design_validator(
    input_path: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(FLINK_DESIGN_VALIDATOR), "--input", str(input_path), *args],
        text=True,
        capture_output=True,
        check=False,
    )


def _run_flink_existing_table_validator(
    input_path: Path, *args: str
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(FLINK_EXISTING_TABLE_VALIDATOR),
            "--input",
            str(input_path),
            *args,
        ],
        text=True,
        capture_output=True,
        check=False,
    )


def _write_design(tmp_path: Path, design: dict) -> Path:
    input_path = tmp_path / "design.json"
    input_path.write_text(json.dumps(design), encoding="utf-8")
    return input_path


def _compatible_existing_envelope(design: dict) -> dict:
    desired = copy.deepcopy(design)
    desired["safety"]["table_lifecycle"] = "existing"
    record_keys = desired["identity"]["record_key_fields"]
    partitions = desired["table"]["partition_fields"]
    if not partitions:
        key_generator = "NON_PARTITION"
    elif len(record_keys) <= 1 and len(partitions) <= 1:
        key_generator = "TIMESTAMP_AVRO"
    else:
        key_generator = "COMPLEX"
    writer_facts = {
        "design_contract_schema": desired["contract_schema"],
        "operation": desired["write"]["operation"],
    }
    if desired["contract_schema"] in (2, 3):
        writer_facts.update(
            {
                "index_bootstrap_enabled": desired["write"]["index"]["bootstrap_enabled"],
                "index_global": desired["write"]["index"]["global"],
                "index_state_ttl_days": desired["write"]["index"]["state_ttl_days"],
                "index_type": desired["write"]["index"]["type"],
            }
        )
    if desired["contract_schema"] == 3:
        writer_facts["compaction_owner"] = desired["table_services"]["compaction"]["owner"]
    timestamp_settings = {
        "timestamp_type": "SCALAR",
        "input_time_unit": "DAYS",
        "output_date_format": "yyyyMMdd",
        "output_timezone": "UTC",
    }
    if partitions:
        writer_facts["timestamp_key_generator"] = timestamp_settings
    return {
        "reconciliation_schema": 1,
        "baseline_id": "hudi-1.2.0-flink-1.20",
        "writer_transition": "cold_start_new_job",
        "desired_contract": desired,
        "evidence": [
            {
                "evidence_id": "table-config-1",
                "kind": "hoodie_properties",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:30:00+08:00",
                "sha256": "1" * 64,
                "facts": {
                    "table_name": desired["table"]["name"],
                    "table_type": desired["write"]["table_type"],
                    "table_version": 9,
                    "initial_table_version": 9,
                    "timeline_layout_version": 2,
                    "base_file_format": "PARQUET",
                    "record_key_fields": record_keys,
                    "ordering_fields": desired.get("ordering", {}).get("fields", []),
                    "partition_fields": partitions,
                    "key_generator_type": key_generator,
                    "record_merge_mode": desired["write"].get(
                        "record_merge_mode", "COMMIT_TIME_ORDERING"
                    ),
                    "hive_style_partitioning": False,
                    "url_encode_partitioning": False,
                },
            },
            {
                "evidence_id": "table-schema-1",
                "kind": "hudi_table_schema",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:30:00+08:00",
                "sha256": "2" * 64,
                "facts": {"columns": desired["table"]["columns"]},
            },
            {
                "evidence_id": "writers-1",
                "kind": "writer_inventory",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:31:00+08:00",
                "sha256": "3" * 64,
                "facts": {
                    "concurrent_writers": False,
                    "previous_writer_engine": "FLINK",
                    "previous_writer_stopped": True,
                },
            },
            {
                "evidence_id": "timeline-1",
                "kind": "timeline_summary",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:31:00+08:00",
                "sha256": "4" * 64,
                "facts": {"pending_instants": []},
            },
            {
                "evidence_id": "identity-1",
                "kind": "hudi_table_identity",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:30:00+08:00",
                "sha256": "5" * 64,
                "facts": {"table_path": desired["table"]["path"]},
            },
            {
                "evidence_id": "writer-config-1",
                "kind": "flink_writer_config",
                "table_path": desired["table"]["path"],
                "observed_at": "2026-10-19T10:30:00+08:00",
                "sha256": "6" * 64,
                "facts": {
                    **writer_facts,
                },
            },
        ],
    }


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


def test_flink_baseline_has_bounded_append_and_mutable_contracts() -> None:
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
    assert manifest["schema_version"] == 5
    assert manifest["fixture_artifacts"] == {
        "hudi_bundle": "org.apache.hudi:hudi-flink1.20-bundle:1.2.0",
        "flink_version": "1.20.1",
        "java_version": 11,
    }
    assert manifest["physical_schema_constraints"] == {
        "binary_routing_types": ["BINARY", "BYTES", "VARBINARY"],
        "field_name_pattern": "^[A-Za-z_][A-Za-z0-9_]*$",
        "reserved_target_field_names": [
            "_hoodie_commit_seqno",
            "_hoodie_commit_time",
            "_hoodie_file_name",
            "_hoodie_operation",
            "_hoodie_partition_path",
            "_hoodie_record_key",
        ],
        "temporal_precision_min": 0,
        "temporal_precision_max": 6,
        "temporal_types": ["TIME", "TIMESTAMP", "TIMESTAMP_LTZ"],
    }
    assert manifest["runtime_constraints"] == {
        "checkpoint_interval_min_ms": 1000,
        "flink_checkpoint_interval_min_ms": 10,
    }
    reconciliation = manifest["existing_table_reconciliation_contract"]
    assert reconciliation["reconciliation_schema"] == 1
    assert reconciliation["supported_design_contract_schemas"] == [1, 2, 3]
    assert reconciliation["assessment_only"] is True
    assert reconciliation["executable_sql"] is False
    assert reconciliation["required_evidence_kinds"] == [
        "hoodie_properties",
        "hudi_table_identity",
        "hudi_table_schema",
        "timeline_summary",
        "writer_inventory",
    ]
    assert reconciliation["comparison_results"] == [
        "CONFLICT",
        "MATCH",
        "NOT_APPLICABLE",
        "UNVERIFIED",
    ]
    assert reconciliation["reconciliation_results"] == [
        "COMPATIBLE",
        "CONFLICT",
        "UNVERIFIED",
    ]
    assert reconciliation["change_dispositions"] == [
        "DEPLOYMENT_CONFIGURATION_CHANGE",
        "MIGRATION_REQUIRED",
        "NO_CHANGE",
        "UNVERIFIED",
    ]
    assert reconciliation["finding_status"][
        "FLINK_EXISTING_WRITER_CONFIG_CONFLICT"
    ] == "REVIEW_REQUIRED"
    assert reconciliation["finding_status"][
        "FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED"
    ] == "REVIEW_REQUIRED"
    assert "first executable" in capability
    assert "`CONFIG_VALIDATED`" in decisions
    assert "Executable eligible: true" in output
    assert "CREATE TABLE" in output
    assert "INSERT INTO" in output


def test_flink_capability_manifest_validates_without_current_checkout_discovery() -> None:
    result = _run_flink_validator()

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "Validated Flink capability baseline hudi-1.2.0-flink-1.20"


def test_flink_capability_validation_emits_pinned_evidence() -> None:
    result = _run_flink_validator("--emit-evidence")

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == {
        "baseline_id": "hudi-1.2.0-flink-1.20",
        "capability_manifest_schema": 5,
        "design_contract_schema": 1,
        "flink_fixture_version": "1.20.1",
        "hudi_source_revision": PINNED_HUDI_REVISION,
        "supported_design_contract_schemas": [1, 2, 3],
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


@pytest.mark.parametrize(
    ("old", "new", "expected_message"),
    [
        (
            'key = "write.insert.cluster"\nvalue = false',
            'key = "write.insert.cluster"\nvalue = true',
            "required_effective_settings does not match",
        ),
        (
            '  "STRING",',
            '  "ROW",',
            "physical_types does not match",
        ),
        (
            'java_test = "testStableKeySinkAndInsertPlan"',
            'java_test = "testSinkOnly"',
            "implemented_acceptance_checks does not match",
        ),
        (
            "temporal_precision_max = 6",
            "temporal_precision_max = 9",
            "physical_schema_constraints does not match",
        ),
        (
            "checkpoint_interval_min_ms = 1000",
            "checkpoint_interval_min_ms = 10",
            "runtime_constraints does not match",
        ),
    ],
)
def test_flink_capability_validation_rejects_executable_contract_drift(
    tmp_path: Path, old: str, new: str, expected_message: str
) -> None:
    manifest_path = tmp_path / "capabilities.toml"
    manifest_path.write_text(_read(CAPABILITY_MANIFEST).replace(old, new), encoding="utf-8")

    result = _run_flink_validator("--manifest", str(manifest_path))

    assert result.returncode == 1
    assert expected_message in result.stderr


def test_flink_capability_manifest_records_executable_acceptance_contracts() -> None:
    manifest = _load_toml(CAPABILITY_MANIFEST)
    python_tests = {
        node.name
        for node in ast.walk(ast.parse(_read(Path(__file__))))
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    java_fixture = _read(PR2_JAVA_FIXTURE)
    effective_settings = {
        setting["applies_to"]: {
            candidate["key"]: candidate["value"]
            for candidate in manifest["required_effective_settings"]
            if candidate["applies_to"] == setting["applies_to"]
        }
        for setting in manifest["required_effective_settings"]
    }
    implemented_checks = {
        check["id"]: (check["python_test"], check["java_test"])
        for check in manifest["implemented_acceptance_checks"]
    }

    assert manifest["mutable_executable_contract"]["allow_replay_behaviors"] == [
        "cannot_occur",
        "duplicates_acceptable",
        "must_collapse",
    ]
    assert manifest["mutable_executable_contract"][
        "reserved_ordering_field_values"
    ] == ["no_precombine"]
    assert effective_settings == {
        "pr2-append-only-cow": {
            "table.type": "COPY_ON_WRITE",
            "write.insert.cluster": False,
            "write.operation": "insert",
        },
        "pr3-mutable-cow": {
            "changelog.enabled": False,
            "hoodie.write.record.merge.mode": "EVENT_TIME_ORDERING",
            "index.bootstrap.enabled": True,
            "index.global.enabled": True,
            "index.state.ttl": 0.0,
            "index.type": "FLINK_STATE",
            "table.type": "COPY_ON_WRITE",
            "write.operation": "upsert",
        },
        "pr4-mutable-mor": {
            "changelog.enabled": False,
            "clustering.async.enabled": False,
            "clustering.schedule.enabled": False,
            "compaction.async.enabled": True,
            "compaction.delta_commits": 5,
            "compaction.operation.execute.async.enabled": True,
            "compaction.schedule.enabled": True,
            "compaction.trigger.strategy": "num_commits",
            "hoodie.write.record.merge.mode": "EVENT_TIME_ORDERING",
            "index.bootstrap.enabled": True,
            "index.global.enabled": True,
            "index.state.ttl": 0.0,
            "index.type": "FLINK_STATE",
            "table.type": "MERGE_ON_READ",
            "write.operation": "upsert",
        },
    }
    assert implemented_checks == {
        "FLINK_APPEND_MODE_CLUSTERING_ENABLED": (
            "test_pr2_rejects_insert_clustering_override",
            "testPinnedAppendModeRequiresInsertClusteringDisabled",
        ),
        "FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED": (
            "test_pr2_rejects_binary_routing_fields",
            "testPinnedBinaryValuesUseObjectIdentityForRouting",
        ),
        "FLINK_RECORD_KEY_FIELD_MISSING": (
            "test_pr2_rejects_record_key_missing_from_append_schema",
            "testPinnedFactorySkipsMissingRecordKeyCheckInAppendMode",
        ),
        "FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY": (
            "test_pr2_rejects_non_append_source_changelog",
            "testStableKeySinkAndInsertPlan",
        ),
        "FLINK_TEMPORAL_PRECISION_UNSUPPORTED": (
            "test_pr2_enforces_pinned_temporal_precision_limits",
            "testPinnedPlannerRejectsTemporalPrecisionAboveSix",
        ),
        "FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED": (
            "test_pr2_rejects_non_avro_physical_field_names",
            "testPinnedPlannerRejectsNonAvroFieldName",
        ),
        "FLINK_HUDI_METADATA_FIELD_CONFLICT": (
            "test_pr2_rejects_reserved_hudi_metadata_field_names",
            "testPinnedWriterRejectsReservedHudiMetadataField",
        ),
        "FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED": (
            "test_pr2_enforces_checkpoint_interval_safety_floor",
            "testPinnedRuntimeCheckpointIntervalBoundary",
        ),
        "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT": (
            "test_pr5_property_and_schema_conflicts_are_blocked_and_preserved",
            "testPinnedExistingTableTypeOverridesConflictingOption",
        ),
        "FLINK_MUTABLE_SOURCE_CHANGELOG_UNSUPPORTED": (
            "test_pr3_rejects_unsupported_source_changelog",
            "testMutableCowSinkAndUpsertPlan",
        ),
        "FLINK_MUTABLE_ORDERING_FIELD_INVALID": (
            "test_pr3_rejects_invalid_ordering_fields",
            "testPinnedMutableOrderingConfiguration",
        ),
        "FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED": (
            "test_pr3_rejects_unsafe_index_configuration",
            "testPinnedMutableIndexConfiguration",
        ),
        "FLINK_MUTABLE_INDEX_BOOTSTRAP_REQUIRED": (
            "test_pr3_requires_index_bootstrap",
            "testPinnedMutableIndexBootstrapConfiguration",
        ),
        "FLINK_MOR_CLUSTERING_UNSUPPORTED": (
            "test_pr4_rejects_clustering",
            "testPinnedMorClusteringDisabled",
        ),
        "FLINK_MOR_APPEND_ONLY_UNSUPPORTED": (
            "test_pr4_rejects_append_only_mor",
            "testPinnedAppendOnlyMorDoesNotScheduleCompaction",
        ),
        "FLINK_MOR_COMPACTION_CONFIGURATION_UNSUPPORTED": (
            "test_pr4_rejects_incompatible_compaction_configuration",
            "testPinnedMorCompactionConfiguration",
        ),
        "FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED": (
            "test_pr4_rejects_strict_read_optimized_freshness",
            "testPinnedMorQuerySemantics",
        ),
        "FLINK_PR4_WRITE_PATH_UNSUPPORTED": (
            "test_pr4_valid_mutable_mor_design_renders_exact_executable_sql",
            "testMutableMorSinkAndCompactionPlan",
        ),
    }
    for check in manifest["implemented_acceptance_checks"]:
        assert check["python_test"] in python_tests
        assert f"void {check['java_test']}(" in java_fixture


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


def test_pr1_safety_gates_keep_deterministic_status_before_executable_validation() -> None:
    contract = _load_toml(GATE_FIXTURE)
    assert contract["schema_version"] == 4
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
        if scenario.get("passes_safety_gates"):
            assert _scenario_finding_codes(contract, scenario) == []
            continue
        statuses = [
            finding_status[code] for code in _scenario_finding_codes(contract, scenario)
        ]
        resolved_status = max(statuses, key=precedence.__getitem__)
        assert resolved_status == scenario["expected_final_status"], scenario["case_id"]
        assert scenario["executable_eligible"] is False

    mutable = next(
        scenario
        for scenario in contract["scenarios"]
        if scenario["case_id"] == "F07_MUTABLE"
    )
    assert mutable["passes_safety_gates"] is True
    assert mutable["next_contract_schema"] == 2


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


def test_auto_key_acceptance_scenarios_derive_findings_from_answers() -> None:
    contract = _load_toml(GATE_FIXTURE)
    scenarios = {scenario["case_id"]: scenario for scenario in contract["scenarios"]}
    finding_status = contract["finding_status"]

    pending = scenarios["F12_AUTO_KEY_PENDING"]
    declined = scenarios["F13_AUTO_KEY_DECLINED"]
    accepted = scenarios["F14_AUTO_KEY_ACCEPTED"]

    assert "finding_codes" not in pending
    assert "finding_codes" not in declined
    assert _scenario_finding_codes(contract, pending) == [
        "FLINK_AUTO_KEY_ACCEPTANCE_REQUIRED"
    ]
    assert _scenario_finding_codes(contract, declined) == ["FLINK_AUTO_KEY_DECLINED"]
    assert _scenario_finding_codes(contract, accepted) == []
    assert accepted["passes_safety_gates"] is True
    assert contract["auto_key_acceptance"]["accepted"] == {
        "warning_code": "FLINK_AUTO_KEY_DURABILITY"
    }
    for answer in ("pending", "declined"):
        transition = contract["auto_key_acceptance"][answer]
        assert transition["status"] == finding_status[transition["finding_code"]]


@pytest.mark.parametrize("fixture_name", ["stable_key", "auto_key"])
def test_pr2_valid_designs_render_exact_executable_sql(fixture_name: str) -> None:
    input_path = PR2_FIXTURE_DIR / f"{fixture_name}.json"
    result = _run_flink_design_validator(input_path)

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["executable_eligible"] is True
    assert assessment["finding_codes"] == []
    assert assessment["artifacts"]["combined_sql"] == _read_sql_golden(
        PR2_FIXTURE_DIR / f"{fixture_name}.sql"
    ).rstrip("\n")
    assert assessment["validation_evidence"] == {
        "baseline_id": "hudi-1.2.0-flink-1.20",
        "capability_manifest_schema": 5,
        "design_contract_schema": 1,
        "flink_fixture_version": "1.20.1",
        "hudi_source_revision": PINNED_HUDI_REVISION,
        "supported_design_contract_schemas": [1, 2, 3],
    }


def test_pr3_valid_mutable_design_renders_exact_executable_sql() -> None:
    result = _run_flink_design_validator(PR3_FIXTURE_DIR / "mutable_cow.json")

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["executable_eligible"] is True
    assert assessment["finding_codes"] == []
    assert assessment["advisory_codes"] == []
    assert assessment["artifacts"]["combined_sql"] == _read_sql_golden(
        PR3_FIXTURE_DIR / "mutable_cow.sql"
    ).rstrip("\n")
    assert assessment["validation_evidence"]["design_contract_schema"] == 2
    assert assessment["validation_evidence"]["capability_manifest_schema"] == 5


def test_pr4_valid_mutable_mor_design_renders_exact_executable_sql() -> None:
    result = _run_flink_design_validator(PR4_FIXTURE_DIR / "mutable_mor.json")

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["executable_eligible"] is True
    assert assessment["finding_codes"] == []
    assert assessment["advisory_codes"] == []
    assert assessment["artifacts"]["combined_sql"] == _read_sql_golden(
        PR4_FIXTURE_DIR / "mutable_mor.sql"
    ).rstrip("\n")
    assert assessment["validation_evidence"] == {
        "baseline_id": "hudi-1.2.0-flink-1.20",
        "capability_manifest_schema": 5,
        "design_contract_schema": 3,
        "flink_fixture_version": "1.20.1",
        "hudi_source_revision": PINNED_HUDI_REVISION,
        "supported_design_contract_schemas": [1, 2, 3],
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schedule_enabled", False),
        ("async_enabled", False),
        ("operation_execute_async_enabled", False),
        ("mode", "inline"),
        ("trigger_strategy", "time_elapsed"),
        ("delta_commits", 6),
    ],
)
def test_pr4_rejects_incompatible_compaction_configuration(
    tmp_path: Path, field: str, value: object
) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["table_services"]["compaction"][field] = value

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_MOR_COMPACTION_CONFIGURATION_UNSUPPORTED" in assessment["finding_codes"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize("field", ["schedule_enabled", "async_enabled"])
def test_pr4_rejects_clustering(tmp_path: Path, field: str) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["write"]["clustering"][field] = True

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_MOR_CLUSTERING_UNSUPPORTED"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


def test_pr4_rejects_strict_read_optimized_freshness(tmp_path: Path) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["read"]["read_optimized_freshness"] = "after_every_commit"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED"
    ]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize("freshness", [None, "unknown"])
def test_pr4_requires_resolved_read_optimized_freshness(
    tmp_path: Path, freshness: object
) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    if freshness is None:
        design["read"].pop("read_optimized_freshness")
    else:
        design["read"]["read_optimized_freshness"] = freshness

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_MOR_QUERY_REQUIREMENT_UNRESOLVED"
    ]
    assert assessment["status"] == "INCOMPLETE"
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("owner", "expected_code", "expected_status"),
    [
        (None, "FLINK_MOR_COMPACTION_OWNER_REQUIRED", "INCOMPLETE"),
        ("standalone_service", "FLINK_MULTI_WRITER_REVIEW", "REVIEW_REQUIRED"),
        ("unknown", "FLINK_WRITER_MODEL_UNRESOLVED", "REVIEW_REQUIRED"),
    ],
)
def test_pr4_requires_same_job_compaction_owner(
    tmp_path: Path, owner: object, expected_code: str, expected_status: str
) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    if owner is None:
        design["table_services"]["compaction"].pop("owner")
    else:
        design["table_services"]["compaction"]["owner"] = owner

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [expected_code]
    assert assessment["status"] == expected_status
    assert "artifacts" not in assessment


def test_pr4_rejects_detailed_compaction_tuning(tmp_path: Path) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["table_services"]["compaction"]["tasks"] = 4

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_MOR_COMPACTION_TUNING_UNSUPPORTED"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


def test_pr4_preserves_combined_findings_and_withholds_artifacts(tmp_path: Path) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["table_services"]["compaction"].pop("owner")
    design["write"]["clustering"]["schedule_enabled"] = True
    design["read"]["read_optimized_freshness"] = "after_every_commit"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_MOR_CLUSTERING_UNSUPPORTED",
        "FLINK_MOR_COMPACTION_OWNER_REQUIRED",
        "FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED",
    ]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


def test_pr4_snapshot_only_contract_does_not_invent_read_optimized_freshness(
    tmp_path: Path,
) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design["read"] = {
        "query_modes": ["snapshot"],
        "read_optimized_freshness": "NOT_APPLICABLE",
    }

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["finding_codes"] == []


def test_pr4_rejects_append_only_mor(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["write"]["table_type"] = "MERGE_ON_READ"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_MOR_APPEND_ONLY_UNSUPPORTED"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("field", "value"),
    [("table_services", []), ("table_services", None), ("read", []), ("read", None)],
)
def test_pr4_malformed_service_and_read_objects_fail_closed(
    tmp_path: Path, field: str, value: object
) -> None:
    design = json.loads(_read(PR4_FIXTURE_DIR / "mutable_mor.json"))
    design[field] = value

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_DESIGN_CONTRACT_INVALID" in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    "design_path",
    [
        PR2_FIXTURE_DIR / "stable_key.json",
        PR2_FIXTURE_DIR / "auto_key.json",
        PR3_FIXTURE_DIR / "mutable_cow.json",
        PR4_FIXTURE_DIR / "mutable_mor.json",
    ],
)
def test_pr5_reconciles_all_bounded_design_schemas_without_rendering_sql(
    tmp_path: Path, design_path: Path
) -> None:
    envelope = _compatible_existing_envelope(json.loads(_read(design_path)))

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["reconciliation_result"] == "COMPATIBLE"
    assert assessment["reconciliation_complete"] is True
    assert assessment["executable_eligible"] is False
    assert assessment["finding_codes"] == []
    assert assessment["change_disposition"] == "NO_CHANGE"
    assert "artifacts" not in assessment
    assert all(item["result"] == "MATCH" for item in assessment["comparisons"])
    assert assessment["validation_evidence"]["reconciliation_schema"] == 1


def test_pr5_checked_in_fixture_is_a_compatible_assessment_only_contract() -> None:
    result = _run_flink_existing_table_validator(
        PR5_FIXTURE_DIR / "compatible_mutable_cow.json"
    )

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["reconciliation_result"] == "COMPATIBLE"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment
    assert all(item["in_scope"] is True for item in assessment["evidence_summary"])
    assert assessment["deployment_checks"] == [
        {"check": "confirm_single_writer", "required": True},
        {"check": "recheck_pending_instants", "required": True},
        {"check": "verify_index_bootstrap_before_first_checkpoint", "required": True},
    ]


def test_pr5_does_not_expand_the_new_table_design_cli(tmp_path: Path) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))

    result = _run_flink_design_validator(
        _write_design(tmp_path, envelope["desired_contract"])
    )

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_EXISTING_TABLE_DEFERRED"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False


def test_pr5_existing_design_api_cannot_render_sql() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json, sys; from pathlib import Path; "
            "sys.path.insert(0, sys.argv[1]); "
            "from validate_flink_design import assess_design; "
            "from validate_flink_capabilities import load_manifest, DEFAULT_MANIFEST; "
            "desired = json.loads(Path(sys.argv[2]).read_text())['desired_contract']; "
            "print(json.dumps(assess_design(desired, load_manifest(DEFAULT_MANIFEST), "
            "expected_table_lifecycle='existing', render_sql=True)))",
            str(SKILL_DIR),
            str(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"),
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


def test_pr5_missing_evidence_is_incomplete_and_retains_every_reason(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"] = []

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "INCOMPLETE"
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["finding_codes"] == ["FLINK_EXISTING_EVIDENCE_REQUIRED"]
    assert len(assessment["findings"][0]["message"].split("; ")) > 4
    assert assessment["executable_eligible"] is False


def test_pr5_property_and_schema_conflicts_are_blocked_and_preserved(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    table_facts = envelope["evidence"][0]["facts"]
    table_facts["table_type"] = "MERGE_ON_READ"
    table_facts["record_key_fields"] = ["name"]
    envelope["evidence"][1]["facts"]["columns"][0]["type"] = "BIGINT"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "BLOCKED"
    assert assessment["reconciliation_result"] == "CONFLICT"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT",
        "FLINK_EXISTING_SCHEMA_CONFLICT",
    ]
    assert sum(item["result"] == "CONFLICT" for item in assessment["comparisons"]) == 3
    assert assessment["change_disposition"] == "MIGRATION_REQUIRED"


def test_pr5_schema_reorder_is_unverified_not_silently_accepted(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    columns = envelope["evidence"][1]["facts"]["columns"]
    columns[0], columns[1] = columns[1], columns[0]

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED"
    ]


def test_pr5_conflicting_sources_do_not_override_table_authority(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"].append(
        {
            "evidence_id": "ddl-1",
            "kind": "flink_table_ddl",
            "table_path": envelope["desired_contract"]["table"]["path"],
            "observed_at": "2026-10-19T10:32:00+08:00",
            "sha256": "5" * 64,
            "facts": {"table_type": "MERGE_ON_READ"},
        }
    )

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["finding_codes"] == ["FLINK_EXISTING_EVIDENCE_CONFLICT"]
    table_type = next(
        item
        for item in assessment["comparisons"]
        if item["field"] == "table_config.table_type"
    )
    assert table_type["result"] == "MATCH"
    assert table_type["source_ids"] == ["table-config-1"]


def test_pr5_cross_engine_pending_timeline_and_property_conflict_keep_all_findings(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["writer_transition"] = "cross_engine_takeover"
    envelope["evidence"][0]["facts"]["partition_fields"] = []
    envelope["evidence"][2]["facts"]["previous_writer_engine"] = "SPARK"
    envelope["evidence"][3]["facts"]["pending_instants"] = ["20261019103000000"]

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "BLOCKED"
    assert assessment["reconciliation_result"] == "CONFLICT"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT",
        "FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED",
        "FLINK_EXISTING_TIMELINE_UNRESOLVED",
    ]


@pytest.mark.parametrize("bad_value", [None, [], {}, "2026-10-19"])
def test_pr5_malformed_evidence_fails_closed_without_traceback(
    tmp_path: Path, bad_value: object
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][0]["observed_at"] = bad_value
    envelope["evidence"][0]["facts"]["password"] = "must-not-appear"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_EXISTING_EVIDENCE_INVALID" in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "must-not-appear" not in result.stdout
    assert "must-not-appear" not in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("path", "bad_value"),
    [
        (("reconciliation_schema",), True),
        (("evidence", 0, "kind"), []),
        (("evidence", 0, "kind"), {}),
        (("evidence", 0, "facts"), []),
        (("evidence", 0, "observed_at"), "20261019T10:30:00+08:00"),
        (("evidence", 0, "observed_at"), "2026-10-19 10:30:00+08:00"),
        (("writer_transition",), []),
        (("desired_contract", "identity"), None),
        (("desired_contract", "table"), None),
        (("desired_contract", "table", "columns"), None),
        (("desired_contract", "table", "columns", 0, "name"), []),
        (("desired_contract", "identity", "record_key_fields"), None),
    ],
)
def test_pr5_malformed_contract_returns_a_report(
    tmp_path: Path, path: tuple, bad_value: object
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    target = envelope
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = bad_value

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["reconciliation_result"] != "COMPATIBLE"
    assert assessment["executable_eligible"] is False
    assert assessment["finding_codes"]
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("record_merge_mode", "COMMIT_TIME_ORDERING"),
        ("hive_style_partitioning", True),
        ("url_encode_partitioning", True),
        ("key_generator_type", "SIMPLE"),
    ],
)
def test_pr5_rejects_incompatible_merge_and_partition_semantics(
    tmp_path: Path, field: str, value: object
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][0]["facts"][field] = value

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "BLOCKED"
    assert assessment["reconciliation_result"] == "CONFLICT"
    assert assessment["finding_codes"] == ["FLINK_EXISTING_TABLE_PROPERTY_CONFLICT"]


@pytest.mark.parametrize("output_format", [None, "missing_field"])
def test_pr5_requires_evidence_for_datetime_partition_format(
    tmp_path: Path, output_format: str | None
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    config = envelope["evidence"][5]["facts"]
    if output_format is None:
        config.pop("timestamp_key_generator")
    elif output_format == "missing_field":
        config["timestamp_key_generator"].pop("output_date_format")

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "INCOMPLETE"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert assessment["reconciliation_result"] == "UNVERIFIED"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("timestamp_type", "UNIX_TIMESTAMP"),
        ("input_time_unit", "HOURS"),
        ("output_date_format", "yyyy-MM-dd"),
        ("output_timezone", "Asia/Shanghai"),
    ],
)
def test_pr5_timestamp_partition_layout_difference_is_not_deployment_only(
    tmp_path: Path, field: str, value: str
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][5]["facts"]["timestamp_key_generator"][field] = value

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED"
    ]


def test_pr5_table_path_has_independent_provenance(tmp_path: Path) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][4]["facts"]["table_path"] = "file:///tmp/another-table"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    comparison = next(
        item for item in assessment["comparisons"]
        if item["field"] == "table_identity.table_path"
    )
    assert comparison["result"] == "CONFLICT"
    assert comparison["source_ids"] == ["identity-1"]


def test_pr5_excludes_another_tables_schema_from_authoritative_comparisons(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][1]["table_path"] = "file:///tmp/a-different-table"
    envelope["evidence"][1]["facts"]["columns"][0]["type"] = "BIGINT"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_EVIDENCE_CONFLICT",
        "FLINK_EXISTING_EVIDENCE_REQUIRED",
    ]
    schema_comparison = next(
        item for item in assessment["comparisons"] if item["field"] == "schema.columns"
    )
    assert schema_comparison["observed"] is None
    assert schema_comparison["result"] == "UNVERIFIED"
    assert schema_comparison["source_ids"] == []
    schema_summary = next(
        item
        for item in assessment["evidence_summary"]
        if item["evidence_id"] == "table-schema-1"
    )
    assert schema_summary["in_scope"] is False


@pytest.mark.parametrize("recovery", [None, False, True])
def test_pr5_same_job_restore_requires_recoverable_state(
    tmp_path: Path, recovery: bool | None
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["writer_transition"] = "same_job_restore"
    if recovery is not None:
        envelope["evidence"][5]["facts"]["checkpoint_recovery_available"] = recovery

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == (0 if recovery else 1), result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == (
        "CONFIG_VALIDATED" if recovery else "INCOMPLETE" if recovery is None else "REVIEW_REQUIRED"
    )
    assert assessment["executable_eligible"] is False


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [("operation", None), ("operation", "INSERT"), ("index_global", False)],
)
def test_pr5_same_job_restore_requires_matching_writer_state(
    tmp_path: Path, field: str, bad_value: object
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["writer_transition"] = "same_job_restore"
    writer_facts = envelope["evidence"][5]["facts"]
    writer_facts["checkpoint_recovery_available"] = True
    if bad_value is None:
        writer_facts.pop(field)
    else:
        writer_facts[field] = bad_value

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == (
        "INCOMPLETE" if bad_value is None else "REVIEW_REQUIRED"
    )
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == (
        "UNVERIFIED" if bad_value is None else "DEPLOYMENT_CONFIGURATION_CHANGE"
    )
    assert assessment["executable_eligible"] is False


def test_pr5_inventory_preserves_known_multi_writer_when_another_fact_is_missing(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    inventory = envelope["evidence"][2]["facts"]
    inventory["concurrent_writers"] = True
    inventory.pop("previous_writer_engine")

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["finding_codes"] == [
        "FLINK_MULTI_WRITER_REVIEW",
        "FLINK_EXISTING_EVIDENCE_REQUIRED",
    ]
    assert assessment["reconciliation_result"] == "UNVERIFIED"


def test_pr5_nullable_column_addition_requires_schema_evolution_review(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    new_column = {"name": "optional_note", "type": "STRING", "nullable": True}
    envelope["desired_contract"]["table"]["columns"].append(new_column)
    envelope["desired_contract"]["source"]["columns"].append(new_column)

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED"
    ]
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"


def test_pr5_retains_schema_conflict_and_evolution_findings(tmp_path: Path) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    new_column = {"name": "optional_note", "type": "STRING", "nullable": True}
    envelope["desired_contract"]["table"]["columns"].append(new_column)
    envelope["desired_contract"]["source"]["columns"].append(new_column)
    envelope["evidence"][1]["facts"]["columns"][0]["type"] = "INT"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "BLOCKED"
    assert set(assessment["finding_codes"]) == {
        "FLINK_EXISTING_SCHEMA_CONFLICT",
        "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED",
    }


def test_pr5_writer_mismatch_with_missing_evidence_is_not_deployment_only(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["writer_transition"] = "same_job_restore"
    writer = envelope["evidence"][5]["facts"]
    writer["checkpoint_recovery_available"] = True
    writer["operation"] = "INSERT"
    writer.pop("index_type")

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert set(assessment["finding_codes"]) == {
        "FLINK_EXISTING_WRITER_CONFIG_CONFLICT",
        "FLINK_EXISTING_EVIDENCE_REQUIRED",
    }


def test_pr5_unknown_key_generator_is_unverified_not_a_proven_conflict(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][0]["facts"]["key_generator_type"] = "USER_PROVIDED"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED"
    ]
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"


@pytest.mark.parametrize(
    ("field", "value", "expected_status", "expected_result"),
    [
        ("base_file_format", "ORC", "BLOCKED", "CONFLICT"),
        ("initial_table_version", 8, "REVIEW_REQUIRED", "UNVERIFIED"),
        ("table_version", 8, "REVIEW_REQUIRED", "UNVERIFIED"),
        ("timeline_layout_version", 1, "REVIEW_REQUIRED", "UNVERIFIED"),
    ],
)
def test_pr5_distinguishes_property_conflicts_from_unverified_versions(
    tmp_path: Path,
    field: str,
    value: object,
    expected_status: str,
    expected_result: str,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["evidence"][0]["facts"][field] = value

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == expected_status
    assert assessment["reconciliation_result"] == expected_result
    assert assessment["change_disposition"] == (
        "MIGRATION_REQUIRED" if expected_result == "CONFLICT" else "UNVERIFIED"
    )


@pytest.mark.parametrize(
    ("mutation", "expected_status", "expected_code"),
    [
        ("missing_required", "BLOCKED", "FLINK_EXISTING_SCHEMA_CONFLICT"),
        ("incompatible_type", "BLOCKED", "FLINK_EXISTING_SCHEMA_CONFLICT"),
        ("nullable_widening", "BLOCKED", "FLINK_EXISTING_SCHEMA_CONFLICT"),
        (
            "extra_physical_field",
            "REVIEW_REQUIRED",
            "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED",
        ),
    ],
)
def test_pr5_classifies_physical_schema_differences(
    tmp_path: Path, mutation: str, expected_status: str, expected_code: str
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    columns = envelope["evidence"][1]["facts"]["columns"]
    if mutation == "missing_required":
        columns.pop(0)
    elif mutation == "incompatible_type":
        columns[0]["type"] = "BIGINT"
    elif mutation == "nullable_widening":
        columns[1]["nullable"] = False
    else:
        columns.append({"name": "legacy_note", "type": "STRING", "nullable": True})

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == expected_status
    assert assessment["finding_codes"] == [expected_code]
    assert assessment["reconciliation_result"] == (
        "CONFLICT" if expected_status == "BLOCKED" else "UNVERIFIED"
    )


@pytest.mark.parametrize(
    ("mutation", "expected_fragment"),
    [
        ("duplicate_id", "unique non-empty string"),
        ("bad_digest", "lowercase SHA-256"),
        ("unknown_kind", "kind is not supported"),
    ],
)
def test_pr5_rejects_invalid_evidence_identity_fields(
    tmp_path: Path, mutation: str, expected_fragment: str
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    if mutation == "duplicate_id":
        envelope["evidence"][1]["evidence_id"] = envelope["evidence"][0]["evidence_id"]
    elif mutation == "bad_digest":
        envelope["evidence"][0]["sha256"] = "not-a-digest"
    else:
        envelope["evidence"][0]["kind"] = "catalog_dump"

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "INCOMPLETE"
    assert "FLINK_EXISTING_EVIDENCE_INVALID" in assessment["finding_codes"]
    assert expected_fragment in next(
        finding["message"]
        for finding in assessment["findings"]
        if finding["code"] == "FLINK_EXISTING_EVIDENCE_INVALID"
    )


def test_pr5_flink_takeover_requires_checkpoint_posture_and_stopped_writer(
    tmp_path: Path,
) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    envelope["writer_transition"] = "flink_writer_takeover"
    envelope["evidence"][2]["facts"]["previous_writer_stopped"] = False

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["finding_codes"] == [
        "FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED",
        "FLINK_EXISTING_EVIDENCE_REQUIRED",
    ]
    assert "previous_writer_stopped" in assessment["findings"][0]["message"]
    assert "checkpoint or savepoint" in assessment["findings"][1]["message"]


def test_pr5_conflicting_property_sources_are_not_silently_merged(tmp_path: Path) -> None:
    envelope = json.loads(_read(PR5_FIXTURE_DIR / "compatible_mutable_cow.json"))
    duplicate = copy.deepcopy(envelope["evidence"][0])
    duplicate["evidence_id"] = "table-config-2"
    duplicate["facts"]["table_type"] = "MERGE_ON_READ"
    envelope["evidence"].append(duplicate)

    result = _run_flink_existing_table_validator(_write_design(tmp_path, envelope))

    assert result.returncode == 1, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert "FLINK_EXISTING_EVIDENCE_CONFLICT" in assessment["finding_codes"]
    assert assessment["reconciliation_result"] == "UNVERIFIED"


@pytest.mark.parametrize(
    "replay_behavior",
    ["cannot_occur", "duplicates_acceptable", "must_collapse"],
)
def test_pr3_accepts_resolved_replay_behaviors(
    tmp_path: Path, replay_behavior: str
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["safety"]["replay_behavior"] = replay_behavior

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["finding_codes"] == []
    assert assessment["artifacts"]["combined_sql"] == _read_sql_golden(
        PR3_FIXTURE_DIR / "mutable_cow.sql"
    ).rstrip("\n")


@pytest.mark.parametrize("replay_behavior", ["unknown", [], {}])
def test_pr3_rejects_unresolved_replay_behaviors(
    tmp_path: Path, replay_behavior: object
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["safety"]["replay_behavior"] = replay_behavior

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_REPLAY_BEHAVIOR_UNRESOLVED"]
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert "artifacts" not in assessment


def test_pr3_accepts_update_only_source_without_delete_payload(tmp_path: Path) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["source"]["emits_deletes"] = False
    design["source"]["delete_payload"] = "NOT_APPLICABLE"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert assessment["finding_codes"] == []


@pytest.mark.parametrize("changelog_mode", ["INSERT_ONLY", "RETRACT", "ALL"])
def test_pr3_rejects_unsupported_source_changelog(
    tmp_path: Path, changelog_mode: str
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["source"]["changelog_mode"] = changelog_mode

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_MUTABLE_SOURCE_CHANGELOG_UNSUPPORTED"
    ]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("field_name", "field_type", "nullable"),
    [
        ("event_version", "STRING", False),
        ("event_version", "TIMESTAMP_LTZ(3)", False),
        ("event_version", "BIGINT", True),
        ("no_precombine", "BIGINT", False),
    ],
)
def test_pr3_rejects_invalid_ordering_fields(
    tmp_path: Path, field_name: str, field_type: str, nullable: bool
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    for section in ("table", "source"):
        column = next(
            column
            for column in design[section]["columns"]
            if column["name"] == "event_version"
        )
        column["name"] = field_name
        column["type"] = field_type
        column["nullable"] = nullable
    design["ordering"]["fields"] = [field_name]

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_MUTABLE_ORDERING_FIELD_INVALID" in assessment["finding_codes"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("field", "value"),
    [("type", "BUCKET"), ("global", False), ("state_ttl_days", 1)],
)
def test_pr3_rejects_unsafe_index_configuration(
    tmp_path: Path, field: str, value: object
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["write"]["index"][field] = value

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED"
    ]
    assert assessment["status"] == "BLOCKED"


def test_pr3_requires_index_bootstrap(tmp_path: Path) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["write"]["index"]["bootstrap_enabled"] = False

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_MUTABLE_INDEX_BOOTSTRAP_REQUIRED"]
    assert assessment["status"] == "BLOCKED"


def test_pr3_rejects_table_property_alias_for_write_merge_mode(tmp_path: Path) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    design["write"]["connector_options"] = {
        "hoodie.record.merge.mode": "EVENT_TIME_ORDERING"
    }

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_OPTION_NOT_VERIFIED"]
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (("identity", "mode", "auto_key"), "FLINK_MUTABLE_AUTO_KEY_UNSUPPORTED"),
        (
            ("table", "partition_fields_mutable", True),
            "FLINK_MUTABLE_PARTITION_EVOLUTION_UNSUPPORTED",
        ),
        (
            ("source", "delete_payload", "KEY_ONLY"),
            "FLINK_MUTABLE_DELETE_PAYLOAD_UNSUPPORTED",
        ),
        (("write", "operation", "insert"), "FLINK_PR3_WRITE_PATH_UNSUPPORTED"),
    ],
)
def test_pr3_fails_closed_outside_the_bounded_mutable_path(
    tmp_path: Path, mutation: tuple[str, str, object], expected_code: str
) -> None:
    design = json.loads(_read(PR3_FIXTURE_DIR / "mutable_cow.json"))
    section, field, value = mutation
    design[section][field] = value
    if section == "identity" and field == "mode":
        design["identity"]["record_key_fields"] = []

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert expected_code in assessment["finding_codes"]
    assert assessment["status"] == "BLOCKED"
    assert "artifacts" not in assessment


def test_pr2_fails_closed_when_capability_manifest_is_malformed(tmp_path: Path) -> None:
    manifest_path = tmp_path / "capabilities.toml"
    manifest_path.write_text(
        _read(CAPABILITY_MANIFEST).replace("[physical_types]", "[physical_types_broken]"),
        encoding="utf-8",
    )

    result = _run_flink_design_validator(
        PR2_FIXTURE_DIR / "stable_key.json", "--manifest", str(manifest_path)
    )

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_BASELINE_EVIDENCE_INVALID"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert assessment["validation_evidence"] == {"design_contract_schema": 1}
    assert "artifacts" not in assessment


@pytest.mark.parametrize("contract_schema", [True, 1.0, 4, [], {}])
def test_flink_design_rejects_non_exact_contract_schema(
    tmp_path: Path, contract_schema: object
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["contract_schema"] = contract_schema

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "Traceback" not in result.stderr
    assert "FLINK_DESIGN_CONTRACT_INVALID" in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


def test_pr2_rejects_record_key_missing_from_append_schema(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["write"]["connector_options"] = {
        "hoodie.datasource.write.recordkey.field": "missing_id"
    }

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_RECORD_KEY_FIELD_MISSING" in assessment["finding_codes"]
    assert assessment["finding_codes"][0] == "FLINK_RECORD_KEY_FIELD_MISSING"
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


def test_pr2_rejects_insert_clustering_override(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["write"]["insert_cluster"] = True

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_APPEND_MODE_CLUSTERING_ENABLED"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False


def test_pr2_rejects_insert_clustering_connector_override(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["write"]["connector_options"] = {"write.insert.cluster": True}

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_APPEND_MODE_CLUSTERING_ENABLED" in assessment["finding_codes"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False


def test_pr2_rejects_non_append_source_changelog(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["source"]["changelog_mode"] = "UPSERT"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False


def test_pr2_treats_unknown_source_changelog_as_incomplete(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["source"].pop("changelog_mode")

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_SOURCE_CONTRACT_REQUIRED"]
    assert assessment["status"] == "INCOMPLETE"
    assert assessment["executable_eligible"] is False


def test_pr2_requires_source_physical_schema(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["source"].pop("columns")

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_SOURCE_CONTRACT_REQUIRED" in assessment["finding_codes"]
    assert assessment["status"] == "INCOMPLETE"
    assert assessment["executable_eligible"] is False


def test_pr2_preserves_combined_validation_findings(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["write"]["insert_cluster"] = True
    design["source"]["changelog_mode"] = "UPSERT"
    design["runtime"]["checkpoint_interval_ms"] = None

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY",
        "FLINK_APPEND_MODE_CLUSTERING_ENABLED",
        "FLINK_CHECKPOINT_INTERVAL_REQUIRED",
    ]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False


def test_pr2_preserves_multiple_reasons_under_one_finding_code(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["name"] = "<TARGET_TABLE>"
    design["table"]["path"] = "s3://user:password@bucket/orders"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    load_bearing_finding = next(
        finding
        for finding in assessment["findings"]
        if finding["code"] == "FLINK_LOAD_BEARING_VALUE_REQUIRED"
    )
    assert "target table" in load_bearing_finding["message"]
    assert "target path" in load_bearing_finding["message"]
    assert assessment["finding_codes"].count("FLINK_LOAD_BEARING_VALUE_REQUIRED") == 1


@pytest.mark.parametrize(
    ("section", "field"),
    [
        ("table", "partition_fields"),
        ("identity", "record_key_fields"),
        ("identity", "record_key_option_fields"),
        ("write", "connector_options"),
        ("runtime", "checkpointing_enabled"),
    ],
)
def test_pr2_requires_explicit_empty_and_boolean_contract_fields(
    tmp_path: Path, section: str, field: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design[section].pop(field)

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_DESIGN_CONTRACT_INVALID" in assessment["finding_codes"]
    assert assessment["status"] == "INCOMPLETE"
    assert assessment["executable_eligible"] is False


@pytest.mark.parametrize(
    ("mutation", "expected_code", "expected_status"),
    [
        (
            ("write", "connector_options", {"ordering.fields": "event_ts"}),
            "FLINK_DESIGN_CONTRACT_INVALID",
            "INCOMPLETE",
        ),
        (
            ("write", "connector_options", {"hoodie.vector.columns": "payload"}),
            "FLINK_OPTION_NOT_VERIFIED",
            "REVIEW_REQUIRED",
        ),
        (
            ("runtime", "target_commit_freshness_ms", 0),
            "FLINK_DESIGN_CONTRACT_INVALID",
            "INCOMPLETE",
        ),
        (
            ("identity", "auto_key_accepted", True),
            "FLINK_DESIGN_CONTRACT_INVALID",
            "INCOMPLETE",
        ),
        (
            ("table", "partition_fields", ["partition_date", "partition_date"]),
            "FLINK_DESIGN_CONTRACT_INVALID",
            "INCOMPLETE",
        ),
    ],
)
def test_pr2_rejects_ignored_or_malformed_contract_values(
    tmp_path: Path,
    mutation: tuple[str, str, object],
    expected_code: str,
    expected_status: str,
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    section, key, value = mutation
    design[section][key] = value

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert expected_code in assessment["finding_codes"]
    assert assessment["status"] == expected_status
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


def test_pr2_rejects_out_of_range_physical_type_parameters(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["columns"][1]["type"] = "VARCHAR(2147483648)"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_SCHEMA_TYPE_UNVERIFIED" in assessment["finding_codes"]
    assert assessment["status"] == "REVIEW_REQUIRED"
    assert assessment["executable_eligible"] is False


@pytest.mark.parametrize("binary_type", ["BYTES", "BINARY(16)", "VARBINARY(16)"])
@pytest.mark.parametrize(
    ("field_name", "expected_message"),
    [
        ("id", "Record-key field 'id'"),
        ("partition_date", "Partition field 'partition_date'"),
    ],
)
def test_pr2_rejects_binary_routing_fields(
    tmp_path: Path, binary_type: str, field_name: str, expected_message: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    for section in ("table", "source"):
        column = next(
            column
            for column in design[section]["columns"]
            if column["name"] == field_name
        )
        column["type"] = binary_type

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment
    assert expected_message in assessment["findings"][0]["message"]


@pytest.mark.parametrize("binary_type", ["BYTES", "BINARY(16)", "VARBINARY(16)"])
def test_pr2_accepts_binary_payload_fields(tmp_path: Path, binary_type: str) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    for section in ("table", "source"):
        design[section]["columns"][0]["type"] = binary_type

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert f"`payload` {binary_type}" in assessment["artifacts"]["combined_sql"]


@pytest.mark.parametrize("type_name", ["TIME", "TIMESTAMP", "TIMESTAMP_LTZ"])
@pytest.mark.parametrize(("precision", "accepted"), [(6, True), (7, False), (9, False)])
def test_pr2_enforces_pinned_temporal_precision_limits(
    tmp_path: Path, type_name: str, precision: int, accepted: bool
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    temporal_type = f"{type_name}({precision})"
    design["table"]["columns"][1]["type"] = temporal_type
    design["source"]["columns"][1]["type"] = temporal_type

    result = _run_flink_design_validator(_write_design(tmp_path, design))
    assessment = json.loads(result.stdout)

    if accepted:
        assert result.returncode == 0, result.stderr
        assert assessment["finding_codes"] == []
        assert assessment["status"] == "CONFIG_VALIDATED"
        assert temporal_type in assessment["artifacts"]["combined_sql"]
    else:
        assert result.returncode == 1
        assert assessment["finding_codes"] == [
            "FLINK_TEMPORAL_PRECISION_UNSUPPORTED"
        ]
        assert assessment["status"] == "BLOCKED"
        assert assessment["executable_eligible"] is False
        assert "artifacts" not in assessment


@pytest.mark.parametrize("field_name", ["user-id", "1user", "user.id"])
def test_pr2_rejects_non_avro_physical_field_names(
    tmp_path: Path, field_name: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    design["table"]["columns"][0]["name"] = field_name
    design["source"]["columns"][0]["name"] = field_name

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


@pytest.mark.parametrize("field_name", ["_user", "user_1"])
def test_pr2_accepts_avro_physical_field_name_boundaries(
    tmp_path: Path, field_name: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    design["table"]["columns"][0]["name"] = field_name
    design["source"]["columns"][0]["name"] = field_name

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert f"`{field_name}` STRING" in assessment["artifacts"]["combined_sql"]


@pytest.mark.parametrize(
    "field_name",
    [
        "_hoodie_commit_seqno",
        "_hoodie_commit_time",
        "_hoodie_file_name",
        "_hoodie_operation",
        "_hoodie_partition_path",
        "_hoodie_record_key",
        "_HOODIE_COMMIT_TIME",
    ],
)
def test_pr2_rejects_reserved_hudi_metadata_field_names(
    tmp_path: Path, field_name: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    design["table"]["columns"][0]["name"] = field_name
    design["source"]["columns"][0]["name"] = field_name

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == ["FLINK_HUDI_METADATA_FIELD_CONFLICT"]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


def test_pr2_does_not_reject_the_entire_hoodie_field_prefix(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    design["table"]["columns"][0]["name"] = "_hoodie_custom"
    design["source"]["columns"][0]["name"] = "_hoodie_custom"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert "`_hoodie_custom` STRING" in assessment["artifacts"]["combined_sql"]


@pytest.mark.parametrize(
    ("interval_ms", "accepted"), [(9, False), (10, False), (999, False), (1000, True)]
)
def test_pr2_enforces_checkpoint_interval_safety_floor(
    tmp_path: Path, interval_ms: int, accepted: bool
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "auto_key.json"))
    design["runtime"]["checkpoint_interval_ms"] = interval_ms

    result = _run_flink_design_validator(_write_design(tmp_path, design))
    assessment = json.loads(result.stdout)

    if accepted:
        assert result.returncode == 0, result.stderr
        assert assessment["finding_codes"] == []
        assert assessment["status"] == "CONFIG_VALIDATED"
        assert f"'{interval_ms} ms'" in assessment["artifacts"]["runtime_sql"]
    else:
        assert result.returncode == 1
        assert assessment["finding_codes"] == [
            "FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED"
        ]
        assert assessment["status"] == "BLOCKED"
        assert assessment["executable_eligible"] is False
        assert "artifacts" not in assessment


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (("table", "path", "<TABLE_PATH>"), "FLINK_LOAD_BEARING_VALUE_REQUIRED"),
        (
            ("table", "path", "s3://user:password@bucket/orders"),
            "FLINK_LOAD_BEARING_VALUE_REQUIRED",
        ),
        (
            ("table", "path", "s3://bucket/orders#token=secret"),
            "FLINK_LOAD_BEARING_VALUE_REQUIRED",
        ),
        (
            ("table", "path", "s3://[invalid-host/orders"),
            "FLINK_LOAD_BEARING_VALUE_REQUIRED",
        ),
        (
            ("table", "path", "file:///tmp/orders\nDROP TABLE target"),
            "FLINK_LOAD_BEARING_VALUE_REQUIRED",
        ),
        (("runtime", "checkpointing_enabled", False), "FLINK_CHECKPOINTING_REQUIRED"),
        (("safety", "replay_behavior", "must_collapse"), "FLINK_REPLAY_IDEMPOTENCE_DEFERRED"),
    ],
)
def test_pr2_withholds_sql_when_load_bearing_contract_is_invalid(
    tmp_path: Path, mutation: tuple[str, str, object], expected_code: str
) -> None:
    design = copy.deepcopy(json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json")))
    section, key, value = mutation
    design[section][key] = value

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert expected_code in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


@pytest.mark.parametrize("scheme", ["abfs", "abfss"])
def test_pr2_accepts_standard_abfs_filesystem_authority(
    tmp_path: Path, scheme: str
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["path"] = (
        f"{scheme}://container@account.dfs.core.windows.net/orders"
    )

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    assessment = json.loads(result.stdout)
    assert assessment["status"] == "CONFIG_VALIDATED"
    assert design["table"]["path"] in assessment["artifacts"]["table_ddl"]


@pytest.mark.parametrize(
    "path",
    [
        "abfss://user:password@account.dfs.core.windows.net/orders",
        "abfss://user@non-azure.example.com/orders",
        "s3://user@bucket/orders",
    ],
)
def test_pr2_rejects_actual_uri_credentials(tmp_path: Path, path: str) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["path"] = path

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_LOAD_BEARING_VALUE_REQUIRED" in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment


@pytest.mark.parametrize(
    "sas_query",
    [
        "sv=2025-01-05&sp=rw&sig=EXAMPLE_SIGNATURE",
        "sv=2025-01-05&sp=rw&SIG=",
    ],
)
def test_pr2_rejects_azure_sas_signatures(tmp_path: Path, sas_query: str) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["path"] = (
        "https://account.blob.core.windows.net/container/orders?" + sas_query
    )

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert "FLINK_LOAD_BEARING_VALUE_REQUIRED" in assessment["finding_codes"]
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment
    assert "EXAMPLE_SIGNATURE" not in result.stdout
    assert "EXAMPLE_SIGNATURE" not in result.stderr


@pytest.mark.parametrize("replay_behavior", [[], {}])
def test_pr2_reports_non_string_replay_behavior_without_losing_findings(
    tmp_path: Path, replay_behavior: object
) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["safety"]["replay_behavior"] = replay_behavior
    design["write"]["insert_cluster"] = True

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 1
    assessment = json.loads(result.stdout)
    assert assessment["finding_codes"] == [
        "FLINK_REPLAY_BEHAVIOR_UNRESOLVED",
        "FLINK_APPEND_MODE_CLUSTERING_ENABLED",
    ]
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert "artifacts" not in assessment
    assert "Traceback" not in result.stderr


def test_pr2_sql_renderer_escapes_identifiers_and_literals(tmp_path: Path) -> None:
    design = json.loads(_read(PR2_FIXTURE_DIR / "stable_key.json"))
    design["table"]["name"] = "analytics.order`s"
    design["source"]["table"] = "staging.source`s"
    design["table"]["path"] = "file:///tmp/architect's-orders"

    result = _run_flink_design_validator(_write_design(tmp_path, design))

    assert result.returncode == 0, result.stderr
    sql = json.loads(result.stdout)["artifacts"]["combined_sql"]
    assert "`analytics`.`order``s`" in sql
    assert "`staging`.`source``s`" in sql
    assert "'file:///tmp/architect''s-orders'" in sql


def test_flink_code_inventory_covers_findings_and_advisories() -> None:
    contract = _load_toml(GATE_FIXTURE)
    decisions = _read(REFERENCES_DIR / "flink-decision-overrides.md")
    warnings = _read(REFERENCES_DIR / "flink-warnings.md")
    codes = set(contract["finding_status"])
    syntax_tree = ast.parse(_read(FLINK_DESIGN_VALIDATOR))
    finding_assignment = next(
        node
        for node in syntax_tree.body
        if isinstance(node, ast.Assign)
        and any(
            isinstance(target, ast.Name) and target.id == "FINDING_STATUS"
            for target in node.targets
        )
    )
    codes.update(ast.literal_eval(finding_assignment.value))
    codes.update(
        {
            "FLINK_AUTO_KEY_DURABILITY",
            "FLINK_CHECKPOINT_SMALL_FILE_RISK",
            "FLINK_STABLE_KEY_NOT_IDEMPOTENT",
            "FLINK_SECRET_REDACTED",
        }
    )

    for code in codes:
        assert f"`{code}`" in decisions
        assert f"## {code}" in warnings


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
        "## F7 — Physical table and source contract",
        "## F8 — Source changelog contract",
        "## F9 — Streaming checkpoint contract",
        "## F10 — Mutable table type, query freshness, and MOR ownership",
        "## F11 — Existing-table evidence and writer transition",
        "## PR2/PR3/PR4/PR5 validation and completion",
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


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            "CREATE TABLE src (id INT) WITH ('connector'='jdbc', "
            "'password'='test-secret');",
            "CREATE TABLE src (id INT) WITH ('connector'='jdbc', "
            "'password'='<redacted>');",
        ),
        (
            'command --password "first second third" --table orders',
            'command --password "<redacted>" --table orders',
        ),
        (
            r'{"password":"first\"second-suffix","table":"orders"}',
            '{"password":"<redacted>","table":"orders"}',
        ),
        (
            "WITH ('password'='first''second', 'table'='orders')",
            "WITH ('password'='<redacted>', 'table'='orders')",
        ),
    ],
)
def test_redactor_consumes_complete_inline_and_quoted_values(
    evidence: str, expected: str
) -> None:
    result = subprocess.run(
        [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
        input=evidence,
        text=True,
        capture_output=True,
        check=True,
    )

    assert result.stdout == expected

    repeated = subprocess.run(
        [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
        input=result.stdout,
        text=True,
        capture_output=True,
        check=True,
    )
    assert repeated.stdout == expected


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        ("password=first&second", "password=<redacted>"),
        (
            "s3.secret-key=first,second;third}fourth",
            "s3.secret-key=<redacted>",
        ),
        ("  password = first)second]third", "  password = <redacted>"),
        ("password=first second#third", "password=<redacted>"),
        ("password=first?token=second&third", "password=<redacted>"),
        ("password=", "password=<redacted>"),
        ("command --passwd=first&second --table orders", "command --passwd=<redacted>"),
        ("command --db.password=first&second", "command --db.password=<redacted>"),
        (
            "endpoint=https://example.com/?pwd=first&second",
            "endpoint=https://example.com/?pwd=<redacted>",
        ),
        (
            "endpoint=https://example.com/?region=us&pwd=first&second",
            "endpoint=https://example.com/?region=us&pwd=<redacted>",
        ),
    ],
)
@pytest.mark.parametrize("ending", ["\n", "\r\n", "\r", ""])
def test_redactor_removes_entire_unquoted_property_value(
    evidence: str, expected: str, ending: str
) -> None:
    # Byte I/O checks that CR/LF survive without subprocess newline normalization.
    following_fact = "table.name=orders" if ending else ""
    source = (evidence + ending + following_fact).encode()
    expected_output = (expected + ending + following_fact).encode()
    for _ in range(2):
        result = subprocess.run(
            [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
            input=source,
            capture_output=True,
            check=True,
        )
        assert result.stdout == expected_output
        source = result.stdout


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            "endpoint=https://example.com/?token=secret&region=us&password=other&limit=10",
            "endpoint=https://example.com/?token=<redacted>&region=us"
            "&password=<redacted>&limit=10",
        ),
        (
            "command --password=first&second --table orders",
            "command --password=<redacted> --table orders",
        ),
    ],
)
def test_redactor_preserves_query_and_cli_value_boundaries(evidence: str, expected: str) -> None:
    for _ in range(2):
        result = subprocess.run(
            [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
            input=evidence,
            text=True,
            capture_output=True,
            check=True,
        )
        assert result.stdout == expected
        evidence = result.stdout


@pytest.mark.parametrize("prefix", ["payload=", "", 'payload="'])
def test_redactor_preserves_long_non_secret_values_without_stalling(prefix: str) -> None:
    evidence = prefix + "x" * 65536 + ('"' if prefix.endswith('"') else "") + "\n"
    result = subprocess.run(
        [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
        input=evidence,
        text=True,
        capture_output=True,
        check=True,
        # Generous startup allowance; the old suffix-by-suffix scan takes tens of seconds.
        timeout=5,
    )
    assert result.stdout == evidence


def _redact_evidence_bytes(evidence: bytes) -> bytes:
    return subprocess.run(
        [sys.executable, str(SKILL_DIR / "redact_sensitive_values.py")],
        input=evidence,
        capture_output=True,
        check=True,
        timeout=5,
    ).stdout


@pytest.mark.parametrize("ending", ["\n", "\r\n"])
@pytest.mark.parametrize("layout", ["before_separator", "after_separator", "both"])
@pytest.mark.parametrize("secret", ["test-secret", 'first"second\\suffix'])
def test_redactor_consumes_newline_separated_json_values(
    ending: str, layout: str, secret: str
) -> None:
    whitespace = ending + "  "
    if layout == "both":
        whitespace = ending + "\t " + ending + "  "
    before = whitespace if layout in ("before_separator", "both") else ""
    after = whitespace if layout in ("after_separator", "both") else " "
    prefix = '{"password"' + before + ":" + after
    suffix = ',' + ending + '  "table": "orders"}'
    evidence = (prefix + json.dumps(secret) + suffix).encode()
    expected = (prefix + '"<redacted>"' + suffix).encode()

    output = _redact_evidence_bytes(evidence)

    assert output == expected
    assert json.loads(output) == {"password": "<redacted>", "table": "orders"}
    assert _redact_evidence_bytes(output) == output


@pytest.mark.parametrize("ending", ["\n", "\r\n"])
@pytest.mark.parametrize("layout", ["before_separator", "after_separator", "both"])
def test_redactor_consumes_newline_separated_sql_properties(ending: str, layout: str) -> None:
    whitespace = ending + "  "
    before = whitespace if layout in ("before_separator", "both") else ""
    after = whitespace if layout in ("after_separator", "both") else " "
    prefix = "CREATE TABLE src (id INT) WITH ('password'" + before + "=" + after
    suffix = ", 'connector'='jdbc');"
    evidence = (prefix + "'first''second'" + suffix).encode()
    expected = (prefix + "'<redacted>'" + suffix).encode()

    output = _redact_evidence_bytes(evidence)

    assert output == expected
    assert _redact_evidence_bytes(output) == output


@pytest.mark.parametrize("ending", ["\n", "\r\n"])
@pytest.mark.parametrize("separator", ["=", ":"])
def test_redactor_keeps_empty_property_values_on_their_own_line(
    ending: str, separator: str
) -> None:
    evidence = ("password " + separator + " \t" + ending + "table.name=orders" + ending).encode()
    expected = (
        "password " + separator + " \t<redacted>" + ending + "table.name=orders" + ending
    ).encode()

    output = _redact_evidence_bytes(evidence)

    assert output == expected
    assert _redact_evidence_bytes(output) == output


@pytest.mark.parametrize(
    ("evidence", "expected"),
    [
        (
            "WITH ('password'=\n'first\nsecond', 'connector'='jdbc')",
            "WITH ('password'=\n'<redacted>', 'connector'='jdbc')",
        ),
        (
            '{"password":\n "first\nsecond',
            '{"password":\n "<redacted>',
        ),
        (
            "password=\"first\nsecond\nthird",
            'password="<redacted>',
        ),
        (
            '{"password":\n first\n second}',
            '{"password":\n <redacted>',
        ),
        (
            "WITH ('password'=\n first\n second)",
            "WITH ('password'=\n <redacted>",
        ),
    ],
)
def test_redactor_does_not_expose_value_continuations(evidence: str, expected: str) -> None:
    output = _redact_evidence_bytes(evidence.encode())

    assert output == expected.encode()
    assert _redact_evidence_bytes(output) == output
