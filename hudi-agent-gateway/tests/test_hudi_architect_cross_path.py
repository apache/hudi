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
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Cross-path invariants for the completed Hudi Architect Flink milestone."""

from __future__ import annotations

import copy
import json
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest

GATEWAY_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = GATEWAY_DIR.parent
FIXTURE_DIR = GATEWAY_DIR / "tests" / "fixtures" / "hudi_architect"
PR6_PROFILES = FIXTURE_DIR / "flink_pr6" / "catalog_profiles.toml"
PR8_FIXTURE_DIR = FIXTURE_DIR / "flink_pr8"
SCENARIO_MATRIX = PR8_FIXTURE_DIR / "cross_path_scenarios.toml"
DESIGN_VALIDATOR = (
    REPO_ROOT / "hudi-ai-operator" / "skills" / "hudi-architect"
    / "validate_flink_design.py"
)
EXISTING_TABLE_VALIDATOR = DESIGN_VALIDATOR.with_name(
    "validate_flink_existing_table.py"
)
STATUS_PRIORITY = {"INCOMPLETE": 0, "REVIEW_REQUIRED": 1, "BLOCKED": 2}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(value, dict)
    return value


def _load_matrix() -> dict[str, Any]:
    with SCENARIO_MATRIX.open("rb") as matrix_file:
        return tomllib.load(matrix_file)


def _scenarios() -> list[dict[str, Any]]:
    return _load_matrix()["scenario"]


def _scenario(case_id: str) -> dict[str, Any]:
    return next(item for item in _scenarios() if item["case_id"] == case_id)


def _scenario_ids() -> list[str]:
    return [scenario["case_id"] for scenario in _scenarios()]


def _design_scenario_ids() -> list[str]:
    return [
        scenario["case_id"]
        for scenario in _scenarios()
        if scenario["validator"] == "design"
    ]


def _write_input(tmp_path: Path, case_id: str, value: dict[str, Any]) -> Path:
    input_path = tmp_path / f"{case_id.lower()}.json"
    input_path.write_text(json.dumps(value), encoding="utf-8")
    return input_path


def _catalog_profile(profile_id: str) -> dict[str, Any]:
    with PR6_PROFILES.open("rb") as profile_file:
        profiles = tomllib.load(profile_file)["profile"]
    profile = next(item for item in profiles if item["id"] == profile_id)
    design = _read_json(FIXTURE_DIR / profile["base_fixture"])
    catalog = json.loads(profile["catalog"])
    design["catalog"] = catalog
    design["safety"]["external_catalog"] = profile_id != "direct_sql_none"
    if "source_table" in profile:
        design["source"]["table"] = profile["source_table"]
    if catalog["resolution_mode"] == "hudi_dfs_catalog":
        design["table"]["path"] = (
            f"{catalog['catalog_path']}/{catalog['database']}/{design['table']['name']}"
        )
    return design


def _direct_sql_hms(design: dict[str, Any]) -> dict[str, Any]:
    result = copy.deepcopy(design)
    result["safety"]["external_catalog"] = True
    catalog: dict[str, Any] = {
        "contract_schema": 1,
        "resolution_mode": "direct_sql",
        "external_discovery": "hive_metastore",
        "sync_owner": "flink_sink",
        "data_lifecycle_owner": "application",
        "database": "analytics",
        "hive_configuration": {
            "source": "metastore_uris",
            "location": "thrift://hms.example:9083",
        },
    }
    if result["write"]["table_type"] == "MERGE_ON_READ":
        catalog["mor_sync_strategy"] = "ALL"
    result["catalog"] = catalog
    return result


def _multi_writer(
    design: dict[str, Any], concurrency_mode: str
) -> dict[str, Any]:
    result = copy.deepcopy(design)
    result["safety"]["writer_model"] = "multi_writer"
    result["writer_contract"] = {
        "contract_schema": 1,
        "primary_writer_id": "flink-orders-stream",
        "additional_writers": [
            {
                "writer_id": "spark-orders-backfill",
                "role": "BACKFILL",
                "relationship_to_primary": "INDEPENDENT_CONCURRENT",
                "engine": "SPARK",
                "engine_version": "3.5",
                "hudi_version": "1.2.0",
                "operation": "BULK_INSERT",
            }
        ],
        "ownership": {
            "schema_evolution_owner_id": "flink-orders-stream",
            "table_configuration_owner_id": "flink-orders-stream",
        },
        "observed_concurrency": {
            "mode": concurrency_mode,
            "lock_provider_class": (
                "org.apache.hudi.client.transaction.lock."
                "FileSystemBasedLockProvider"
            ),
            "lock_configuration_consistency": "CONSISTENT",
        },
    }
    return result


def _scenario_input(case_id: str) -> tuple[str, dict[str, Any]]:
    source = _scenario(case_id)["input"]
    if source == "flink_pr2/stable_key.json":
        return "design", _read_json(FIXTURE_DIR / "flink_pr2" / "stable_key.json")
    if source == "catalog_profile:hudi_dfs_catalog_hive_metastore":
        return "design", _catalog_profile("hudi_dfs_catalog_hive_metastore")
    if source == "flink_pr4/mutable_mor.json+direct_sql_hive_metastore":
        design = _read_json(FIXTURE_DIR / "flink_pr4" / "mutable_mor.json")
        return "design", _direct_sql_hms(design)
    if source == "flink_pr5/compatible_mutable_cow.json":
        return "existing_table", _read_json(
            FIXTURE_DIR / "flink_pr5" / "compatible_mutable_cow.json"
        )
    if source == "flink_pr5/compatible_mutable_cow.json+direct_sql_hive_metastore":
        envelope = _read_json(
            FIXTURE_DIR / "flink_pr5" / "compatible_mutable_cow.json"
        )
        envelope["desired_contract"] = _direct_sql_hms(
            envelope["desired_contract"]
        )
        return "existing_table", envelope
    if source == "flink_pr3/mutable_cow.json+multi_writer_occ":
        design = _read_json(FIXTURE_DIR / "flink_pr3" / "mutable_cow.json")
        return "design", _multi_writer(
            design, "OPTIMISTIC_CONCURRENCY_CONTROL"
        )
    if source == "flink_pr4/mutable_mor.json+multi_writer_nbcc":
        design = _read_json(FIXTURE_DIR / "flink_pr4" / "mutable_mor.json")
        return "design", _multi_writer(
            design, "NON_BLOCKING_CONCURRENCY_CONTROL"
        )
    if source == (
        "flink_pr5/compatible_mutable_cow.json+"
        "flink_pr8/existing_multi_writer_out_of_scope.json"
    ):
        envelope = _read_json(
            FIXTURE_DIR / "flink_pr5" / "compatible_mutable_cow.json"
        )
        envelope["evidence"].append(
            _read_json(PR8_FIXTURE_DIR / "existing_multi_writer_out_of_scope.json")
        )
        return "existing_table", envelope
    if source == "flink_pr8/combined_safety_gates.json":
        return "design", _read_json(PR8_FIXTURE_DIR / "combined_safety_gates.json")
    if source == "flink_pr8/malformed_cross_path.json":
        return "existing_table", _read_json(
            PR8_FIXTURE_DIR / "malformed_cross_path.json"
        )
    raise AssertionError(f"Unknown PR8 scenario input: {source}")


def _run_scenario_process(
    tmp_path: Path, case_id: str, *, emit_sql: bool = False
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any]]:
    validator_name, input_value = _scenario_input(case_id)
    validator = (
        DESIGN_VALIDATOR
        if validator_name == "design"
        else EXISTING_TABLE_VALIDATOR
    )
    command = [
        sys.executable,
        str(validator),
        "--input",
        str(_write_input(tmp_path, case_id, input_value)),
    ]
    if emit_sql:
        assert validator_name == "design"
        command.append("--emit-sql")
    result = subprocess.run(
        command,
        text=True,
        capture_output=True,
        check=False,
    )
    return result, input_value


def _run_scenario(
    tmp_path: Path, case_id: str
) -> tuple[subprocess.CompletedProcess[str], dict[str, Any], dict[str, Any]]:
    result, input_value = _run_scenario_process(tmp_path, case_id)
    return result, json.loads(result.stdout), input_value


def test_pr8_matrix_is_bounded_and_reuses_the_pinned_baseline() -> None:
    matrix = _load_matrix()

    assert matrix["schema_version"] == 1
    assert matrix["baseline_id"] == "hudi-1.2.0-flink-1.20"
    assert _scenario_ids() == [
        "X01_APPEND_COW_DIRECT_SQL",
        "X02_MUTABLE_COW_DFS_CATALOG_HMS",
        "X03_MUTABLE_MOR_DIRECT_SQL_HMS",
        "X04_EXISTING_COMPATIBLE",
        "X05_EXISTING_CATALOG_MUTATION",
        "X06_MULTI_WRITER_OCC",
        "X07_MULTI_WRITER_NBCC",
        "X08_EXISTING_OUT_OF_SCOPE_MULTI_WRITER_EVIDENCE",
        "X09_COMBINED_SAFETY_GATES",
        "X10_MALFORMED_CROSS_PATH",
    ]
    for scenario in _scenarios():
        findings = scenario["expected_findings"]
        expected_status = (
            "CONFIG_VALIDATED"
            if not findings
            else max(
                (finding["status"] for finding in findings),
                key=STATUS_PRIORITY.__getitem__,
            )
        )
        assert scenario["expected_status"] == expected_status

        if scenario["validator"] == "design":
            expected_returncode = 0 if scenario["executable_eligible"] else 1
        else:
            assert scenario["validator"] == "existing_table"
            expected_returncode = (
                0 if scenario["expected_status"] == "CONFIG_VALIDATED" else 1
            )
        assert scenario["expected_returncode"] == expected_returncode


@pytest.mark.parametrize("case_id", _scenario_ids())
def test_pr8_cross_path_matrix_preserves_status_and_executable_boundaries(
    tmp_path: Path, case_id: str
) -> None:
    expected = _scenario(case_id)
    validator_name, _ = _scenario_input(case_id)
    result, assessment, _ = _run_scenario(tmp_path, case_id)

    assert validator_name == expected["validator"]
    assert result.returncode == expected["expected_returncode"]
    assert "Traceback" not in result.stderr
    assert assessment["status"] == expected["expected_status"]
    assert assessment["executable_eligible"] is expected["executable_eligible"]
    assert [
        {"code": finding["code"], "status": finding["status"]}
        for finding in assessment["findings"]
    ] == expected["expected_findings"]
    assert assessment["finding_codes"] == [
        finding["code"] for finding in expected["expected_findings"]
    ]
    if assessment["executable_eligible"]:
        assert assessment["artifacts"]["combined_sql"]
    else:
        assert "artifacts" not in assessment
        assert assessment.get("catalog_setup_sql", "") == ""


@pytest.mark.parametrize("case_id", _design_scenario_ids())
def test_pr8_design_emit_sql_preserves_executable_boundary(
    tmp_path: Path, case_id: str
) -> None:
    expected = _scenario(case_id)
    json_result, assessment, _ = _run_scenario(tmp_path, case_id)
    sql_result, _ = _run_scenario_process(tmp_path, case_id, emit_sql=True)

    assert json_result.returncode == expected["expected_returncode"]
    assert sql_result.returncode == expected["expected_returncode"]
    if expected["executable_eligible"]:
        assert sql_result.stdout == f"{assessment['artifacts']['combined_sql']}\n"
        assert sql_result.stderr == ""
    else:
        assert sql_result.stdout == ""
        assert json.loads(sql_result.stderr) == assessment


def test_pr8_new_table_compositions_keep_existing_execution_semantics(
    tmp_path: Path,
) -> None:
    _, append, _ = _run_scenario(tmp_path, "X01_APPEND_COW_DIRECT_SQL")
    _, cow_catalog, cow_input = _run_scenario(
        tmp_path, "X02_MUTABLE_COW_DFS_CATALOG_HMS"
    )
    _, mor_catalog, mor_input = _run_scenario(
        tmp_path, "X03_MUTABLE_MOR_DIRECT_SQL_HMS"
    )

    assert append["catalog_profile"] == "direct_sql_none"
    assert append["artifacts"]["combined_sql"].count("INSERT INTO") == 1

    qualified_source = cow_input["source"]["table"]
    assert qualified_source.count(".") == 2
    assert (
        "FROM `default_catalog`.`default_database`.`orders_mutable_source`;"
        in cow_catalog["artifacts"]["insert_sql"]
    )
    assert cow_catalog["catalog_profile"] == "hudi_dfs_catalog_hive_metastore"

    assert mor_input["table_services"]["compaction"]["owner"] == "flink_writer"
    assert mor_catalog["catalog_profile"] == "direct_sql_hive_metastore"
    assert mor_catalog["effective_catalog_configuration"]["catalog_freshness"] == (
        "post_commit_eventual"
    )
    assert mor_input["runtime"]["target_commit_freshness_ms"] == 120000
    assert "'compaction.async.enabled' = 'true'" in mor_catalog["artifacts"][
        "table_ddl"
    ]


def test_pr8_existing_paths_never_render_mutating_sql(tmp_path: Path) -> None:
    _, compatible, _ = _run_scenario(tmp_path, "X04_EXISTING_COMPATIBLE")
    _, catalog_mutation, _ = _run_scenario(
        tmp_path, "X05_EXISTING_CATALOG_MUTATION"
    )

    assert compatible["reconciliation_result"] == "COMPATIBLE"
    assert compatible["change_disposition"] == "NO_CHANGE"
    assert compatible["executable_eligible"] is False
    assert "artifacts" not in compatible

    assert catalog_mutation["reconciliation_result"] == "UNVERIFIED"
    assert catalog_mutation["change_disposition"] == "UNVERIFIED"
    assert catalog_mutation["catalog_setup_sql"] == ""
    assert "artifacts" not in catalog_mutation


def test_pr8_multi_writer_paths_remain_assessment_only(tmp_path: Path) -> None:
    _, occ, occ_input = _run_scenario(tmp_path, "X06_MULTI_WRITER_OCC")
    _, nbcc, nbcc_input = _run_scenario(tmp_path, "X07_MULTI_WRITER_NBCC")

    assert occ["writer_contract_result"] == "COMPLETE"
    assert occ["multi_writer_capability"] == "UNVERIFIED"
    assert occ["executable_eligible"] is False

    assert nbcc["writer_contract_result"] == "COMPLETE"
    assert nbcc["multi_writer_capability"] == "UNSUPPORTED"
    assert nbcc_input["write"]["index"]["type"] == "FLINK_STATE"
    assert nbcc_input["writer_contract"]["observed_concurrency"]["mode"] == (
        "NON_BLOCKING_CONCURRENCY_CONTROL"
    )
    assert nbcc["executable_eligible"] is False


def test_pr8_out_of_scope_evidence_retains_provenance_without_authority(
    tmp_path: Path,
) -> None:
    _, assessment, _ = _run_scenario(
        tmp_path, "X08_EXISTING_OUT_OF_SCOPE_MULTI_WRITER_EVIDENCE"
    )

    unrelated = next(
        item
        for item in assessment["evidence_summary"]
        if item["evidence_id"] == "unrelated-writers-1"
    )
    assert unrelated["in_scope"] is False
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert assessment["change_disposition"] != "MIGRATION_REQUIRED"
    assert assessment["writer_contract_result"] == "NOT_APPLICABLE"
    assert assessment["multi_writer_capability"] == "NOT_APPLICABLE"
    assert not any(
        "unrelated-writers-1" in comparison["source_ids"]
        for comparison in assessment["comparisons"]
    )


def test_pr8_combined_gates_preserve_independent_findings(tmp_path: Path) -> None:
    _, assessment, _ = _run_scenario(tmp_path, "X09_COMBINED_SAFETY_GATES")

    assert assessment["status"] == "BLOCKED"
    assert assessment["writer_contract_result"] == "COMPLETE"
    assert assessment["multi_writer_capability"] == "UNSUPPORTED"
    assert {
        "INCOMPLETE",
        "REVIEW_REQUIRED",
        "BLOCKED",
    } == {finding["status"] for finding in assessment["findings"]}
    assert assessment["status"] == max(
        (finding["status"] for finding in assessment["findings"]),
        key=STATUS_PRIORITY.__getitem__,
    )
    assert "artifacts" not in assessment


@pytest.mark.parametrize("case_id", _scenario_ids())
def test_pr8_cross_path_outputs_are_deterministic(
    tmp_path: Path, case_id: str
) -> None:
    first_result, first, _ = _run_scenario(tmp_path, case_id)
    second_result, second, _ = _run_scenario(tmp_path, case_id)

    assert first_result.returncode == second_result.returncode
    assert first == second


def test_pr8_malformed_cross_path_is_structured_and_redacted(tmp_path: Path) -> None:
    result, assessment, _ = _run_scenario(tmp_path, "X10_MALFORMED_CROSS_PATH")

    output = result.stdout + result.stderr
    assert result.returncode == 1
    assert "Traceback" not in output
    assert "pr8-sensitive-value-must-not-leak" not in output
    assert assessment["status"] == "BLOCKED"
    assert assessment["executable_eligible"] is False
    assert assessment["reconciliation_result"] == "UNVERIFIED"
    assert assessment["change_disposition"] == "UNVERIFIED"
    assert assessment["writer_contract_result"] == "INCOMPLETE"
    assert assessment["multi_writer_capability"] == "UNVERIFIED"
    assert "artifacts" not in assessment
