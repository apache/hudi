#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Validate the immutable capability input for the Hudi Architect Flink path.

Normal validation reads only the checked-in manifest.  It never discovers
options from the enclosing checkout.  ``--verify-source`` is an explicit
maintainer check that compares manifest hashes with files at the pinned Git
revision; it still does not read those files from the current working tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

SKILL_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = (
    SKILL_DIR / "references" / "flink-1.20-hudi-1.2.0-capabilities.toml"
)

EXPECTED_SCHEMA_VERSION = 2
EXPECTED_BASELINE_ID = "hudi-1.2.0-flink-1.20"
EXPECTED_HUDI_VERSION = "1.2.0"
EXPECTED_HUDI_SOURCE_REVISION = "f05c83f2b97732de7a558ff9b26959e1139c05f5"
EXPECTED_FLINK_LINE = "1.20"
EXPECTED_FLINK_FIXTURE_VERSION = "1.20.1"
EXPECTED_REQUIRED_SETTINGS = [
    {
        "key": "table.type",
        "value": "COPY_ON_WRITE",
        "applies_to": "pr2-append-only-cow",
    },
    {
        "key": "write.insert.cluster",
        "value": False,
        "applies_to": "pr2-append-only-cow",
    },
    {
        "key": "write.operation",
        "value": "insert",
        "applies_to": "pr2-append-only-cow",
    },
]
EXPECTED_PHYSICAL_TYPES = {
    "simple": [
        "BIGINT",
        "BOOLEAN",
        "BYTES",
        "DATE",
        "DOUBLE",
        "FLOAT",
        "INT",
        "INTEGER",
        "SMALLINT",
        "STRING",
        "TINYINT",
    ],
    "parameterized": [
        "BINARY",
        "CHAR",
        "DECIMAL",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMP_LTZ",
        "VARBINARY",
        "VARCHAR",
    ],
}
EXPECTED_PHYSICAL_SCHEMA_CONSTRAINTS = {
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
EXPECTED_RUNTIME_CONSTRAINTS = {
    "checkpoint_interval_min_ms": 1000,
    "flink_checkpoint_interval_min_ms": 10,
}
EXPECTED_ACCEPTANCE_EVIDENCE = {
    "FLINK_APPEND_MODE_CLUSTERING_ENABLED": (
        "test_pr2_rejects_insert_clustering_override",
        "testPinnedAppendModeRequiresInsertClusteringDisabled",
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
}
SHA1_PATTERN = re.compile(r"[0-9a-f]{40}")
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")


def load_manifest(path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    """Load the checked-in TOML capability manifest."""

    with path.open("rb") as manifest_file:
        return tomllib.load(manifest_file)


def validate_manifest(manifest: dict[str, Any]) -> list[str]:
    """Return deterministic manifest contract violations."""

    errors: list[str] = []

    expected_scalars = {
        "schema_version": EXPECTED_SCHEMA_VERSION,
        "baseline_id": EXPECTED_BASELINE_ID,
        "validation_surface": "initial-flink-sql-sink",
    }
    for key, expected in expected_scalars.items():
        if manifest.get(key) != expected:
            errors.append(f"{key} must be {expected!r}")

    hudi = manifest.get("hudi")
    if not isinstance(hudi, dict):
        errors.append("hudi must be a table")
        hudi = {}
    if hudi.get("version") != EXPECTED_HUDI_VERSION:
        errors.append(f"hudi.version must be {EXPECTED_HUDI_VERSION!r}")
    if hudi.get("release_ref") != "release-1.2.0":
        errors.append("hudi.release_ref must be 'release-1.2.0'")
    revision = hudi.get("source_revision")
    if revision != EXPECTED_HUDI_SOURCE_REVISION:
        errors.append(
            "hudi.source_revision must be the immutable Hudi 1.2.0 release commit "
            f"{EXPECTED_HUDI_SOURCE_REVISION}"
        )
    elif not SHA1_PATTERN.fullmatch(revision):
        errors.append("hudi.source_revision must be a full 40-character Git SHA")

    flink = manifest.get("flink")
    if not isinstance(flink, dict):
        errors.append("flink must be a table")
        flink = {}
    if flink.get("compatibility_line") != EXPECTED_FLINK_LINE:
        errors.append(f"flink.compatibility_line must be {EXPECTED_FLINK_LINE!r}")
    if flink.get("fixture_version") != EXPECTED_FLINK_FIXTURE_VERSION:
        errors.append(f"flink.fixture_version must be {EXPECTED_FLINK_FIXTURE_VERSION!r}")

    anchors = manifest.get("source_anchors")
    anchor_ids: set[str] = set()
    if not isinstance(anchors, list) or not anchors:
        errors.append("source_anchors must be a non-empty array")
        anchors = []
    for index, anchor in enumerate(anchors):
        if not isinstance(anchor, dict):
            errors.append(f"source_anchors[{index}] must be a table")
            continue
        anchor_id = anchor.get("id")
        if not isinstance(anchor_id, str) or not anchor_id:
            errors.append(f"source_anchors[{index}].id must be a non-empty string")
        elif anchor_id in anchor_ids:
            errors.append(f"duplicate source anchor id: {anchor_id}")
        else:
            anchor_ids.add(anchor_id)
        path = anchor.get("path")
        valid_path = (
            isinstance(path, str)
            and bool(path)
            and not path.startswith("/")
            and ".." not in Path(path).parts
        )
        if not valid_path:
            errors.append(f"source_anchors[{index}].path must be a repository-relative path")
        digest = anchor.get("sha256")
        if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
            errors.append(f"source_anchors[{index}].sha256 must be a lowercase SHA-256")
        if not isinstance(anchor.get("purpose"), str) or not anchor["purpose"]:
            errors.append(f"source_anchors[{index}].purpose must be a non-empty string")

    options = manifest.get("verified_options")
    option_keys: list[str] = []
    if not isinstance(options, list) or not options:
        errors.append("verified_options must be a non-empty array")
        options = []
    for index, option in enumerate(options):
        if not isinstance(option, dict):
            errors.append(f"verified_options[{index}] must be a table")
            continue
        option_key = option.get("key")
        if not isinstance(option_key, str) or not option_key:
            errors.append(f"verified_options[{index}].key must be a non-empty string")
        else:
            option_keys.append(option_key)
        if option.get("value_type") not in {"boolean", "double", "integer", "long", "string"}:
            errors.append(f"verified_options[{index}].value_type is unsupported")
        if not isinstance(option.get("source_symbol"), str) or not option["source_symbol"]:
            errors.append(f"verified_options[{index}].source_symbol must be non-empty")
        referenced_anchors = option.get("source_anchors")
        if not isinstance(referenced_anchors, list) or not referenced_anchors:
            errors.append(f"verified_options[{index}].source_anchors must be non-empty")
        elif not all(isinstance(anchor_id, str) for anchor_id in referenced_anchors):
            errors.append(f"verified_options[{index}].source_anchors must contain strings")
        else:
            unknown = sorted(set(referenced_anchors) - anchor_ids)
            if unknown:
                errors.append(
                    f"verified_options[{index}] references unknown source anchors: {unknown}"
                )

    if option_keys != sorted(option_keys):
        errors.append("verified_options must be sorted by key")
    if len(option_keys) != len(set(option_keys)):
        errors.append("verified_options contains duplicate keys")

    settings = manifest.get("required_effective_settings")
    if not isinstance(settings, list) or not settings:
        errors.append("required_effective_settings must be a non-empty array")
        settings = []
    for index, setting in enumerate(settings):
        if not isinstance(setting, dict):
            errors.append(f"required_effective_settings[{index}] must be a table")
            continue
        if setting.get("key") not in option_keys:
            errors.append(
                f"required_effective_settings[{index}].key is not a verified option"
            )
        if "value" not in setting:
            errors.append(f"required_effective_settings[{index}].value is required")
        if setting.get("applies_to") != "pr2-append-only-cow":
            errors.append(
                f"required_effective_settings[{index}].applies_to must be "
                "'pr2-append-only-cow'"
            )
    if settings != EXPECTED_REQUIRED_SETTINGS:
        errors.append("required_effective_settings does not match the bounded PR2 path")

    fixture_artifacts = manifest.get("fixture_artifacts")
    if not isinstance(fixture_artifacts, dict):
        errors.append("fixture_artifacts must be a table")
        fixture_artifacts = {}
    if fixture_artifacts.get("hudi_bundle") != (
        "org.apache.hudi:hudi-flink1.20-bundle:1.2.0"
    ):
        errors.append("fixture_artifacts.hudi_bundle must pin the Hudi 1.2.0 Flink bundle")
    if fixture_artifacts.get("flink_version") != EXPECTED_FLINK_FIXTURE_VERSION:
        errors.append(
            f"fixture_artifacts.flink_version must be {EXPECTED_FLINK_FIXTURE_VERSION!r}"
        )
    if fixture_artifacts.get("java_version") != 11:
        errors.append("fixture_artifacts.java_version must be 11")

    executable_contract = manifest.get("executable_contract")
    expected_contract = {
        "contract_schema": 1,
        "table_lifecycle": "new",
        "writer_model": "single_writer",
        "external_catalog": False,
        "mutability": "append_only",
        "source_changelog_mode": "INSERT_ONLY",
        "execution_mode": "STREAMING",
        "require_checkpointing": True,
        "allow_record_key_modes": ["stable_key", "auto_key"],
        "allow_replay_behaviors": ["cannot_occur", "duplicates_acceptable"],
    }
    if executable_contract != expected_contract:
        errors.append("executable_contract does not match the bounded PR2 path")

    physical_types = manifest.get("physical_types")
    if not isinstance(physical_types, dict):
        errors.append("physical_types must be a table")
    elif physical_types != EXPECTED_PHYSICAL_TYPES:
        errors.append("physical_types does not match the bounded PR2 scalar surface")
    else:
        for type_group in ("simple", "parameterized"):
            values = physical_types.get(type_group)
            if (
                not isinstance(values, list)
                or not values
                or not all(isinstance(value, str) and value for value in values)
                or values != sorted(values)
                or len(values) != len(set(values))
            ):
                errors.append(f"physical_types.{type_group} must be sorted unique strings")

    physical_schema_constraints = manifest.get("physical_schema_constraints")
    if physical_schema_constraints != EXPECTED_PHYSICAL_SCHEMA_CONSTRAINTS:
        errors.append(
            "physical_schema_constraints does not match the pinned Hudi connector limits"
        )

    runtime_constraints = manifest.get("runtime_constraints")
    if runtime_constraints != EXPECTED_RUNTIME_CONSTRAINTS:
        errors.append("runtime_constraints does not match the bounded PR2 runtime limits")

    implemented_checks = manifest.get("implemented_acceptance_checks")
    implemented_ids: list[str] = []
    if not isinstance(implemented_checks, list) or not implemented_checks:
        errors.append("implemented_acceptance_checks must be a non-empty array")
        implemented_checks = []
    for index, check in enumerate(implemented_checks):
        if not isinstance(check, dict):
            errors.append(f"implemented_acceptance_checks[{index}] must be a table")
            continue
        check_id = check.get("id")
        if not isinstance(check_id, str) or not check_id.startswith("FLINK_"):
            errors.append(
                f"implemented_acceptance_checks[{index}].id must be a stable FLINK_ identifier"
            )
        else:
            implemented_ids.append(check_id)
        if not isinstance(check.get("requirement"), str) or not check["requirement"]:
            errors.append(
                f"implemented_acceptance_checks[{index}].requirement must be non-empty"
            )
        for evidence_key in ("python_test", "java_test"):
            if not isinstance(check.get(evidence_key), str) or not check[evidence_key]:
                errors.append(
                    f"implemented_acceptance_checks[{index}].{evidence_key} must be non-empty"
                )
    if len(implemented_ids) != len(set(implemented_ids)):
        errors.append("implemented_acceptance_checks contains duplicate ids")
    implemented_evidence = {
        check.get("id"): (check.get("python_test"), check.get("java_test"))
        for check in implemented_checks
        if isinstance(check, dict) and isinstance(check.get("id"), str)
    }
    if implemented_evidence != EXPECTED_ACCEPTANCE_EVIDENCE:
        errors.append(
            "implemented_acceptance_checks does not match the required PR2 evidence"
        )

    return errors


def supported_option_keys(manifest: dict[str, Any]) -> set[str]:
    """Return the allowlisted option keys for the initial Flink SQL sink path."""

    return {option["key"] for option in manifest["verified_options"]}


def validation_evidence(manifest: dict[str, Any]) -> dict[str, Any]:
    """Build the stable baseline evidence included in every Flink assessment."""

    return {
        "baseline_id": manifest["baseline_id"],
        "capability_manifest_schema": manifest["schema_version"],
        "design_contract_schema": manifest["executable_contract"]["contract_schema"],
        "flink_fixture_version": manifest["flink"]["fixture_version"],
        "hudi_source_revision": manifest["hudi"]["source_revision"],
    }


def _find_repo_root(start: Path) -> Path | None:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists() and (candidate / "hudi-common").is_dir():
            return candidate
    return None


def verify_pinned_sources(manifest: dict[str, Any]) -> list[str]:
    """Verify source hashes using Git objects at the pinned revision."""

    repo_root = _find_repo_root(SKILL_DIR)
    if repo_root is None:
        return ["could not locate the enclosing Hudi Git checkout"]

    revision = manifest["hudi"]["source_revision"]
    errors: list[str] = []
    for anchor in manifest["source_anchors"]:
        object_name = f"{revision}:{anchor['path']}"
        result = subprocess.run(
            ["git", "show", object_name],
            cwd=repo_root,
            capture_output=True,
            check=False,
        )
        if result.returncode != 0:
            errors.append(f"cannot read pinned source object {object_name}")
            continue
        digest = hashlib.sha256(result.stdout).hexdigest()
        if digest != anchor["sha256"]:
            errors.append(
                f"source hash mismatch for {anchor['path']}: "
                f"expected {anchor['sha256']}, got {digest}"
            )
    return errors


def _flink_version_matches(version: str) -> bool:
    return version in {"1.20", "1.20.x"} or re.fullmatch(r"1\.20\.\d+", version) is not None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--hudi-version")
    parser.add_argument("--flink-version")
    parser.add_argument("--check-option", action="append", default=[])
    parser.add_argument("--emit-evidence", action="store_true")
    parser.add_argument(
        "--verify-source",
        action="store_true",
        help="Compare manifest hashes with files at the pinned Git revision.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        manifest = load_manifest(args.manifest)
    except (OSError, tomllib.TOMLDecodeError) as error:
        print(f"Invalid Flink capability manifest: {error}", file=sys.stderr)
        return 1

    errors = validate_manifest(manifest)
    if args.hudi_version and args.hudi_version != EXPECTED_HUDI_VERSION:
        errors.append(
            f"Hudi {args.hudi_version} is outside baseline {EXPECTED_HUDI_VERSION}"
        )
    if args.flink_version and not _flink_version_matches(args.flink_version):
        errors.append(
            f"Flink {args.flink_version} is outside compatibility line {EXPECTED_FLINK_LINE}"
        )

    supported = supported_option_keys(manifest) if not errors else set()
    for option in args.check_option:
        if option not in supported:
            errors.append(
                f"Option {option!r} is not verified by baseline {EXPECTED_BASELINE_ID}"
            )

    if args.verify_source and not errors:
        errors.extend(verify_pinned_sources(manifest))

    if errors:
        for violation in errors:
            print(f"ERROR: {violation}", file=sys.stderr)
        return 1

    if args.emit_evidence:
        print(json.dumps(validation_evidence(manifest), sort_keys=True))
    else:
        print(f"Validated Flink capability baseline {EXPECTED_BASELINE_ID}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
