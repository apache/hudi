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

EXPECTED_SCHEMA_VERSION = 6
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
    {
        "key": "changelog.enabled",
        "value": False,
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "hoodie.write.record.merge.mode",
        "value": "EVENT_TIME_ORDERING",
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "index.bootstrap.enabled",
        "value": True,
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "index.global.enabled",
        "value": True,
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "index.state.ttl",
        "value": 0.0,
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "index.type",
        "value": "FLINK_STATE",
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "table.type",
        "value": "COPY_ON_WRITE",
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "write.operation",
        "value": "upsert",
        "applies_to": "pr3-mutable-cow",
    },
    {
        "key": "changelog.enabled",
        "value": False,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "clustering.async.enabled",
        "value": False,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "clustering.schedule.enabled",
        "value": False,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "compaction.async.enabled",
        "value": True,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "compaction.delta_commits",
        "value": 5,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "compaction.operation.execute.async.enabled",
        "value": True,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "compaction.schedule.enabled",
        "value": True,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "compaction.trigger.strategy",
        "value": "num_commits",
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "hoodie.write.record.merge.mode",
        "value": "EVENT_TIME_ORDERING",
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "index.bootstrap.enabled",
        "value": True,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "index.global.enabled",
        "value": True,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "index.state.ttl",
        "value": 0.0,
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "index.type",
        "value": "FLINK_STATE",
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "table.type",
        "value": "MERGE_ON_READ",
        "applies_to": "pr4-mutable-mor",
    },
    {
        "key": "write.operation",
        "value": "upsert",
        "applies_to": "pr4-mutable-mor",
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
EXPECTED_RUNTIME_CONSTRAINTS = {
    "checkpoint_interval_min_ms": 1000,
    "flink_checkpoint_interval_min_ms": 10,
}
EXPECTED_CATALOG_COMPOSITION_CONTRACT = {
    "contract_schema": 1,
    "supported_design_contract_schemas": [1, 2, 3],
    "new_table_executable": True,
    "existing_table_assessment_only": True,
    "catalog_freshness": "post_commit_eventual",
    "resolution_modes": ["direct_sql", "hudi_dfs_catalog", "hudi_hms_catalog"],
    "external_discovery_modes": ["none", "hive_metastore", "aws_glue"],
    "sync_owners": ["none", "flink_sink", "hudi_hms_catalog"],
    "data_lifecycle_owners": ["application", "catalog"],
    "deployment_checks": [
        "verify_catalog_dependencies",
        "verify_catalog_path_exists",
        "verify_catalog_path_permissions",
        "verify_hive_configuration_available",
        "verify_hms_connectivity",
        "verify_catalog_database_registration",
        "verify_table_registration_after_commit",
        "verify_partition_registration_after_commit",
        "verify_mor_catalog_table_names",
        "verify_external_engine_visibility",
    ],
    "finding_status": {
        "FLINK_CATALOG_COMPOSITION_CONFLICT": "BLOCKED",
        "FLINK_CATALOG_CONTRACT_INVALID": "INCOMPLETE",
        "FLINK_CATALOG_CONTRACT_REQUIRED": "INCOMPLETE",
        "FLINK_CATALOG_CREDENTIAL_REJECTED": "BLOCKED",
        "FLINK_CATALOG_LIFECYCLE_UNRESOLVED": "REVIEW_REQUIRED",
        "FLINK_CATALOG_MODE_UNSUPPORTED": "BLOCKED",
        "FLINK_CATALOG_PATH_CONFLICT": "BLOCKED",
        "FLINK_EXISTING_CATALOG_MUTATION_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_GLUE_CATALOG_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_MOR_CATALOG_NAMING_UNVERIFIED": "REVIEW_REQUIRED",
    },
}
EXPECTED_CATALOG_PROFILES = [
    {
        "id": "direct_sql_none",
        "resolution_mode": "direct_sql",
        "external_discovery": "none",
        "sync_owner": "none",
        "data_lifecycle_owner": "application",
    },
    {
        "id": "direct_sql_hive_metastore",
        "resolution_mode": "direct_sql",
        "external_discovery": "hive_metastore",
        "sync_owner": "flink_sink",
        "data_lifecycle_owner": "application",
    },
    {
        "id": "hudi_dfs_catalog_none",
        "resolution_mode": "hudi_dfs_catalog",
        "external_discovery": "none",
        "sync_owner": "none",
        "data_lifecycle_owner": "catalog",
    },
    {
        "id": "hudi_dfs_catalog_hive_metastore",
        "resolution_mode": "hudi_dfs_catalog",
        "external_discovery": "hive_metastore",
        "sync_owner": "flink_sink",
        "data_lifecycle_owner": "catalog",
    },
    {
        "id": "hudi_hms_catalog_hive_metastore",
        "resolution_mode": "hudi_hms_catalog",
        "external_discovery": "hive_metastore",
        "sync_owner": "hudi_hms_catalog",
        "data_lifecycle_owner": "application",
    },
]
EXPECTED_CATALOG_EFFECTIVE_DEFAULTS = {
    "hive_sync_ignore_exceptions": False,
    "hive_sync_skip_ro_suffix": False,
    "hive_sync_support_timestamp": True,
    "meta_sync_snapshot_with_table_name": True,
    "hive_sync_table_strategy": "ALL",
}
EXPECTED_CATALOG_OPTION_TYPES = [
    {
        "key": "catalog.path",
        "value_type": "string",
        "source_anchors": ["catalog_options", "dfs_catalog"],
        "source_symbol": "CATALOG_PATH",
    },
    {
        "key": "default-database",
        "value_type": "string",
        "source_anchors": ["catalog_options"],
        "source_symbol": "DEFAULT_DATABASE",
    },
    {
        "key": "hive.conf.dir",
        "value_type": "string",
        "source_anchors": ["catalog_options", "catalog_util"],
        "source_symbol": "HIVE_CONF_DIR",
    },
    {
        "key": "mode",
        "value_type": "string",
        "source_anchors": ["catalog_options", "catalog_factory"],
        "source_symbol": "MODE",
    },
    {
        "key": "table.external",
        "value_type": "boolean",
        "source_anchors": ["catalog_options", "hms_catalog"],
        "source_symbol": "TABLE_EXTERNAL",
    },
]
EXPECTED_EXISTING_TABLE_RECONCILIATION_CONTRACT = {
    "reconciliation_schema": 1,
    "supported_design_contract_schemas": [1, 2, 3],
    "assessment_only": True,
    "executable_sql": False,
    "allowed_evidence_kinds": [
        "flink_table_ddl",
        "flink_writer_config",
        "hoodie_properties",
        "hudi_table_identity",
        "hudi_table_schema",
        "timeline_summary",
        "writer_inventory",
    ],
    "required_evidence_kinds": [
        "hoodie_properties",
        "hudi_table_identity",
        "hudi_table_schema",
        "timeline_summary",
        "writer_inventory",
    ],
    "allowed_writer_transitions": [
        "cold_start_new_job",
        "cross_engine_takeover",
        "flink_writer_takeover",
        "same_job_restore",
        "unknown",
    ],
    "supported_table_versions": [9],
    "supported_initial_table_versions": [9],
    "supported_timeline_layout_versions": [2],
    "supported_base_file_formats": ["PARQUET"],
    "required_hoodie_property_facts": [
        "base_file_format",
        "key_generator_type",
        "initial_table_version",
        "ordering_fields",
        "partition_fields",
        "record_key_fields",
        "table_name",
        "table_type",
        "table_version",
        "timeline_layout_version",
    ],
    "conditional_hoodie_property_facts": [
        "hive_style_partitioning",
        "record_merge_mode",
        "url_encode_partitioning",
    ],
    "required_identity_facts": ["table_path"],
    "required_schema_facts": ["columns"],
    "required_timeline_facts": ["pending_instants"],
    "required_writer_inventory_facts": ["concurrent_writers"],
    "conditional_writer_inventory_facts": [
        "previous_writer_engine",
        "previous_writer_stopped",
    ],
    "comparison_fields": [
        "schema.columns",
        "table_config.base_file_format",
        "table_config.hive_style_partitioning",
        "table_config.initial_table_version",
        "table_config.key_generator_type",
        "table_config.ordering_fields",
        "table_config.partition_fields",
        "table_config.record_key_fields",
        "table_config.record_merge_mode",
        "table_config.table_name",
        "table_config.table_type",
        "table_config.table_version",
        "table_config.timeline_layout_version",
        "table_config.url_encode_partitioning",
        "evidence_scope.table_path",
        "table_identity.table_path",
        "timeline.pending_instants",
        "writer_config.checkpoint_recovery_available",
        "writer_config.compaction_owner",
        "writer_config.design_contract_schema",
        "writer_config.index_bootstrap_enabled",
        "writer_config.index_global",
        "writer_config.index_state_ttl_days",
        "writer_config.index_type",
        "writer_config.operation",
        "writer_config.timestamp_key_generator",
        "writer_inventory.concurrent_writers",
        "writer_inventory.previous_writer_engine",
        "writer_inventory.previous_writer_stopped",
    ],
    "comparison_results": ["CONFLICT", "MATCH", "NOT_APPLICABLE", "UNVERIFIED"],
    "reconciliation_results": ["COMPATIBLE", "CONFLICT", "UNVERIFIED"],
    "change_dispositions": [
        "DEPLOYMENT_CONFIGURATION_CHANGE",
        "MIGRATION_REQUIRED",
        "NO_CHANGE",
        "UNVERIFIED",
    ],
    "finding_status": {
        "FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_EVIDENCE_CONFLICT": "REVIEW_REQUIRED",
        "FLINK_EXISTING_EVIDENCE_INVALID": "INCOMPLETE",
        "FLINK_EXISTING_EVIDENCE_REQUIRED": "INCOMPLETE",
        "FLINK_EXISTING_SCHEMA_CONFLICT": "BLOCKED",
        "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT": "BLOCKED",
        "FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_TABLE_VERSION_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_TIMELINE_UNRESOLVED": "REVIEW_REQUIRED",
        "FLINK_EXISTING_WRITER_CONFIG_CONFLICT": "REVIEW_REQUIRED",
        "FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED": "REVIEW_REQUIRED",
    },
}
EXPECTED_ACCEPTANCE_EVIDENCE = {
    "FLINK_APPEND_MODE_CLUSTERING_ENABLED": (
        "test_pr2_rejects_insert_clustering_override",
        "testPinnedAppendModeRequiresInsertClusteringDisabled",
    ),
    "FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED": (
        "test_pr2_rejects_binary_routing_fields",
        "testPinnedBinaryValuesUseObjectIdentityForRouting",
    ),
    "FLINK_CATALOG_MODE_UNSUPPORTED": (
        "test_pr6_rejects_unsupported_catalog_compositions",
        "testPinnedCatalogFactoryRejectsUnknownMode",
    ),
    "FLINK_CATALOG_PATH_CONFLICT": (
        "test_pr6_rejects_dfs_catalog_path_mismatch",
        "testPinnedDfsCatalogPathDerivation",
    ),
    "FLINK_CATALOG_LIFECYCLE_UNRESOLVED": (
        "test_pr6_hms_catalog_requires_external_table",
        "testPinnedHmsCatalogExternalFlag",
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
    "FLINK_MOR_CATALOG_NAMING_UNVERIFIED": (
        "test_pr6_rejects_mor_catalog_naming_drift",
        "testPinnedMorHiveSyncNamingDefaults",
    ),
    "FLINK_PR4_WRITE_PATH_UNSUPPORTED": (
        "test_pr4_valid_mutable_mor_design_renders_exact_executable_sql",
        "testMutableMorSinkAndCompactionPlan",
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
        "validation_surface": (
            "flink-sql-sink-existing-table-reconciliation-and-catalog-composition"
        ),
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
        if setting.get("applies_to") not in {
            "pr2-append-only-cow",
            "pr3-mutable-cow",
            "pr4-mutable-mor",
        }:
            errors.append(
                f"required_effective_settings[{index}].applies_to is unsupported"
            )
    if settings != EXPECTED_REQUIRED_SETTINGS:
        errors.append("required_effective_settings does not match the bounded paths")

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

    mutable_executable_contract = manifest.get("mutable_executable_contract")
    expected_mutable_contract = {
        "contract_schema": 2,
        "table_lifecycle": "new",
        "writer_model": "single_writer",
        "external_catalog": False,
        "mutability": "mutable",
        "source_changelog_mode": "UPSERT",
        "execution_mode": "STREAMING",
        "require_checkpointing": True,
        "record_key_mode": "stable_key",
        "allow_replay_behaviors": [
            "cannot_occur",
            "duplicates_acceptable",
            "must_collapse",
        ],
        "ordering_mode": "event_time",
        "ordering_field_types": ["BIGINT", "TIMESTAMP"],
        "reserved_ordering_field_values": ["no_precombine"],
        "allow_emits_deletes": [False, True],
        "delete_payload_when_deletes": "FULL_ROW",
        "delete_payload_without_deletes": "NOT_APPLICABLE",
        "partition_fields_mutable": False,
    }
    if mutable_executable_contract != expected_mutable_contract:
        errors.append(
            "mutable_executable_contract does not match the bounded PR3 path"
        )

    mutable_mor_executable_contract = manifest.get("mutable_mor_executable_contract")
    expected_mutable_mor_contract = {
        **expected_mutable_contract,
        "contract_schema": 3,
        "table_type": "MERGE_ON_READ",
        "operation": "upsert",
        "compaction_owner": "flink_writer",
        "compaction_mode": "async",
        "compaction_trigger_strategy": "num_commits",
        "compaction_delta_commits": 5,
        "query_modes": ["snapshot", "read_optimized"],
        "read_optimized_freshness": "after_compaction",
        "clustering_enabled": False,
    }
    if mutable_mor_executable_contract != expected_mutable_mor_contract:
        errors.append(
            "mutable_mor_executable_contract does not match the bounded PR4 path"
        )

    existing_reconciliation = manifest.get("existing_table_reconciliation_contract")
    if existing_reconciliation != EXPECTED_EXISTING_TABLE_RECONCILIATION_CONTRACT:
        errors.append(
            "existing_table_reconciliation_contract does not match the bounded PR5 path"
        )

    catalog_contract = manifest.get("catalog_composition_contract")
    if catalog_contract != EXPECTED_CATALOG_COMPOSITION_CONTRACT:
        errors.append(
            "catalog_composition_contract does not match the bounded PR6 path"
        )

    catalog_profiles = manifest.get("catalog_profiles")
    if catalog_profiles != EXPECTED_CATALOG_PROFILES:
        errors.append("catalog_profiles must contain exactly the five PR6 profiles")

    catalog_defaults = manifest.get("catalog_effective_defaults")
    if catalog_defaults != EXPECTED_CATALOG_EFFECTIVE_DEFAULTS:
        errors.append(
            "catalog_effective_defaults do not match the pinned Hudi 1.2.0 behavior"
        )

    catalog_options = manifest.get("catalog_option_types")
    if catalog_options != EXPECTED_CATALOG_OPTION_TYPES:
        errors.append("catalog_option_types do not match the pinned catalog surface")
    else:
        for index, option in enumerate(catalog_options):
            unknown = sorted(set(option["source_anchors"]) - anchor_ids)
            if unknown:
                errors.append(
                    f"catalog_option_types[{index}] references unknown source anchors: {unknown}"
                )

    physical_types = manifest.get("physical_types")
    if not isinstance(physical_types, dict):
        errors.append("physical_types must be a table")
    elif physical_types != EXPECTED_PHYSICAL_TYPES:
        errors.append("physical_types does not match the bounded scalar surface")
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
            "implemented_acceptance_checks does not match the required evidence"
        )

    return errors


def supported_option_keys(manifest: dict[str, Any]) -> set[str]:
    """Return the allowlisted option keys for the initial Flink SQL sink path."""

    return {option["key"] for option in manifest["verified_options"]}


def validation_evidence(
    manifest: dict[str, Any],
    contract_schema: int | None = None,
    catalog_profile: str = "direct_sql_none",
) -> dict[str, Any]:
    """Build the stable baseline evidence included in every Flink assessment."""

    return {
        "baseline_id": manifest["baseline_id"],
        "capability_manifest_schema": manifest["schema_version"],
        "catalog_contract_schema": manifest["catalog_composition_contract"][
            "contract_schema"
        ],
        "catalog_profile": catalog_profile,
        "design_contract_schema": (
            manifest["executable_contract"]["contract_schema"]
            if contract_schema is None
            else contract_schema
        ),
        "supported_design_contract_schemas": [
            manifest["executable_contract"]["contract_schema"],
            manifest["mutable_executable_contract"]["contract_schema"],
            manifest["mutable_mor_executable_contract"]["contract_schema"],
        ],
        "flink_fixture_version": manifest["flink"]["fixture_version"],
        "hudi_source_revision": manifest["hudi"]["source_revision"],
        "supported_catalog_profiles": [
            profile["id"] for profile in manifest["catalog_profiles"]
        ],
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
