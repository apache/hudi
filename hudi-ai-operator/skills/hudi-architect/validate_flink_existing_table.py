#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
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

"""Reconcile a bounded Flink design with structured existing-table evidence.

This is deliberately an assessment-only path.  It never reads a live table,
executes supplied evidence, mutates table state, or renders SQL.  The caller
must supply an explicit PR2, PR3, or PR4 desired contract plus normalized,
provenance-bearing observations.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, TypeGuard

from flink_writer_contract import (
    CONCURRENCY_MODES,
    LOCK_CONFIGURATION_CONSISTENCY,
    assess_writer_contract,
)
from validate_flink_capabilities import (
    DEFAULT_MANIFEST,
    load_manifest,
    validate_manifest,
    validation_evidence,
)
from validate_flink_design import Assessment, _path_validation_error, assess_design

RECONCILIATION_SCHEMA_VERSION = 1
SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
RFC3339_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})"
)
FIELD_NAME_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
ALLOWED_TRANSITIONS = {
    "same_job_restore",
    "cold_start_new_job",
    "flink_writer_takeover",
    "cross_engine_takeover",
    "unknown",
}
ALLOWED_EVIDENCE_FACTS = {
    "hoodie_properties": {
        "base_file_format",
        "hive_style_partitioning",
        "initial_table_version",
        "key_generator_type",
        "ordering_fields",
        "partition_fields",
        "record_key_fields",
        "record_merge_mode",
        "table_name",
        "table_type",
        "table_version",
        "timeline_layout_version",
        "url_encode_partitioning",
    },
    "hudi_table_identity": {"table_path"},
    "hudi_table_schema": {"columns"},
    "flink_table_ddl": {
        "columns",
        "ordering_fields",
        "partition_fields",
        "record_key_fields",
        "table_name",
        "table_path",
        "table_type",
    },
    "flink_writer_config": {
        "checkpoint_recovery_available",
        "compaction_owner",
        "design_contract_schema",
        "index_bootstrap_enabled",
        "index_global",
        "index_state_ttl_days",
        "index_type",
        "lock_configuration_consistency",
        "lock_provider_class",
        "operation",
        "timestamp_key_generator",
        "write_concurrency_mode",
    },
    "timeline_summary": {"pending_instants"},
    "writer_inventory": {
        "active_writers",
        "concurrent_writers",
        "previous_writer_engine",
        "previous_writer_stopped",
        "schema_evolution_owner_id",
    },
}
REQUIRED_EVIDENCE_KINDS = {
    "hoodie_properties",
    "hudi_table_identity",
    "hudi_table_schema",
    "timeline_summary",
    "writer_inventory",
}
REQUIRED_TABLE_FACTS = {
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
}
LIST_FACTS = {
    "active_writers",
    "ordering_fields",
    "partition_fields",
    "pending_instants",
    "record_key_fields",
}
UPPERCASE_FACTS = {
    "base_file_format",
    "key_generator_type",
    "compaction_owner",
    "index_type",
    "operation",
    "previous_writer_engine",
    "record_merge_mode",
    "table_type",
    "write_concurrency_mode",
    "lock_configuration_consistency",
}
KNOWN_KEY_GENERATORS = {
    "AUTO_RECORD",
    "AUTO_RECORD_AVRO",
    "COMPLEX",
    "COMPLEX_AVRO",
    "GLOBAL_DELETE",
    "GLOBAL_DELETE_AVRO",
    "NON_PARTITION",
    "NON_PARTITION_AVRO",
    "SIMPLE",
    "SIMPLE_AVRO",
    "TIMESTAMP",
    "TIMESTAMP_AVRO",
}
SENSITIVE_KEY = re.compile(
    r"(?:access[_-]?key|api[_-]?key|credential|password|secret|signature|token)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Observation:
    """One normalized value and the evidence items that establish it."""

    value: Any
    source_ids: tuple[str, ...]


def _check_keys(
    value: dict[str, Any], allowed: set[str], field: str, assessment: Assessment
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} contains unsupported fields: {unknown}",
        )


def _non_empty_string(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _string_list(value: Any, field: str, assessment: Assessment) -> list[str] | None:
    if not isinstance(value, list) or not all(_non_empty_string(item) for item in value):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} must be an array of non-empty strings",
        )
        return None
    if len(value) != len(set(value)):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} must not contain duplicates",
        )
        return None
    return value


def _valid_observed_at(value: Any) -> bool:
    if not isinstance(value, str) or RFC3339_PATTERN.fullmatch(value) is None:
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _normalize_type(value: Any) -> str | None:
    if not _non_empty_string(value):
        return None
    normalized = " ".join(value.upper().split())
    normalized = re.sub(r"\s*,\s*", ",", normalized)
    normalized = re.sub(r"\s*\(\s*", "(", normalized)
    normalized = re.sub(r"\s*\)", ")", normalized)
    return "INT" if normalized == "INTEGER" else normalized


def _normalize_columns(
    value: Any, field: str, assessment: Assessment
) -> list[dict[str, Any]] | None:
    if not isinstance(value, list) or not value:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} must be a non-empty array",
        )
        return None
    columns: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw_column in enumerate(value):
        item_field = f"{field}[{index}]"
        if not isinstance(raw_column, dict):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID", f"{item_field} must be an object"
            )
            continue
        _check_keys(raw_column, {"name", "nullable", "type"}, item_field, assessment)
        name = raw_column.get("name")
        normalized_type = _normalize_type(raw_column.get("type"))
        nullable = raw_column.get("nullable")
        if (
            not _non_empty_string(name)
            or FIELD_NAME_PATTERN.fullmatch(name) is None
            or normalized_type is None
            or not isinstance(nullable, bool)
        ):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{item_field} needs a valid name, type, and explicit nullable flag",
            )
            continue
        if name in names:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field} contains duplicate field {name!r}",
            )
            continue
        names.add(name)
        columns.append({"name": name, "type": normalized_type, "nullable": nullable})
    return columns if len(columns) == len(value) else None


def _normalize_fact(
    kind: str, name: str, value: Any, field: str, assessment: Assessment
) -> Any | None:
    if name in LIST_FACTS:
        values = _string_list(value, field, assessment)
        if values is None:
            return None
        if name == "partition_fields":
            # HoodieTableConfig stores custom partition-key types after a colon.
            return [item.split(":", 1)[0] for item in values]
        if name == "active_writers":
            return sorted(values)
        return values
    if name == "columns":
        return _normalize_columns(value, field, assessment)
    if name == "timestamp_key_generator":
        allowed = {"timestamp_type", "input_time_unit", "output_date_format", "output_timezone"}
        if (
            not isinstance(value, dict)
            or not set(value) <= allowed
            or not all(_non_empty_string(item) for item in value.values())
        ):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field} must contain only normalized timestamp key-generator settings",
            )
            return None
        return value
    if name in {
        "design_contract_schema",
        "index_state_ttl_days",
        "initial_table_version",
        "table_version",
        "timeline_layout_version",
    }:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field} must be a non-negative integer",
            )
            return None
        return value
    if name in {
        "checkpoint_recovery_available",
        "concurrent_writers",
        "previous_writer_stopped",
        "hive_style_partitioning",
        "index_bootstrap_enabled",
        "index_global",
        "url_encode_partitioning",
    }:
        if not isinstance(value, bool):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID", f"{field} must be a boolean"
            )
            return None
        return value
    if not _non_empty_string(value):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID", f"{field} must be a non-empty string"
        )
        return None
    normalized = value.strip()
    if name == "table_path" and _path_validation_error(normalized) is not None:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} must be a credential-free path",
        )
        return None
    normalized = normalized.upper() if name in UPPERCASE_FACTS else normalized
    enum_values = {
        "write_concurrency_mode": CONCURRENCY_MODES,
        "lock_configuration_consistency": LOCK_CONFIGURATION_CONSISTENCY,
    }
    allowed_enum_values = enum_values.get(name)
    if allowed_enum_values is not None and normalized not in allowed_enum_values:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID",
            f"{field} must be one of {sorted(allowed_enum_values)}",
        )
        return None
    return normalized


def _parse_evidence(
    raw_evidence: Any,
    target_table_path: str | None,
    assessment: Assessment,
) -> tuple[dict[str, dict[str, Observation]], list[dict[str, Any]]]:
    observations: dict[str, dict[str, Observation]] = {}
    summaries: list[dict[str, Any]] = []
    if not isinstance(raw_evidence, list):
        assessment.add("FLINK_EXISTING_EVIDENCE_INVALID", "evidence must be an array")
        return observations, summaries

    evidence_ids: set[str] = set()
    collected: dict[tuple[str, str], list[tuple[Any, str]]] = {}
    in_scope_ids: list[str] = []
    conflicting_facts: set[str] = set()
    for index, raw_item in enumerate(raw_evidence):
        field = f"evidence[{index}]"
        if not isinstance(raw_item, dict):
            assessment.add("FLINK_EXISTING_EVIDENCE_INVALID", f"{field} must be an object")
            continue
        _check_keys(
            raw_item,
            {"evidence_id", "facts", "kind", "observed_at", "sha256", "table_path"},
            field,
            assessment,
        )
        evidence_id = raw_item.get("evidence_id")
        kind = raw_item.get("kind")
        observed_at = raw_item.get("observed_at")
        digest = raw_item.get("sha256")
        table_path = raw_item.get("table_path")
        facts = raw_item.get("facts")
        valid_header = True
        if not _non_empty_string(evidence_id) or evidence_id in evidence_ids:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.evidence_id must be a unique non-empty string",
            )
            valid_header = False
        else:
            evidence_ids.add(evidence_id)
        supported_kind = isinstance(kind, str) and kind in ALLOWED_EVIDENCE_FACTS
        if not supported_kind:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.kind is not supported",
            )
            valid_header = False
        if not _valid_observed_at(observed_at):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.observed_at must be RFC 3339 with a timezone",
            )
            valid_header = False
        if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.sha256 must be a lowercase SHA-256",
            )
            valid_header = False
        if not _non_empty_string(table_path) or _path_validation_error(table_path) is not None:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.table_path must be a credential-free path",
            )
            valid_header = False
        if not isinstance(facts, dict):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID", f"{field}.facts must be an object"
            )
            continue
        if any(SENSITIVE_KEY.search(str(key)) for key in facts):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_INVALID",
                f"{field}.facts contains a credential-shaped field",
            )
            continue
        if isinstance(kind, str) and supported_kind:
            _check_keys(facts, ALLOWED_EVIDENCE_FACTS[kind], f"{field}.facts", assessment)
        if not valid_header or not isinstance(kind, str) or not isinstance(evidence_id, str):
            continue
        assert isinstance(table_path, str)
        in_scope = target_table_path is not None and table_path == target_table_path
        if target_table_path is not None and not in_scope:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_CONFLICT",
                f"Evidence {evidence_id} is scoped to {table_path}, not {target_table_path}",
            )
        if in_scope:
            in_scope_ids.append(evidence_id)
        normalized_facts: list[str] = []
        for name in sorted(facts):
            if name not in ALLOWED_EVIDENCE_FACTS[kind]:
                continue
            normalized = _normalize_fact(
                kind, name, facts[name], f"{field}.facts.{name}", assessment
            )
            if normalized is None:
                continue
            if in_scope:
                collected.setdefault((kind, name), []).append((normalized, evidence_id))
            normalized_facts.append(name)
        summaries.append(
            {
                "evidence_id": evidence_id,
                "fact_names": normalized_facts,
                "kind": kind,
                "in_scope": in_scope,
                "observed_at": observed_at,
                "sha256": digest,
                "table_path": table_path,
            }
        )

    for (kind, name), candidates in sorted(collected.items()):
        first_value = candidates[0][0]
        source_ids = tuple(candidate[1] for candidate in candidates)
        if any(candidate[0] != first_value for candidate in candidates[1:]):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_CONFLICT",
                f"Evidence sources {', '.join(source_ids)} disagree about {kind}.{name}",
            )
            conflicting_facts.add(f"{kind}.{name}")
            continue
        observations.setdefault(kind, {})[name] = Observation(first_value, source_ids)
    if target_table_path is not None and in_scope_ids:
        observations.setdefault("evidence_scope", {})["table_path"] = Observation(
            target_table_path, tuple(in_scope_ids)
        )
    observations["evidence_conflicts"] = {
        name: Observation(True, ()) for name in sorted(conflicting_facts)
    }
    return observations, summaries


def _add_comparison(
    comparisons: list[dict[str, Any]],
    field: str,
    desired: Any,
    observed: Observation | None,
    *,
    mismatch_code: str,
    assessment: Assessment,
    mismatch_result: str = "CONFLICT",
) -> None:
    if observed is None:
        comparisons.append(
            {
                "desired": desired,
                "field": field,
                "observed": None,
                "result": "UNVERIFIED",
                "source_ids": [],
            }
        )
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_REQUIRED", f"Evidence is required for {field}"
        )
        return
    result = "MATCH" if desired == observed.value else mismatch_result
    comparisons.append(
        {
            "desired": desired,
            "field": field,
            "observed": observed.value,
            "result": result,
            "source_ids": list(observed.source_ids),
        }
    )
    if result != "MATCH":
        assessment.add(mismatch_code, f"Existing-table value differs at {field}")


def _expected_key_generator(desired: dict[str, Any]) -> set[str]:
    identity = desired.get("identity")
    table = desired.get("table")
    identity = identity if isinstance(identity, dict) else {}
    table = table if isinstance(table, dict) else {}
    partition_fields = table.get("partition_fields")
    partition_fields = partition_fields if isinstance(partition_fields, list) else []
    record_keys = identity.get("record_key_fields", [])
    record_keys = record_keys if isinstance(record_keys, list) else []
    if not partition_fields:
        return {"NON_PARTITION", "NON_PARTITION_AVRO"}
    if len(record_keys) <= 1 and len(partition_fields) <= 1:
        if _partition_field_type(table) in {"DATE", "TIMESTAMP"}:
            return {"TIMESTAMP", "TIMESTAMP_AVRO"}
        return {"SIMPLE", "SIMPLE_AVRO"}
    return {"COMPLEX", "COMPLEX_AVRO"}


def _partition_field_type(table: dict[str, Any]) -> str | None:
    partitions = table.get("partition_fields")
    columns = table.get("columns")
    if not isinstance(partitions, list) or len(partitions) != 1 or not isinstance(columns, list):
        return None
    for column in columns:
        if not isinstance(column, dict) or column.get("name") != partitions[0]:
            continue
        field_type = _normalize_type(column.get("type")) or ""
        if field_type == "DATE":
            return "DATE"
        if field_type.startswith("TIMESTAMP"):
            return "TIMESTAMP"
    return None


def _expected_timestamp_settings(table: dict[str, Any]) -> dict[str, str]:
    partitions = table.get("partition_fields")
    columns = table.get("columns")
    if not isinstance(partitions, list) or len(partitions) != 1 or not isinstance(columns, list):
        return {}
    for column in columns:
        if not isinstance(column, dict) or column.get("name") != partitions[0]:
            continue
        field_type = _normalize_type(column.get("type")) or ""
        if field_type == "DATE":
            return {
                "timestamp_type": "SCALAR",
                "input_time_unit": "DAYS",
                "output_date_format": "yyyyMMdd",
                "output_timezone": "UTC",
            }
        if field_type in {"TIMESTAMP(0)", "TIMESTAMP(3)"}:
            return {
                "timestamp_type": (
                    "UNIX_TIMESTAMP" if field_type.endswith("(0)") else "EPOCHMILLISECONDS"
                ),
                "output_date_format": "yyyyMMddHH",
                "output_timezone": "UTC",
            }
    return {}


def _compare_schema(
    desired_columns: Any,
    observed: Observation | None,
    assessment: Assessment,
    comparisons: list[dict[str, Any]],
) -> None:
    if observed is None:
        _add_comparison(
            comparisons,
            "schema.columns",
            desired_columns,
            None,
            mismatch_code="FLINK_EXISTING_SCHEMA_CONFLICT",
            assessment=assessment,
        )
        return
    desired = [
        {
            "name": column.get("name"),
            "type": _normalize_type(column.get("type")),
            "nullable": column.get("nullable"),
        }
        for column in (desired_columns if isinstance(desired_columns, list) else [])
        if isinstance(column, dict) and _non_empty_string(column.get("name"))
    ]
    actual = observed.value
    result = "MATCH"
    mismatch_code: str | None = None
    if desired != actual:
        desired_by_name = {
            column["name"]: column for column in desired if isinstance(column["name"], str)
        }
        actual_by_name = {column["name"]: column for column in actual}
        missing_required = sorted(
            name
            for name in set(desired_by_name) - set(actual_by_name)
            if desired_by_name[name]["nullable"] is not True
        )
        missing_nullable = sorted(
            name
            for name in set(desired_by_name) - set(actual_by_name)
            if desired_by_name[name]["nullable"] is True
        )
        incompatible_types = sorted(
            name
            for name in set(desired_by_name) & set(actual_by_name)
            if desired_by_name[name]["type"] != actual_by_name[name]["type"]
        )
        nullable_conflicts = sorted(
            name
            for name in set(desired_by_name) & set(actual_by_name)
            if desired_by_name[name]["nullable"] is True
            and actual_by_name[name]["nullable"] is False
        )
        nullable_evolution = sorted(
            name
            for name in set(desired_by_name) & set(actual_by_name)
            if desired_by_name[name]["nullable"] is False
            and actual_by_name[name]["nullable"] is True
        )
        extra = sorted(set(actual_by_name) - set(desired_by_name))
        if missing_required or incompatible_types or nullable_conflicts:
            result = "CONFLICT"
            mismatch_code = "FLINK_EXISTING_SCHEMA_CONFLICT"
        if missing_nullable or nullable_evolution or extra:
            assessment.add(
                "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED",
                "Schema additions, nullability changes or extra physical columns require review",
            )
        if result != "CONFLICT":
            result = "UNVERIFIED"
            mismatch_code = "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED"
    comparisons.append(
        {
            "desired": desired,
            "field": "schema.columns",
            "observed": actual,
            "result": result,
            "source_ids": list(observed.source_ids),
        }
    )
    if mismatch_code:
        assessment.add(mismatch_code, "Existing physical schema differs from the desired schema")


def _compare_ddl_with_authority(
    observations: dict[str, dict[str, Observation]], assessment: Assessment
) -> None:
    ddl = observations.get("flink_table_ddl", {})
    table = {
        **observations.get("hoodie_properties", {}),
        **observations.get("hudi_table_identity", {}),
    }
    schema = observations.get("hudi_table_schema", {})
    for name in sorted(set(ddl) & set(table)):
        if ddl[name].value != table[name].value:
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_CONFLICT",
                f"Flink DDL disagrees with authoritative table evidence for {name}",
            )
    if "columns" in ddl and "columns" in schema and ddl["columns"].value != schema["columns"].value:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_CONFLICT",
            "Flink DDL disagrees with the authoritative Hudi table schema",
        )


def _expected_writer_state(desired: dict[str, Any]) -> dict[str, Any]:
    """Return the bounded writer state that must survive a stateful restore."""

    raw_write = desired.get("write")
    write = raw_write if isinstance(raw_write, dict) else {}
    expected: dict[str, Any] = {
        "design_contract_schema": desired.get("contract_schema"),
        "operation": str(write.get("operation", "")).upper(),
    }
    if desired.get("contract_schema") in (2, 3):
        raw_index = write.get("index")
        index = raw_index if isinstance(raw_index, dict) else {}
        expected.update(
            {
                "index_bootstrap_enabled": index.get("bootstrap_enabled"),
                "index_global": index.get("global"),
                "index_state_ttl_days": index.get("state_ttl_days"),
                "index_type": str(index.get("type", "")).upper(),
            }
        )
    if desired.get("contract_schema") == 3:
        raw_services = desired.get("table_services")
        services = raw_services if isinstance(raw_services, dict) else {}
        raw_compaction = services.get("compaction")
        compaction = raw_compaction if isinstance(raw_compaction, dict) else {}
        expected["compaction_owner"] = str(compaction.get("owner", "")).upper()
    return expected


def assess_reconciliation(
    envelope: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any]:
    """Return one deterministic, non-executable existing-table assessment."""

    assessment = Assessment()
    manifest_errors = validate_manifest(manifest)
    if manifest_errors:
        assessment.add("FLINK_BASELINE_EVIDENCE_INVALID", "; ".join(manifest_errors))
        return {
            "status": assessment.final_status(),
            "reconciliation_result": "UNVERIFIED",
            "reconciliation_complete": False,
            "executable_eligible": False,
            "finding_codes": [finding["code"] for finding in assessment.findings],
            "findings": assessment.findings,
            "comparisons": [],
            "evidence_summary": [],
        }

    _check_keys(
        envelope,
        {
            "baseline_id",
            "desired_contract",
            "evidence",
            "reconciliation_schema",
            "writer_transition",
        },
        "reconciliation",
        assessment,
    )
    schema_version = envelope.get("reconciliation_schema")
    if type(schema_version) is not int or schema_version != RECONCILIATION_SCHEMA_VERSION:
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID", "reconciliation_schema must be 1"
        )
    if envelope.get("baseline_id") != manifest.get("baseline_id"):
        assessment.add(
            "FLINK_VERSION_UNVERIFIED", "The reconciliation baseline does not match"
        )

    desired = envelope.get("desired_contract")
    if not isinstance(desired, dict):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_INVALID", "desired_contract must be an object"
        )
        desired = {}
    desired_result = assess_design(
        desired,
        manifest,
        expected_table_lifecycle="existing",
        render_sql=False,
    )
    for finding in desired_result["findings"]:
        assessment.add(finding["code"], finding["message"])
    for advisory in desired_result["advisory_codes"]:
        assessment.advise(advisory)

    desired_table = desired.get("table", {}) if isinstance(desired.get("table"), dict) else {}
    raw_target_table_path = desired_table.get("path")
    target_table_path = (
        raw_target_table_path
        if _non_empty_string(raw_target_table_path)
        and _path_validation_error(raw_target_table_path) is None
        else None
    )
    observations, evidence_summary = _parse_evidence(
        envelope.get("evidence"), target_table_path, assessment
    )
    writer_evidence: dict[str, Any] = {}
    writer_evidence_source_ids: dict[str, set[str]] = {}
    for kind in ("writer_inventory", "flink_writer_config"):
        for name, observation in observations.get(kind, {}).items():
            field = f"{kind}.{name}"
            writer_evidence[field] = observation.value
            writer_evidence_source_ids[field] = set(observation.source_ids)
    writer_contract_state = assess_writer_contract(
        desired,
        evidence=writer_evidence,
        conflicting_evidence_fields=set(observations.get("evidence_conflicts", {})),
        evidence_source_ids=writer_evidence_source_ids,
    )
    for finding in writer_contract_state["findings"]:
        assessment.add(finding["code"], finding["message"])
    for kind in sorted(REQUIRED_EVIDENCE_KINDS - set(observations)):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_REQUIRED", f"{kind} evidence is required"
        )
    table_observations = observations.get("hoodie_properties", {})
    for fact in sorted(REQUIRED_TABLE_FACTS - set(table_observations)):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_REQUIRED",
            f"hoodie_properties.{fact} is required",
        )
    _compare_ddl_with_authority(observations, assessment)

    comparisons: list[dict[str, Any]] = []
    desired_identity = (
        desired.get("identity", {}) if isinstance(desired.get("identity"), dict) else {}
    )
    desired_ordering = (
        desired.get("ordering", {}) if isinstance(desired.get("ordering"), dict) else {}
    )
    desired_write = desired.get("write", {}) if isinstance(desired.get("write"), dict) else {}
    property_expectations = {
        "base_file_format": manifest["existing_table_reconciliation_contract"][
            "supported_base_file_formats"
        ][0],
        "ordering_fields": desired_ordering.get("fields", []),
        "partition_fields": desired_table.get("partition_fields", []),
        "record_key_fields": desired_identity.get("record_key_fields", []),
        "table_name": desired_table.get("name"),
        "table_type": desired_write.get("table_type"),
        "initial_table_version": manifest["existing_table_reconciliation_contract"][
            "supported_initial_table_versions"
        ][0],
        "table_version": manifest["existing_table_reconciliation_contract"][
            "supported_table_versions"
        ][0],
        "timeline_layout_version": manifest["existing_table_reconciliation_contract"][
            "supported_timeline_layout_versions"
        ][0],
    }
    if desired.get("contract_schema") in (2, 3):
        property_expectations["record_merge_mode"] = desired_write.get("record_merge_mode")
    if desired_table.get("partition_fields"):
        property_expectations.update(
            {"hive_style_partitioning": False, "url_encode_partitioning": False}
        )
    _add_comparison(
        comparisons,
        "table_identity.table_path",
        desired_table.get("path"),
        observations.get("hudi_table_identity", {}).get("table_path"),
        mismatch_code="FLINK_EXISTING_EVIDENCE_CONFLICT",
        mismatch_result="UNVERIFIED",
        assessment=assessment,
    )
    _add_comparison(
        comparisons,
        "evidence_scope.table_path",
        desired_table.get("path"),
        observations.get("evidence_scope", {}).get("table_path"),
        mismatch_code="FLINK_EXISTING_EVIDENCE_CONFLICT",
        mismatch_result="UNVERIFIED",
        assessment=assessment,
    )
    for name, expected in property_expectations.items():
        mismatch_code = (
            "FLINK_EXISTING_TABLE_VERSION_UNVERIFIED"
            if name in {"initial_table_version", "table_version", "timeline_layout_version"}
            else "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT"
        )
        mismatch_result = (
            "UNVERIFIED"
            if name in {"initial_table_version", "table_version", "timeline_layout_version"}
            else "CONFLICT"
        )
        _add_comparison(
            comparisons,
            f"table_config.{name}",
            expected,
            table_observations.get(name),
            mismatch_code=mismatch_code,
            mismatch_result=mismatch_result,
            assessment=assessment,
        )

    expected_key_generators = _expected_key_generator(desired)
    if "TIMESTAMP_AVRO" in expected_key_generators:
        timestamp_expected = _expected_timestamp_settings(desired_table)
        timestamp_observed = observations.get("flink_writer_config", {}).get(
            "timestamp_key_generator"
        )
        if not timestamp_expected:
            comparisons.append(
                {
                    "desired": None,
                    "field": "writer_config.timestamp_key_generator",
                    "observed": timestamp_observed.value if timestamp_observed else None,
                    "result": "UNVERIFIED",
                    "source_ids": (
                        list(timestamp_observed.source_ids) if timestamp_observed else []
                    ),
                }
            )
            assessment.add(
                "FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED",
                "Datetime partition precision is outside the pinned writer-setting mappings",
            )
        elif timestamp_observed is not None and not set(timestamp_expected) <= set(
            timestamp_observed.value
        ):
            assessment.add(
                "FLINK_EXISTING_EVIDENCE_REQUIRED",
                "Timestamp key-generator evidence is missing required settings",
            )
            timestamp_observed = None
        if timestamp_expected:
            _add_comparison(
                comparisons,
                "writer_config.timestamp_key_generator",
                timestamp_expected,
                timestamp_observed,
                mismatch_code="FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED",
                mismatch_result="UNVERIFIED",
                assessment=assessment,
            )
    observed_key_generator = table_observations.get("key_generator_type")
    if observed_key_generator is None:
        _add_comparison(
            comparisons,
            "table_config.key_generator_type",
            sorted(expected_key_generators),
            None,
            mismatch_code="FLINK_EXISTING_TABLE_PROPERTY_CONFLICT",
            assessment=assessment,
        )
    else:
        keygen_matches = observed_key_generator.value in expected_key_generators
        keygen_known = observed_key_generator.value in KNOWN_KEY_GENERATORS
        result = "MATCH" if keygen_matches else "CONFLICT" if keygen_known else "UNVERIFIED"
        comparisons.append(
            {
                "desired": sorted(expected_key_generators),
                "field": "table_config.key_generator_type",
                "observed": observed_key_generator.value,
                "result": result,
                "source_ids": list(observed_key_generator.source_ids),
            }
        )
        if result == "CONFLICT":
            assessment.add(
                "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT",
                "Existing key generator conflicts with the desired identity and partition shape",
            )
        elif result == "UNVERIFIED":
            assessment.add(
                "FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED",
                "Existing key generator is outside the pinned built-in mappings",
            )

    _compare_schema(
        desired_table.get("columns", []),
        observations.get("hudi_table_schema", {}).get("columns"),
        assessment,
        comparisons,
    )

    transition = envelope.get("writer_transition")
    if not isinstance(transition, str) or transition not in ALLOWED_TRANSITIONS:
        transition = "unknown"
    if transition == "unknown":
        assessment.add(
            "FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED",
            "Writer transition must be explicitly resolved",
        )
    elif transition == "cross_engine_takeover":
        assessment.add(
            "FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED",
            "Cross-engine writer takeover is outside the bounded reconciliation path",
        )
    elif transition == "flink_writer_takeover":
        assessment.add(
            "FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED",
            "A separate Flink writer takeover requires human review",
        )

    inventory = observations.get("writer_inventory", {})
    concurrent = inventory.get("concurrent_writers")
    stopped = inventory.get("previous_writer_stopped")
    previous_engine = inventory.get("previous_writer_engine")
    raw_safety = desired.get("safety")
    safety = raw_safety if isinstance(raw_safety, dict) else {}
    writer_model = safety.get("writer_model")
    if writer_model == "single_writer":
        _add_comparison(
            comparisons,
            "writer_inventory.concurrent_writers",
            False,
            concurrent,
            mismatch_code="FLINK_MULTI_WRITER_REVIEW",
            mismatch_result="UNVERIFIED",
            assessment=assessment,
        )
    elif concurrent is None:
        comparisons.append(
            {
                "desired": None,
                "field": "writer_inventory.concurrent_writers",
                "observed": None,
                "result": "UNVERIFIED",
                "source_ids": [],
            }
        )
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_REQUIRED",
            "Evidence is required for writer_inventory.concurrent_writers",
        )
    else:
        comparisons.append(
            {
                "desired": None,
                "field": "writer_inventory.concurrent_writers",
                "observed": concurrent.value,
                "result": "UNVERIFIED",
                "source_ids": list(concurrent.source_ids),
            }
        )
    active_writers = inventory.get("active_writers")
    if writer_model == "single_writer" and active_writers is not None:
        comparisons.append(
            {
                "desired": {"maximum_count": 1},
                "field": "writer_inventory.active_writers",
                "observed": active_writers.value,
                "result": (
                    "MATCH" if len(active_writers.value) <= 1 else "UNVERIFIED"
                ),
                "source_ids": list(active_writers.source_ids),
            }
        )
    normalized_writer_contract = writer_contract_state["normalized_writer_contract"]
    if normalized_writer_contract:
        declared_writer_ids = sorted(
            [normalized_writer_contract["primary_writer_id"]]
            + [
                writer["writer_id"]
                for writer in normalized_writer_contract["additional_writers"]
            ]
        )
        if active_writers is not None:
            active_writer_result = (
                "MATCH"
                if set(active_writers.value) <= set(declared_writer_ids)
                else "UNVERIFIED"
            )
            comparisons.append(
                {
                    "desired": {"allowed_writer_ids": declared_writer_ids},
                    "field": "writer_inventory.active_writers",
                    "observed": active_writers.value,
                    "result": active_writer_result,
                    "source_ids": list(active_writers.source_ids),
                }
            )
            if active_writer_result != "MATCH":
                assessment.add(
                    "FLINK_WRITER_CONTRACT_CONFLICT",
                    "Existing-table value differs at writer_inventory.active_writers",
                )
        schema_owner = inventory.get("schema_evolution_owner_id")
        if schema_owner is not None:
            _add_comparison(
                comparisons,
                "writer_inventory.schema_evolution_owner_id",
                normalized_writer_contract["ownership"]["schema_evolution_owner_id"],
                schema_owner,
                mismatch_code="FLINK_WRITER_CONTRACT_CONFLICT",
                mismatch_result="UNVERIFIED",
                assessment=assessment,
            )
    elif writer_model != "single_writer" and active_writers is not None:
        comparisons.append(
            {
                "desired": None,
                "field": "writer_inventory.active_writers",
                "observed": active_writers.value,
                "result": "UNVERIFIED",
                "source_ids": list(active_writers.source_ids),
            }
        )

    observed_concurrency = (
        normalized_writer_contract.get("observed_concurrency")
        if normalized_writer_contract
        else None
    )
    config_observations = observations.get("flink_writer_config", {})
    concurrency_expectations = {
        "writer_config.write_concurrency_mode": (
            observed_concurrency.get("mode")
            if isinstance(observed_concurrency, dict)
            else None,
            config_observations.get("write_concurrency_mode"),
        ),
        "writer_config.lock_provider_class": (
            observed_concurrency.get("lock_provider_class")
            if isinstance(observed_concurrency, dict)
            else None,
            config_observations.get("lock_provider_class"),
        ),
        "writer_config.lock_configuration_consistency": (
            observed_concurrency.get("lock_configuration_consistency")
            if isinstance(observed_concurrency, dict)
            else None,
            config_observations.get("lock_configuration_consistency"),
        ),
    }
    for field, (expected, observed) in concurrency_expectations.items():
        if observed is None:
            continue
        if writer_model == "multi_writer" and expected is not None:
            _add_comparison(
                comparisons,
                field,
                expected,
                observed,
                mismatch_code="FLINK_WRITER_CONTRACT_CONFLICT",
                mismatch_result="UNVERIFIED",
                assessment=assessment,
            )
        elif writer_model == "single_writer" and field.endswith(
            "write_concurrency_mode"
        ):
            _add_comparison(
                comparisons,
                field,
                "SINGLE_WRITER",
                observed,
                mismatch_code="FLINK_MULTI_WRITER_REVIEW",
                mismatch_result="UNVERIFIED",
                assessment=assessment,
            )
        else:
            result = "UNVERIFIED"
            if writer_model == "single_writer":
                result = (
                    "UNVERIFIED"
                    if field.endswith("lock_configuration_consistency")
                    and observed.value == "CONFLICTING"
                    else "NOT_APPLICABLE"
                )
            comparisons.append(
                {
                    "desired": None,
                    "field": field,
                    "observed": observed.value,
                    "result": result,
                    "source_ids": list(observed.source_ids),
                }
            )
    if transition in {"same_job_restore", "cold_start_new_job", "flink_writer_takeover"}:
        _add_comparison(
            comparisons,
            "writer_inventory.previous_writer_engine",
            "FLINK",
            previous_engine,
            mismatch_code="FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED",
            mismatch_result="UNVERIFIED",
            assessment=assessment,
        )
    else:
        comparisons.append(
            {
                "desired": None,
                "field": "writer_inventory.previous_writer_engine",
                "observed": previous_engine.value if previous_engine else None,
                "result": "NOT_APPLICABLE",
                "source_ids": list(previous_engine.source_ids) if previous_engine else [],
            }
        )
    if transition in {"cold_start_new_job", "flink_writer_takeover", "cross_engine_takeover"}:
        _add_comparison(
            comparisons,
            "writer_inventory.previous_writer_stopped",
            True,
            stopped,
            mismatch_code="FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED",
            mismatch_result="UNVERIFIED",
            assessment=assessment,
        )
    else:
        comparisons.append(
            {
                "desired": None,
                "field": "writer_inventory.previous_writer_stopped",
                "observed": stopped.value if stopped else None,
                "result": "NOT_APPLICABLE",
                "source_ids": list(stopped.source_ids) if stopped else [],
            }
        )

    writer_config = observations.get("flink_writer_config", {})
    if transition == "same_job_restore":
        recovery = writer_config.get("checkpoint_recovery_available")
        _add_comparison(
            comparisons,
            "writer_config.checkpoint_recovery_available",
            True,
            recovery,
            mismatch_code="FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED",
            mismatch_result="UNVERIFIED",
            assessment=assessment,
        )
        for name, expected in _expected_writer_state(desired).items():
            _add_comparison(
                comparisons,
                f"writer_config.{name}",
                expected,
                writer_config.get(name),
                mismatch_code="FLINK_EXISTING_WRITER_CONFIG_CONFLICT",
                mismatch_result="UNVERIFIED",
                assessment=assessment,
            )
    elif (
        transition == "flink_writer_takeover"
        and "checkpoint_recovery_available" not in writer_config
    ):
        assessment.add(
            "FLINK_EXISTING_EVIDENCE_REQUIRED",
            "Flink writer takeover must state whether checkpoint or savepoint state is inherited",
        )
    elif transition == "flink_writer_takeover":
        recovery = writer_config["checkpoint_recovery_available"]
        comparisons.append(
            {
                "desired": "EXPLICIT",
                "field": "writer_config.checkpoint_recovery_available",
                "observed": recovery.value,
                "result": "MATCH",
                "source_ids": list(recovery.source_ids),
            }
        )

    timeline = observations.get("timeline_summary", {})
    pending = timeline.get("pending_instants")
    _add_comparison(
        comparisons,
        "timeline.pending_instants",
        [],
        pending,
        mismatch_code="FLINK_EXISTING_TIMELINE_UNRESOLVED",
        mismatch_result="UNVERIFIED",
        assessment=assessment,
    )

    status = assessment.final_status()
    comparison_results = {item["result"] for item in comparisons}
    if "CONFLICT" in comparison_results:
        reconciliation_result = "CONFLICT"
    elif status != "CONFIG_VALIDATED" or "UNVERIFIED" in comparison_results:
        reconciliation_result = "UNVERIFIED"
    else:
        reconciliation_result = "COMPATIBLE"
    complete = status == "CONFIG_VALIDATED"
    finding_codes = [finding["code"] for finding in assessment.findings]
    if reconciliation_result == "COMPATIBLE":
        change_disposition = "NO_CHANGE"
    elif reconciliation_result == "CONFLICT":
        change_disposition = "MIGRATION_REQUIRED"
    elif set(finding_codes) == {"FLINK_EXISTING_WRITER_CONFIG_CONFLICT"}:
        change_disposition = "DEPLOYMENT_CONFIGURATION_CHANGE"
    else:
        change_disposition = "UNVERIFIED"
    deployment_checks = []
    if writer_model == "single_writer":
        deployment_checks.append(
            {"check": "confirm_single_writer", "required": True}
        )
    deployment_checks.append(
        {"check": "recheck_pending_instants", "required": True}
    )
    if transition == "cold_start_new_job" and desired.get("contract_schema") in (2, 3):
        deployment_checks.append(
            {"check": "verify_index_bootstrap_before_first_checkpoint", "required": True}
        )
    baseline_evidence = validation_evidence(
        manifest,
        desired.get("contract_schema"),
        desired_result.get("catalog_profile", "direct_sql_none"),
    )
    baseline_evidence["reconciliation_schema"] = RECONCILIATION_SCHEMA_VERSION
    return {
        "status": status,
        "reconciliation_result": reconciliation_result,
        "reconciliation_complete": complete,
        "writer_transition": transition,
        "executable_eligible": False,
        "catalog_contract_schema": desired_result.get("catalog_contract_schema"),
        "catalog_profile": desired_result.get("catalog_profile", "direct_sql_none"),
        "catalog_setup_sql": "",
        "effective_catalog_configuration": desired_result.get(
            "effective_catalog_configuration", {}
        ),
        "catalog_deployment_checks": desired_result.get(
            "catalog_deployment_checks", []
        ),
        "writer_contract_result": writer_contract_state["writer_contract_result"],
        "multi_writer_capability": writer_contract_state["multi_writer_capability"],
        "normalized_writer_contract": writer_contract_state[
            "normalized_writer_contract"
        ],
        "writer_capability_notes": writer_contract_state["writer_capability_notes"],
        "writer_deployment_checks": writer_contract_state["writer_deployment_checks"],
        "finding_codes": finding_codes,
        "findings": assessment.findings,
        "advisory_codes": assessment.advisories,
        "comparisons": comparisons,
        "evidence_summary": evidence_summary,
        "change_disposition": change_disposition,
        "deployment_checks": deployment_checks,
        "validation_evidence": baseline_evidence,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        envelope = json.loads(args.input.read_text(encoding="utf-8"))
        manifest = load_manifest(args.manifest)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Invalid Flink existing-table input: {error}", file=sys.stderr)
        return 2
    if not isinstance(envelope, dict):
        print(
            "Invalid Flink existing-table input: top-level value must be an object",
            file=sys.stderr,
        )
        return 2
    result = assess_reconciliation(envelope, manifest)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["status"] == "CONFIG_VALIDATED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
