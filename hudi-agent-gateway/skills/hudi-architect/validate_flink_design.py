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

"""Validate and render bounded Hudi Architect Flink SQL design contracts.

The caller supplies every architecture decision.  This program does not select
table type, identity, ordering, partitioning, replay behavior, or checkpoint
cadence.  It only rejects incomplete or unsupported combinations and serializes
a validated contract into deterministic Flink SQL.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, TypeGuard
from urllib.parse import SplitResult, parse_qsl, urlsplit

from validate_flink_capabilities import (
    DEFAULT_MANIFEST,
    load_manifest,
    supported_option_keys,
    validate_manifest,
    validation_evidence,
)

APPEND_CONTRACT_SCHEMA_VERSION = 1
MUTABLE_CONTRACT_SCHEMA_VERSION = 2
SUPPORTED_CONTRACT_SCHEMAS = {
    APPEND_CONTRACT_SCHEMA_VERSION,
    MUTABLE_CONTRACT_SCHEMA_VERSION,
}
MAX_TYPE_LENGTH = 2_147_483_647
STATUS_PRIORITY = {"INCOMPLETE": 0, "REVIEW_REQUIRED": 1, "BLOCKED": 2}
AZURE_FILESYSTEM_SCHEMES = frozenset({"abfs", "abfss"})
AZURE_FILESYSTEM_NAME = re.compile(
    r"^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])?$", re.IGNORECASE
)
AZURE_DFS_HOST = re.compile(
    r"^[a-z0-9]{3,24}\.dfs\.core\.windows\.net$", re.IGNORECASE
)
SENSITIVE_QUERY_KEY = re.compile(
    r"(?:access[_-]?key|api[_-]?key|credential|password|secret|signature|token)",
    re.IGNORECASE,
)
PLACEHOLDER = re.compile(r"(?:<[^>]+>|\$\{[^}]+\}|\b(?:TBD|TODO)\b)", re.IGNORECASE)
PARAMETERIZED_TYPE = re.compile(
    r"^(?P<name>DECIMAL|CHAR|VARCHAR|BINARY|VARBINARY|TIME|TIMESTAMP|TIMESTAMP_LTZ)"
    r"\((?P<args>\d+(?:\s*,\s*\d+)?)\)$"
)

FINDING_STATUS = {
    "FLINK_APPEND_MODE_CLUSTERING_ENABLED": "BLOCKED",
    "FLINK_AUTO_KEY_ACCEPTANCE_REQUIRED": "INCOMPLETE",
    "FLINK_AUTO_KEY_DECLINED": "BLOCKED",
    "FLINK_BASELINE_EVIDENCE_INVALID": "BLOCKED",
    "FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED": "BLOCKED",
    "FLINK_CATALOG_REQUIREMENT_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_CHECKPOINTING_REQUIRED": "BLOCKED",
    "FLINK_CHECKPOINT_INTERVAL_REQUIRED": "INCOMPLETE",
    "FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED": "BLOCKED",
    "FLINK_DESIGN_CONTRACT_INVALID": "INCOMPLETE",
    "FLINK_EXISTING_TABLE_DEFERRED": "BLOCKED",
    "FLINK_EXTERNAL_CATALOG_REVIEW": "REVIEW_REQUIRED",
    "FLINK_HUDI_METADATA_FIELD_CONFLICT": "BLOCKED",
    "FLINK_LOAD_BEARING_VALUE_REQUIRED": "INCOMPLETE",
    "FLINK_MULTI_WRITER_REVIEW": "REVIEW_REQUIRED",
    "FLINK_MUTABILITY_REQUIRED": "INCOMPLETE",
    "FLINK_MUTABLE_AUTO_KEY_UNSUPPORTED": "BLOCKED",
    "FLINK_MUTABLE_COW_DEFERRED": "BLOCKED",
    "FLINK_MUTABLE_DELETE_PAYLOAD_UNSUPPORTED": "BLOCKED",
    "FLINK_MUTABLE_INDEX_BOOTSTRAP_REQUIRED": "BLOCKED",
    "FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED": "BLOCKED",
    "FLINK_MUTABLE_ORDERING_FIELD_INVALID": "BLOCKED",
    "FLINK_MUTABLE_ORDERING_FIELD_REQUIRED": "INCOMPLETE",
    "FLINK_MUTABLE_PARTITION_EVOLUTION_UNSUPPORTED": "BLOCKED",
    "FLINK_MUTABLE_SOURCE_CHANGELOG_UNSUPPORTED": "BLOCKED",
    "FLINK_OPTION_NOT_VERIFIED": "REVIEW_REQUIRED",
    "FLINK_PARTITION_FIELD_MISSING": "BLOCKED",
    "FLINK_PHYSICAL_SCHEMA_REQUIRED": "INCOMPLETE",
    "FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT": "BLOCKED",
    "FLINK_RECORD_KEY_FIELD_MISSING": "BLOCKED",
    "FLINK_RECORD_KEY_NULLABLE": "BLOCKED",
    "FLINK_REPLAY_BEHAVIOR_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_REPLAY_IDEMPOTENCE_DEFERRED": "BLOCKED",
    "FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED": "BLOCKED",
    "FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY": "BLOCKED",
    "FLINK_SOURCE_CONTRACT_REQUIRED": "INCOMPLETE",
    "FLINK_SOURCE_SCHEMA_MISMATCH": "BLOCKED",
    "FLINK_TABLE_LIFECYCLE_REQUIRED": "INCOMPLETE",
    "FLINK_TEMPORAL_PRECISION_UNSUPPORTED": "BLOCKED",
    "FLINK_VERSION_REQUIRED": "INCOMPLETE",
    "FLINK_VERSION_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_WRITER_MODEL_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_PR2_WRITE_PATH_UNSUPPORTED": "BLOCKED",
    "FLINK_PR3_WRITE_PATH_UNSUPPORTED": "BLOCKED",
    "FLINK_SCHEMA_TYPE_UNVERIFIED": "REVIEW_REQUIRED",
}


class Assessment:
    """Accumulate stable findings without discarding independent reasons."""

    def __init__(self) -> None:
        self.findings: list[dict[str, str]] = []
        self.advisories: list[str] = []

    def add(self, code: str, message: str) -> None:
        if code not in FINDING_STATUS:
            raise ValueError(f"Unregistered finding code: {code}")
        existing = next(
            (finding for finding in self.findings if finding["code"] == code), None
        )
        if existing is None:
            self.findings.append(
                {"code": code, "status": FINDING_STATUS[code], "message": message}
            )
        elif message not in existing["message"]:
            existing["message"] += f"; {message}"

    def advise(self, code: str) -> None:
        if code not in self.advisories:
            self.advisories.append(code)

    def final_status(self) -> str:
        if not self.findings:
            return "CONFIG_VALIDATED"
        return max(
            (finding["status"] for finding in self.findings),
            key=STATUS_PRIORITY.__getitem__,
        )


def _dict(value: Any, field: str, assessment: Assessment) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    assessment.add("FLINK_DESIGN_CONTRACT_INVALID", f"{field} must be an object")
    return {}


def _check_keys(
    value: dict[str, Any], allowed: set[str], field: str, assessment: Assessment
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            f"{field} contains unsupported fields: {unknown}",
        )


def _list(value: Any, field: str, assessment: Assessment) -> list[Any]:
    if isinstance(value, list):
        return value
    assessment.add("FLINK_DESIGN_CONTRACT_INVALID", f"{field} must be an array")
    return []


def _non_empty_string(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _normalize_type(
    value: Any, manifest: dict[str, Any], assessment: Assessment
) -> str | None:
    if not _non_empty_string(value):
        assessment.add("FLINK_PHYSICAL_SCHEMA_REQUIRED", "Every field needs a Flink SQL type")
        return None
    normalized = " ".join(value.upper().split())
    if normalized == "INTEGER":
        normalized = "INT"
    if normalized in manifest["physical_types"]["simple"]:
        return normalized

    matched = PARAMETERIZED_TYPE.fullmatch(normalized)
    if matched is None or matched.group("name") not in manifest["physical_types"][
        "parameterized"
    ]:
        assessment.add(
            "FLINK_SCHEMA_TYPE_UNVERIFIED",
            f"Flink SQL type {value!r} is outside the bounded scalar surface",
        )
        return None

    type_name = matched.group("name")
    arguments = [int(part.strip()) for part in matched.group("args").split(",")]
    valid = False
    if type_name == "DECIMAL":
        valid = (
            len(arguments) == 2
            and 1 <= arguments[0] <= 38
            and 0 <= arguments[1] <= arguments[0]
        )
    elif type_name in {"CHAR", "VARCHAR", "BINARY", "VARBINARY"}:
        valid = len(arguments) == 1 and 1 <= arguments[0] <= MAX_TYPE_LENGTH
    elif type_name in manifest["physical_schema_constraints"]["temporal_types"]:
        constraints = manifest["physical_schema_constraints"]
        if len(arguments) == 1 and not (
            constraints["temporal_precision_min"]
            <= arguments[0]
            <= constraints["temporal_precision_max"]
        ):
            assessment.add(
                "FLINK_TEMPORAL_PRECISION_UNSUPPORTED",
                f"Hudi 1.2.0 supports {type_name} precision only between "
                f"{constraints['temporal_precision_min']} and "
                f"{constraints['temporal_precision_max']}",
            )
            return None
        valid = len(arguments) == 1
    if not valid:
        assessment.add(
            "FLINK_SCHEMA_TYPE_UNVERIFIED",
            f"Flink SQL type {value!r} has unsupported parameters",
        )
        return None
    return f"{type_name}({','.join(str(argument) for argument in arguments)})"


def _columns(
    raw_columns: Any,
    field: str,
    manifest: dict[str, Any],
    assessment: Assessment,
    *,
    reject_hudi_metadata_fields: bool = False,
) -> list[dict[str, Any]]:
    columns: list[dict[str, Any]] = []
    names: set[str] = set()
    for index, raw_column in enumerate(_list(raw_columns, field, assessment)):
        column = _dict(raw_column, f"{field}[{index}]", assessment)
        _check_keys(column, {"name", "nullable", "type"}, f"{field}[{index}]", assessment)
        name = column.get("name")
        nullable = column.get("nullable")
        if not _non_empty_string(name):
            assessment.add("FLINK_PHYSICAL_SCHEMA_REQUIRED", f"{field}[{index}] needs a name")
            continue
        if re.fullmatch(
            manifest["physical_schema_constraints"]["field_name_pattern"], name
        ) is None:
            assessment.add(
                "FLINK_SCHEMA_FIELD_NAME_UNSUPPORTED",
                f"{field}[{index}].name {name!r} cannot be represented by the "
                "Avro-backed Hudi sink schema",
            )
            continue
        reserved_names = manifest["physical_schema_constraints"][
            "reserved_target_field_names"
        ]
        if reject_hudi_metadata_fields and name.lower() in reserved_names:
            assessment.add(
                "FLINK_HUDI_METADATA_FIELD_CONFLICT",
                f"{field}[{index}].name {name!r} is reserved for Hudi metadata",
            )
        if name in names:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID", f"Duplicate field {name!r} in {field}"
            )
            continue
        names.add(name)
        if not isinstance(nullable, bool):
            assessment.add(
                "FLINK_PHYSICAL_SCHEMA_REQUIRED",
                f"{field}[{index}].nullable must be explicitly true or false",
            )
            continue
        normalized_type = _normalize_type(column.get("type"), manifest, assessment)
        if normalized_type is None:
            continue
        columns.append({"name": name, "type": normalized_type, "nullable": nullable})
    if not columns:
        assessment.add("FLINK_PHYSICAL_SCHEMA_REQUIRED", f"{field} must contain physical fields")
    return columns


def _string_list(value: Any, field: str, assessment: Assessment) -> list[str]:
    values = _list(value, field, assessment)
    if not all(_non_empty_string(item) for item in values):
        assessment.add("FLINK_DESIGN_CONTRACT_INVALID", f"{field} must contain field names")
        return []
    if len(values) != len(set(values)):
        assessment.add("FLINK_DESIGN_CONTRACT_INVALID", f"{field} must not contain duplicates")
    return values


def _check_concrete_value(value: Any) -> TypeGuard[str]:
    return _non_empty_string(value) and PLACEHOLDER.search(value) is None


def _is_binary_routing_type(type_name: str, manifest: dict[str, Any]) -> bool:
    return type_name.partition("(")[0] in manifest["physical_schema_constraints"][
        "binary_routing_types"
    ]


def _is_standard_abfs_authority(parsed: SplitResult) -> bool:
    return (
        parsed.scheme.lower() in AZURE_FILESYSTEM_SCHEMES
        and parsed.username is not None
        and parsed.password is None
        and AZURE_FILESYSTEM_NAME.fullmatch(parsed.username) is not None
        and parsed.hostname is not None
        and AZURE_DFS_HOST.fullmatch(parsed.hostname) is not None
    )


def _is_sensitive_query_key(key: str) -> bool:
    # Azure SAS uses the exact, abbreviated key "sig". Keep it exact so benign
    # keys such as "design" do not become false positives.
    return key.casefold() == "sig" or SENSITIVE_QUERY_KEY.search(key) is not None


def _path_validation_error(path: str) -> str | None:
    if any(ord(character) < 32 for character in path):
        return "The target path must not contain control characters"
    try:
        parsed = urlsplit(path)
    except ValueError:
        return "The target path must be a valid URI or filesystem path"
    if (parsed.username is not None or parsed.password is not None) and not (
        _is_standard_abfs_authority(parsed)
    ):
        return "The target path must not contain credentials or secret query parameters"
    parameters = [
        *parse_qsl(parsed.query, keep_blank_values=True),
        *parse_qsl(parsed.fragment, keep_blank_values=True),
    ]
    if any(_is_sensitive_query_key(key) for key, _ in parameters):
        return "The target path must not contain credentials or secret query parameters"
    return None


def _quote_identifier(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"


def _quote_qualified_identifier(identifier: str) -> str:
    return ".".join(_quote_identifier(part) for part in identifier.split("."))


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _render_sql(
    contract: dict[str, Any],
    table_columns: list[dict[str, Any]],
    partition_fields: list[str],
    record_key_fields: list[str],
) -> dict[str, str]:
    table = contract["table"]
    source = contract["source"]
    runtime = contract["runtime"]
    identity = contract["identity"]
    mutable = contract["contract_schema"] == MUTABLE_CONTRACT_SCHEMA_VERSION

    definitions = [
        f"  {_quote_identifier(column['name'])} {column['type']}"
        + ("" if column["nullable"] else " NOT NULL")
        for column in table_columns
    ]
    if identity["mode"] == "stable_key":
        key_sql = ", ".join(_quote_identifier(field) for field in record_key_fields)
        definitions.append(f"  PRIMARY KEY ({key_sql}) NOT ENFORCED")

    table_ddl = (
        f"CREATE TABLE {_quote_qualified_identifier(table['name'])} (\n"
        + ",\n".join(definitions)
        + "\n)"
    )
    if partition_fields:
        partition_sql = ", ".join(_quote_identifier(field) for field in partition_fields)
        table_ddl += f"\nPARTITIONED BY ({partition_sql})"
    if mutable:
        ordering_field = contract["ordering"]["fields"][0]
        table_ddl += (
            "\nWITH (\n"
            "  'connector' = 'hudi',\n"
            f"  'path' = {_sql_string(table['path'])},\n"
            "  'table.type' = 'COPY_ON_WRITE',\n"
            "  'write.operation' = 'upsert',\n"
            f"  'ordering.fields' = {_sql_string(ordering_field)},\n"
            "  'hoodie.write.record.merge.mode' = 'EVENT_TIME_ORDERING',\n"
            "  'index.type' = 'FLINK_STATE',\n"
            "  'index.global.enabled' = 'true',\n"
            "  'index.state.ttl' = '0',\n"
            "  'index.bootstrap.enabled' = 'true',\n"
            "  'changelog.enabled' = 'false'\n"
            ");"
        )
    else:
        table_ddl += (
            "\nWITH (\n"
            "  'connector' = 'hudi',\n"
            f"  'path' = {_sql_string(table['path'])},\n"
            "  'table.type' = 'COPY_ON_WRITE',\n"
            "  'write.operation' = 'insert',\n"
            "  'write.insert.cluster' = 'false'\n"
            ");"
        )

    field_sql = ",\n  ".join(_quote_identifier(column["name"]) for column in table_columns)
    insert_sql = (
        f"INSERT INTO {_quote_qualified_identifier(table['name'])} (\n  {field_sql}\n)\n"
        f"SELECT\n  {field_sql}\n"
        f"FROM {_quote_qualified_identifier(source['table'])};"
    )
    runtime_sql = (
        "SET 'execution.runtime-mode' = 'streaming';\n"
        "SET 'execution.checkpointing.interval' = "
        f"'{runtime['checkpoint_interval_ms']} ms';"
    )
    return {
        "runtime_sql": runtime_sql,
        "table_ddl": table_ddl,
        "insert_sql": insert_sql,
        "combined_sql": f"{runtime_sql}\n\n{table_ddl}\n\n{insert_sql}",
    }


def assess_design(
    contract: dict[str, Any], manifest: dict[str, Any]
) -> dict[str, Any]:
    """Validate one explicit append-only or mutable COW contract."""

    assessment = Assessment()
    manifest_errors = validate_manifest(manifest)
    if manifest_errors:
        assessment.add(
            "FLINK_BASELINE_EVIDENCE_INVALID", "; ".join(manifest_errors)
        )
        return {
            "status": assessment.final_status(),
            "executable_eligible": False,
            "finding_codes": [finding["code"] for finding in assessment.findings],
            "findings": assessment.findings,
            "advisory_codes": assessment.advisories,
            "validation_evidence": {
                "design_contract_schema": contract.get("contract_schema"),
            },
        }

    _check_keys(
        contract,
        {
            "baseline_id",
            "contract_schema",
            "flink_version",
            "hudi_version",
            "identity",
            "ordering",
            "runtime",
            "safety",
            "source",
            "table",
            "write",
        },
        "contract",
        assessment,
    )
    contract_schema = contract.get("contract_schema")
    if (
        not isinstance(contract_schema, int)
        or isinstance(contract_schema, bool)
        or contract_schema not in SUPPORTED_CONTRACT_SCHEMAS
    ):
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "contract_schema must be 1 (append-only) or 2 (mutable COW)",
        )
    mutable_contract = contract_schema == MUTABLE_CONTRACT_SCHEMA_VERSION
    if not mutable_contract and "ordering" in contract:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "ordering is supported only by contract_schema 2",
        )
    if contract.get("baseline_id") != manifest.get("baseline_id"):
        assessment.add("FLINK_VERSION_UNVERIFIED", "The capability baseline does not match")
    if not _non_empty_string(contract.get("hudi_version")) or not _non_empty_string(
        contract.get("flink_version")
    ):
        assessment.add("FLINK_VERSION_REQUIRED", "Concrete Hudi and Flink versions are required")
    else:
        if contract["hudi_version"] != manifest["hudi"]["version"]:
            assessment.add("FLINK_VERSION_UNVERIFIED", "The Hudi version is outside the baseline")
        if not re.fullmatch(r"1\.20(?:\.\d+|\.x)?", contract["flink_version"]):
            assessment.add("FLINK_VERSION_UNVERIFIED", "The Flink version is outside the baseline")

    safety = _dict(contract.get("safety"), "safety", assessment)
    _check_keys(
        safety,
        {
            "external_catalog",
            "mutability",
            "replay_behavior",
            "table_lifecycle",
            "writer_model",
        },
        "safety",
        assessment,
    )
    lifecycle = safety.get("table_lifecycle")
    if lifecycle is None:
        assessment.add("FLINK_TABLE_LIFECYCLE_REQUIRED", "Table lifecycle is required")
    elif lifecycle == "existing":
        assessment.add(
            "FLINK_EXISTING_TABLE_DEFERRED",
            "The executable contracts support only a new Architect-managed table",
        )
    elif lifecycle != "new":
        assessment.add("FLINK_TABLE_LIFECYCLE_REQUIRED", "Table lifecycle is unresolved")
    writer_model = safety.get("writer_model")
    if writer_model is None:
        assessment.add("FLINK_WRITER_MODEL_UNRESOLVED", "Writer topology is required")
    elif writer_model == "multi_writer":
        assessment.add(
            "FLINK_MULTI_WRITER_REVIEW",
            "The executable contracts support only a confirmed single writer",
        )
    elif writer_model != "single_writer":
        assessment.add("FLINK_WRITER_MODEL_UNRESOLVED", "Writer topology is unresolved")
    external_catalog = safety.get("external_catalog")
    if external_catalog is True:
        assessment.add("FLINK_EXTERNAL_CATALOG_REVIEW", "Catalog composition is deferred")
    elif external_catalog is not False:
        assessment.add("FLINK_CATALOG_REQUIREMENT_UNRESOLVED", "Catalog requirement is unresolved")
    mutability = safety.get("mutability")
    if mutable_contract:
        if mutability != "mutable":
            assessment.add(
                "FLINK_MUTABILITY_REQUIRED",
                "contract_schema 2 requires mutable input",
            )
    elif mutability == "mutable":
        assessment.add(
            "FLINK_MUTABLE_COW_DEFERRED",
            "Mutable COW requires the explicit contract_schema 2 safety contract",
        )
    elif mutability != "append_only":
        assessment.add("FLINK_MUTABILITY_REQUIRED", "Append-only input must be confirmed")
    replay_behavior = safety.get("replay_behavior")
    if not _non_empty_string(replay_behavior):
        assessment.add("FLINK_REPLAY_BEHAVIOR_UNRESOLVED", "Replay behavior is unresolved")
    elif replay_behavior == "must_collapse" and not mutable_contract:
        assessment.add("FLINK_REPLAY_IDEMPOTENCE_DEFERRED", "Replay deduplication needs upsert")
    elif replay_behavior not in (
        {"must_collapse"} if mutable_contract else {"cannot_occur", "duplicates_acceptable"}
    ):
        assessment.add("FLINK_REPLAY_BEHAVIOR_UNRESOLVED", "Replay behavior is unresolved")

    table = _dict(contract.get("table"), "table", assessment)
    table_keys = {"columns", "name", "partition_fields", "path"}
    if mutable_contract:
        table_keys.add("partition_fields_mutable")
    _check_keys(table, table_keys, "table", assessment)
    table_name = table.get("name")
    table_path = table.get("path")
    if not _check_concrete_value(table_name) or any(
        not part for part in str(table_name).split(".")
    ):
        assessment.add("FLINK_LOAD_BEARING_VALUE_REQUIRED", "A concrete target table is required")
    if not _check_concrete_value(table_path):
        assessment.add("FLINK_LOAD_BEARING_VALUE_REQUIRED", "A concrete target path is required")
    elif path_error := _path_validation_error(table_path):
        assessment.add(
            "FLINK_LOAD_BEARING_VALUE_REQUIRED",
            path_error,
        )
    table_columns = _columns(
        table.get("columns"),
        "table.columns",
        manifest,
        assessment,
        reject_hudi_metadata_fields=True,
    )
    table_by_name = {column["name"]: column for column in table_columns}
    partition_fields = _string_list(
        table.get("partition_fields"), "table.partition_fields", assessment
    )
    if mutable_contract:
        if table.get("partition_fields_mutable") is True:
            assessment.add(
                "FLINK_MUTABLE_PARTITION_EVOLUTION_UNSUPPORTED",
                "The first mutable path does not move a key between partitions",
            )
        elif table.get("partition_fields_mutable") is not False:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "table.partition_fields_mutable must be explicitly false",
            )
    for field in partition_fields:
        if field not in table_by_name:
            assessment.add(
                "FLINK_PARTITION_FIELD_MISSING",
                f"Partition field {field!r} is absent from the physical schema",
            )
        elif _is_binary_routing_type(table_by_name[field]["type"], manifest):
            assessment.add(
                "FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED",
                f"Partition field {field!r} cannot use binary type "
                f"{table_by_name[field]['type']!r} until deterministic encoding is verified",
            )

    identity = _dict(contract.get("identity"), "identity", assessment)
    _check_keys(
        identity,
        {
            "auto_key_accepted",
            "mode",
            "record_key_fields",
            "record_key_option_fields",
        },
        "identity",
        assessment,
    )
    identity_mode = identity.get("mode")
    record_key_fields = _string_list(
        identity.get("record_key_fields"), "identity.record_key_fields", assessment
    )
    option_fields = _string_list(
        identity.get("record_key_option_fields"),
        "identity.record_key_option_fields",
        assessment,
    )
    raw_write = contract.get("write")
    raw_connector_options = (
        raw_write.get("connector_options") if isinstance(raw_write, dict) else None
    )
    if isinstance(raw_connector_options, dict):
        raw_record_key = raw_connector_options.get(
            "hoodie.datasource.write.recordkey.field"
        )
        if raw_record_key is not None:
            if not _non_empty_string(raw_record_key):
                assessment.add(
                    "FLINK_DESIGN_CONTRACT_INVALID",
                    "The record-key connector option must contain field names",
                )
            else:
                raw_option_fields = [field.strip() for field in raw_record_key.split(",")]
                if not all(raw_option_fields) or len(raw_option_fields) != len(
                    set(raw_option_fields)
                ):
                    assessment.add(
                        "FLINK_DESIGN_CONTRACT_INVALID",
                        "The record-key connector option must contain unique field names",
                    )
                else:
                    if option_fields and option_fields != raw_option_fields:
                        assessment.add(
                            "FLINK_DESIGN_CONTRACT_INVALID",
                            "Normalized and raw record-key option fields do not match",
                        )
                    option_fields = list(dict.fromkeys([*option_fields, *raw_option_fields]))
    for field in option_fields:
        if field not in table_by_name:
            assessment.add(
                "FLINK_RECORD_KEY_FIELD_MISSING",
                f"Record-key option field {field!r} is absent from the physical schema",
            )
    if identity_mode == "stable_key":
        if not mutable_contract:
            assessment.advise("FLINK_STABLE_KEY_NOT_IDEMPOTENT")
        if "auto_key_accepted" in identity:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "identity.auto_key_accepted applies only to auto_key mode",
            )
        if not record_key_fields:
            assessment.add("FLINK_PHYSICAL_SCHEMA_REQUIRED", "Stable-key fields are required")
        for field in record_key_fields:
            if field not in table_by_name:
                assessment.add(
                    "FLINK_RECORD_KEY_FIELD_MISSING",
                    f"Record-key field {field!r} is absent from the physical schema",
                )
            else:
                if table_by_name[field]["nullable"]:
                    assessment.add(
                        "FLINK_RECORD_KEY_NULLABLE",
                        f"Record-key field {field!r} must be NOT NULL",
                    )
                if _is_binary_routing_type(table_by_name[field]["type"], manifest):
                    assessment.add(
                        "FLINK_BINARY_ROUTING_FIELD_UNSUPPORTED",
                        f"Record-key field {field!r} cannot use binary type "
                        f"{table_by_name[field]['type']!r} until deterministic encoding "
                        "is verified",
                    )
        if option_fields:
            assessment.add(
                "FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT",
                "The canonical stable-key DDL uses PRIMARY KEY syntax, not a duplicate option",
            )
    elif identity_mode == "auto_key":
        if record_key_fields or option_fields:
            assessment.add(
                "FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT",
                "The auto-key path cannot contain primary-key or record-key fields",
            )
        accepted = identity.get("auto_key_accepted")
        if mutable_contract:
            assessment.add(
                "FLINK_MUTABLE_AUTO_KEY_UNSUPPORTED",
                "Mutable upsert/delete requires a stable non-null record key",
            )
        elif accepted is True:
            assessment.advise("FLINK_AUTO_KEY_DURABILITY")
        elif accepted is False:
            assessment.add("FLINK_AUTO_KEY_DECLINED", "Auto-generated keys were declined")
        else:
            assessment.add(
                "FLINK_AUTO_KEY_ACCEPTANCE_REQUIRED",
                "Auto-generated key durability must be explicitly accepted",
            )
    else:
        assessment.add("FLINK_DESIGN_CONTRACT_INVALID", "identity.mode is required")

    ordering_fields: list[str] = []
    if mutable_contract:
        ordering = _dict(contract.get("ordering"), "ordering", assessment)
        _check_keys(ordering, {"fields", "mode"}, "ordering", assessment)
        ordering_fields = _string_list(
            ordering.get("fields"), "ordering.fields", assessment
        )
        if ordering.get("mode") != "event_time":
            assessment.add(
                "FLINK_MUTABLE_ORDERING_FIELD_REQUIRED",
                "The first mutable path requires event_time ordering",
            )
        if len(ordering_fields) != 1:
            assessment.add(
                "FLINK_MUTABLE_ORDERING_FIELD_REQUIRED",
                "Exactly one ordering field is required",
            )
        for field in ordering_fields:
            column = table_by_name.get(field)
            if column is None:
                assessment.add(
                    "FLINK_MUTABLE_ORDERING_FIELD_INVALID",
                    f"Ordering field {field!r} is absent from the physical schema",
                )
            elif column["nullable"]:
                assessment.add(
                    "FLINK_MUTABLE_ORDERING_FIELD_INVALID",
                    f"Ordering field {field!r} must be NOT NULL",
                )
            elif column["type"] != "BIGINT" and re.fullmatch(
                r"TIMESTAMP\([0-6]\)", column["type"]
            ) is None:
                assessment.add(
                    "FLINK_MUTABLE_ORDERING_FIELD_INVALID",
                    f"Ordering field {field!r} must use BIGINT or TIMESTAMP(p<=6)",
                )

    source = _dict(contract.get("source"), "source", assessment)
    source_keys = {"changelog_mode", "columns", "table"}
    if mutable_contract:
        source_keys.update({"delete_payload", "emits_deletes", "primary_key_fields"})
    _check_keys(source, source_keys, "source", assessment)
    source_name = source.get("table")
    if not _check_concrete_value(source_name) or any(
        not part for part in str(source_name).split(".")
    ):
        assessment.add("FLINK_SOURCE_CONTRACT_REQUIRED", "A concrete source table is required")
    source_columns = _columns(source.get("columns"), "source.columns", manifest, assessment)
    if not source_columns:
        assessment.add(
            "FLINK_SOURCE_CONTRACT_REQUIRED",
            "The source contract must include a physical schema",
        )
    source_by_name = {column["name"]: column for column in source_columns}
    if source_columns:
        for name, target_column in table_by_name.items():
            source_column = source_by_name.get(name)
            if source_column is None or source_column != target_column:
                assessment.add(
                    "FLINK_SOURCE_SCHEMA_MISMATCH",
                    f"Source field {name!r} must match the target name, type, and nullability",
                )
    if mutable_contract:
        source_primary_key_fields = _string_list(
            source.get("primary_key_fields"), "source.primary_key_fields", assessment
        )
        if not source_primary_key_fields:
            assessment.add(
                "FLINK_SOURCE_CONTRACT_REQUIRED",
                "The mutable source must declare its primary-key fields",
            )
        elif record_key_fields and source_primary_key_fields != record_key_fields:
            assessment.add(
                "FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT",
                "Source primary-key fields must exactly match target record-key fields",
            )
        for field in source_primary_key_fields:
            source_column = source_by_name.get(field)
            if source_column is None or source_column["nullable"]:
                assessment.add(
                    "FLINK_PRIMARY_KEY_RECORD_KEY_CONFLICT",
                    f"Source primary-key field {field!r} must exist and be NOT NULL",
                )
        emits_deletes = source.get("emits_deletes")
        if not isinstance(emits_deletes, bool):
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "source.emits_deletes must be explicitly true or false",
            )
        delete_payload = source.get("delete_payload")
        if emits_deletes is True and delete_payload != "FULL_ROW":
            assessment.add(
                "FLINK_MUTABLE_DELETE_PAYLOAD_UNSUPPORTED",
                "Deletes must carry key, ordering, partition, and projected row values",
            )
        elif emits_deletes is False and delete_payload != "NOT_APPLICABLE":
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "source.delete_payload must be NOT_APPLICABLE when deletes are absent",
            )

    changelog_mode = source.get("changelog_mode")
    if not _non_empty_string(changelog_mode) or changelog_mode == "UNKNOWN":
        assessment.add(
            "FLINK_SOURCE_CONTRACT_REQUIRED",
            "The source changelog mode must be explicitly confirmed",
        )
    elif mutable_contract and changelog_mode != "UPSERT":
        assessment.add(
            "FLINK_MUTABLE_SOURCE_CHANGELOG_UNSUPPORTED",
            "The mutable source must declare normalized UPSERT changelog (I/UA/D, no UB)",
        )
    elif not mutable_contract and changelog_mode != "INSERT_ONLY":
        assessment.add(
            "FLINK_SOURCE_CHANGELOG_NOT_APPEND_ONLY",
            "The PR2 source contract must be INSERT_ONLY",
        )

    write = _dict(contract.get("write"), "write", assessment)
    write_keys = {"connector_options", "operation", "table_type"}
    if mutable_contract:
        write_keys.update({"changelog_enabled", "index", "record_merge_mode"})
    else:
        write_keys.add("insert_cluster")
    _check_keys(
        write,
        write_keys,
        "write",
        assessment,
    )
    table_type = write.get("table_type")
    operation = write.get("operation")
    if table_type is None or operation is None:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "write.table_type and write.operation must be explicit",
        )
    elif mutable_contract and (
        table_type != "COPY_ON_WRITE" or operation != "upsert"
    ):
        assessment.add(
            "FLINK_PR3_WRITE_PATH_UNSUPPORTED",
            "The first mutable path supports only COPY_ON_WRITE upsert",
        )
    elif not mutable_contract and (
        table_type != "COPY_ON_WRITE" or operation != "insert"
    ):
        assessment.add(
            "FLINK_PR2_WRITE_PATH_UNSUPPORTED",
            "PR2 supports only COPY_ON_WRITE with write.operation=insert",
        )
    if mutable_contract:
        if write.get("record_merge_mode") != "EVENT_TIME_ORDERING":
            assessment.add(
                "FLINK_PR3_WRITE_PATH_UNSUPPORTED",
                "write.record_merge_mode must be EVENT_TIME_ORDERING",
            )
        if write.get("changelog_enabled") is not False:
            assessment.add(
                "FLINK_PR3_WRITE_PATH_UNSUPPORTED",
                "write.changelog_enabled must be explicitly false",
            )
        index = _dict(write.get("index"), "write.index", assessment)
        _check_keys(
            index,
            {"bootstrap_enabled", "global", "state_ttl_days", "type"},
            "write.index",
            assessment,
        )
        if index.get("type") != "FLINK_STATE" or index.get("global") is not True:
            assessment.add(
                "FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED",
                "The first mutable path requires global FLINK_STATE index",
            )
        state_ttl = index.get("state_ttl_days")
        if (
            not isinstance(state_ttl, (int, float))
            or isinstance(state_ttl, bool)
            or state_ttl != 0
        ):
            assessment.add(
                "FLINK_MUTABLE_INDEX_CONFIGURATION_UNSUPPORTED",
                "write.index.state_ttl_days must be 0 (no state expiry)",
            )
        if index.get("bootstrap_enabled") is not True:
            assessment.add(
                "FLINK_MUTABLE_INDEX_BOOTSTRAP_REQUIRED",
                "FLINK_STATE must bootstrap existing keys after state loss or cold restart",
            )
    else:
        insert_cluster = write.get("insert_cluster")
        if insert_cluster is True:
            assessment.add(
                "FLINK_APPEND_MODE_CLUSTERING_ENABLED",
                "COW insert is append mode only with write.insert.cluster=false",
            )
        elif insert_cluster is not False:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "write.insert_cluster must be explicitly true or false",
            )
    connector_options = write.get("connector_options")
    if not isinstance(connector_options, dict):
        assessment.add("FLINK_DESIGN_CONTRACT_INVALID", "write.connector_options must be an object")
    else:
        unknown_options = sorted(set(connector_options) - supported_option_keys(manifest))
        if unknown_options:
            assessment.add(
                "FLINK_OPTION_NOT_VERIFIED",
                f"Options outside the pinned allowlist: {unknown_options}",
            )
        raw_insert_cluster = connector_options.get("write.insert.cluster")
        if not mutable_contract and "write.insert.cluster" in connector_options and not (
            raw_insert_cluster is False
            or (
                isinstance(raw_insert_cluster, str)
                and raw_insert_cluster.lower() == "false"
            )
        ):
            assessment.add(
                "FLINK_APPEND_MODE_CLUSTERING_ENABLED",
                "A connector-option override makes write.insert.cluster non-false",
            )
        raw_table_type = connector_options.get("table.type")
        raw_operation = connector_options.get("write.operation")
        expected_operation = "upsert" if mutable_contract else "insert"
        if (
            "table.type" in connector_options
            and raw_table_type != "COPY_ON_WRITE"
        ) or (
            "write.operation" in connector_options
            and raw_operation != expected_operation
        ):
            assessment.add(
                (
                    "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                    if mutable_contract
                    else "FLINK_PR2_WRITE_PATH_UNSUPPORTED"
                ),
                "A connector-option override leaves the bounded COW path",
            )
        verified_overrides = sorted(set(connector_options) - set(unknown_options))
        if verified_overrides:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "Connector settings must use canonical structured fields; "
                f"pass-through overrides are not rendered: {verified_overrides}",
            )

    runtime = _dict(contract.get("runtime"), "runtime", assessment)
    _check_keys(
        runtime,
        {
            "checkpoint_interval_ms",
            "checkpointing_enabled",
            "execution_mode",
            "target_commit_freshness_ms",
        },
        "runtime",
        assessment,
    )
    execution_mode = runtime.get("execution_mode")
    if execution_mode is None:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID", "runtime.execution_mode must be explicit"
        )
    elif execution_mode != "STREAMING":
        assessment.add(
            (
                "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                if mutable_contract
                else "FLINK_PR2_WRITE_PATH_UNSUPPORTED"
            ),
            "The executable path supports streaming execution",
        )
    checkpointing_enabled = runtime.get("checkpointing_enabled")
    if checkpointing_enabled is False:
        assessment.add("FLINK_CHECKPOINTING_REQUIRED", "Streaming commits require checkpointing")
    elif checkpointing_enabled is not True:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "runtime.checkpointing_enabled must be explicitly true or false",
        )
    interval = runtime.get("checkpoint_interval_ms")
    if not isinstance(interval, int) or isinstance(interval, bool) or interval <= 0:
        assessment.add(
            "FLINK_CHECKPOINT_INTERVAL_REQUIRED", "A positive checkpoint interval is required"
        )
    elif interval < manifest["runtime_constraints"]["checkpoint_interval_min_ms"]:
        assessment.add(
            "FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED",
            "The checkpoint interval must be at least "
            f"{manifest['runtime_constraints']['checkpoint_interval_min_ms']} ms for the "
            "bounded executable path",
        )
    target_freshness = runtime.get("target_commit_freshness_ms")
    if target_freshness is not None and (
        not isinstance(target_freshness, int)
        or isinstance(target_freshness, bool)
        or target_freshness <= 0
    ):
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "runtime.target_commit_freshness_ms must be a positive integer when supplied",
        )

    status = assessment.final_status()
    executable = status == "CONFIG_VALIDATED"
    result: dict[str, Any] = {
        "status": status,
        "executable_eligible": executable,
        "finding_codes": [finding["code"] for finding in assessment.findings],
        "findings": assessment.findings,
        "advisory_codes": assessment.advisories,
        "validation_evidence": validation_evidence(manifest, contract_schema),
    }
    if executable:
        result["artifacts"] = _render_sql(
            contract, table_columns, partition_fields, record_key_fields
        )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--emit-sql",
        action="store_true",
        help="Print combined SQL instead of the JSON result; requires CONFIG_VALIDATED.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        contract = json.loads(args.input.read_text(encoding="utf-8"))
        manifest = load_manifest(args.manifest)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Invalid Flink design input: {error}", file=sys.stderr)
        return 2
    if not isinstance(contract, dict):
        print("Invalid Flink design input: top-level value must be an object", file=sys.stderr)
        return 2

    result = assess_design(contract, manifest)
    if args.emit_sql:
        if not result["executable_eligible"]:
            print(json.dumps(result, sort_keys=True), file=sys.stderr)
            return 1
        print(result["artifacts"]["combined_sql"])
        return 0
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["executable_eligible"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
