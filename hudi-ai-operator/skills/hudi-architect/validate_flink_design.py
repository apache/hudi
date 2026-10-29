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
from urllib.parse import SplitResult, parse_qsl, unquote, urlsplit

from flink_writer_contract import assess_writer_contract
from validate_flink_capabilities import (
    DEFAULT_MANIFEST,
    load_manifest,
    supported_option_keys,
    validate_manifest,
    validation_evidence,
)

APPEND_CONTRACT_SCHEMA_VERSION = 1
MUTABLE_CONTRACT_SCHEMA_VERSION = 2
MUTABLE_MOR_CONTRACT_SCHEMA_VERSION = 3
CATALOG_CONTRACT_SCHEMA_VERSION = 1
CATALOG_MANAGED_RESOLUTION_MODES = frozenset(
    {"hudi_dfs_catalog", "hudi_hms_catalog"}
)
SUPPORTED_CONTRACT_SCHEMAS = {
    APPEND_CONTRACT_SCHEMA_VERSION,
    MUTABLE_CONTRACT_SCHEMA_VERSION,
    MUTABLE_MOR_CONTRACT_SCHEMA_VERSION,
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
SENSITIVE_CATALOG_KEY = re.compile(
    r"(?:access[_-]?key|api[_-]?key|credential|iam|keytab|password|secret|token|username)",
    re.IGNORECASE,
)
SENSITIVE_URI_ASSIGNMENT = re.compile(
    r"(?:^|[/;,&])(?:access[_-]?key|api[_-]?key|credential|iam|keytab|password|secret|sig|signature|token|username)\s*=",
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
    "FLINK_CATALOG_COMPOSITION_CONFLICT": "BLOCKED",
    "FLINK_CATALOG_CONTRACT_INVALID": "INCOMPLETE",
    "FLINK_CATALOG_CONTRACT_REQUIRED": "INCOMPLETE",
    "FLINK_CATALOG_CREDENTIAL_REJECTED": "BLOCKED",
    "FLINK_CATALOG_LIFECYCLE_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_CATALOG_MODE_UNSUPPORTED": "BLOCKED",
    "FLINK_CATALOG_PATH_CONFLICT": "BLOCKED",
    "FLINK_CHECKPOINTING_REQUIRED": "BLOCKED",
    "FLINK_CHECKPOINT_INTERVAL_REQUIRED": "INCOMPLETE",
    "FLINK_CHECKPOINT_INTERVAL_UNSUPPORTED": "BLOCKED",
    "FLINK_DESIGN_CONTRACT_INVALID": "INCOMPLETE",
    "FLINK_EXISTING_TABLE_DEFERRED": "BLOCKED",
    "FLINK_EXISTING_EVIDENCE_CONFLICT": "REVIEW_REQUIRED",
    "FLINK_EXISTING_EVIDENCE_INVALID": "INCOMPLETE",
    "FLINK_EXISTING_EVIDENCE_REQUIRED": "INCOMPLETE",
    "FLINK_EXISTING_ENGINE_TAKEOVER_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_CATALOG_MUTATION_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_SCHEMA_CONFLICT": "BLOCKED",
    "FLINK_EXISTING_SCHEMA_EVOLUTION_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_TABLE_PROPERTY_CONFLICT": "BLOCKED",
    "FLINK_EXISTING_TABLE_PROPERTY_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_TABLE_VERSION_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_TIMESTAMP_PARTITION_LAYOUT_UNVERIFIED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_TIMELINE_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_WRITER_TRANSITION_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_EXISTING_WRITER_CONFIG_CONFLICT": "REVIEW_REQUIRED",
    "FLINK_EXTERNAL_CATALOG_REVIEW": "REVIEW_REQUIRED",
    "FLINK_GLUE_CATALOG_UNVERIFIED": "REVIEW_REQUIRED",
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
    "FLINK_MOR_APPEND_ONLY_UNSUPPORTED": "BLOCKED",
    "FLINK_MOR_CLUSTERING_UNSUPPORTED": "BLOCKED",
    "FLINK_MOR_COMPACTION_CONFIGURATION_UNSUPPORTED": "BLOCKED",
    "FLINK_MOR_COMPACTION_OWNER_REQUIRED": "INCOMPLETE",
    "FLINK_MOR_COMPACTION_TUNING_UNSUPPORTED": "BLOCKED",
    "FLINK_MOR_QUERY_REQUIREMENT_UNRESOLVED": "INCOMPLETE",
    "FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED": "BLOCKED",
    "FLINK_MOR_CATALOG_NAMING_UNVERIFIED": "REVIEW_REQUIRED",
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
    "FLINK_WRITER_CONTRACT_REQUIRED": "INCOMPLETE",
    "FLINK_WRITER_CONTRACT_INVALID": "INCOMPLETE",
    "FLINK_WRITER_OWNERSHIP_UNRESOLVED": "REVIEW_REQUIRED",
    "FLINK_WRITER_CONTRACT_CONFLICT": "REVIEW_REQUIRED",
    "FLINK_WRITER_COMBINATION_UNSUPPORTED": "BLOCKED",
    "FLINK_PR2_WRITE_PATH_UNSUPPORTED": "BLOCKED",
    "FLINK_PR3_WRITE_PATH_UNSUPPORTED": "BLOCKED",
    "FLINK_PR4_WRITE_PATH_UNSUPPORTED": "BLOCKED",
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


def _check_catalog_keys(
    value: dict[str, Any], allowed: set[str], field: str, assessment: Assessment
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
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


def _has_sensitive_catalog_key(value: Any) -> bool:
    if isinstance(value, dict):
        return any(
            SENSITIVE_CATALOG_KEY.search(str(key)) is not None
            or _has_sensitive_catalog_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_has_sensitive_catalog_key(item) for item in value)
    return False


def _metastore_uris_valid(value: Any) -> bool:
    if not _check_concrete_value(value):
        return False
    for raw_uri in value.split(","):
        uri = raw_uri.strip()
        try:
            parsed = urlsplit(uri)
            port = parsed.port
        except ValueError:
            return False
        if (
            parsed.scheme.lower() != "thrift"
            or not parsed.hostname
            or port is None
            or not 1 <= port <= 65535
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
        ):
            return False
    return True


def _metastore_uris_have_credentials(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    for raw_uri in value.split(","):
        if SENSITIVE_URI_ASSIGNMENT.search(unquote(raw_uri)) is not None:
            return True
        try:
            parsed = urlsplit(raw_uri.strip())
        except ValueError:
            continue
        if parsed.username is not None or parsed.password is not None:
            return True
        parameters = [
            *parse_qsl(parsed.query, keep_blank_values=True),
            *parse_qsl(parsed.fragment, keep_blank_values=True),
        ]
        if any(_is_sensitive_query_key(key) for key, _ in parameters):
            return True
    return False


def _catalog_profile_id(resolution_mode: Any, external_discovery: Any) -> str | None:
    if not isinstance(resolution_mode, str) or not isinstance(external_discovery, str):
        return None
    profiles = {
        ("direct_sql", "none"): "direct_sql_none",
        ("direct_sql", "hive_metastore"): "direct_sql_hive_metastore",
        ("hudi_dfs_catalog", "none"): "hudi_dfs_catalog_none",
        ("hudi_dfs_catalog", "hive_metastore"):
            "hudi_dfs_catalog_hive_metastore",
        ("hudi_hms_catalog", "hive_metastore"):
            "hudi_hms_catalog_hive_metastore",
    }
    return profiles.get((resolution_mode, external_discovery))


def _mor_sync_strategy(contract: dict[str, Any]) -> str | None:
    raw_read = contract.get("read")
    read = raw_read if isinstance(raw_read, dict) else {}
    raw_modes = read.get("query_modes")
    modes = (
        set(raw_modes)
        if isinstance(raw_modes, list)
        and all(isinstance(mode, str) for mode in raw_modes)
        else set()
    )
    if modes == {"snapshot"}:
        return "RT"
    if modes == {"read_optimized"}:
        return "RO"
    if modes == {"snapshot", "read_optimized"}:
        return "ALL"
    return None


def _registered_table_names(
    table_name: Any, table_type: Any, strategy: str | None
) -> list[str]:
    if not _non_empty_string(table_name):
        return []
    base_name = table_name.split(".")[-1]
    if table_type != "MERGE_ON_READ":
        return [base_name]
    if strategy == "RO":
        return [base_name]
    if strategy == "RT":
        return [base_name]
    if strategy == "ALL":
        return [f"{base_name}_ro", f"{base_name}_rt", base_name]
    return []


def _assess_catalog(
    contract: dict[str, Any],
    manifest: dict[str, Any],
    assessment: Assessment,
    *,
    external_catalog: Any,
    expected_table_lifecycle: str,
) -> dict[str, Any]:
    """Validate the orthogonal PR6 catalog contract without selecting it."""

    explicit = "catalog" in contract
    raw_catalog = contract.get("catalog")
    if not explicit:
        if external_catalog is True:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_REQUIRED",
                "safety.external_catalog=true requires an explicit catalog contract",
            )
        legacy_profile = external_catalog is False
        resolution = "direct_sql" if legacy_profile else "unresolved"
        discovery = "none" if legacy_profile else "unresolved"
        normalized_sync_owner = "none" if legacy_profile else "unresolved"
        normalized_lifecycle_owner = "application" if legacy_profile else "unresolved"
        return {
            "contract_schema": CATALOG_CONTRACT_SCHEMA_VERSION,
            "profile": "direct_sql_none" if legacy_profile else "unresolved",
            "resolution_mode": resolution,
            "external_discovery": discovery,
            "sync_owner": normalized_sync_owner,
            "data_lifecycle_owner": normalized_lifecycle_owner,
            "catalog_setup_sql": "",
            "effective_configuration": {
                "catalog_freshness": "not_applicable",
                "data_lifecycle_owner": normalized_lifecycle_owner,
                "external_discovery": discovery,
                "resolution_mode": resolution,
                "sync_owner": normalized_sync_owner,
            },
            "deployment_checks": [],
        }

    if isinstance(raw_catalog, dict):
        catalog = raw_catalog
    else:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID", "catalog must be an object"
        )
        catalog = {}
    _check_catalog_keys(
        catalog,
        {
            "catalog_name",
            "catalog_path",
            "contract_schema",
            "data_lifecycle_owner",
            "database",
            "external_discovery",
            "hive_configuration",
            "mor_sync_strategy",
            "resolution_mode",
            "sync_owner",
            "table_external",
        },
        "catalog",
        assessment,
    )
    if _has_sensitive_catalog_key(catalog):
        assessment.add(
            "FLINK_CATALOG_CREDENTIAL_REJECTED",
            "Catalog contracts must reference credential-free deployment configuration",
        )
    if catalog.get("contract_schema") != CATALOG_CONTRACT_SCHEMA_VERSION:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            f"catalog.contract_schema must be {CATALOG_CONTRACT_SCHEMA_VERSION}",
        )

    resolution_mode = catalog.get("resolution_mode")
    external_discovery = catalog.get("external_discovery")
    sync_owner = catalog.get("sync_owner")
    lifecycle_owner = catalog.get("data_lifecycle_owner")
    resolution_modes = {
        "direct_sql",
        "hudi_dfs_catalog",
        "hudi_hms_catalog",
    }
    discovery_modes = {"none", "hive_metastore", "aws_glue"}
    sync_owners = {"none", "flink_sink", "hudi_hms_catalog"}
    lifecycle_owners = {"application", "catalog"}
    resolution_valid = isinstance(resolution_mode, str) and resolution_mode in resolution_modes
    discovery_valid = (
        isinstance(external_discovery, str)
        and external_discovery in discovery_modes
    )
    sync_owner_valid = isinstance(sync_owner, str) and sync_owner in sync_owners
    lifecycle_owner_valid = (
        isinstance(lifecycle_owner, str) and lifecycle_owner in lifecycle_owners
    )
    catalog_managed_resolution = (
        resolution_valid and resolution_mode in CATALOG_MANAGED_RESOLUTION_MODES
    )
    if not _non_empty_string(resolution_mode):
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.resolution_mode must be explicit",
        )
    elif not resolution_valid:
        assessment.add(
            "FLINK_CATALOG_MODE_UNSUPPORTED",
            "catalog.resolution_mode is outside the Hudi 1.2.0 PR6 profiles",
        )
    if not _non_empty_string(external_discovery):
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.external_discovery must be explicit",
        )
    elif not discovery_valid:
        assessment.add(
            "FLINK_CATALOG_MODE_UNSUPPORTED",
            "catalog.external_discovery is unsupported",
        )
    if not _non_empty_string(sync_owner):
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.sync_owner must be explicit",
        )
    elif not sync_owner_valid:
        assessment.add(
            "FLINK_CATALOG_MODE_UNSUPPORTED", "catalog.sync_owner is unsupported"
        )
    if not _non_empty_string(lifecycle_owner):
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.data_lifecycle_owner must be explicit",
        )
    elif not lifecycle_owner_valid:
        assessment.add(
            "FLINK_CATALOG_MODE_UNSUPPORTED",
            "catalog.data_lifecycle_owner is unsupported",
        )
    if external_discovery == "aws_glue":
        assessment.add(
            "FLINK_GLUE_CATALOG_UNVERIFIED",
            "AWS Glue is present in Hudi 1.2.0 but is not executable without a "
            "Flink-specific fixture",
        )

    profile = _catalog_profile_id(resolution_mode, external_discovery)
    if profile is None:
        if external_discovery == "aws_glue":
            profile = "aws_glue_unverified"
        elif resolution_valid and discovery_valid:
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                "The requested table-resolution and external-discovery composition is unsupported",
            )
            profile = "unsupported"
        else:
            profile = "unresolved"

    expected_external_gate = (
        resolution_mode != "direct_sql" or external_discovery != "none"
    )
    if (
        resolution_valid
        and discovery_valid
        and isinstance(external_catalog, bool)
        and external_catalog != expected_external_gate
    ):
        assessment.add(
            "FLINK_CATALOG_COMPOSITION_CONFLICT",
            "safety.external_catalog conflicts with the explicit catalog composition",
        )

    expected_owners = {
        "direct_sql_none": ("none", "application"),
        "direct_sql_hive_metastore": ("flink_sink", "application"),
        "hudi_dfs_catalog_none": ("none", "catalog"),
        "hudi_dfs_catalog_hive_metastore": ("flink_sink", "catalog"),
        "hudi_hms_catalog_hive_metastore": ("hudi_hms_catalog", "application"),
    }
    if profile in expected_owners:
        expected_sync_owner, expected_lifecycle_owner = expected_owners[profile]
        if sync_owner_valid and sync_owner != expected_sync_owner:
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                f"{profile} requires sync_owner={expected_sync_owner}",
            )
        if lifecycle_owner is None or (
            isinstance(lifecycle_owner, str) and not lifecycle_owner.strip()
        ):
            assessment.add(
                "FLINK_CATALOG_LIFECYCLE_UNRESOLVED",
                f"{profile} requires an explicit data lifecycle owner",
            )
        elif lifecycle_owner_valid and lifecycle_owner != expected_lifecycle_owner:
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                f"{profile} requires data_lifecycle_owner={expected_lifecycle_owner}",
            )

    catalog_name = catalog.get("catalog_name")
    database = catalog.get("database")
    catalog_path = catalog.get("catalog_path")
    if resolution_mode == "direct_sql":
        if "catalog_name" in catalog or "catalog_path" in catalog or "table_external" in catalog:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "Direct SQL forbids catalog_name, catalog_path, and table_external",
            )
    else:
        if not _check_concrete_value(catalog_name):
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "Catalog resolution requires a concrete catalog_name",
            )
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", catalog_name) is None:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "catalog.catalog_name must be a simple Flink SQL identifier",
            )
    if resolution_mode != "direct_sql" or external_discovery == "hive_metastore":
        if not _check_concrete_value(database):
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "Catalog or Hive metastore composition requires a concrete database",
            )
        elif re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database) is None:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "catalog.database must be a simple Flink SQL identifier",
            )
    elif "database" in catalog:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.database is not applicable to direct SQL without external discovery",
        )

    raw_table = contract.get("table")
    table: dict[str, Any] = raw_table if isinstance(raw_table, dict) else {}
    table_name = table.get("name")
    table_path = table.get("path")
    if catalog_managed_resolution and (
        not _non_empty_string(table_name) or "." in table_name
    ):
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "Catalog-managed table.name must be an unqualified identifier; catalog and "
            "database are separate fields",
        )
    if resolution_mode == "hudi_dfs_catalog":
        if not _check_concrete_value(catalog_path):
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "DFS catalog resolution requires a concrete catalog_path",
            )
        elif path_error := _path_validation_error(catalog_path):
            assessment.add("FLINK_CATALOG_CREDENTIAL_REJECTED", path_error)
        if all(
            _check_concrete_value(value)
            for value in (catalog_path, database, table_name)
        ):
            expected_path = f"{catalog_path}/{database}/{table_name}"
            if table_path != expected_path:
                assessment.add(
                    "FLINK_CATALOG_PATH_CONFLICT",
                    "The desired table path must exactly equal "
                    "${catalog.path}/${database}/${table.name}",
                )
        if "table_external" in catalog:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "catalog.table_external applies only to hudi_hms_catalog",
            )
    elif "catalog_path" in catalog:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.catalog_path applies only to hudi_dfs_catalog",
        )

    if resolution_mode == "hudi_hms_catalog":
        table_external = catalog.get("table_external")
        if "table_external" not in catalog:
            assessment.add(
                "FLINK_CATALOG_LIFECYCLE_UNRESOLVED",
                "The first HMS Catalog path requires table.external=true and "
                "application-owned data",
            )
        elif not isinstance(table_external, bool):
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "catalog.table_external must be explicitly true or false",
            )
        elif table_external is False:
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                "The bounded HMS Catalog path does not support managed tables; "
                "table.external must be true",
            )
    elif "table_external" in catalog:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.table_external applies only to hudi_hms_catalog",
        )

    raw_hive = catalog.get("hive_configuration")
    hive_configuration: dict[str, Any] = {}
    if external_discovery == "hive_metastore":
        if isinstance(raw_hive, dict):
            hive_configuration = raw_hive
        else:
            assessment.add(
                "FLINK_CATALOG_CONTRACT_INVALID",
                "catalog.hive_configuration must be an object",
            )
        _check_catalog_keys(
            hive_configuration,
            {"location", "source"},
            "catalog.hive_configuration",
            assessment,
        )
        source = hive_configuration.get("source")
        location = hive_configuration.get("location")
        if resolution_mode == "hudi_hms_catalog":
            if source == "hive_conf_dir":
                if not _check_concrete_value(location) or not Path(location).is_absolute():
                    assessment.add(
                        "FLINK_CATALOG_CONTRACT_INVALID",
                        "hive_conf_dir requires an absolute credential-free location",
                    )
                elif _path_validation_error(location) is not None:
                    assessment.add(
                        "FLINK_CATALOG_CREDENTIAL_REJECTED",
                        "hive_conf_dir must not contain credentials or secret parameters",
                    )
            elif source == "classpath":
                if "location" in hive_configuration:
                    assessment.add(
                        "FLINK_CATALOG_CONTRACT_INVALID",
                        "Classpath Hive configuration must not include a location",
                    )
            else:
                assessment.add(
                    "FLINK_CATALOG_CONTRACT_INVALID",
                    "HMS Catalog supports hive_conf_dir or classpath Hive configuration",
                )
        else:
            if _metastore_uris_have_credentials(location):
                assessment.add(
                    "FLINK_CATALOG_CREDENTIAL_REJECTED",
                    "Hive metastore URIs must not contain credentials or secret parameters",
                )
            if source != "metastore_uris" or not _metastore_uris_valid(location):
                assessment.add(
                    "FLINK_CATALOG_CONTRACT_INVALID",
                    "Direct and DFS sink-owned Hive sync requires credential-free thrift "
                    "metastore_uris",
                )
    elif "hive_configuration" in catalog:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.hive_configuration is forbidden when external_discovery=none",
        )

    mutable_mor = contract.get("contract_schema") == MUTABLE_MOR_CONTRACT_SCHEMA_VERSION
    strategy = catalog.get("mor_sync_strategy")
    expected_strategy = _mor_sync_strategy(contract) if mutable_mor else None
    if mutable_mor and external_discovery == "hive_metastore":
        if strategy != expected_strategy:
            assessment.add(
                "FLINK_MOR_CATALOG_NAMING_UNVERIFIED",
                "MOR sync strategy must be derived from the declared query modes",
            )
    elif "mor_sync_strategy" in catalog:
        assessment.add(
            "FLINK_CATALOG_CONTRACT_INVALID",
            "catalog.mor_sync_strategy applies only to mutable MOR with Hive discovery",
        )

    raw_write = contract.get("write")
    write = raw_write if isinstance(raw_write, dict) else {}
    raw_options = write.get("connector_options")
    connector_options = raw_options if isinstance(raw_options, dict) else {}
    if _has_sensitive_catalog_key(connector_options):
        assessment.add(
            "FLINK_CATALOG_CREDENTIAL_REJECTED",
            "Connector options must not contain catalog credentials",
        )
    if (
        connector_options.get("hive_sync.skip_ro_suffix") is True
        or connector_options.get("hive_sync.skip_ro_suffix") in ("true", "TRUE")
        or connector_options.get("hoodie.meta.sync.sync_snapshot_with_table_name")
        is False
        or connector_options.get("hoodie.meta.sync.sync_snapshot_with_table_name")
        in ("false", "FALSE")
        or (
            "hive_sync.table.strategy" in connector_options
            and connector_options.get("hive_sync.table.strategy") != expected_strategy
        )
    ):
        assessment.add(
            "FLINK_MOR_CATALOG_NAMING_UNVERIFIED",
            "Custom MOR catalog naming is outside the fixed PR6 naming contract",
        )

    if expected_table_lifecycle == "existing" and profile != "direct_sql_none":
        assessment.add(
            "FLINK_EXISTING_CATALOG_MUTATION_UNVERIFIED",
            "Existing-table registration, alteration, and catalog takeover remain assessment-only",
        )

    catalog_sql = ""
    if catalog_managed_resolution:
        options: list[tuple[str, Any]] = [("type", "hudi")]
        if resolution_mode == "hudi_dfs_catalog":
            options.extend([("mode", "dfs"), ("catalog.path", catalog_path)])
        else:
            options.extend([("mode", "hms"), ("table.external", "true")])
            if hive_configuration.get("source") == "hive_conf_dir":
                options.append(("hive.conf.dir", hive_configuration.get("location")))
        options.append(("default-database", database))
        if all(_check_concrete_value(item) for item in (catalog_name, database)) and all(
            _check_concrete_value(value) for _, value in options
        ):
            option_sql = ",\n".join(
                f"  {_sql_string(key)} = {_sql_string(str(value))}"
                for key, value in options
            )
            catalog_sql = (
                f"CREATE CATALOG {_quote_identifier(str(catalog_name))} WITH "
                f"(\n{option_sql}\n);\n"
                f"USE CATALOG {_quote_identifier(str(catalog_name))};\n"
                f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(str(database))};\n"
                f"USE {_quote_identifier(str(database))};"
            )

    table_type = write.get("table_type")
    registered_names = _registered_table_names(table_name, table_type, strategy)
    effective_configuration: dict[str, Any] = {
        "catalog_freshness": (
            "post_commit_eventual"
            if external_discovery == "hive_metastore"
            else "not_applicable"
        ),
        "data_lifecycle_owner": lifecycle_owner,
        "external_discovery": external_discovery,
        "resolution_mode": resolution_mode,
        "sync_owner": sync_owner,
    }
    if external_discovery == "hive_metastore":
        effective_configuration["hive_sync"] = {
            "database": database,
            "enabled": True,
            "ignore_exceptions": False,
            "metastore_uris": (
                hive_configuration.get("location")
                if hive_configuration.get("source") == "metastore_uris"
                and _metastore_uris_valid(hive_configuration.get("location"))
                else (
                    "unavailable"
                    if hive_configuration.get("source") == "metastore_uris"
                    else "from_hive_configuration"
                )
            ),
            "mode": "hms",
            "partition_fields": table.get("partition_fields", []),
            "registered_table_names": registered_names,
            "skip_ro_suffix": False,
            "snapshot_with_table_name": True,
            "support_timestamp": True,
            "table": table_name.split(".")[-1]
            if _non_empty_string(table_name)
            else table_name,
            "table_strategy": strategy if mutable_mor else "NOT_APPLICABLE",
        }

    required_checks: set[str] = set()
    if catalog_managed_resolution:
        required_checks.update(
            {"verify_catalog_dependencies", "verify_catalog_database_registration"}
        )
    if resolution_mode == "hudi_dfs_catalog":
        required_checks.update(
            {"verify_catalog_path_exists", "verify_catalog_path_permissions"}
        )
    if external_discovery == "hive_metastore":
        required_checks.update(
            {
                "verify_hive_configuration_available",
                "verify_hms_connectivity",
                "verify_table_registration_after_commit",
            }
        )
        if table.get("partition_fields"):
            required_checks.add("verify_partition_registration_after_commit")
        if mutable_mor:
            required_checks.add("verify_mor_catalog_table_names")
        required_checks.add("verify_external_engine_visibility")
    checks = [
        check
        for check in manifest["catalog_composition_contract"]["deployment_checks"]
        if check in required_checks
    ]

    return {
        "contract_schema": CATALOG_CONTRACT_SCHEMA_VERSION,
        "profile": profile,
        "resolution_mode": resolution_mode,
        "external_discovery": external_discovery,
        "sync_owner": sync_owner,
        "data_lifecycle_owner": lifecycle_owner,
        "catalog_setup_sql": catalog_sql,
        "effective_configuration": effective_configuration,
        "deployment_checks": checks,
    }


def _render_sql(
    contract: dict[str, Any],
    table_columns: list[dict[str, Any]],
    partition_fields: list[str],
    record_key_fields: list[str],
    catalog_state: dict[str, Any],
) -> dict[str, str]:
    table = contract["table"]
    source = contract["source"]
    runtime = contract["runtime"]
    identity = contract["identity"]
    mutable = contract["contract_schema"] in {
        MUTABLE_CONTRACT_SCHEMA_VERSION,
        MUTABLE_MOR_CONTRACT_SCHEMA_VERSION,
    }
    mutable_mor = contract["contract_schema"] == MUTABLE_MOR_CONTRACT_SCHEMA_VERSION
    resolution_mode = catalog_state["resolution_mode"]
    external_discovery = catalog_state["external_discovery"]
    catalog_managed_resolution = (
        isinstance(resolution_mode, str)
        and resolution_mode in CATALOG_MANAGED_RESOLUTION_MODES
    )
    target_identifier = (
        _quote_identifier(table["name"])
        if catalog_managed_resolution
        else _quote_qualified_identifier(table["name"])
    )

    definitions = [
        f"  {_quote_identifier(column['name'])} {column['type']}"
        + ("" if column["nullable"] else " NOT NULL")
        for column in table_columns
    ]
    if identity["mode"] == "stable_key":
        key_sql = ", ".join(_quote_identifier(field) for field in record_key_fields)
        definitions.append(f"  PRIMARY KEY ({key_sql}) NOT ENFORCED")

    table_ddl = f"CREATE TABLE {target_identifier} (\n" + ",\n".join(definitions) + "\n)"
    if partition_fields:
        partition_sql = ", ".join(_quote_identifier(field) for field in partition_fields)
        table_ddl += f"\nPARTITIONED BY ({partition_sql})"
    table_options: list[tuple[str, str]] = [("connector", "hudi")]
    if resolution_mode != "hudi_dfs_catalog":
        table_options.append(("path", table["path"]))
    if mutable:
        ordering_field = contract["ordering"]["fields"][0]
        table_type = "MERGE_ON_READ" if mutable_mor else "COPY_ON_WRITE"
        table_options.extend(
            [
                ("table.type", table_type),
                ("write.operation", "upsert"),
                ("ordering.fields", ordering_field),
                ("hoodie.write.record.merge.mode", "EVENT_TIME_ORDERING"),
                ("index.type", "FLINK_STATE"),
                ("index.global.enabled", "true"),
                ("index.state.ttl", "0"),
                ("index.bootstrap.enabled", "true"),
                ("changelog.enabled", "false"),
            ]
        )
        if mutable_mor:
            table_options.extend(
                [
                    ("compaction.schedule.enabled", "true"),
                    ("compaction.async.enabled", "true"),
                    ("compaction.operation.execute.async.enabled", "true"),
                    ("compaction.trigger.strategy", "num_commits"),
                    ("compaction.delta_commits", "5"),
                    ("clustering.schedule.enabled", "false"),
                    ("clustering.async.enabled", "false"),
                ]
            )
    else:
        table_options.extend(
            [
                ("table.type", "COPY_ON_WRITE"),
                ("write.operation", "insert"),
                ("write.insert.cluster", "false"),
            ]
        )

    catalog = contract.get("catalog", {})
    if external_discovery == "hive_metastore":
        if catalog_state["sync_owner"] == "flink_sink":
            hive_configuration = catalog["hive_configuration"]
            table_options.extend(
                [
                    ("hive_sync.enabled", "true"),
                    ("hive_sync.mode", "hms"),
                    ("hive_sync.metastore.uris", hive_configuration["location"]),
                    ("hive_sync.db", catalog["database"]),
                    ("hive_sync.table", table["name"].split(".")[-1]),
                ]
            )
        if mutable_mor:
            table_options.append(("hive_sync.table.strategy", catalog["mor_sync_strategy"]))

    option_sql = ",\n".join(
        f"  {_sql_string(key)} = {_sql_string(value)}" for key, value in table_options
    )
    table_ddl += f"\nWITH (\n{option_sql}\n);"

    field_sql = ",\n  ".join(_quote_identifier(column["name"]) for column in table_columns)
    insert_sql = (
        f"INSERT INTO {target_identifier} (\n  {field_sql}\n)\n"
        f"SELECT\n  {field_sql}\n"
        f"FROM {_quote_qualified_identifier(source['table'])};"
    )
    runtime_sql = (
        "SET 'execution.runtime-mode' = 'streaming';\n"
        "SET 'execution.checkpointing.interval' = "
        f"'{runtime['checkpoint_interval_ms']} ms';"
    )
    catalog_setup_sql = catalog_state["catalog_setup_sql"]
    statements = [runtime_sql]
    if catalog_setup_sql:
        statements.append(catalog_setup_sql)
    statements.extend([table_ddl, insert_sql])
    return {
        "runtime_sql": runtime_sql,
        "catalog_setup_sql": catalog_setup_sql,
        "table_ddl": table_ddl,
        "insert_sql": insert_sql,
        "combined_sql": "\n\n".join(statements),
    }


def assess_design(
    contract: dict[str, Any],
    manifest: dict[str, Any],
    *,
    expected_table_lifecycle: str = "new",
    render_sql: bool = True,
) -> dict[str, Any]:
    """Validate one explicit append-only COW, mutable COW, or mutable MOR contract.

    The public CLI keeps the executable new-table defaults.  The existing-table
    reconciler uses the same bounded design checks with ``render_sql=False`` and
    an explicit existing lifecycle; accepting that lifecycle here never makes
    the ordinary design-validator CLI an existing-table renderer.
    """

    if expected_table_lifecycle not in {"new", "existing"}:
        raise ValueError("expected_table_lifecycle must be 'new' or 'existing'")

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
            "catalog",
            "contract_schema",
            "flink_version",
            "hudi_version",
            "identity",
            "ordering",
            "read",
            "runtime",
            "safety",
            "source",
            "table",
            "table_services",
            "write",
            "writer_contract",
        },
        "contract",
        assessment,
    )
    contract_schema = contract.get("contract_schema")
    supported_contract_schema = (
        isinstance(contract_schema, int)
        and not isinstance(contract_schema, bool)
        and contract_schema in SUPPORTED_CONTRACT_SCHEMAS
    )
    if not supported_contract_schema:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "contract_schema must be 1 (append-only), 2 (mutable COW), or 3 (mutable MOR)",
        )
    mutable_contract = supported_contract_schema and contract_schema in {
        MUTABLE_CONTRACT_SCHEMA_VERSION,
        MUTABLE_MOR_CONTRACT_SCHEMA_VERSION,
    }
    mutable_mor_contract = (
        supported_contract_schema
        and contract_schema == MUTABLE_MOR_CONTRACT_SCHEMA_VERSION
    )
    if mutable_mor_contract:
        contract_profile = manifest["mutable_mor_executable_contract"]
    elif mutable_contract:
        contract_profile = manifest["mutable_executable_contract"]
    else:
        contract_profile = manifest["executable_contract"]
    if not mutable_contract and "ordering" in contract:
        assessment.add(
            "FLINK_DESIGN_CONTRACT_INVALID",
            "ordering is supported only by mutable contract schemas 2 and 3",
        )
    if not mutable_mor_contract:
        if "read" in contract:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "read is supported only by contract_schema 3",
            )
        if "table_services" in contract:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "table_services is supported only by contract_schema 3",
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
    elif lifecycle == "existing" and expected_table_lifecycle == "new":
        assessment.add(
            "FLINK_EXISTING_TABLE_DEFERRED",
            "The executable contracts support only a new Architect-managed table",
        )
    elif lifecycle != expected_table_lifecycle:
        assessment.add("FLINK_TABLE_LIFECYCLE_REQUIRED", "Table lifecycle is unresolved")
    writer_model = safety.get("writer_model")
    if writer_model is None:
        assessment.add("FLINK_WRITER_MODEL_UNRESOLVED", "Writer topology is required")
    elif writer_model not in {"single_writer", "multi_writer"}:
        assessment.add("FLINK_WRITER_MODEL_UNRESOLVED", "Writer topology is unresolved")
    writer_contract_state = assess_writer_contract(contract)
    for finding in writer_contract_state["findings"]:
        assessment.add(finding["code"], finding["message"])
    external_catalog = safety.get("external_catalog")
    if type(external_catalog) is not bool:
        assessment.add("FLINK_CATALOG_REQUIREMENT_UNRESOLVED", "Catalog requirement is unresolved")
    catalog_state = _assess_catalog(
        contract,
        manifest,
        assessment,
        external_catalog=external_catalog,
        expected_table_lifecycle=expected_table_lifecycle,
    )
    mutability = safety.get("mutability")
    if mutable_contract:
        if mutability != "mutable":
            assessment.add(
                "FLINK_MUTABILITY_REQUIRED",
                "Mutable contract schemas 2 and 3 require mutable input",
            )
    elif mutability == "mutable":
        assessment.add(
            "FLINK_MUTABLE_COW_DEFERRED",
            "Mutable input requires an explicit mutable safety contract",
        )
    elif mutability != "append_only":
        assessment.add("FLINK_MUTABILITY_REQUIRED", "Append-only input must be confirmed")
    replay_behavior = safety.get("replay_behavior")
    if not _non_empty_string(replay_behavior):
        assessment.add("FLINK_REPLAY_BEHAVIOR_UNRESOLVED", "Replay behavior is unresolved")
    elif replay_behavior == "must_collapse" and not mutable_contract:
        assessment.add("FLINK_REPLAY_IDEMPOTENCE_DEFERRED", "Replay deduplication needs upsert")
    elif replay_behavior not in contract_profile["allow_replay_behaviors"]:
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
    if (
        catalog_state["resolution_mode"] == "hudi_hms_catalog"
        and partition_fields
        and len(partition_fields) == len(set(partition_fields))
        and all(field in table_by_name for field in partition_fields)
    ):
        schema_partition_order = [
            column["name"]
            for column in table_columns
            if column["name"] in partition_fields
        ]
        if schema_partition_order != partition_fields:
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                "HMS Catalog requires table.partition_fields to follow their order in "
                "table.columns",
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
        reserved_ordering_values = set(
            contract_profile["reserved_ordering_field_values"]
        )
        for field in ordering_fields:
            if field in reserved_ordering_values:
                assessment.add(
                    "FLINK_MUTABLE_ORDERING_FIELD_INVALID",
                    f"Ordering field {field!r} is reserved by Hudi and disables ordering",
                )
                continue
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
    source_parts = source_name.split(".") if _check_concrete_value(source_name) else []
    source_catalog_managed = (
        isinstance(catalog_state["resolution_mode"], str)
        and catalog_state["resolution_mode"] in CATALOG_MANAGED_RESOLUTION_MODES
    )
    if not source_parts or any(not part for part in source_parts):
        assessment.add("FLINK_SOURCE_CONTRACT_REQUIRED", "A concrete source table is required")
    elif source_catalog_managed and len(source_parts) != 3:
        assessment.add(
            "FLINK_SOURCE_CONTRACT_REQUIRED",
            "Catalog-managed profiles require source.table to be a fully qualified "
            "catalog.database.table identifier",
        )
    elif expected_table_lifecycle == "new" and source_catalog_managed:
        raw_catalog = contract.get("catalog")
        catalog = raw_catalog if isinstance(raw_catalog, dict) else {}
        target_parts = [
            catalog.get("catalog_name"),
            catalog.get("database"),
            table_name,
        ]
        if (
            all(_check_concrete_value(part) for part in target_parts)
            and source_parts == target_parts
        ):
            assessment.add(
                "FLINK_CATALOG_COMPOSITION_CONFLICT",
                "The streaming new-table path cannot read from and write to the same "
                "catalog.database.table object",
            )
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
        if mutable_mor_contract:
            write_keys.add("clustering")
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
    elif mutable_mor_contract and (
        table_type != "MERGE_ON_READ" or operation != "upsert"
    ):
        assessment.add(
            "FLINK_PR4_WRITE_PATH_UNSUPPORTED",
            "The first mutable MOR path supports only MERGE_ON_READ upsert",
        )
    elif mutable_contract and not mutable_mor_contract and (
        table_type != "COPY_ON_WRITE" or operation != "upsert"
    ):
        assessment.add(
            "FLINK_PR3_WRITE_PATH_UNSUPPORTED",
            "The first mutable path supports only COPY_ON_WRITE upsert",
        )
    elif not mutable_contract and (
        table_type != "COPY_ON_WRITE" or operation != "insert"
    ):
        if table_type == "MERGE_ON_READ":
            assessment.add(
                "FLINK_MOR_APPEND_ONLY_UNSUPPORTED",
                "The bounded append-only path remains COPY_ON_WRITE",
            )
        else:
            assessment.add(
                "FLINK_PR2_WRITE_PATH_UNSUPPORTED",
                "PR2 supports only COPY_ON_WRITE with write.operation=insert",
            )
    if mutable_contract:
        if write.get("record_merge_mode") != "EVENT_TIME_ORDERING":
            assessment.add(
                (
                    "FLINK_PR4_WRITE_PATH_UNSUPPORTED"
                    if mutable_mor_contract
                    else "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                ),
                "write.record_merge_mode must be EVENT_TIME_ORDERING",
            )
        if write.get("changelog_enabled") is not False:
            assessment.add(
                (
                    "FLINK_PR4_WRITE_PATH_UNSUPPORTED"
                    if mutable_mor_contract
                    else "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                ),
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
        if mutable_mor_contract:
            clustering = _dict(write.get("clustering"), "write.clustering", assessment)
            _check_keys(
                clustering,
                {"async_enabled", "schedule_enabled"},
                "write.clustering",
                assessment,
            )
            if (
                clustering.get("schedule_enabled") is not False
                or clustering.get("async_enabled") is not False
            ):
                assessment.add(
                    "FLINK_MOR_CLUSTERING_UNSUPPORTED",
                    "The bounded MOR path requires clustering schedule and execution disabled",
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
        expected_table_type = "MERGE_ON_READ" if mutable_mor_contract else "COPY_ON_WRITE"
        if (
            "table.type" in connector_options
            and raw_table_type != expected_table_type
        ) or (
            "write.operation" in connector_options
            and raw_operation != expected_operation
        ):
            assessment.add(
                (
                    "FLINK_PR4_WRITE_PATH_UNSUPPORTED"
                    if mutable_mor_contract
                    else (
                        "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                        if mutable_contract
                        else "FLINK_PR2_WRITE_PATH_UNSUPPORTED"
                    )
                ),
                "A connector-option override leaves the bounded write path",
            )
        if mutable_mor_contract:
            if any(key.startswith("clustering.") for key in connector_options):
                assessment.add(
                    "FLINK_MOR_CLUSTERING_UNSUPPORTED",
                    "Clustering connector overrides are not supported by the bounded MOR path",
                )
            if any(
                key in {
                    "compaction.tasks",
                    "compaction.delta_seconds",
                    "compaction.target.io",
                    "compaction.timeout.seconds",
                }
                for key in connector_options
            ):
                assessment.add(
                    "FLINK_MOR_COMPACTION_TUNING_UNSUPPORTED",
                    "Detailed compaction tuning is outside the bounded Architect path",
                )
        verified_overrides = sorted(set(connector_options) - set(unknown_options))
        if verified_overrides:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "Connector settings must use canonical structured fields; "
                f"pass-through overrides are not rendered: {verified_overrides}",
            )

    if mutable_mor_contract:
        table_services = _dict(
            contract.get("table_services"), "table_services", assessment
        )
        _check_keys(table_services, {"compaction"}, "table_services", assessment)
        compaction = _dict(
            table_services.get("compaction"),
            "table_services.compaction",
            assessment,
        )
        compaction_keys = {
            "async_enabled",
            "delta_commits",
            "mode",
            "operation_execute_async_enabled",
            "owner",
            "schedule_enabled",
            "trigger_strategy",
        }
        tuning_keys = {
            "delta_seconds",
            "target_io_mb",
            "tasks",
            "timeout_seconds",
        }
        unknown_compaction_keys = set(compaction) - compaction_keys
        if unknown_compaction_keys & tuning_keys:
            assessment.add(
                "FLINK_MOR_COMPACTION_TUNING_UNSUPPORTED",
                "Detailed compaction tuning is outside the bounded Architect path: "
                f"{sorted(unknown_compaction_keys & tuning_keys)}",
            )
        remaining_unknown = unknown_compaction_keys - tuning_keys
        if remaining_unknown:
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "table_services.compaction contains unsupported fields: "
                f"{sorted(remaining_unknown)}",
            )
        owner = compaction.get("owner")
        if owner is None:
            assessment.add(
                "FLINK_MOR_COMPACTION_OWNER_REQUIRED",
                "The MOR compaction owner must be explicit",
            )
        elif owner == "standalone_service":
            assessment.add(
                "FLINK_MULTI_WRITER_REVIEW",
                "A standalone compactor is an independent table-service writer",
            )
        elif owner != "flink_writer":
            assessment.add(
                "FLINK_WRITER_MODEL_UNRESOLVED",
                "The MOR compaction owner is unresolved",
            )
        expected_compaction = {
            "async_enabled": True,
            "delta_commits": 5,
            "mode": "async",
            "operation_execute_async_enabled": True,
            "schedule_enabled": True,
            "trigger_strategy": "num_commits",
        }
        mismatches = [
            key
            for key, expected_value in expected_compaction.items()
            if compaction.get(key) != expected_value
        ]
        if mismatches:
            assessment.add(
                "FLINK_MOR_COMPACTION_CONFIGURATION_UNSUPPORTED",
                "The bounded MOR compaction contract has incompatible fields: "
                f"{mismatches}",
            )

        read = _dict(contract.get("read"), "read", assessment)
        _check_keys(
            read,
            {"query_modes", "read_optimized_freshness"},
            "read",
            assessment,
        )
        query_modes = _string_list(read.get("query_modes"), "read.query_modes", assessment)
        allowed_query_modes = {"snapshot", "read_optimized"}
        if not query_modes or not set(query_modes).issubset(allowed_query_modes):
            assessment.add(
                "FLINK_MOR_QUERY_REQUIREMENT_UNRESOLVED",
                "MOR readers must explicitly require snapshot and/or read_optimized queries",
            )
        read_optimized_freshness = read.get("read_optimized_freshness")
        if "read_optimized" in query_modes:
            if read_optimized_freshness is None or read_optimized_freshness == "unknown":
                assessment.add(
                    "FLINK_MOR_QUERY_REQUIREMENT_UNRESOLVED",
                    "The acceptable read-optimized freshness must be explicit",
                )
            elif read_optimized_freshness != "after_compaction":
                assessment.add(
                    "FLINK_MOR_READ_OPTIMIZED_FRESHNESS_UNSUPPORTED",
                    "Read-optimized results can be current only after compaction",
                )
        elif read_optimized_freshness != "NOT_APPLICABLE":
            assessment.add(
                "FLINK_DESIGN_CONTRACT_INVALID",
                "read.read_optimized_freshness must be NOT_APPLICABLE without "
                "read_optimized queries",
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
                "FLINK_PR4_WRITE_PATH_UNSUPPORTED"
                if mutable_mor_contract
                else (
                    "FLINK_PR3_WRITE_PATH_UNSUPPORTED"
                    if mutable_contract
                    else "FLINK_PR2_WRITE_PATH_UNSUPPORTED"
                )
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
    executable = status == "CONFIG_VALIDATED" and render_sql and expected_table_lifecycle == "new"
    result: dict[str, Any] = {
        "status": status,
        "executable_eligible": executable,
        "catalog_contract_schema": catalog_state["contract_schema"],
        "catalog_profile": catalog_state["profile"],
        "catalog_setup_sql": catalog_state["catalog_setup_sql"] if executable else "",
        "effective_catalog_configuration": catalog_state[
            "effective_configuration"
        ],
        "catalog_deployment_checks": catalog_state["deployment_checks"],
        "writer_contract_result": writer_contract_state["writer_contract_result"],
        "multi_writer_capability": writer_contract_state["multi_writer_capability"],
        "normalized_writer_contract": writer_contract_state[
            "normalized_writer_contract"
        ],
        "writer_capability_notes": writer_contract_state["writer_capability_notes"],
        "writer_deployment_checks": writer_contract_state["writer_deployment_checks"],
        "finding_codes": [finding["code"] for finding in assessment.findings],
        "findings": assessment.findings,
        "advisory_codes": assessment.advisories,
        "validation_evidence": validation_evidence(
            manifest, contract_schema, catalog_state["profile"]
        ),
    }
    if executable:
        result["artifacts"] = _render_sql(
            contract,
            table_columns,
            partition_fields,
            record_key_fields,
            catalog_state,
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
