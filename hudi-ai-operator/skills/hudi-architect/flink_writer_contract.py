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

"""Assess the bounded Flink multi-writer Writer Contract.

This module validates explicit writer facts.  It does not select a concurrency
mode, lock provider, index, failed-writes policy, or recovery procedure, and it
never makes a multi-writer design executable.
"""

from __future__ import annotations

from typing import Any, TypeGuard

WRITER_CONTRACT_SCHEMA_VERSION = 1

WRITER_ROLES = frozenset(
    {
        "INGESTION",
        "BACKFILL",
        "CLEANUP",
        "STANDALONE_COMPACTION",
        "STANDALONE_CLUSTERING",
        "OTHER",
    }
)
WRITER_RELATIONSHIPS = frozenset({"INDEPENDENT_CONCURRENT", "MAY_OVERLAP"})
WRITER_ENGINES = frozenset({"FLINK", "SPARK", "OTHER", "UNKNOWN"})
WRITE_OPERATIONS = frozenset(
    {
        "INSERT",
        "INSERT_PREPPED",
        "UPSERT",
        "UPSERT_PREPPED",
        "BULK_INSERT",
        "BULK_INSERT_PREPPED",
        "DELETE",
        "DELETE_PREPPED",
        "BOOTSTRAP",
        "INSERT_OVERWRITE",
        "BUCKET_RESCALE",
        "CLUSTER",
        "DELETE_PARTITION",
        "INSERT_OVERWRITE_TABLE",
        "COMPACT",
        "INDEX",
        "ALTER_SCHEMA",
        "LOG_COMPACT",
        "UNKNOWN",
    }
)
CONCURRENCY_MODES = frozenset(
    {
        "SINGLE_WRITER",
        "OPTIMISTIC_CONCURRENCY_CONTROL",
        "NON_BLOCKING_CONCURRENCY_CONTROL",
        "UNKNOWN",
    }
)
LOCK_CONFIGURATION_CONSISTENCY = frozenset({"CONSISTENT", "CONFLICTING", "UNKNOWN"})
RESERVED_OWNER_IDS = frozenset({"EXTERNAL_CONTROL_PLANE", "UNKNOWN"})

_CONTRACT_KEYS = {
    "contract_schema",
    "primary_writer_id",
    "additional_writers",
    "ownership",
    "observed_concurrency",
}
_WRITER_KEYS = {
    "writer_id",
    "role",
    "relationship_to_primary",
    "engine",
    "engine_version",
    "hudi_version",
    "operation",
}
_OWNERSHIP_KEYS = {
    "schema_evolution_owner_id",
    "table_configuration_owner_id",
}
_CONCURRENCY_KEYS = {
    "mode",
    "lock_provider_class",
    "lock_configuration_consistency",
}


def _non_empty_string(value: Any) -> TypeGuard[str]:
    return isinstance(value, str) and bool(value.strip())


def _finding(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _add_finding(findings: list[dict[str, str]], code: str, message: str) -> None:
    existing = next((item for item in findings if item["code"] == code), None)
    if existing is None:
        findings.append(_finding(code, message))
    elif message not in existing["message"]:
        existing["message"] += f"; {message}"


def _invalid(findings: list[dict[str, str]], message: str) -> None:
    _add_finding(findings, "FLINK_WRITER_CONTRACT_INVALID", message)


def _check_keys(
    value: dict[str, Any], allowed: set[str], field: str, findings: list[dict[str, str]]
) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        _invalid(findings, f"{field} contains unsupported fields: {unknown}")


def _deployment_checks() -> list[dict[str, Any]]:
    return [
        {"check": "verify_concurrent_writer_inventory", "required": True},
        {"check": "verify_uniform_concurrency_mode", "required": True},
        {"check": "verify_lock_provider_configuration", "required": True},
        {"check": "verify_lazy_failed_writes_cleaner_policy", "required": True},
        {"check": "verify_writer_recovery_procedure", "required": True},
        {"check": "verify_schema_evolution_ownership", "required": True},
    ]


def _not_applicable(
    findings: list[dict[str, str]],
    *,
    capability: str = "NOT_APPLICABLE",
    notes: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    return {
        "writer_contract_result": "NOT_APPLICABLE",
        "multi_writer_capability": capability,
        "normalized_writer_contract": {},
        "writer_capability_notes": notes or [],
        "writer_deployment_checks": [],
        "findings": findings,
    }


def _normalize_writer(
    raw_writer: Any,
    index: int,
    findings: list[dict[str, str]],
) -> dict[str, str] | None:
    field = f"writer_contract.additional_writers[{index}]"
    if not isinstance(raw_writer, dict):
        _invalid(findings, f"{field} must be an object")
        return None
    _check_keys(raw_writer, _WRITER_KEYS, field, findings)

    normalized: dict[str, str] = {}
    for name in sorted(_WRITER_KEYS):
        value = raw_writer.get(name)
        if not _non_empty_string(value):
            _invalid(findings, f"{field}.{name} must be a non-empty string")
            continue
        normalized[name] = value.strip()

    enum_fields = {
        "role": WRITER_ROLES,
        "relationship_to_primary": WRITER_RELATIONSHIPS,
        "engine": WRITER_ENGINES,
        "operation": WRITE_OPERATIONS,
    }
    for name, allowed in enum_fields.items():
        value = normalized.get(name)
        if value is not None and value not in allowed:
            _invalid(
                findings,
                f"{field}.{name} must be one of {sorted(allowed)}",
            )
    return normalized if set(normalized) == _WRITER_KEYS else None


def _normalize_ownership(
    raw_ownership: Any,
    writer_ids: set[str],
    findings: list[dict[str, str]],
) -> dict[str, str] | None:
    field = "writer_contract.ownership"
    if not isinstance(raw_ownership, dict):
        _invalid(findings, f"{field} must be an object")
        return None
    _check_keys(raw_ownership, _OWNERSHIP_KEYS, field, findings)

    normalized: dict[str, str] = {}
    schema_owner = raw_ownership.get("schema_evolution_owner_id")
    if not _non_empty_string(schema_owner):
        _invalid(findings, f"{field}.schema_evolution_owner_id is required")
    else:
        normalized["schema_evolution_owner_id"] = schema_owner.strip()

    table_owner = raw_ownership.get("table_configuration_owner_id")
    if table_owner is not None:
        if not _non_empty_string(table_owner):
            _invalid(
                findings,
                f"{field}.table_configuration_owner_id must be a non-empty string",
            )
        else:
            normalized["table_configuration_owner_id"] = table_owner.strip()

    for name, owner_id in normalized.items():
        if owner_id not in writer_ids | RESERVED_OWNER_IDS:
            _invalid(
                findings,
                f"{field}.{name} must reference a declared writer or a reserved owner",
            )
        elif owner_id == "UNKNOWN":
            _add_finding(
                findings,
                "FLINK_WRITER_OWNERSHIP_UNRESOLVED",
                f"{field}.{name} is unresolved",
            )
    return normalized if "schema_evolution_owner_id" in normalized else None


def _normalize_concurrency(
    raw_concurrency: Any,
    findings: list[dict[str, str]],
) -> dict[str, str] | None:
    if raw_concurrency is None:
        return None
    field = "writer_contract.observed_concurrency"
    if not isinstance(raw_concurrency, dict):
        _invalid(findings, f"{field} must be an object")
        return None
    _check_keys(raw_concurrency, _CONCURRENCY_KEYS, field, findings)
    normalized: dict[str, str] = {}
    for name in sorted(_CONCURRENCY_KEYS):
        value = raw_concurrency.get(name)
        if not _non_empty_string(value):
            _invalid(findings, f"{field}.{name} must be a non-empty string")
            continue
        normalized[name] = value.strip()
    mode = normalized.get("mode")
    if mode is not None and mode not in CONCURRENCY_MODES:
        _invalid(findings, f"{field}.mode must be one of {sorted(CONCURRENCY_MODES)}")
    consistency = normalized.get("lock_configuration_consistency")
    if consistency is not None and consistency not in LOCK_CONFIGURATION_CONSISTENCY:
        _invalid(
            findings,
            f"{field}.lock_configuration_consistency must be one of "
            f"{sorted(LOCK_CONFIGURATION_CONSISTENCY)}",
        )
    return normalized if set(normalized) == _CONCURRENCY_KEYS else None


def _add_note(notes: list[dict[str, str]], code: str, message: str) -> None:
    if not any(note["code"] == code for note in notes):
        notes.append({"code": code, "message": message})


def _assess_contract_concurrency(
    concurrency: dict[str, Any],
    findings: list[dict[str, str]],
    notes: list[dict[str, str]],
) -> tuple[bool, bool]:
    """Assess explicit multi-writer contract concurrency facts."""

    conflict = False
    unsupported = False
    mode = concurrency.get("mode")
    consistency = concurrency.get("lock_configuration_consistency")
    if mode == "SINGLE_WRITER":
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_CONFLICT",
            "A multi-writer contract cannot observe SINGLE_WRITER concurrency mode",
        )
        conflict = True
    elif mode == "OPTIMISTIC_CONCURRENCY_CONTROL":
        _add_note(
            notes,
            "FLINK_OCC_DEPLOYMENT_UNVERIFIED",
            "OCC is recognized by the pinned source, but concurrent, lock, and "
            "recovery behavior is not exercised by this assessment.",
        )
    elif mode == "NON_BLOCKING_CONCURRENCY_CONTROL":
        unsupported = True
        _add_finding(
            findings,
            "FLINK_WRITER_COMBINATION_UNSUPPORTED",
            "The existing COW or FLINK_STATE-indexed MOR paths do not satisfy the "
            "pinned NBCC requirement for MOR with simple bucket index",
        )
    if consistency == "CONFLICTING":
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_CONFLICT",
            "Observed lock configuration is conflicting across writers",
        )
        conflict = True
    return conflict, unsupported


def _evidence_concurrency(
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        name: evidence[field]
        for name, field in {
            "mode": "flink_writer_config.write_concurrency_mode",
            "lock_provider_class": "flink_writer_config.lock_provider_class",
            "lock_configuration_consistency": (
                "flink_writer_config.lock_configuration_consistency"
            ),
        }.items()
        if field in evidence
    }


def _assess_evidence_concurrency(
    concurrency: dict[str, Any],
    writer_model: Any,
    findings: list[dict[str, str]],
    notes: list[dict[str, str]],
) -> tuple[bool, bool, bool, bool]:
    """Assess observed concurrency independently of Writer Contract validity."""

    if not concurrency:
        return False, False, False, False

    conflict = False
    unsupported = False
    relevant = False
    conflicting_lock_evidence = False
    mode = concurrency.get("mode")
    consistency = concurrency.get("lock_configuration_consistency")
    if mode == "SINGLE_WRITER":
        relevant = writer_model == "multi_writer"
        if writer_model == "multi_writer":
            _add_finding(
                findings,
                "FLINK_WRITER_CONTRACT_CONFLICT",
                "A multi-writer design cannot observe SINGLE_WRITER concurrency mode",
            )
            conflict = True
    elif mode == "OPTIMISTIC_CONCURRENCY_CONTROL":
        relevant = True
        _add_note(
            notes,
            "FLINK_OCC_DEPLOYMENT_UNVERIFIED",
            "OCC is recognized by the pinned source, but concurrent, lock, and "
            "recovery behavior is not exercised by this assessment.",
        )
        if writer_model != "multi_writer":
            _add_finding(
                findings,
                "FLINK_MULTI_WRITER_REVIEW",
                "Observed concurrency configuration requires multi-writer review",
            )
    elif mode == "NON_BLOCKING_CONCURRENCY_CONTROL":
        relevant = True
        unsupported = True
        _add_finding(
            findings,
            "FLINK_WRITER_COMBINATION_UNSUPPORTED",
            "The existing COW or FLINK_STATE-indexed MOR paths do not satisfy the "
            "pinned NBCC requirement for MOR with simple bucket index",
        )
        if writer_model != "multi_writer":
            _add_finding(
                findings,
                "FLINK_MULTI_WRITER_REVIEW",
                "Observed concurrency configuration requires multi-writer review",
            )

    if consistency == "CONFLICTING":
        relevant = True
        conflicting_lock_evidence = True
        _add_finding(
            findings,
            "FLINK_EXISTING_EVIDENCE_CONFLICT",
            "Observed lock configuration is conflicting across writers",
        )
        if writer_model != "multi_writer":
            _add_finding(
                findings,
                "FLINK_MULTI_WRITER_REVIEW",
                "Observed concurrency configuration requires multi-writer review",
            )
    return conflict, unsupported, relevant, conflicting_lock_evidence


def _writer_topology_evidence_findings(
    writer_model: Any,
    evidence: dict[str, Any],
    conflicting_evidence_fields: set[str],
    evidence_source_ids: dict[str, set[str]],
    findings: list[dict[str, str]],
) -> None:
    inventory_fields = {
        "writer_inventory.active_writers",
        "writer_inventory.concurrent_writers",
    }
    conflicting = sorted(inventory_fields & conflicting_evidence_fields)
    if conflicting:
        _add_finding(
            findings,
            "FLINK_EXISTING_EVIDENCE_CONFLICT",
            f"Existing-table evidence disagrees about writer inventory: {conflicting}",
        )

    active_writers = evidence.get("writer_inventory.active_writers")
    concurrent_writers = evidence.get("writer_inventory.concurrent_writers")
    multiple_active = isinstance(active_writers, list) and len(active_writers) > 1
    if writer_model != "multi_writer" and (
        multiple_active or concurrent_writers is True
    ):
        _add_finding(
            findings,
            "FLINK_MULTI_WRITER_REVIEW",
            "Existing-table writer inventory requires multi-writer review",
        )
    active_sources = evidence_source_ids.get("writer_inventory.active_writers", set())
    concurrent_sources = evidence_source_ids.get(
        "writer_inventory.concurrent_writers", set()
    )
    if (
        multiple_active
        and concurrent_writers is False
        and bool(active_sources & concurrent_sources)
    ):
        _add_finding(
            findings,
            "FLINK_EXISTING_EVIDENCE_CONFLICT",
            "Writer inventory reports multiple active writers while concurrent_writers is false",
        )


def _evidence_conflicts(
    normalized: dict[str, Any],
    evidence: dict[str, Any],
    conflicting_evidence_fields: set[str],
    findings: list[dict[str, str]],
) -> bool:
    contract_fact_fields = {
        "writer_inventory.active_writers",
        "writer_inventory.schema_evolution_owner_id",
        "flink_writer_config.write_concurrency_mode",
        "flink_writer_config.lock_provider_class",
        "flink_writer_config.lock_configuration_consistency",
    }
    conflicting = sorted(contract_fact_fields & conflicting_evidence_fields)
    conflict = False
    if conflicting:
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_CONFLICT",
            f"Existing-table evidence disagrees about Writer Contract facts: {conflicting}",
        )
        conflict = True

    declared_writer_ids = {
        normalized["primary_writer_id"],
        *(writer["writer_id"] for writer in normalized["additional_writers"]),
    }
    expected: dict[str, Any] = {
        "writer_inventory.schema_evolution_owner_id": normalized["ownership"][
            "schema_evolution_owner_id"
        ],
    }
    concurrency = normalized.get("observed_concurrency")
    if isinstance(concurrency, dict):
        expected.update(
            {
                "flink_writer_config.write_concurrency_mode": concurrency["mode"],
                "flink_writer_config.lock_provider_class": concurrency[
                    "lock_provider_class"
                ],
                "flink_writer_config.lock_configuration_consistency": concurrency[
                    "lock_configuration_consistency"
                ],
            }
        )

    mismatches = [
        field
        for field, observed in evidence.items()
        if field in expected and observed != expected[field]
    ]
    active_writers = evidence.get("writer_inventory.active_writers")
    if isinstance(active_writers, list) and not set(active_writers) <= declared_writer_ids:
        mismatches.append("writer_inventory.active_writers")
    mismatches = sorted(mismatches)
    if mismatches:
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_CONFLICT",
            f"Existing-table evidence conflicts with Writer Contract facts: {mismatches}",
        )
        conflict = True
    return conflict


def assess_writer_contract(
    design: dict[str, Any],
    *,
    evidence: dict[str, Any] | None = None,
    conflicting_evidence_fields: set[str] | None = None,
    evidence_source_ids: dict[str, set[str]] | None = None,
) -> dict[str, Any]:
    """Return a deterministic, assessment-only Writer Contract result."""

    findings: list[dict[str, str]] = []
    raw_safety = design.get("safety")
    safety = raw_safety if isinstance(raw_safety, dict) else {}
    writer_model = safety.get("writer_model")
    raw_contract = design.get("writer_contract")
    supplied_evidence = evidence or {}
    supplied_conflicts = conflicting_evidence_fields or set()
    supplied_sources = evidence_source_ids or {}
    notes: list[dict[str, str]] = []

    _writer_topology_evidence_findings(
        writer_model,
        supplied_evidence,
        supplied_conflicts,
        supplied_sources,
        findings,
    )
    (
        evidence_conflict,
        evidence_unsupported,
        evidence_relevant,
        conflicting_lock_evidence,
    ) = _assess_evidence_concurrency(
        _evidence_concurrency(supplied_evidence),
        writer_model,
        findings,
        notes,
    )
    capability = (
        "UNSUPPORTED"
        if evidence_unsupported
        else "UNVERIFIED"
        if writer_model == "multi_writer" or evidence_relevant
        else "NOT_APPLICABLE"
    )

    if writer_model == "single_writer":
        if raw_contract is not None:
            _add_finding(
                findings,
                "FLINK_WRITER_CONTRACT_CONFLICT",
                "writer_contract must be absent for a confirmed single-writer design",
            )
            result = _not_applicable(findings, capability=capability, notes=notes)
            result["writer_contract_result"] = "CONFLICT"
            return result
        return _not_applicable(findings, capability=capability, notes=notes)

    if writer_model != "multi_writer":
        if raw_contract is not None:
            _add_finding(
                findings,
                "FLINK_WRITER_CONTRACT_CONFLICT",
                "Writer topology must be resolved before a Writer Contract is assessed",
            )
        return _not_applicable(findings, capability=capability, notes=notes)

    _add_finding(
        findings,
        "FLINK_MULTI_WRITER_REVIEW",
        "Multi-writer execution remains assessment-only and requires deployment review",
    )
    if raw_contract is None:
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_REQUIRED",
            "A Writer Contract is required for a confirmed multi-writer design",
        )
        return {
            "writer_contract_result": "INCOMPLETE",
            "multi_writer_capability": capability,
            "normalized_writer_contract": {},
            "writer_capability_notes": notes,
            "writer_deployment_checks": _deployment_checks(),
            "findings": findings,
        }
    if not isinstance(raw_contract, dict):
        _invalid(findings, "writer_contract must be an object")
        return {
            "writer_contract_result": "INCOMPLETE",
            "multi_writer_capability": capability,
            "normalized_writer_contract": {},
            "writer_capability_notes": notes,
            "writer_deployment_checks": _deployment_checks(),
            "findings": findings,
        }

    _check_keys(raw_contract, _CONTRACT_KEYS, "writer_contract", findings)
    contract_schema = raw_contract.get("contract_schema")
    if (
        type(contract_schema) is not int
        or contract_schema != WRITER_CONTRACT_SCHEMA_VERSION
    ):
        _invalid(
            findings,
            f"writer_contract.contract_schema must be {WRITER_CONTRACT_SCHEMA_VERSION}",
        )

    primary_writer_id = raw_contract.get("primary_writer_id")
    if not _non_empty_string(primary_writer_id):
        _invalid(findings, "writer_contract.primary_writer_id is required")
        normalized_primary_id = ""
    else:
        normalized_primary_id = primary_writer_id.strip()

    raw_writers = raw_contract.get("additional_writers")
    normalized_writers: list[dict[str, str]] = []
    if not isinstance(raw_writers, list) or not raw_writers:
        _invalid(
            findings,
            "writer_contract.additional_writers must be a non-empty array",
        )
    else:
        for index, raw_writer in enumerate(raw_writers):
            normalized_writer = _normalize_writer(raw_writer, index, findings)
            if normalized_writer is not None:
                normalized_writers.append(normalized_writer)

    writer_ids = [writer["writer_id"] for writer in normalized_writers]
    if len(writer_ids) != len(set(writer_ids)):
        _invalid(
            findings,
            "writer_contract.additional_writers has duplicate writer_id values",
        )
    if normalized_primary_id and normalized_primary_id in writer_ids:
        _invalid(
            findings,
            "writer_contract.primary_writer_id must differ from every additional writer_id",
        )

    declared_writer_ids = set(writer_ids)
    if normalized_primary_id:
        declared_writer_ids.add(normalized_primary_id)
    ownership = _normalize_ownership(
        raw_contract.get("ownership"), declared_writer_ids, findings
    )
    concurrency = _normalize_concurrency(
        raw_contract.get("observed_concurrency"), findings
    )

    invalid = any(
        finding["code"] == "FLINK_WRITER_CONTRACT_INVALID" for finding in findings
    )
    if invalid:
        return {
            "writer_contract_result": "INCOMPLETE",
            "multi_writer_capability": capability,
            "normalized_writer_contract": {},
            "writer_capability_notes": notes,
            "writer_deployment_checks": _deployment_checks(),
            "findings": findings,
        }

    raw_write = design.get("write")
    write = raw_write if isinstance(raw_write, dict) else {}
    primary_operation = str(write.get("operation", "UNKNOWN")).upper()
    if primary_operation not in WRITE_OPERATIONS:
        primary_operation = "UNKNOWN"
    normalized: dict[str, Any] = {
        "contract_schema": WRITER_CONTRACT_SCHEMA_VERSION,
        "primary_writer_id": normalized_primary_id,
        "primary_writer_facts": {
            "engine": "FLINK",
            "engine_version": design.get("flink_version", "UNKNOWN"),
            "hudi_version": design.get("hudi_version", "UNKNOWN"),
            "operation": primary_operation,
        },
        "additional_writers": sorted(
            normalized_writers, key=lambda writer: writer["writer_id"]
        ),
        "ownership": ownership,
    }
    if concurrency is not None:
        normalized["observed_concurrency"] = concurrency

    conflict = evidence_conflict
    if conflicting_lock_evidence:
        _add_finding(
            findings,
            "FLINK_WRITER_CONTRACT_CONFLICT",
            "Observed lock configuration is conflicting across writers",
        )
        conflict = True
    if concurrency is not None:
        contract_conflict, contract_unsupported = _assess_contract_concurrency(
            concurrency, findings, notes
        )
        conflict = conflict or contract_conflict
        if contract_unsupported:
            capability = "UNSUPPORTED"

    if any(writer["engine"] != "FLINK" for writer in normalized_writers):
        _add_note(
            notes,
            "FLINK_CROSS_ENGINE_WRITER_UNVERIFIED",
            "Cross-engine writer compatibility is not certified by this assessment.",
        )
    if any(
        writer["role"] in {"STANDALONE_COMPACTION", "STANDALONE_CLUSTERING"}
        for writer in normalized_writers
    ):
        _add_note(
            notes,
            "FLINK_STANDALONE_TABLE_SERVICE_UNVERIFIED",
            "Standalone table-service writers require deployment review.",
        )
    if any(
        writer["hudi_version"] != design.get("hudi_version")
        for writer in normalized_writers
    ):
        _add_finding(
            findings,
            "FLINK_VERSION_UNVERIFIED",
            "Writer Hudi versions are not uniformly pinned to the design baseline",
        )
    if any(
        writer["engine"] == "FLINK"
        and writer["engine_version"] != design.get("flink_version")
        for writer in normalized_writers
    ):
        _add_finding(
            findings,
            "FLINK_VERSION_UNVERIFIED",
            "Flink writer versions are not uniformly pinned to the design baseline",
        )

    conflict = (
        _evidence_conflicts(
            normalized,
            supplied_evidence,
            supplied_conflicts,
            findings,
        )
        or conflict
    )
    return {
        "writer_contract_result": "CONFLICT" if conflict else "COMPLETE",
        "multi_writer_capability": capability,
        "normalized_writer_contract": normalized,
        "writer_capability_notes": notes,
        "writer_deployment_checks": _deployment_checks(),
        "findings": findings,
    }
