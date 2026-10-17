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
#

"""Typed read tools that answer common questions directly.

The generic tools return raw parsed tables and leave extraction, counting, and
arithmetic to the calling model -- which smaller models get wrong (reading
``hoodie.timeline.layout.version`` as the table version, counting 10 rows as 4,
averaging file sizes by eye). These tools do that deterministic work server-side,
**live on every call** (nothing is cached or hardcoded), and return small typed
payloads the model only has to relay.
"""

from __future__ import annotations

import json
import re

from hudi_cli_mcp.commands import quote_arg
from hudi_cli_mcp.executor import ExecutionResult, HudiCliExecutor
from hudi_cli_mcp.session import NotConnectedError, SessionManager

# "427.1 KB", "1.2 MB", "0.0 B" -- Hudi renders sizes via commons-io style
# binary units.
_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(B|KB|MB|GB|TB)\s*$", re.IGNORECASE)
_SIZE_UNITS = {"B": 1, "KB": 1024, "MB": 1024**2, "GB": 1024**3, "TB": 1024**4}

_INSTANT_RE = re.compile(r"^\d{8,}$")


def parse_size(text: str) -> int | None:
    """Parse a human-rendered size like ``427.1 KB`` into bytes, or None."""
    m = _SIZE_RE.match(text or "")
    if not m:
        return None
    return int(float(m.group(1)) * _SIZE_UNITS[m.group(2).upper()])


def human_size(n: int) -> str:
    value = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{n} B"


def _int(value: str) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _resolve_path(path: str, session: SessionManager) -> str:
    return path.strip() or session.require_connection()


def _run(
    executor: HudiCliExecutor, path: str, command: str
) -> ExecutionResult:
    return executor.execute([f"connect --path {quote_arg(path)}", command])


def _error(message: str, hint: str = "") -> str:
    payload: dict = {"success": False, "error": message}
    if hint:
        payload["hint"] = hint
    return json.dumps(payload, indent=2)


def _props_from_result(result: ExecutionResult) -> dict[str, str]:
    """Flatten every Property/Value table in the result into one dict."""
    props: dict[str, str] = {}
    for table in result.parsed.tables:
        if table.headers[:2] == ["Property", "Value"]:
            for row in table.rows:
                key = (row.get("Property") or "").strip()
                if key:
                    props[key] = (row.get("Value") or "").strip()
    return props


def _split_csv(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def get_table_info(
    executor: HudiCliExecutor,
    session: SessionManager,
    path: str = "",
) -> str:
    """Core table facts extracted server-side from `desc`."""
    try:
        table_path = _resolve_path(path, session)
    except NotConnectedError as e:
        return _error(str(e))
    result = _run(executor, table_path, "desc")
    props = _props_from_result(result)
    if not result.is_success() or not props:
        return _error(
            f"could not describe table at {table_path}",
            "Check the path; connect_to_table validates a table location.",
        )
    session.connect(table_path)
    # Deliberately typed: the table version is hoodie.table.version -- models
    # routinely misread hoodie.timeline.layout.version out of the raw desc rows.
    info = {
        "success": True,
        "table_name": props.get("hoodie.table.name", ""),
        "table_type": props.get("hoodie.table.type", ""),
        "table_version": props.get("hoodie.table.version", ""),
        "partition_fields": _split_csv(props.get("hoodie.table.partition.fields", "")),
        "record_key_fields": _split_csv(props.get("hoodie.table.recordkey.fields", "")),
        "ordering_fields": _split_csv(
            props.get("hoodie.table.ordering.fields", "")
            or props.get("hoodie.table.precombine.field", "")
        ),
        "metadata_indexes": _split_csv(props.get("hoodie.table.metadata.partitions", "")),
        "base_path": props.get("basePath", table_path),
    }
    return json.dumps(info, indent=2)


def get_commit_summary(
    executor: HudiCliExecutor,
    session: SessionManager,
    path: str = "",
    limit: int = 10,
) -> str:
    """Active-timeline commit count plus recent commits, computed server-side."""
    try:
        table_path = _resolve_path(path, session)
    except NotConnectedError as e:
        return _error(str(e))
    # No --limit: fetch the full active timeline so commit_count is the real
    # count (computed fresh from this call's rows), then slice `recent` locally.
    result = _run(executor, table_path, "commits show --desc true")
    if not result.is_success():
        errs = result.parsed.errors
        return _error(errs[0] if errs else f"commits show failed for {table_path}")
    session.connect(table_path)
    rows: list[dict[str, str]] = []
    for table in result.parsed.tables:
        if "CommitTime" in table.headers:
            rows = [r for r in table.rows if _INSTANT_RE.match((r.get("CommitTime") or "").strip())]
            break
    recent = []
    for r in rows[: max(1, limit)]:
        recent.append(
            {
                "instant": r.get("CommitTime", "").strip(),
                "bytes_written": parse_size(r.get("Total Bytes Written", "")),
                "bytes_written_human": r.get("Total Bytes Written", "").strip(),
                "files_added": _int(r.get("Total Files Added", "0")),
                "files_updated": _int(r.get("Total Files Updated", "0")),
                "partitions_written": _int(r.get("Total Partitions Written", "0")),
                "records_written": _int(r.get("Total Records Written", "0")),
                "update_records_written": _int(r.get("Total Update Records Written", "0")),
                "errors": _int(r.get("Total Errors", "0")),
            }
        )
    out = {
        "success": True,
        # Computed from THIS call's rows -- grows/shrinks with the live timeline.
        "commit_count": len(rows),
        "recent": recent,
        "total_records_written": sum(x["records_written"] for x in recent),
        "total_errors": sum(x["errors"] for x in recent),
        "note": "commit_count covers the active timeline; older commits may be archived "
        "(see `show archived commit`).",
    }
    return json.dumps(out, indent=2)


def get_file_size_stats(
    executor: HudiCliExecutor,
    session: SessionManager,
    path: str = "",
) -> str:
    """Base-file size stats computed server-side from the file-system view.

    `show fsview all` lists every file slice, including historical versions of a
    file group; stats here are over the LATEST slice per (partition, fileId), so
    they describe the table's current files. Computed live on each call.
    """
    try:
        table_path = _resolve_path(path, session)
    except NotConnectedError as e:
        return _error(str(e))
    result = _run(executor, table_path, "show fsview all")
    if not result.is_success():
        errs = result.parsed.errors
        return _error(errs[0] if errs else f"show fsview all failed for {table_path}")
    session.connect(table_path)
    latest: dict[tuple[str, str], dict[str, str]] = {}
    for table in result.parsed.tables:
        if "FileId" not in table.headers or "Data-File Size" not in table.headers:
            continue
        for row in table.rows:
            key = (row.get("Partition", ""), row.get("FileId", ""))
            instant = (row.get("Base-Instant") or "").strip()
            prior = latest.get(key)
            if prior is None or instant > (prior.get("Base-Instant") or "").strip():
                latest[key] = row
    sizes = []
    per_partition: dict[str, int] = {}
    log_files = 0
    for (partition, _fid), row in latest.items():
        n = parse_size(row.get("Data-File Size", ""))
        if n is not None and n > 0:
            sizes.append(n)
            per_partition[partition] = per_partition.get(partition, 0) + 1
        log_files += _int(row.get("Num Delta Files", "0"))
    if not sizes:
        return _error(
            f"no base files found in the file-system view for {table_path}",
            "For a MERGE_ON_READ table with only log files, run a compaction first.",
        )
    out = {
        "success": True,
        "base_file_count": len(sizes),
        "min_bytes": min(sizes),
        "avg_bytes": int(sum(sizes) / len(sizes)),
        "max_bytes": max(sizes),
        "total_bytes": sum(sizes),
        "human": {
            "min": human_size(min(sizes)),
            "avg": human_size(int(sum(sizes) / len(sizes))),
            "max": human_size(max(sizes)),
            "total": human_size(sum(sizes)),
        },
        "log_file_count": log_files,
        "files_per_partition": per_partition,
    }
    return json.dumps(out, indent=2)
