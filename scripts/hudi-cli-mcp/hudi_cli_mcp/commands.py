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

"""Command validation and building utilities."""

from __future__ import annotations

from enum import Enum


class RiskLevel(Enum):
    """Risk classification for write operations."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# Read-only commands that are safe to execute.
# Uses prefix matching — a command is allowed if it starts with any of these prefixes.
READONLY_COMMAND_PREFIXES = [
    # Connection and table info
    "connect",
    "desc",
    "refresh",
    "help",
    "fetch table schema",
    # Commits (read)
    "commits show",
    "commits showarchived",
    "commit showpartitions",
    "commit showfiles",
    "commit show_write_stats",
    "commits compare",
    "commits sync",
    # Compactions (read)
    "compactions show all",
    "compaction show",
    "compactions showarchived",
    "compaction showarchived",
    "compaction validate",
    # Cleans (read)
    "cleans show",
    "clean showpartitions",
    # File system view
    "show fsview all",
    "show fsview latest",
    # Stats
    "stats wa",
    "stats filesizes",
    # Savepoints (read)
    "savepoints show",
    # Rollbacks/Restores (read)
    "show rollbacks",
    "show rollback",
    "show restores",
    "show restore",
    # Timeline
    "timeline show active",
    "timeline show incomplete",
    "metadata timeline show active",
    "metadata timeline show incomplete",
    # Metadata (read)
    "metadata stats",
    "metadata list-partitions",
    "metadata list-files",
    "metadata validate-files",
    # Archived commits
    "show archived commit",
    # Log files
    "show logfile metadata",
    "show logfile records",
    # Environment
    "show envs all",
    "show env",
    # Diff
    "diff file",
    "diff partition",
    # Export
    "export instants",
    # Locks (read)
    "locks audit status",
    "locks audit validate",
    # Temp views (read)
    "temp_query",
    "temp query",
    "temps_show",
    "temps show",
]


# Write commands mapped to risk tiers.
# LOW: safe, non-destructive — executes immediately.
# MEDIUM: requires confirmation token before execution.
# HIGH: requires confirmation token + auto-preview of impact.
WRITE_COMMAND_PREFIXES: dict[str, RiskLevel] = {
    # LOW risk — non-destructive
    "savepoint create": RiskLevel.LOW,
    "locks audit enable": RiskLevel.LOW,
    "locks audit disable": RiskLevel.LOW,
    # MEDIUM risk — requires confirmation
    "compaction schedule": RiskLevel.MEDIUM,
    "clustering schedule": RiskLevel.MEDIUM,
    "compaction unschedule": RiskLevel.MEDIUM,
    "compaction unscheduleFileId": RiskLevel.MEDIUM,
    "savepoint delete": RiskLevel.MEDIUM,
    "marker delete": RiskLevel.MEDIUM,
    "metadata create": RiskLevel.MEDIUM,
    "metadata delete": RiskLevel.MEDIUM,
    "metadata delete-record-index": RiskLevel.MEDIUM,
    "metadata init": RiskLevel.MEDIUM,
    "trigger archival": RiskLevel.MEDIUM,
    "repair addpartitionmeta": RiskLevel.MEDIUM,
    "repair corrupted clean files": RiskLevel.MEDIUM,
    "repair migrate-partition-meta": RiskLevel.MEDIUM,
    "locks audit cleanup": RiskLevel.MEDIUM,
    "table recover-configs": RiskLevel.MEDIUM,
    # HIGH risk — destructive, requires confirmation + preview
    "commit rollback": RiskLevel.HIGH,
    "savepoint rollback": RiskLevel.HIGH,
    "cleans run": RiskLevel.HIGH,
    "compaction run": RiskLevel.HIGH,
    "compaction scheduleAndExecute": RiskLevel.HIGH,
    "compaction repair": RiskLevel.HIGH,
    "clustering run": RiskLevel.HIGH,
    "clustering scheduleAndExecute": RiskLevel.HIGH,
    "repair deduplicate": RiskLevel.HIGH,
    "repair overwrite-hoodie-props": RiskLevel.HIGH,
    "repair deprecated partition": RiskLevel.HIGH,
    "rename partition": RiskLevel.HIGH,
    "create": RiskLevel.HIGH,
    "table update-configs": RiskLevel.HIGH,
    "table delete-configs": RiskLevel.HIGH,
    "bootstrap run": RiskLevel.HIGH,
    "upgrade table": RiskLevel.HIGH,
    "downgrade table": RiskLevel.HIGH,
}


class CommandNotAllowedError(Exception):
    """Raised when a command is not in the allowlist."""

    pass


def _matches_prefix(cmd: str, prefix: str) -> bool:
    """True if ``cmd`` is exactly ``prefix`` or ``prefix`` followed by arguments.

    A word boundary is required after the prefix so that e.g. the read-only
    prefix ``desc`` does not accept an unrelated command like ``describe-x``,
    and ``compaction schedule`` does not accept ``compaction scheduleAndExecute``
    (which has its own, higher risk tier). Both sides are already lowercased.
    """
    return cmd == prefix or cmd.startswith(prefix + " ")


def is_readonly_command(command: str) -> bool:
    """Check if a command is in the read-only allowlist."""
    cmd = command.strip().lower()
    return any(_matches_prefix(cmd, prefix.lower()) for prefix in READONLY_COMMAND_PREFIXES)


def is_write_command(command: str) -> bool:
    """Check if a command is in the write command list."""
    cmd = command.strip().lower()
    return any(_matches_prefix(cmd, prefix.lower()) for prefix in WRITE_COMMAND_PREFIXES)


# Match the longest prefix first, so a command that extends a shorter prefix is
# classified by its most specific entry (belt-and-braces alongside the word-boundary
# check in _matches_prefix). Recomputed once at import time.
_WRITE_PREFIXES_BY_LENGTH = sorted(
    WRITE_COMMAND_PREFIXES.items(), key=lambda kv: len(kv[0]), reverse=True
)


def get_risk_level(command: str) -> RiskLevel | None:
    """Return the risk level for a write command, or None if not a write command."""
    cmd = command.strip().lower()
    for prefix, level in _WRITE_PREFIXES_BY_LENGTH:
        if _matches_prefix(cmd, prefix.lower()):
            return level
    return None


def _reject_injection(command: str) -> None:
    """Reject a command that smuggles extra commands via an embedded newline.

    The allowlist validates only the first line, but the executor writes each
    command as its own line in the CLI script file -- so a newline would let an
    unvalidated second command (e.g. a rollback) run. The caller is an LLM, so
    prompt-injected content reaching a command argument is an expected threat.
    """
    if "\n" in command or "\r" in command:
        raise CommandNotAllowedError(
            "Command contains an embedded newline (possible injection); "
            "commands must be single-line."
        )


# Guidance appended when a command isn't a recognized read-only command, so the
# caller (often an LLM) is steered to a real command instead of guessing again.
_READ_COMMAND_HINT = (
    "Supported read commands include: commits show, desc, metadata stats, stats wa, "
    "show fsview all, timeline show active, cleans show, compactions show all, "
    "savepoints show. To see which metadata indexes the table has, run `desc` and read "
    "the `hoodie.table.metadata.partitions` property (e.g. files, column_stats, "
    "partition_stats, record_index)."
)


def validate_command(command: str) -> None:
    """Validate that a command is an allowed read-only command.

    Distinguishes three cases so the error is actionable: (1) allowed, (2) a known
    write command that belongs on a dedicated write tool, (3) an unrecognized
    command -- do NOT tell the caller a nonexistent command is a "write command".
    """
    _reject_injection(command)
    if is_readonly_command(command):
        return
    if is_write_command(command):
        raise CommandNotAllowedError(
            f"'{command}' is a write operation and cannot be run via "
            "execute_hudi_command. Use the dedicated write tool for it."
        )
    raise CommandNotAllowedError(
        f"'{command}' is not a recognized read-only Hudi CLI command. " + _READ_COMMAND_HINT
    )


def validate_write_command(command: str) -> None:
    """Validate that a command is in the write command allowlist.

    Raises CommandNotAllowedError if the command is not recognized.
    """
    _reject_injection(command)
    if not is_write_command(command):
        raise CommandNotAllowedError(
            f"Command '{command}' is not a recognized write operation."
        )


def validate_commands(commands: list[str]) -> None:
    """Validate that all commands in a list are allowed."""
    for cmd in commands:
        # Reject a user-supplied connect: the session is the single source of truth
        # for the connected table (it auto-prepends its own connect). Allowing one
        # here would let a batch silently re-target a different table.
        if cmd.strip().lower().startswith("connect"):
            raise CommandNotAllowedError(
                "Do not include 'connect' in commands; the server connects to the "
                "session's table automatically. Use connect_to_table to switch tables."
            )
        validate_command(cmd)


def build_command(base: str, **kwargs) -> str:
    """Build a CLI command string from a base command and keyword arguments.

    Handles:
    - None values: skipped
    - bool True: added as flag (--key)
    - bool False: skipped
    - Empty string: skipped
    - Other values: added as --key value
    """
    parts = [base]
    for key, value in kwargs.items():
        if value is None:
            continue
        if isinstance(value, bool):
            if value:
                parts.append(f"--{key}")
        elif isinstance(value, str) and value == "":
            continue
        else:
            parts.append(f"--{key}")
            parts.append(quote_arg(str(value)))
    return " ".join(parts)


def quote_arg(value: str) -> str:
    """Quote a value that contains whitespace so it stays a single CLI argument.

    Spring Shell splits script-mode arguments on whitespace, so an unquoted path
    like ``/data/my table`` would be parsed as two arguments. Used by
    ``build_command`` and by every ``connect --path`` interpolation.
    """
    if value and any(c.isspace() for c in value):
        escaped = value.replace('"', '\\"')
        return f'"{escaped}"'
    return value


# Known hudi-cli output quirks, keyed by command prefix. These are surfaced as a
# ``hint`` alongside the affected command's result so an LLM caller is not misled
# by output that is technically returned but known to be wrong or misleading on
# current CLI builds. Keeping this knowledge server-side means every MCP client
# benefits without prompt engineering.
KNOWN_CLI_QUIRKS: dict[str, str] = {
    "stats filesizes": (
        "Known hudi-cli quirk: `stats filesizes` reports all zeros on some CLI "
        "builds even when data files exist. Read base-file sizes from "
        "`show fsview all` (Data-File Size column) instead."
    ),
    "metadata list-partitions": (
        "Known hudi-cli quirk: `metadata list-partitions` can fail with an internal "
        "exception on some CLI builds. Partitions are visible in `show fsview all` "
        "(Partition column)."
    ),
    "compaction schedule": (
        "Known hudi-cli quirk: a successful `compaction schedule` may still print "
        "'Failed to run compaction'. Verify the plan with `compactions show all` -- "
        "a new instant in REQUESTED state means scheduling succeeded."
    ),
    "metadata list-files": (
        "Known hudi-cli quirk: `metadata list-files` can hang until the timeout on "
        "some CLI builds. Files are visible in `show fsview all` instead."
    ),
    "metadata validate-files": (
        "Known hudi-cli quirk: `metadata validate-files` can hang until the timeout "
        "on some CLI builds."
    ),
    "show archived commit": (
        "Known hudi-cli quirk: `show archived commit` fails with an internal error "
        "on some CLI builds. Use `commits showarchived` to list archived commits."
    ),
    "export instants": (
        "Known hudi-cli quirk: `export instants` fails on some CLI builds even with "
        "a valid local folder. Read instants from `timeline show active` or "
        "`commits showarchived` instead."
    ),
}


def quirk_hint(command: str) -> str | None:
    """Return a known-quirk hint for ``command``, or None."""
    cmd = command.strip().lower()
    for prefix, hint in KNOWN_CLI_QUIRKS.items():
        if _matches_prefix(cmd, prefix):
            return hint
    return None
