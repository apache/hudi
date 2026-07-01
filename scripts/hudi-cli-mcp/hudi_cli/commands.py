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


def is_readonly_command(command: str) -> bool:
    """Check if a command is in the read-only allowlist."""
    cmd = command.strip().lower()
    return any(cmd.startswith(prefix.lower()) for prefix in READONLY_COMMAND_PREFIXES)


def is_write_command(command: str) -> bool:
    """Check if a command is in the write command list."""
    cmd = command.strip().lower()
    return any(cmd.startswith(prefix.lower()) for prefix in WRITE_COMMAND_PREFIXES)


def get_risk_level(command: str) -> RiskLevel | None:
    """Return the risk level for a write command, or None if not a write command."""
    cmd = command.strip().lower()
    for prefix, level in WRITE_COMMAND_PREFIXES.items():
        if cmd.startswith(prefix.lower()):
            return level
    return None


def validate_command(command: str) -> None:
    """Validate that a command is allowed (read-only).

    Raises CommandNotAllowedError if the command is not allowed.
    """
    if not is_readonly_command(command):
        raise CommandNotAllowedError(
            f"Command '{command}' is not allowed in read-only mode. "
            f"Use the dedicated write operation tools for write commands."
        )


def validate_write_command(command: str) -> None:
    """Validate that a command is in the write command allowlist.

    Raises CommandNotAllowedError if the command is not recognized.
    """
    if not is_write_command(command):
        raise CommandNotAllowedError(
            f"Command '{command}' is not a recognized write operation."
        )


def validate_commands(commands: list[str]) -> None:
    """Validate that all commands in a list are allowed."""
    for cmd in commands:
        # Skip connect — it's always allowed and auto-prepended
        if cmd.strip().lower().startswith("connect"):
            continue
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
            parts.append(str(value))
    return " ".join(parts)
