"""Write operation tools — individual write commands with safety controls."""

from __future__ import annotations

import json

from hudi_cli.commands import (
    CommandNotAllowedError,
    RiskLevel,
    build_command,
    get_risk_level,
    validate_write_command,
)
from hudi_cli.executor import HudiCliExecutor
from hudi_cli.safety import SafetyManager
from hudi_cli.session import NotConnectedError, SessionManager


def _execute_write_operation(
    command: str,
    description: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    dry_run_command: str | None = None,
) -> str:
    """Core write execution with tiered safety controls.

    LOW risk: executes immediately.
    MEDIUM/HIGH risk: creates a pending operation and returns a confirmation token.
    If dry_run_command is provided, runs it first and includes output in the response.
    """
    # Validate
    try:
        validate_write_command(command)
    except CommandNotAllowedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    try:
        table_path = session.require_connection()
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    risk_level = get_risk_level(command)
    if risk_level is None:
        return json.dumps(
            {"success": False, "error": f"Cannot determine risk level for: {command}"},
            indent=2,
        )

    # LOW risk — execute immediately
    if risk_level == RiskLevel.LOW:
        commands = session.build_command_list([command])
        result = executor.execute(commands)
        output = result.to_dict()
        output["success"] = result.return_code == 0
        output["command"] = command
        output["risk_level"] = risk_level.value
        return json.dumps(output, indent=2)

    # MEDIUM/HIGH risk — run optional dry-run, then create pending operation
    dry_run_result = None
    if dry_run_command:
        dry_commands = session.build_command_list([dry_run_command])
        dry_result = executor.execute(dry_commands)
        dry_run_result = json.dumps(dry_result.to_dict(), indent=2)

    op = safety.prepare_operation(
        command=command,
        risk_level=risk_level,
        table_path=table_path,
        description=description,
        dry_run_result=dry_run_result,
    )

    return json.dumps(
        {
            "success": True,
            "status": "pending_confirmation",
            "message": f"This is a {risk_level.value}-risk operation. "
            f"Call confirm_operation with the token to execute, "
            f"or cancel_operation to abort.",
            **op.to_dict(),
        },
        indent=2,
    )


# --- LOW risk operations ---


def create_savepoint(
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Create a savepoint at a specific commit instant."""
    command = build_command("savepoint create", commit=commit_instant)
    return _execute_write_operation(
        command,
        f"Create a savepoint at commit {commit_instant}. "
        f"This is a non-destructive recovery point.",
        executor,
        session,
        safety,
    )


def toggle_lock_audit(
    enable: bool,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Enable or disable lock audit logging."""
    command = "locks audit enable" if enable else "locks audit disable"
    action = "Enable" if enable else "Disable"
    return _execute_write_operation(
        command,
        f"{action} lock audit logging on the table.",
        executor,
        session,
        safety,
    )


# --- MEDIUM risk operations ---


def schedule_compaction(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Schedule a compaction (does not execute it)."""
    command = "compaction schedule"
    return _execute_write_operation(
        command,
        "Schedule a new compaction plan. This does NOT execute compaction — "
        "it only creates a plan that can be executed later.",
        executor,
        session,
        safety,
    )


def schedule_clustering(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Schedule a clustering operation (does not execute it)."""
    command = "clustering schedule"
    return _execute_write_operation(
        command,
        "Schedule a new clustering plan. This does NOT execute clustering — "
        "it only creates a plan that can be executed later.",
        executor,
        session,
        safety,
    )


def unschedule_compaction(
    instant_time: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Remove a scheduled compaction plan."""
    command = build_command("compaction unschedule", instant=instant_time)
    return _execute_write_operation(
        command,
        f"Remove the scheduled compaction plan at instant {instant_time}.",
        executor,
        session,
        safety,
    )


def delete_savepoint(
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Delete a savepoint."""
    command = build_command("savepoint delete", commit=commit_instant)
    return _execute_write_operation(
        command,
        f"Delete the savepoint at commit {commit_instant}. "
        f"This removes the recovery point.",
        executor,
        session,
        safety,
    )


def delete_markers(
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Delete write markers for a commit."""
    command = build_command("marker delete", commit=commit_instant)
    return _execute_write_operation(
        command,
        f"Delete write markers for commit {commit_instant}.",
        executor,
        session,
        safety,
    )


def manage_metadata(
    action: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Create, delete, init, or delete-record-index on the metadata table."""
    valid_actions = {"create", "delete", "delete-record-index", "init"}
    if action not in valid_actions:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid action '{action}'. Must be one of: {', '.join(sorted(valid_actions))}",
            },
            indent=2,
        )

    command = f"metadata {action}"
    descriptions = {
        "create": "Create the metadata table if it does not exist.",
        "delete": "Delete the metadata table (backup will be created).",
        "delete-record-index": "Delete the record index from the metadata table.",
        "init": "Initialize/update the metadata table from commit timeline.",
    }
    return _execute_write_operation(
        command,
        descriptions[action],
        executor,
        session,
        safety,
    )


def trigger_archival(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Trigger timeline archival of old commits."""
    command = "trigger archival"
    return _execute_write_operation(
        command,
        "Trigger archival to move old commits from the active timeline to the archive.",
        executor,
        session,
        safety,
    )


def repair_table(
    repair_type: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    dry_run: bool = True,
) -> str:
    """Run a table repair operation."""
    valid_types = {
        "addpartitionmeta": "Add partition metadata to partitions that are missing it.",
        "corrupted-clean-files": "Remove corrupted clean action files from the timeline.",
        "migrate-partition-meta": "Migrate partition metadata from text to base file format.",
    }
    if repair_type not in valid_types:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid repair type '{repair_type}'. "
                f"Must be one of: {', '.join(sorted(valid_types))}",
            },
            indent=2,
        )

    if repair_type == "corrupted-clean-files":
        command = "repair corrupted clean files"
    else:
        command = build_command(f"repair {repair_type}", dryrun=str(dry_run).lower())

    description = valid_types[repair_type]
    if dry_run and repair_type != "corrupted-clean-files":
        description += " (DRY RUN — no changes will be made)"

    return _execute_write_operation(
        command,
        description,
        executor,
        session,
        safety,
    )


def recover_table_configs(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Recover table configs from a failed update/delete operation."""
    command = "table recover-configs"
    return _execute_write_operation(
        command,
        "Recover table configuration from a previous failed update or delete operation.",
        executor,
        session,
        safety,
    )


# --- HIGH risk operations ---


def rollback_commit(
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Roll back a specific commit. Shows affected partitions/files first."""
    command = build_command("commit rollback", commit=commit_instant)
    # Preview: show what this commit touched
    preview_command = f"commit showpartitions --commit {commit_instant}"
    return _execute_write_operation(
        command,
        f"ROLLBACK commit {commit_instant}. This will undo all writes from this commit. "
        f"This operation is DESTRUCTIVE and cannot be undone.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def rollback_to_savepoint(
    savepoint_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Roll back the table to a savepoint."""
    command = build_command("savepoint rollback", savepoint=savepoint_instant)
    # Preview: show savepoints and recent commits
    preview_command = "savepoints show"
    return _execute_write_operation(
        command,
        f"ROLLBACK to savepoint {savepoint_instant}. All commits after this savepoint "
        f"will be PERMANENTLY undone. This is DESTRUCTIVE and cannot be reversed.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def run_compaction(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    instant_time: str | None = None,
) -> str:
    """Execute a compaction operation."""
    if instant_time:
        command = build_command("compaction run", compactionInstant=instant_time)
        description = f"Execute compaction for instant {instant_time}."
    else:
        command = "compaction run"
        description = "Execute the next pending compaction plan."

    # Preview: show pending compactions
    preview_command = "compactions show all"
    return _execute_write_operation(
        command,
        description + " This will rewrite data files and is resource-intensive.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def run_clustering(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    instant_time: str | None = None,
) -> str:
    """Execute a clustering operation."""
    if instant_time:
        command = build_command("clustering run", clusteringInstant=instant_time)
        description = f"Execute clustering for instant {instant_time}."
    else:
        command = "clustering run"
        description = "Execute the next pending clustering plan."

    preview_command = "compactions show all"
    return _execute_write_operation(
        command,
        description + " This reorganizes data files and is resource-intensive.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def run_clean(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Execute a clean operation (deletes old file versions)."""
    command = "cleans run"
    preview_command = "cleans show --limit 5"
    return _execute_write_operation(
        command,
        "Execute cleaning to DELETE old file versions based on the retention policy. "
        "Deleted files CANNOT be recovered.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def update_table_configs(
    props_file: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Update table configuration from a properties file."""
    command = build_command("table update-configs", **{"props-file": props_file})
    preview_command = "desc"
    return _execute_write_operation(
        command,
        f"Update table configuration with properties from {props_file}. "
        f"This modifies hoodie.properties.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def delete_table_configs(
    config_keys: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Delete specific table configuration keys."""
    command = build_command(
        "table delete-configs", **{"comma-separated-configs": config_keys}
    )
    preview_command = "desc"
    return _execute_write_operation(
        command,
        f"DELETE table configuration keys: {config_keys}. "
        f"This modifies hoodie.properties and may affect table behavior.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )


def upgrade_or_downgrade_table(
    direction: str,
    to_version: int,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Upgrade or downgrade the table version."""
    if direction not in ("upgrade", "downgrade"):
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid direction '{direction}'. Must be 'upgrade' or 'downgrade'.",
            },
            indent=2,
        )

    command = build_command(f"{direction} table", toVersion=str(to_version))
    preview_command = "desc"
    return _execute_write_operation(
        command,
        f"{direction.upper()} table to version {to_version}. "
        f"This modifies the table structure and may not be reversible.",
        executor,
        session,
        safety,
        dry_run_command=preview_command,
    )
