"""Composite write workflow tools that combine read + write operations."""

from __future__ import annotations

import json

from hudi_cli.commands import RiskLevel, build_command
from hudi_cli.executor import HudiCliExecutor
from hudi_cli.safety import SafetyManager
from hudi_cli.session import NotConnectedError, SessionManager


def _gather_context(
    commands: list[str],
    executor: HudiCliExecutor,
    session: SessionManager,
    table_path: str,
) -> dict:
    """Run read-only commands to gather context before a write operation."""
    full_commands = [f"connect --path {table_path}"] + commands
    result = executor.execute(full_commands)
    return result.to_dict()


def compaction_workflow(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    instant_time: str | None = None,
) -> str:
    """Show pending compactions, then prepare a compaction run for confirmation.

    1. Shows all pending compaction plans
    2. Prepares a compaction run command with confirmation token
    """
    try:
        table_path = session.require_connection()
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Gather context: show pending compactions
    context = _gather_context(
        ["compactions show all"], executor, session, table_path
    )

    # Build the compaction run command
    if instant_time:
        command = build_command("compaction run", compactionInstant=instant_time)
        description = f"Execute compaction for instant {instant_time}."
    else:
        command = "compaction run"
        description = "Execute the next pending compaction plan."

    op = safety.prepare_operation(
        command=command,
        risk_level=RiskLevel.HIGH,
        table_path=table_path,
        description=description + " This will rewrite data files.",
        dry_run_result=json.dumps(context, indent=2),
    )

    return json.dumps(
        {
            "success": True,
            "status": "pending_confirmation",
            "workflow": "compaction",
            "message": "Review the pending compactions below, then confirm or cancel.",
            "pending_compactions": context,
            **op.to_dict(),
        },
        indent=2,
    )


def clustering_workflow(
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
    instant_time: str | None = None,
) -> str:
    """Show table state, then prepare a clustering run for confirmation."""
    try:
        table_path = session.require_connection()
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Gather context: show file sizes and fsview
    context = _gather_context(
        ["stats filesizes", "show fsview latest"],
        executor,
        session,
        table_path,
    )

    if instant_time:
        command = build_command("clustering run", clusteringInstant=instant_time)
        description = f"Execute clustering for instant {instant_time}."
    else:
        command = "clustering run"
        description = "Execute the next pending clustering plan."

    op = safety.prepare_operation(
        command=command,
        risk_level=RiskLevel.HIGH,
        table_path=table_path,
        description=description + " This will reorganize data files.",
        dry_run_result=json.dumps(context, indent=2),
    )

    return json.dumps(
        {
            "success": True,
            "status": "pending_confirmation",
            "workflow": "clustering",
            "message": "Review the storage layout below, then confirm or cancel.",
            "storage_context": context,
            **op.to_dict(),
        },
        indent=2,
    )


def safe_rollback_workflow(
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Show commit impact, then prepare a rollback for confirmation.

    1. Shows partition and file details for the target commit
    2. Shows current savepoints
    3. Prepares the rollback command with full context
    """
    try:
        table_path = session.require_connection()
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Gather context: what did this commit touch?
    context = _gather_context(
        [
            f"commit showpartitions --commit {commit_instant}",
            f"commit showfiles --commit {commit_instant}",
            "savepoints show",
        ],
        executor,
        session,
        table_path,
    )

    command = build_command("commit rollback", commit=commit_instant)
    op = safety.prepare_operation(
        command=command,
        risk_level=RiskLevel.HIGH,
        table_path=table_path,
        description=(
            f"ROLLBACK commit {commit_instant}. All data written by this commit "
            f"will be permanently removed. This CANNOT be undone."
        ),
        dry_run_result=json.dumps(context, indent=2),
    )

    return json.dumps(
        {
            "success": True,
            "status": "pending_confirmation",
            "workflow": "safe_rollback",
            "message": "Review the commit details below. This rollback is IRREVERSIBLE.",
            "commit_context": context,
            **op.to_dict(),
        },
        indent=2,
    )


def table_repair_workflow(
    repair_type: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Run a repair in dry-run mode first, then prepare the real repair for confirmation.

    1. Runs the repair with --dryrun true to preview changes
    2. Prepares the actual repair (--dryrun false) for confirmation
    """
    valid_types = {
        "addpartitionmeta": "repair addpartitionmeta",
        "migrate-partition-meta": "repair migrate-partition-meta",
    }
    if repair_type not in valid_types:
        return json.dumps(
            {
                "success": False,
                "error": f"Invalid repair type '{repair_type}'. "
                f"Dry-run workflow supports: {', '.join(sorted(valid_types))}. "
                f"For 'corrupted-clean-files', use the repair_table tool directly.",
            },
            indent=2,
        )

    try:
        table_path = session.require_connection()
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Step 1: dry-run
    base_cmd = valid_types[repair_type]
    dry_run_cmd = build_command(base_cmd, dryrun="true")
    dry_context = _gather_context([dry_run_cmd], executor, session, table_path)

    # Step 2: prepare real repair
    real_cmd = build_command(base_cmd, dryrun="false")
    op = safety.prepare_operation(
        command=real_cmd,
        risk_level=RiskLevel.MEDIUM,
        table_path=table_path,
        description=(
            f"Execute {repair_type} repair (for real this time — not a dry run). "
            f"Review the dry-run results below before confirming."
        ),
        dry_run_result=json.dumps(dry_context, indent=2),
    )

    return json.dumps(
        {
            "success": True,
            "status": "pending_confirmation",
            "workflow": "table_repair",
            "message": "Dry-run complete. Review results below, then confirm or cancel.",
            "dry_run_results": dry_context,
            **op.to_dict(),
        },
        indent=2,
    )
