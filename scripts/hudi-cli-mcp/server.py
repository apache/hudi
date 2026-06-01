"""Hudi CLI MCP Server — exposes Hudi CLI commands as MCP tools for Claude."""

from __future__ import annotations

from fastmcp import FastMCP

from hudi_cli.executor import HudiCliExecutor
from hudi_cli.safety import SafetyManager
from hudi_cli.session import SessionManager
from tools.confirmation import (
    cancel_operation as _cancel_operation,
    confirm_operation as _confirm_operation,
    list_pending_operations as _list_pending_operations,
)
from tools.connection import (
    connect_to_table as _connect_to_table,
    disconnect as _disconnect,
    show_connection as _show_connection,
)
from tools.generic import (
    execute_hudi_command as _execute_hudi_command,
    execute_hudi_commands as _execute_hudi_commands,
)
from tools.workflows import (
    commit_details as _commit_details,
    storage_analysis as _storage_analysis,
    table_health_check as _table_health_check,
    table_overview as _table_overview,
    timeline_summary as _timeline_summary,
)
from tools.write_ops import (
    create_savepoint as _create_savepoint,
    delete_markers as _delete_markers,
    delete_savepoint as _delete_savepoint,
    delete_table_configs as _delete_table_configs,
    manage_metadata as _manage_metadata,
    recover_table_configs as _recover_table_configs,
    repair_table as _repair_table,
    rollback_commit as _rollback_commit,
    rollback_to_savepoint as _rollback_to_savepoint,
    run_clean as _run_clean,
    run_clustering as _run_clustering,
    run_compaction as _run_compaction,
    schedule_clustering as _schedule_clustering,
    schedule_compaction as _schedule_compaction,
    toggle_lock_audit as _toggle_lock_audit,
    trigger_archival as _trigger_archival,
    unschedule_compaction as _unschedule_compaction,
    update_table_configs as _update_table_configs,
    upgrade_or_downgrade_table as _upgrade_or_downgrade_table,
)
from tools.write_workflows import (
    clustering_workflow as _clustering_workflow,
    compaction_workflow as _compaction_workflow,
    safe_rollback_workflow as _safe_rollback_workflow,
    table_repair_workflow as _table_repair_workflow,
)

# Initialize shared components
executor = HudiCliExecutor()
session = SessionManager()
safety = SafetyManager()

# Create MCP server
mcp = FastMCP("hudi-cli")


# =============================================================================
# Connection Tools
# =============================================================================


@mcp.tool()
def connect_to_table(path: str) -> str:
    """Connect to a Hudi table at the given path.

    This must be called before running any table commands.
    Validates the connection by running 'desc' and returns table metadata.

    Args:
        path: Base path of the Hudi table (e.g., /tmp/trips_table, s3://bucket/table)
    """
    return _connect_to_table(path, executor, session)


@mcp.tool()
def disconnect() -> str:
    """Disconnect from the currently connected Hudi table."""
    return _disconnect(session)


@mcp.tool()
def show_connection() -> str:
    """Show the current connection status and connected table path."""
    return _show_connection(session)


# =============================================================================
# Generic Read-Only Execution Tools
# =============================================================================


@mcp.tool()
def execute_hudi_command(command: str) -> str:
    """Execute any single read-only Hudi CLI command and return structured results.

    The table connection is automatically handled — no need to include 'connect' in the command.
    Only read-only commands are allowed. For write operations, use the dedicated write tools.

    Args:
        command: The Hudi CLI command to execute (e.g., "commits show --limit 10 --desc true")

    Common commands:
        - commits show --limit N --desc true: Show recent commits
        - commit showpartitions --commit <instant>: Partition stats for a commit
        - commit showfiles --commit <instant>: File details for a commit
        - compactions show all: Show pending compactions
        - cleans show --limit N: Show clean operations
        - stats wa: Write amplification stats
        - stats filesizes --partitionPath <path>: File size distribution
        - show fsview all / show fsview latest: File system view
        - timeline show active --limit N: Active timeline instants
        - metadata list-partitions: List all partitions
        - metadata list-files --partition <path>: List files in a partition
        - savepoints show: Show savepoints
        - show rollbacks: Show rollback history
    """
    return _execute_hudi_command(command, executor, session)


@mcp.tool()
def execute_hudi_commands(commands: list[str]) -> str:
    """Execute multiple read-only Hudi CLI commands in a single invocation.

    Batches commands into one CLI session to avoid multiple JVM startups (~5-8s each).
    The table connection is automatically handled.
    Only read-only commands are allowed.

    Args:
        commands: List of CLI commands to execute in sequence
    """
    return _execute_hudi_commands(commands, executor, session)


# =============================================================================
# Read-Only Workflow Tools
# =============================================================================


@mcp.tool()
def table_health_check(path: str) -> str:
    """Run a comprehensive health check on a Hudi table.

    Connects to the table and runs: desc, commits show (last 10),
    cleans show (last 5), compactions show all, and write amplification stats.

    Args:
        path: Base path of the Hudi table
    """
    return _table_health_check(path, executor, session)


@mcp.tool()
def commit_details(path: str, commit_instant: str) -> str:
    """Get detailed information about a specific commit instant.

    Shows partition-level stats, file-level details, and write statistics
    for the given commit.

    Args:
        path: Base path of the Hudi table
        commit_instant: The commit instant timestamp (e.g., "20240101120000")
    """
    return _commit_details(path, commit_instant, executor, session)


@mcp.tool()
def table_overview(path: str) -> str:
    """Get a comprehensive overview of a Hudi table.

    Shows table properties/configuration and lists all partitions.

    Args:
        path: Base path of the Hudi table
    """
    return _table_overview(path, executor, session)


@mcp.tool()
def timeline_summary(path: str, limit: int = 20) -> str:
    """Get a summary of the table's active and incomplete timeline instants.

    Args:
        path: Base path of the Hudi table
        limit: Maximum number of active instants to show (default: 20)
    """
    return _timeline_summary(path, executor, session, limit)


@mcp.tool()
def storage_analysis(path: str) -> str:
    """Analyze storage layout, file sizes, and metadata statistics.

    Shows file size distribution, latest file system view, and metadata table stats.

    Args:
        path: Base path of the Hudi table
    """
    return _storage_analysis(path, executor, session)


# =============================================================================
# Confirmation Tools (for write operations)
# =============================================================================


@mcp.tool()
def confirm_operation(token: str) -> str:
    """Confirm and execute a previously prepared write operation.

    Write operations with MEDIUM or HIGH risk return a confirmation token
    instead of executing immediately. Use this tool with that token to
    actually execute the operation.

    Args:
        token: The confirmation token returned by the write operation tool.
    """
    return _confirm_operation(token, executor, session, safety)


@mcp.tool()
def cancel_operation(token: str) -> str:
    """Cancel a pending write operation without executing it.

    Args:
        token: The confirmation token to cancel.
    """
    return _cancel_operation(token, safety)


@mcp.tool()
def list_pending_operations() -> str:
    """List all pending write operations awaiting confirmation.

    Shows tokens, descriptions, risk levels, and expiry times for all
    operations that have been prepared but not yet confirmed or cancelled.
    """
    return _list_pending_operations(safety)


# =============================================================================
# Write Operations — LOW Risk (execute immediately)
# =============================================================================


@mcp.tool()
def create_savepoint(commit_instant: str) -> str:
    """Create a savepoint at the given commit instant.

    A savepoint marks a recovery point that can be rolled back to later.
    This is a safe, non-destructive operation that executes immediately.

    Args:
        commit_instant: The commit timestamp to savepoint (e.g., "20240101120000")
    """
    return _create_savepoint(commit_instant, executor, session, safety)


@mcp.tool()
def toggle_lock_audit(enable: bool) -> str:
    """Enable or disable lock audit logging on the connected table.

    This is a safe, non-destructive operation that executes immediately.

    Args:
        enable: True to enable audit logging, False to disable.
    """
    return _toggle_lock_audit(enable, executor, session, safety)


# =============================================================================
# Write Operations — MEDIUM Risk (confirmation token required)
# =============================================================================


@mcp.tool()
def schedule_compaction() -> str:
    """Schedule a compaction operation on the connected table.

    This schedules compaction but does NOT execute it. A compaction plan
    will be created that can later be executed with run_compaction.
    Requires confirmation before the schedule command runs.
    """
    return _schedule_compaction(executor, session, safety)


@mcp.tool()
def schedule_clustering() -> str:
    """Schedule a clustering operation on the connected table.

    This schedules clustering but does NOT execute it. A clustering plan
    will be created that can later be executed with run_clustering.
    Requires confirmation.
    """
    return _schedule_clustering(executor, session, safety)


@mcp.tool()
def unschedule_compaction(instant_time: str) -> str:
    """Remove a scheduled compaction plan.

    Requires confirmation.

    Args:
        instant_time: The compaction instant to unschedule.
    """
    return _unschedule_compaction(instant_time, executor, session, safety)


@mcp.tool()
def delete_savepoint(commit_instant: str) -> str:
    """Delete a savepoint. Requires confirmation.

    Args:
        commit_instant: The savepoint commit timestamp to delete.
    """
    return _delete_savepoint(commit_instant, executor, session, safety)


@mcp.tool()
def delete_markers(commit_instant: str) -> str:
    """Delete write markers for a commit instant. Requires confirmation.

    Args:
        commit_instant: The commit timestamp whose markers to delete.
    """
    return _delete_markers(commit_instant, executor, session, safety)


@mcp.tool()
def manage_metadata(action: str) -> str:
    """Create, delete, initialize, or delete-record-index on the metadata table.

    Requires confirmation.

    Args:
        action: One of "create", "delete", "delete-record-index", or "init".
    """
    return _manage_metadata(action, executor, session, safety)


@mcp.tool()
def trigger_archival() -> str:
    """Trigger timeline archival to move old commits to the archive.

    Requires confirmation.
    """
    return _trigger_archival(executor, session, safety)


@mcp.tool()
def repair_table(repair_type: str, dry_run: bool = True) -> str:
    """Run a table repair operation.

    Defaults to dry-run mode (preview only). Set dry_run=False to actually apply.
    Requires confirmation.

    Args:
        repair_type: One of "addpartitionmeta", "corrupted-clean-files",
                     or "migrate-partition-meta".
        dry_run: If True (default), only shows what would be changed.
    """
    return _repair_table(repair_type, executor, session, safety, dry_run)


@mcp.tool()
def recover_table_configs() -> str:
    """Recover table configs from a failed config update/delete operation.

    Requires confirmation.
    """
    return _recover_table_configs(executor, session, safety)


# =============================================================================
# Write Operations — HIGH Risk (confirmation token + auto-preview)
# =============================================================================


@mcp.tool()
def rollback_commit(commit_instant: str) -> str:
    """Roll back a specific commit. HIGH RISK — destructive and irreversible.

    Shows the affected partitions/files before requesting confirmation.
    All data written by this commit will be permanently removed.

    Args:
        commit_instant: The commit timestamp to roll back.
    """
    return _rollback_commit(commit_instant, executor, session, safety)


@mcp.tool()
def rollback_to_savepoint(savepoint_instant: str) -> str:
    """Roll back the table to a savepoint. HIGH RISK — destructive and irreversible.

    All commits after the savepoint will be permanently undone.
    Requires confirmation.

    Args:
        savepoint_instant: The savepoint timestamp to roll back to.
    """
    return _rollback_to_savepoint(savepoint_instant, executor, session, safety)


@mcp.tool()
def run_compaction(instant_time: str | None = None) -> str:
    """Execute a compaction operation. HIGH RISK — rewrites data files.

    Shows pending compaction plans before requesting confirmation.

    Args:
        instant_time: Specific compaction instant to execute. If omitted, runs the next pending plan.
    """
    return _run_compaction(executor, session, safety, instant_time)


@mcp.tool()
def run_clustering(instant_time: str | None = None) -> str:
    """Execute a clustering operation. HIGH RISK — reorganizes data files.

    Requires confirmation.

    Args:
        instant_time: Specific clustering instant to execute. If omitted, runs the next pending plan.
    """
    return _run_clustering(executor, session, safety, instant_time)


@mcp.tool()
def run_clean() -> str:
    """Execute a clean operation. HIGH RISK — deletes old file versions.

    Shows recent clean history before requesting confirmation.
    Deleted files CANNOT be recovered.
    """
    return _run_clean(executor, session, safety)


@mcp.tool()
def update_table_configs(props_file: str) -> str:
    """Update table configuration from a properties file. HIGH RISK.

    Shows current table description before requesting confirmation.

    Args:
        props_file: Path to properties file with new configuration values.
    """
    return _update_table_configs(props_file, executor, session, safety)


@mcp.tool()
def delete_table_configs(config_keys: str) -> str:
    """Delete specific table configuration keys. HIGH RISK.

    Shows current table description before requesting confirmation.

    Args:
        config_keys: Comma-separated list of config keys to delete.
    """
    return _delete_table_configs(config_keys, executor, session, safety)


@mcp.tool()
def upgrade_or_downgrade_table(direction: str, to_version: int) -> str:
    """Upgrade or downgrade the table version. HIGH RISK.

    Shows current table description before requesting confirmation.

    Args:
        direction: "upgrade" or "downgrade".
        to_version: Target table version number.
    """
    return _upgrade_or_downgrade_table(direction, to_version, executor, session, safety)


# =============================================================================
# Write Workflow Tools (composite read + write operations)
# =============================================================================


@mcp.tool()
def compaction_workflow(instant_time: str | None = None) -> str:
    """Show pending compactions, then prepare a compaction run for confirmation.

    A guided workflow that first shows pending compaction plans, then prepares
    the execution for confirmation.

    Args:
        instant_time: Specific compaction instant to execute. If omitted, runs the next pending plan.
    """
    return _compaction_workflow(executor, session, safety, instant_time)


@mcp.tool()
def clustering_workflow(instant_time: str | None = None) -> str:
    """Show storage layout, then prepare a clustering run for confirmation.

    A guided workflow that first shows file sizes and filesystem view, then
    prepares clustering execution for confirmation.

    Args:
        instant_time: Specific clustering instant to execute. If omitted, runs the next pending plan.
    """
    return _clustering_workflow(executor, session, safety, instant_time)


@mcp.tool()
def safe_rollback_workflow(commit_instant: str) -> str:
    """Guided rollback workflow: shows commit impact, then prepares rollback.

    1. Shows partition and file details for the target commit
    2. Shows current savepoints
    3. Prepares the rollback command for confirmation with full context

    Args:
        commit_instant: The commit timestamp to roll back.
    """
    return _safe_rollback_workflow(commit_instant, executor, session, safety)


@mcp.tool()
def table_repair_workflow(repair_type: str) -> str:
    """Guided repair workflow: runs dry-run first, then prepares real repair.

    1. Executes the repair in dry-run mode to preview changes
    2. Prepares the actual repair for confirmation

    Args:
        repair_type: One of "addpartitionmeta" or "migrate-partition-meta".
    """
    return _table_repair_workflow(repair_type, executor, session, safety)


if __name__ == "__main__":
    mcp.run()
