"""Higher-level workflow tools that compose multiple CLI commands."""

from __future__ import annotations

import json

from hudi_cli.executor import HudiCliExecutor
from hudi_cli.session import SessionManager


def _execute_workflow(
    path: str,
    commands: list[str],
    executor: HudiCliExecutor,
    session: SessionManager,
    workflow_name: str,
) -> str:
    """Common execution pattern for workflow tools.

    Connects to the table, runs the commands, and returns structured output.
    Also updates the session to remember the connected path.
    """
    full_commands = [f"connect --path {path}"] + commands
    result = executor.execute(full_commands)

    # Update session on successful connect
    if result.return_code == 0:
        session.connect(path)

    output = result.to_dict()
    output["success"] = result.return_code == 0
    output["workflow"] = workflow_name
    output["table_path"] = path
    return json.dumps(output, indent=2)


def table_health_check(
    path: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Run a comprehensive health check on a Hudi table.

    Executes: desc, commits show, cleans show, compactions show all, stats wa
    """
    commands = [
        "desc",
        "commits show --limit 10 --desc true",
        "cleans show --limit 5",
        "compactions show all",
        "stats wa",
    ]
    return _execute_workflow(path, commands, executor, session, "table_health_check")


def commit_details(
    path: str,
    commit_instant: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Get detailed information about a specific commit.

    Executes: commit showpartitions, commit showfiles, commit show_write_stats
    """
    commands = [
        f"commit showpartitions --commit {commit_instant}",
        f"commit showfiles --commit {commit_instant}",
        f"commit show_write_stats --commit {commit_instant}",
    ]
    return _execute_workflow(path, commands, executor, session, "commit_details")


def table_overview(
    path: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Get a comprehensive overview of a Hudi table.

    Executes: desc, fetch table schema, metadata list-partitions
    """
    commands = [
        "desc",
        "metadata list-partitions",
    ]
    return _execute_workflow(path, commands, executor, session, "table_overview")


def timeline_summary(
    path: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    limit: int = 20,
) -> str:
    """Get a summary of the table's timeline.

    Executes: timeline show active, timeline show incomplete
    """
    commands = [
        f"timeline show active --limit {limit}",
        "timeline show incomplete",
    ]
    return _execute_workflow(path, commands, executor, session, "timeline_summary")


def storage_analysis(
    path: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Analyze storage layout and file sizes.

    Executes: stats filesizes, show fsview latest, metadata stats
    """
    commands = [
        "stats filesizes",
        "show fsview latest",
        "metadata stats",
    ]
    return _execute_workflow(path, commands, executor, session, "storage_analysis")
