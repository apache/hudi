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

    # A composite workflow runs several read commands in one JVM. A single
    # sub-command that doesn't apply to this table (e.g. `compactions show all` on a
    # COPY_ON_WRITE table) must not fail the whole workflow -- and it can even flip
    # the CLI's exit code non-zero. Treat the run as successful if it produced any
    # table output (i.e. connected and the other commands ran); the offending
    # sub-command's error is surfaced under `errors` with a `partial` flag. A true
    # failure (bad path, timeout) yields no tables.
    ran = not result.timed_out and (
        result.return_code == 0 or len(result.parsed.tables) > 0
    )

    if ran:
        session.connect(path)

    output = result.to_dict()
    output["success"] = ran
    if ran and result.parsed.errors:
        output["partial"] = True
        output["notice"] = (
            "Some sub-commands did not apply to this table (see 'errors'); "
            "the rest of the workflow completed."
        )
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

    Executes: desc, metadata list-partitions
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
