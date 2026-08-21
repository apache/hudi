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

"""Confirmation tools for pending write operations."""

from __future__ import annotations

import json

from hudi_cli_mcp.commands import quirk_hint, quote_arg
from hudi_cli_mcp.executor import WRITE_TIMEOUT, HudiCliExecutor
from hudi_cli_mcp.safety import SafetyManager, TokenExpiredError, TokenNotFoundError
from hudi_cli_mcp.session import SessionManager


def confirm_operation(
    token: str,
    executor: HudiCliExecutor,
    session: SessionManager,
    safety: SafetyManager,
) -> str:
    """Confirm and execute a pending write operation."""
    try:
        op = safety.confirm(token)
    except (TokenNotFoundError, TokenExpiredError) as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Execute the confirmed command. Write operations launch real Spark jobs
    # (compaction, clustering, rollback) that routinely take many minutes, so
    # they get the write-path timeout, not the 120s read default.
    commands = [f"connect --path {quote_arg(op.table_path)}", op.command]
    result = executor.execute(commands, timeout=WRITE_TIMEOUT)

    output = result.to_dict()
    output["success"] = result.is_success()
    output["confirmed_command"] = op.command
    output["risk_level"] = op.risk_level.value
    output["table_path"] = op.table_path
    hint = quirk_hint(op.command)
    if hint:
        output["hint"] = hint
    return json.dumps(output, indent=2)


def cancel_operation(
    token: str,
    safety: SafetyManager,
) -> str:
    """Cancel a pending write operation."""
    try:
        op = safety.cancel(token)
    except TokenNotFoundError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    return json.dumps(
        {
            "success": True,
            "message": "Operation cancelled.",
            "cancelled_command": op.command,
        },
        indent=2,
    )


def list_pending_operations(safety: SafetyManager) -> str:
    """List all pending write operations awaiting confirmation."""
    pending = safety.list_pending()
    return json.dumps(
        {
            "success": True,
            "pending_count": len(pending),
            "operations": [op.to_dict() for op in pending],
        },
        indent=2,
    )
