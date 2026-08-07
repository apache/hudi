"""Confirmation tools for pending write operations."""

from __future__ import annotations

import json

from hudi_cli.executor import HudiCliExecutor
from hudi_cli.safety import SafetyManager, TokenExpiredError, TokenNotFoundError
from hudi_cli.session import SessionManager


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

    # Execute the confirmed command
    commands = [f"connect --path {op.table_path}", op.command]
    result = executor.execute(commands)

    output = result.to_dict()
    output["success"] = result.return_code == 0
    output["confirmed_command"] = op.command
    output["risk_level"] = op.risk_level.value
    output["table_path"] = op.table_path
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
