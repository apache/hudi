"""Connection management tools."""

from __future__ import annotations

import json

from hudi_cli.executor import HudiCliExecutor
from hudi_cli.session import SessionManager


def connect_to_table(
    path: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Connect to a Hudi table and return its description.

    Runs connect + desc to validate the table and return metadata.
    """
    commands = [
        f"connect --path {path}",
        "desc",
    ]
    result = executor.execute(commands)

    if result.return_code != 0:
        error_output = result.parsed.to_dict()
        error_output["success"] = False
        error_output["error"] = f"Failed to connect to table at: {path}"
        return json.dumps(error_output, indent=2)

    # Check for connection errors in messages
    for msg in result.parsed.messages:
        if "exception" in msg.lower() or "error" in msg.lower():
            error_output = result.parsed.to_dict()
            error_output["success"] = False
            error_output["error"] = msg
            return json.dumps(error_output, indent=2)

    # Connection succeeded — store in session
    session.connect(path)

    output = result.to_dict()
    output["success"] = True
    output["connected_path"] = path
    return json.dumps(output, indent=2)


def disconnect(session: SessionManager) -> str:
    """Disconnect from the current table."""
    if not session.is_connected:
        return json.dumps({"success": True, "message": "No table was connected."})

    path = session.connected_path
    session.disconnect()
    return json.dumps(
        {"success": True, "message": f"Disconnected from {path}."}
    )


def show_connection(session: SessionManager) -> str:
    """Show the current connection status."""
    if session.is_connected:
        return json.dumps(
            {
                "connected": True,
                "path": session.connected_path,
            }
        )
    return json.dumps({"connected": False, "message": "Not connected to any table."})
