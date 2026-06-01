"""Generic command execution tools."""

from __future__ import annotations

import json

from hudi_cli.commands import CommandNotAllowedError, validate_command, validate_commands
from hudi_cli.executor import HudiCliExecutor
from hudi_cli.session import NotConnectedError, SessionManager


def execute_hudi_command(
    command: str,
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Execute a single Hudi CLI command.

    The command is validated against the read-only allowlist.
    If connected to a table, the connect command is auto-prepended.
    """
    command = command.strip()

    # Validate read-only
    try:
        validate_command(command)
    except CommandNotAllowedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Build command list with auto-connect
    try:
        commands = session.build_command_list([command])
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    result = executor.execute(commands)

    output = result.to_dict()
    output["success"] = result.return_code == 0
    output["command"] = command
    return json.dumps(output, indent=2)


def execute_hudi_commands(
    commands: list[str],
    executor: HudiCliExecutor,
    session: SessionManager,
) -> str:
    """Execute multiple Hudi CLI commands in a single CLI invocation.

    All commands are validated against the read-only allowlist.
    Saves JVM startup time by batching commands.
    """
    commands = [cmd.strip() for cmd in commands if cmd.strip()]

    if not commands:
        return json.dumps(
            {"success": False, "error": "No commands provided."}, indent=2
        )

    # Validate all commands
    try:
        validate_commands(commands)
    except CommandNotAllowedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    # Build command list with auto-connect
    try:
        full_commands = session.build_command_list(commands)
    except NotConnectedError as e:
        return json.dumps({"success": False, "error": str(e)}, indent=2)

    result = executor.execute(full_commands)

    output = result.to_dict()
    output["success"] = result.return_code == 0
    output["commands"] = commands
    return json.dumps(output, indent=2)
