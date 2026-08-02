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
    output["success"] = result.is_success()
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
    output["success"] = result.is_success()
    output["commands"] = commands
    return json.dumps(output, indent=2)
