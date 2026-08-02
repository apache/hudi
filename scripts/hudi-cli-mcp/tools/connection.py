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

    # is_success() already folds in captured error lines from the CLI; use it
    # instead of scanning messages for the substrings "error"/"exception", which
    # false-fails on any table path or property that merely contains those words
    # (e.g. /data/error_logs_table).
    if not result.is_success():
        error_output = result.to_dict()
        error_output["success"] = False
        error_output["error"] = (
            result.parsed.errors[0]
            if result.parsed.errors
            else f"Failed to connect to table at: {path}"
        )
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
