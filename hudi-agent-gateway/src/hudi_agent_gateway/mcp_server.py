# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The MCP surface: the same tool registry, exposed to external agents."""

from __future__ import annotations

from fastmcp import FastMCP

from hudi_agent_gateway import __version__
from hudi_agent_gateway.tools.registry import ToolRegistry

_INSTRUCTIONS = (
    "Tools for querying an Apache Hudi lakehouse through Trino. Use list_tables "
    "and describe_table to discover the schema, then query_lakehouse with a "
    "single read-only SELECT statement (Trino SQL). Results are row-capped and "
    "may be truncated (indicated in the payload)."
)


def build_mcp(registry: ToolRegistry) -> FastMCP:
    mcp: FastMCP = FastMCP(
        name="hudi-lakehouse",
        version=__version__,
        instructions=_INSTRUCTIONS,
    )
    registry.attach_to_mcp(mcp)
    return mcp
