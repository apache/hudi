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

"""Tool assembly: the single place where tool modules get registered.

Adding a new tool module = write the module with a ``register(registry, ...)``
function, then call it here.
"""

from __future__ import annotations

from hudi_agent_gateway.config import GatewaySettings
from hudi_agent_gateway.tools import trino_tools
from hudi_agent_gateway.tools.registry import ToolRegistry
from hudi_agent_gateway.tools.trino_client import TrinoClient


def build_registry(
    settings: GatewaySettings, trino_client: TrinoClient | None = None
) -> ToolRegistry:
    registry = ToolRegistry()
    client = trino_client or TrinoClient(settings)
    trino_tools.register(registry, client, settings)
    return registry
