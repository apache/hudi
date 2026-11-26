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

"""Track tool tasks belonging to one streaming chat turn."""

import asyncio
from contextvars import ContextVar


class ToolCancellationScope:
    def __init__(self) -> None:
        self.tasks: set[asyncio.Task] = set()
        self.cancelled = False

    def add(self, task: asyncio.Task) -> None:
        if self.cancelled:
            raise asyncio.CancelledError
        self.tasks.add(task)

    def cancel(self) -> None:
        self.cancelled = True
        for task in tuple(self.tasks):
            task.cancel()


tool_cancellation_scope: ContextVar[ToolCancellationScope | None] = ContextVar(
    "tool_cancellation_scope", default=None
)
