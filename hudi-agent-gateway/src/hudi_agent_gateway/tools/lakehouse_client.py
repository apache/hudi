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

"""Result and error contract shared by the lakehouse backends."""

from dataclasses import dataclass, field
from typing import Any, Protocol


class QueryError(Exception):
    """A lakehouse query failed."""


class QueryTimeoutError(QueryError):
    pass


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list[Any]]
    row_count: int = field(init=False)

    def __post_init__(self) -> None:
        self.row_count = len(self.rows)


class LakehouseClient(Protocol):
    async def execute(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult: ...
    async def describe(self, sql: str, *, timeout: float, max_rows: int) -> QueryResult: ...
    async def ping(self, timeout: float = 5.0, cache_seconds: float = 10.0) -> bool: ...
    def close(self) -> None: ...
