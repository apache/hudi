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

"""Check the running gateway's real MCP -> Connect -> HMS -> MinIO path, without an LLM."""
import asyncio
import json
import signal
import urllib.request

from fastmcp import Client


async def main():
    with urllib.request.urlopen("http://127.0.0.1:8000/ui/", timeout=10) as response:
        assert response.status == 200
    async with Client("http://127.0.0.1:8000/mcp/") as client:
        tools = {tool.name for tool in await client.list_tools()}
        assert {"list_tables", "describe_table", "query_lakehouse"} <= tools, tools

        async def call(name, arguments):
            result = await client.call_tool(name, arguments)
            payload = json.loads(result.content[0].text)
            assert "error" not in payload, payload
            return payload

        tables = await call("list_tables", {})
        assert {"trips", "drivers"} <= {row[1] for row in tables["rows"]}, tables
        schema = await call("describe_table", {"table": "trips"})
        assert "trip_id" in {row[0] for row in schema["rows"]}, schema
        result = await call("query_lakehouse", {"sql":
            "SELECT d.home_city, count(*) AS trips FROM trips t "
            "JOIN drivers d ON t.driver_id = d.driver_id "
            "GROUP BY d.home_city ORDER BY d.home_city"})
        assert result["rows"] == [["chennai", 3], ["san_francisco", 3], ["sao_paulo", 3]], result
        print("PASS: chat UI, MCP discovery, schema and Hudi join through Spark Connect", flush=True)


if __name__ == '__main__':
    signal.alarm(180)
    asyncio.run(main())
