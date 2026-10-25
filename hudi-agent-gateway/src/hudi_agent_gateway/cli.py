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

"""Command-line entrypoint: ``hudi-agent-gateway serve``."""

from __future__ import annotations

import argparse

import uvicorn

from hudi_agent_gateway import __version__


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="hudi-agent-gateway",
        description="The Apache Hudi agent gateway server.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="Run the gateway server.")
    serve.add_argument("--host", default=None, help="Bind address (overrides GATEWAY_HOST).")
    serve.add_argument("--port", type=int, default=None, help="Bind port (overrides GATEWAY_PORT).")

    args = parser.parse_args(argv)
    if args.command == "serve":
        # Import lazily so `--version`/`--help` never require full config.
        from hudi_agent_gateway.config import GatewaySettings

        settings = GatewaySettings()
        uvicorn.run(
            "hudi_agent_gateway.app:create_app",
            factory=True,
            host=args.host or settings.host,
            port=args.port or settings.port,
            log_config=None,  # the gateway configures logging itself (JSON)
        )
