#!/usr/bin/env python3
#
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
#
"""Redact common credentials from evidence before the skill quotes it.

This utility deliberately treats its input as opaque text. It does not parse or
execute DDL, shell commands, URLs, or configuration supplied by the user.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

REDACTED = "<redacted>"

SENSITIVE_KEY = re.compile(
    r"(?ix)(?:"
    r"authorization|password|passwd|pwd|secret|token|credential|"
    r"api[._-]?key|access[._-]?key|secret[._-]?key|client[._-]?secret|"
    r"private[._-]?key"
    r")"
)

ASSIGNMENT = re.compile(
    r"^(?P<prefix>\s*[\"']?(?P<key>[A-Za-z0-9_.-]+)[\"']?\s*(?:=|:)\s*)"
    r"(?P<value>.*)$"
)

JSON_ASSIGNMENT = re.compile(
    r"(?P<prefix>[\"'](?P<key>[^\"']+)[\"']\s*:\s*)"
    r"(?P<quote>[\"'])(?P<value>.*?)(?P=quote)"
)

URI_USERINFO = re.compile(
    r"(?P<scheme>\b[a-zA-Z][a-zA-Z0-9+.-]*://)(?P<userinfo>[^/@\s]+@)"
)

SENSITIVE_QUERY_PARAMETER = re.compile(
    r"(?i)(?P<prefix>[?&](?:access_token|api_key|apikey|client_secret|password|"
    r"secret|secret_key|token)=)(?P<value>[^&#\s\"']+)"
)

SENSITIVE_CLI_FLAG = re.compile(
    r"(?i)(?P<prefix>--(?:access[-_]?key|api[-_]?key|client[-_]?secret|password|"
    r"secret|secret[-_]?key|token)(?:=|\s+))(?P<value>[^\s]+)"
)

PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN(?: [A-Z0-9]+)* PRIVATE KEY-----.*?"
    r"-----END(?: [A-Z0-9]+)* PRIVATE KEY-----",
    re.DOTALL,
)

AWS_ACCESS_KEY_ID = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def _redact_assignment(line: str) -> str:
    match = ASSIGNMENT.match(line)
    if match is None or SENSITIVE_KEY.search(match.group("key")) is None:
        return line

    value = match.group("value")
    stripped = value.rstrip()
    suffix = "," if stripped.endswith(",") else ""
    return f"{match.group('prefix')}{REDACTED}{suffix}"


def _redact_json_assignment(match: re.Match[str]) -> str:
    if SENSITIVE_KEY.search(match.group("key")) is None:
        return match.group(0)
    quote = match.group("quote")
    return f"{match.group('prefix')}{quote}{REDACTED}{quote}"


def redact_sensitive_text(text: str) -> str:
    """Return text with common secret-bearing forms replaced."""
    redacted = PRIVATE_KEY_BLOCK.sub("<redacted-private-key>", text)
    redacted = JSON_ASSIGNMENT.sub(_redact_json_assignment, redacted)
    redacted = URI_USERINFO.sub(lambda match: f"{match.group('scheme')}{REDACTED}@", redacted)
    redacted = SENSITIVE_QUERY_PARAMETER.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", redacted
    )
    redacted = SENSITIVE_CLI_FLAG.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", redacted
    )
    redacted = AWS_ACCESS_KEY_ID.sub(REDACTED, redacted)

    lines = redacted.splitlines(keepends=True)
    output: list[str] = []
    for line in lines:
        ending = ""
        content = line
        if line.endswith("\r\n"):
            content, ending = line[:-2], "\r\n"
        elif line.endswith("\n"):
            content, ending = line[:-1], "\n"
        output.append(_redact_assignment(content) + ending)
    return "".join(output)


def _read_input(path: str | None) -> str:
    if path is None or path == "-":
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8", errors="replace")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Redact credentials from untrusted Hudi Architect evidence."
    )
    parser.add_argument(
        "path",
        nargs="?",
        help="Input file. Omit or use '-' to read standard input.",
    )
    args = parser.parse_args(argv)
    sys.stdout.write(redact_sensitive_text(_read_input(args.path)))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
