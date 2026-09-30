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
    # Try only token starts, not every suffix of long non-secret values.
    r"(?<![A-Za-z0-9_.-])"
    r"(?P<prefix>(?P<key_quote>[\"']?)(?P<key>[A-Za-z0-9_.-]+)"
    r"(?P=key_quote)[ \t]*(?:=|:)[ \t]*)"
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
    r"secret|secret[-_]?key|token)(?:=|\s+))"
)

AUTHORIZATION_HEADER = re.compile(
    r"(?im)(?P<prefix>^[ \t]*authorization[ \t]*:[ \t]*)[^\r\n]*"
)

PRIVATE_KEY_BLOCK = re.compile(
    r"-----BEGIN(?: [A-Z0-9]+)* PRIVATE KEY-----.*?"
    r"-----END(?: [A-Z0-9]+)* PRIVATE KEY-----",
    re.DOTALL,
)

AWS_ACCESS_KEY_ID = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")


def _consume_quoted_value(text: str, start: int) -> tuple[int, str]:
    """Consume one complete quoted value, including escapes and SQL doubled quotes."""
    quote = text[start]
    index = start + 1
    while index < len(text):
        character = text[index]
        if character in "\r\n":
            # Fail closed for malformed input without swallowing unrelated later lines.
            return index, f"{quote}{REDACTED}"
        if character == "\\":
            index = min(index + 2, len(text))
            continue
        if character == quote:
            if index + 1 < len(text) and text[index + 1] == quote:
                index += 2
                continue
            return index + 1, f"{quote}{REDACTED}{quote}"
        index += 1
    return index, f"{quote}{REDACTED}"


def _consume_assignment_value(text: str, start: int) -> tuple[int, str]:
    if start < len(text) and text[start] in "\"'":
        return _consume_quoted_value(text, start)

    # Punctuation can be part of an unquoted property credential. Without a quoted
    # boundary, conservatively omit the rest of the line, even in inline evidence.
    index = start
    while index < len(text) and text[index] not in "\r\n":
        index += 1
    return index, REDACTED


def _consume_cli_value(text: str, start: int) -> tuple[int, str]:
    if start < len(text) and text[start] in "\"'":
        return _consume_quoted_value(text, start)

    index = start
    while index < len(text) and not text[index].isspace():
        index += 1
    return index, REDACTED


def _redact_cli_values(text: str) -> str:
    output: list[str] = []
    position = 0
    while match := SENSITIVE_CLI_FLAG.search(text, position):
        output.append(text[position : match.end("prefix")])
        value_end, replacement = _consume_cli_value(text, match.end("prefix"))
        output.append(replacement)
        position = value_end
    output.append(text[position:])
    return "".join(output)


def _redact_assignments(text: str) -> str:
    output: list[str] = []
    position = 0
    while match := ASSIGNMENT.search(text, position):
        # Keep the boundaries of values already handled by the query/CLI consumers.
        # Other sensitive keys still need conservative assignment redaction.
        if (
            SENSITIVE_KEY.search(match.group("key")) is None
            or SENSITIVE_CLI_FLAG.match(text, match.start()) is not None
            or (
                match.start() > 0
                and SENSITIVE_QUERY_PARAMETER.match(text, match.start() - 1) is not None
            )
        ):
            output.append(text[position : match.end("prefix")])
            position = match.end("prefix")
            continue

        output.append(text[position : match.end("prefix")])
        value_end, replacement = _consume_assignment_value(text, match.end("prefix"))
        output.append(replacement)
        position = value_end
    output.append(text[position:])
    return "".join(output)


def redact_sensitive_text(text: str) -> str:
    """Return text with common secret-bearing forms replaced."""
    redacted = PRIVATE_KEY_BLOCK.sub("<redacted-private-key>", text)
    redacted = URI_USERINFO.sub(lambda match: f"{match.group('scheme')}{REDACTED}@", redacted)
    redacted = SENSITIVE_QUERY_PARAMETER.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", redacted
    )
    redacted = AUTHORIZATION_HEADER.sub(
        lambda match: f"{match.group('prefix')}{REDACTED}", redacted
    )
    redacted = _redact_cli_values(redacted)
    redacted = _redact_assignments(redacted)
    redacted = AWS_ACCESS_KEY_ID.sub(REDACTED, redacted)
    return redacted


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
