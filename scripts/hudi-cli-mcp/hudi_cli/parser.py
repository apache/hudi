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

"""Parse Hudi CLI FlipTable ASCII output into structured data."""

from __future__ import annotations

import re
from dataclasses import dataclass, field


# Patterns to filter out CLI noise
NOISE_PATTERNS = [
    re.compile(r"^SLF4J:", re.IGNORECASE),
    re.compile(r"^log4j:", re.IGNORECASE),
    re.compile(r"^WARNING:", re.IGNORECASE),
    re.compile(r"^\d{2}/\d{2}/\d{2}\s"),  # Date-prefixed log lines
    re.compile(r"^WARN\s"),
    re.compile(r"^INFO\s"),
    re.compile(r"^DEBUG\s"),
    re.compile(r"^ERROR\s"),
    re.compile(r"Spring Shell"),
    re.compile(r"spring-shell\.log"),
    re.compile(r"^hudi[:\w]*->\s*$", re.IGNORECASE),  # CLI prompt (empty prompt only)
    re.compile(r"^\s*$"),  # Empty lines
    re.compile(r"^Main called"),
    re.compile(r"^Spark context"),
    re.compile(r"^Using Spark"),
    re.compile(r"^Setting default"),
    re.compile(r"^Spark Web UI"),
    re.compile(r"^Added JAR"),
]

# Border-only lines (no data)
BORDER_CHARS = set("╔╗╚╝═╤╧╠╣╪─┌┐└┘┬┴├┤┼+|-")

# Lines that indicate a real command failure. These are surfaced in a dedicated
# ``errors`` field rather than dropped as noise, so callers can tell "the command
# failed" apart from "the command returned an empty result" -- the CLI's script
# mode does not reliably propagate inner-command failures to the process exit code.
ERROR_MARKERS = [
    re.compile(r"^ERROR\s"),
    re.compile(r"Exception(:|\b)"),  # e.g. org.apache.hudi.exception.HoodieException
    re.compile(r"\bcommand (failed|error)\b", re.IGNORECASE),
    re.compile(r"\bfailed to\b", re.IGNORECASE),
    re.compile(r"can only be run", re.IGNORECASE),  # e.g. compaction on a COW table
]


def _is_error_line(line: str) -> bool:
    """Check if a line signals a real command failure (not routine log noise)."""
    return any(p.search(line) for p in ERROR_MARKERS)


@dataclass
class ParsedTable:
    """A single parsed table from CLI output."""

    headers: list[str]
    rows: list[dict[str, str]]
    row_count: int = 0

    def __post_init__(self):
        self.row_count = len(self.rows)


@dataclass
class ParsedOutput:
    """Complete parsed output from a CLI invocation."""

    tables: list[ParsedTable] = field(default_factory=list)
    messages: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    raw: str = ""

    def to_dict(self) -> dict:
        return {
            "tables": [
                {
                    "headers": t.headers,
                    "rows": t.rows,
                    "row_count": t.row_count,
                }
                for t in self.tables
            ],
            "messages": self.messages,
            "errors": self.errors,
        }


# Pattern to strip CLI prompt prefixes like "hudi->" or "hudi:tablename->"
PROMPT_PREFIX = re.compile(r"^hudi[:\w]*->\s*")

# ANSI SGR color codes (the CLI prints errors in red); strip so captured error and
# message text is clean rather than wrapped in escape sequences.
ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(line: str) -> str:
    return ANSI_ESCAPE.sub("", line)


def _strip_prompt_prefix(line: str) -> str:
    """Strip CLI prompt prefix from a line (e.g., 'hudi->Metadata...' -> 'Metadata...')."""
    return PROMPT_PREFIX.sub("", line)


def _is_noise(line: str) -> bool:
    """Check if a line is CLI noise (logs, banners, prompts)."""
    return any(p.search(line) for p in NOISE_PATTERNS)


def _is_border_only(line: str) -> bool:
    """Check if a line contains only border characters."""
    stripped = line.strip()
    return len(stripped) > 0 and all(c in BORDER_CHARS for c in stripped)


def _is_table_row(line: str) -> bool:
    """Check if a line is a FlipTable data row (contains ║)."""
    return "║" in line


def _parse_table_row(line: str) -> list[str]:
    """Extract cell values from a FlipTable row.

    Handles format: ║ val1 │ val2 │ val3 ║
    """
    # Strip leading/trailing ║
    stripped = line.strip()
    if stripped.startswith("║"):
        stripped = stripped[1:]
    if stripped.endswith("║"):
        stripped = stripped[:-1]

    # Split on │ and strip each cell
    cells = [cell.strip() for cell in stripped.split("│")]
    return cells


def _is_table_inner_border(line: str) -> bool:
    """Check if a line is an inner FlipTable border between rows.

    Hudi's CLI (FlipTable) uses two distinct inter-row separators: a heavy
    ``╠═══╪═══╣`` after the header, and a light ``╟───┼───╢`` between data rows
    (some tables use ``├───┼───┤``). Both must be recognized as inner borders --
    otherwise every data row after the first is treated as the start of a new
    single-row table and its values are misread as headers.
    """
    stripped = line.strip()
    if not stripped:
        return False
    return stripped[0] in "╠╟├" and all(c in set("╠╣═╪╟╢─┼├┤ ") for c in stripped)


def _is_table_end_border(line: str) -> bool:
    """Check if a line is the bottom border of a FlipTable (╚═══╧═══╝)."""
    stripped = line.strip()
    if not stripped:
        return False
    return stripped.startswith("╚") and all(c in set("╚╝═╧ ") for c in stripped)


def _is_table_start_border(line: str) -> bool:
    """Check if a line is the top border of a FlipTable (╔═══╤═══╗)."""
    stripped = line.strip()
    if not stripped:
        return False
    return stripped.startswith("╔") and all(c in set("╔╗═╤ ") for c in stripped)


def _group_table_blocks(lines: list[str]) -> list[list[str]]:
    """Group consecutive table rows into blocks.

    Uses FlipTable borders to determine table boundaries:
    - ╔ starts a new table
    - ╠ is an inner border (between rows)
    - ╚ ends a table
    """
    blocks: list[list[str]] = []
    current_block: list[str] = []

    for line in lines:
        if _is_table_row(line):
            current_block.append(line)
        elif _is_table_start_border(line):
            # Start of a new table — close any existing block first
            if current_block:
                blocks.append(current_block)
                current_block = []
        elif _is_table_end_border(line):
            # End of current table — close the block
            if current_block:
                blocks.append(current_block)
                current_block = []
        elif _is_table_inner_border(line):
            # Inner border — keep block alive
            continue
        else:
            if current_block:
                blocks.append(current_block)
                current_block = []

    if current_block:
        blocks.append(current_block)

    return blocks


def parse_cli_output(raw_output: str) -> ParsedOutput:
    """Parse raw Hudi CLI output into structured data.

    Handles:
    - FlipTable ASCII tables (║ and │ delimiters)
    - Plain text messages (connect confirmation, etc.)
    - Filters out log noise, banners, prompts
    """
    raw_lines = raw_output.splitlines()
    # Strip ANSI color codes and prompt prefixes (e.g., "hudi->" or "hudi:trips->")
    lines = [_strip_prompt_prefix(_strip_ansi(line)) for line in raw_lines]
    result = ParsedOutput(raw=raw_output)

    # Separate table rows from other content
    message_lines: list[str] = []
    error_lines: list[str] = []

    for line in lines:
        # Capture real failures first, before they are dropped as noise -- the
        # error markers overlap the noise patterns (e.g. lines starting "ERROR ").
        if _is_error_line(line):
            cleaned = line.strip()
            if cleaned:
                error_lines.append(cleaned)
            continue
        if _is_noise(line):
            continue
        if _is_border_only(line):
            continue
        if _is_table_row(line):
            continue  # Handled in block grouping below
        else:
            cleaned = line.strip()
            if cleaned:
                message_lines.append(cleaned)

    result.messages = message_lines
    result.errors = error_lines

    # Group table rows into blocks and parse each
    # Re-scan all cleaned lines to detect block boundaries
    non_noise_lines = [line for line in lines if not _is_noise(line)]
    blocks = _group_table_blocks(non_noise_lines)

    for block in blocks:
        if len(block) < 1:
            continue

        # First row is headers
        headers = _parse_table_row(block[0])

        # Remaining rows are data
        rows: list[dict[str, str]] = []
        for row_line in block[1:]:
            cells = _parse_table_row(row_line)
            # Pad or truncate to match header count
            row_dict = {}
            for i, header in enumerate(headers):
                if i < len(cells):
                    row_dict[header] = cells[i]
                else:
                    row_dict[header] = ""
            rows.append(row_dict)

        result.tables.append(ParsedTable(headers=headers, rows=rows))

    # Remove table-row content from messages (they were double-counted)
    result.messages = [
        m
        for m in result.messages
        if not _is_table_row(m) and not _is_border_only(m)
    ]

    return result
