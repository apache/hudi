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
        }


# Pattern to strip CLI prompt prefixes like "hudi->" or "hudi:tablename->"
PROMPT_PREFIX = re.compile(r"^hudi[:\w]*->\s*")


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
    """Check if a line is an inner FlipTable border (╠═══╪═══╣) — between rows."""
    stripped = line.strip()
    if not stripped:
        return False
    return stripped.startswith("╠") and all(c in set("╠╣═╪ ") for c in stripped)


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
    # Strip prompt prefixes (e.g., "hudi->" or "hudi:trips->")
    lines = [_strip_prompt_prefix(line) for line in raw_lines]
    result = ParsedOutput(raw=raw_output)

    # Separate table rows from other content
    message_lines: list[str] = []

    for line in lines:
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
