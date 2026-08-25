#!/usr/bin/env python3
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
"""Validate hoodie.* config keys referenced by the hudi-architect skill.

Extracts every `hoodie.*` key mentioned in the skill's markdown (SKILL.md +
references/*.md) and checks that each one is a real config key defined in the
Hudi source tree — either a primary key (`.key("...")`) or a declared
alternative (`withAlternatives("...")`). Keys built from prefix constants
(e.g. METADATA_PREFIX + ".record.index.growth.factor") are resolved by
scanning String constants and folding one level of `CONST + "literal"`
concatenation to a fixpoint.

Exit codes: 0 = all keys valid (or allowlisted), 1 = unknown keys found.

Intentional exceptions (e.g. keys targeting a future Hudi version that the
skill discusses but does not emit) live in validate_config_keys_allowlist.txt,
one key per line, '#' comments allowed.

Run from anywhere:  python3 hudi-agent-gateway/skills/hudi-architect/validate_config_keys.py
"""

import re
import sys
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent


def _find_repo_root(start: Path) -> Path:
    """Walk up from the skill directory to the enclosing Hudi checkout.

    Located by marker rather than by counting parents, so relocating the skill
    within the tree cannot silently break source-tree resolution.
    """
    for candidate in (start, *start.parents):
        if (candidate / "hudi-common").is_dir() and (candidate / "pom.xml").is_file():
            return candidate
    return start.parents[-1]


REPO_ROOT = _find_repo_root(SKILL_DIR)
ALLOWLIST_FILE = SKILL_DIR / "validate_config_keys_allowlist.txt"

# Source modules that define ConfigProperty keys.
SOURCE_GLOBS = [
    "hudi-common/src/main/**/*.java",
    "hudi-client/**/src/main/**/*.java",
    "hudi-utilities/src/main/**/*.java",
    "hudi-spark-datasource/**/src/main/**/*.scala",
    "hudi-spark-datasource/**/src/main/**/*.java",
    "hudi-flink-datasource/**/src/main/**/*.java",
    "hudi-sync/**/src/main/**/*.java",
    # Cloud lock providers live in their own modules; the multi-writer references
    # emit hoodie.write.lock.dynamodb.* keys defined only here.
    "hudi-aws/src/main/**/*.java",
    "hudi-gcp/src/main/**/*.java",
]

MARKDOWN_FILES = [SKILL_DIR / "SKILL.md"] + sorted((SKILL_DIR / "references").glob("*.md"))

# --- extraction from markdown -------------------------------------------------

KEY_TOKEN = re.compile(r"\bhoodie\.[a-z0-9_]+(?:\.[a-z0-9_]+|\.\{[a-z0-9_,]+\})+")


def expand_braces(token: str):
    """hoodie.a.{min,max}.b -> [hoodie.a.min.b, hoodie.a.max.b]"""
    m = re.search(r"\{([a-z0-9_,]+)\}", token)
    if not m:
        return [token]
    out = []
    for alt in m.group(1).split(","):
        out.extend(expand_braces(token[: m.start()] + alt + token[m.end():]))
    return out


def extract_markdown_keys():
    found = {}  # key -> list of "file:line"
    for md in MARKDOWN_FILES:
        for lineno, line in enumerate(md.read_text().splitlines(), 1):
            for tok in KEY_TOKEN.findall(line):
                for key in expand_braces(tok):
                    found.setdefault(key, []).append(f"{md.name}:{lineno}")
    return found


# --- valid-key set from the source tree ----------------------------------------

CONST_DEF = re.compile(r'String\s+(\w+)\s*=\s*(.+?);')
STRING_LIT = re.compile(r'"((?:hoodie|_hoodie)[^"]*)"')
KEY_CALL = re.compile(r'(?:\.key|withAlternatives)\(([^;]*?)\)')


def build_valid_keys():
    constants = {}   # NAME -> string value (only fully-resolved ones kept)
    key_exprs = []   # raw argument expressions of .key(...) / withAlternatives(...)
    literals = set()

    files = []
    for pattern in SOURCE_GLOBS:
        files.extend(REPO_ROOT.glob(pattern))

    pending_defs = []
    for f in files:
        try:
            text = f.read_text(errors="ignore")
        except OSError:
            continue
        # No content gate: files like KafkaSourceConfig build every key from
        # imported prefix constants and may contain no literal "hoodie." at all.
        for m in CONST_DEF.finditer(text):
            pending_defs.append((m.group(1), m.group(2)))
        for m in KEY_CALL.finditer(text):
            key_exprs.append(m.group(1))
        # any quoted hoodie.* literal counts as evidence the key exists somewhere
        # in config-definition context; keep them for prefix resolution only.

    def eval_expr(expr):
        """Resolve '"lit"', 'CONST + "lit"', etc. to the SET of possible strings.

        Constant names like PREFIX repeat across files with different values, so
        constants map to value-sets and concatenation is cartesian. A key is
        considered valid if any combination resolves to it — still strong
        evidence, since every combination pairs a real prefix with a real
        .key()/.withAlternatives() suffix.
        """
        parts = [p.strip() for p in expr.split("+")]
        out = {""}
        for p in parts:
            if len(p) >= 2 and p.startswith('"') and p.endswith('"'):
                out = {o + p[1:-1] for o in out}
            elif p in constants:
                out = {o + v for o in out for v in constants[p]}
                if len(out) > 512:  # runaway cartesian guard
                    return set()
            else:
                return set()
        return out

    # resolve constants to a fixpoint (handles PREFIX = OTHER_PREFIX + "x.")
    changed = True
    while changed:
        changed = False
        for name, expr in pending_defs:
            for val in eval_expr(expr):
                if "hoodie." in val and val not in constants.setdefault(name, set()):
                    constants[name].add(val)
                    changed = True

    valid = set()
    for expr in key_exprs:
        for val in eval_expr(expr):
            if val.startswith("hoodie."):
                valid.add(val)
    # plain constants that are themselves complete keys
    for vals in constants.values():
        for val in vals:
            if val.startswith("hoodie.") and not val.endswith("."):
                valid.add(val)
    return valid


def load_allowlist():
    if not ALLOWLIST_FILE.exists():
        return set()
    out = set()
    for line in ALLOWLIST_FILE.read_text().splitlines():
        line = line.split("#", 1)[0].strip()
        if line:
            out.add(line)
    return out


def main():
    md_keys = extract_markdown_keys()
    valid = build_valid_keys()
    allow = load_allowlist()

    if not valid:
        print("ERROR: no config keys resolved from the source tree.")
        print(f"       Looked under: {REPO_ROOT}")
        print("       This script must run from a copy of the skill INSIDE a Hudi checkout")
        print("       (hudi-agent-gateway/skills/hudi-architect/), not from an installed copy under")
        print("       ~/.claude/skills/. Run the repo copy instead.")
        return 2

    unknown = {}
    for key, locations in sorted(md_keys.items()):
        if key in valid or key in allow:
            continue
        unknown[key] = locations

    print(f"Checked {len(md_keys)} distinct hoodie.* keys from {len(MARKDOWN_FILES)} markdown files")
    print(f"against {len(valid)} keys resolved from the Hudi source tree "
          f"({len(allow)} allowlisted).")
    if not unknown:
        print("OK — every referenced key exists in source (or is allowlisted).")
        return 0

    print(f"\nUNKNOWN KEYS ({len(unknown)}) — not defined in source, not allowlisted:")
    for key, locations in unknown.items():
        locs = ", ".join(locations[:3]) + (" …" if len(locations) > 3 else "")
        print(f"  {key}\n      at {locs}")
    print("\nFix the reference file, or add to validate_config_keys_allowlist.txt "
          "with a comment explaining why.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
