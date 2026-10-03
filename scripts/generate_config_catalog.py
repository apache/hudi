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

"""Extract every Hudi configuration property from the source tree, with code context.

The published configuration reference tells a user a config's key, default and
description. It cannot tell them where the value is actually read, what else is
read beside it, or under what condition the read happens at all. That last part
is what makes a config silently do nothing. This script recovers it from source.

Three passes:

  1. Declarations. Parse ``ConfigProperty`` builder chains (and Flink's
     ``ConfigOptions`` chains whose key is a literal ``hoodie.*``) out of the
     main source trees. Chains span many lines and end at the statement
     semicolon, so the parser works on brace/paren-balanced statement text
     rather than on single lines.

  2. Accessors. Find config-class methods whose body is a single
     ``getInt(SOME_CONFIG)`` style read, so that call sites of
     ``getInlineCompactDeltaCommitMax()`` can be attributed back to
     ``hoodie.compact.inline.max.delta.commits``.

  3. Read sites. Walk every source file again, attributing each mention of a
     config constant or of a resolved accessor to the enclosing method, and
     recording the other configs mentioned in that same method (co-configs) and
     the enclosing ``if``/``switch``/``case`` conditions (gating). The gating
     pass is a textual heuristic; every condition it emits is marked as such.

Standard library only. Written to degrade: anything it cannot fully parse still
appears in the catalog, carrying a ``parseWarnings`` list.

Usage:
    python3 scripts/generate_config_catalog.py [--repo-root DIR] [--out-dir DIR]
"""

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict, OrderedDict

# --------------------------------------------------------------------------
# Tree walking
# --------------------------------------------------------------------------

# Modules that declare configs. Anything under the repo root that is not one of
# these is still scanned for *read sites*; declarations are only looked for in
# main source trees, which SOURCE_DIR_MARKER enforces.
SOURCE_DIR_MARKER = os.path.join("src", "main")

EXCLUDED_PATH_PARTS = (
    os.sep + "target" + os.sep,
    os.sep + "test" + os.sep,
    os.sep + "src" + os.sep + "test" + os.sep,
    os.sep + "node_modules" + os.sep,
    os.sep + ".git" + os.sep,
)

# Directories we never descend into at all.
PRUNED_DIR_NAMES = {"target", "node_modules", ".git", ".idea", "docker", "rfc"}

READ_SITE_EXTENSIONS = (".java", ".scala")

MAX_READ_SITES = 20
MAX_CO_CONFIGS = 25
MAX_GATES_PER_SITE = 3


def is_excluded(path):
    normalized = os.sep + path.strip(os.sep) + os.sep
    return any(part in normalized for part in EXCLUDED_PATH_PARTS)


def walk_source_files(repo_root, extensions):
    """Yield absolute paths of non-test, non-generated source files."""
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [d for d in dirnames if d not in PRUNED_DIR_NAMES]
        if is_excluded(dirpath):
            continue
        for filename in filenames:
            if filename.endswith(extensions):
                full = os.path.join(dirpath, filename)
                if not is_excluded(full):
                    yield full


# --------------------------------------------------------------------------
# Lightweight Java/Scala lexing helpers
# --------------------------------------------------------------------------

def strip_comments_and_strings(text, keep_string_bodies=False, scala=False):
    """Blank out comments and (optionally) string bodies, preserving offsets.

    Offsets are preserved so that any index computed on the stripped text maps
    straight back onto the original. Newlines survive so line numbers hold.

    ``scala`` switches on the two lexical rules that differ enough to corrupt
    offsets if Java's are applied: triple-quoted strings, and the fact that a
    single quote opens a character literal in Java but far more often opens a
    symbol or a type parameter in Scala (``Option['a]``, ``case 'x'``). Treating
    a lone Scala quote as a string opener would blank out the rest of the file.
    """
    out = list(text)
    i = 0
    n = len(text)
    while i < n:
        ch = text[i]
        if scala and ch == '"' and text.startswith('"""', i):
            # Triple-quoted: ends at the next `"""`, and may span lines.
            out[i] = out[i + 1] = out[i + 2] = " "
            i += 3
            while i < n and not text.startswith('"""', i):
                if not keep_string_bodies and text[i] != "\n":
                    out[i] = " "
                i += 1
            for offset in range(3):
                if i + offset < n:
                    out[i + offset] = " "
            i += 3
            continue
        if scala and ch == "'":
            # Only a genuine `'c'` char literal; anything else is a symbol or a
            # type parameter and must be left alone.
            if i + 2 < n and text[i + 2] == "'" and text[i + 1] != "\\":
                if not keep_string_bodies:
                    out[i + 1] = " "
                i += 3
            elif i + 3 < n and text[i + 1] == "\\" and text[i + 3] == "'":
                if not keep_string_bodies:
                    out[i + 1] = out[i + 2] = " "
                i += 4
            else:
                i += 1
            continue
        if ch == "/" and i + 1 < n and text[i + 1] == "/":
            while i < n and text[i] != "\n":
                out[i] = " "
                i += 1
        elif ch == "/" and i + 1 < n and text[i + 1] == "*":
            out[i] = out[i + 1] = " "
            i += 2
            while i < n and not (text[i] == "*" and i + 1 < n and text[i + 1] == "/"):
                if text[i] != "\n":
                    out[i] = " "
                i += 1
            if i < n:
                out[i] = " "
                if i + 1 < n:
                    out[i + 1] = " "
                i += 2
        elif ch in ('"', "'"):
            quote = ch
            i += 1
            while i < n:
                if text[i] == "\\":
                    if not keep_string_bodies:
                        out[i] = " "
                        if i + 1 < n:
                            out[i + 1] = " "
                    i += 2
                    continue
                if text[i] == quote:
                    break
                # A single-quoted string never spans a line. Stopping here keeps
                # one unterminated quote from blanking the rest of the file.
                if text[i] == "\n":
                    break
                if not keep_string_bodies:
                    out[i] = " "
                i += 1
            i += 1
        else:
            i += 1
    return "".join(out)


def line_of(text, index):
    return text.count("\n", 0, index) + 1


def find_statement_end(text, start):
    """Index just past the ``;`` ending the statement that starts at ``start``.

    Respects nesting and ignores semicolons inside strings, chars and comments.
    """
    masked = strip_comments_and_strings(text[start:])
    depth_paren = depth_brace = depth_bracket = 0
    for offset, ch in enumerate(masked):
        if ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1
        elif ch == ";" and depth_paren <= 0 and depth_brace <= 0 and depth_bracket <= 0:
            return start + offset + 1
    return -1


def find_scala_statement_end(text, start):
    """Index just past the end of a Scala builder chain starting at ``start``.

    Scala has no statement terminator, so the chain ends at the first newline
    that leaves every bracket balanced and is not followed by a continuation --
    another ``.step(...)``, or a binary operator left dangling from the previous
    line. Builder chains here do carry multi-line lambdas (``withInferFunction``
    takes one), which is why balance has to be tracked rather than stopping at
    the first line break.
    """
    masked = strip_comments_and_strings(text[start:], scala=True)
    depth_paren = depth_brace = depth_bracket = 0
    limit = min(len(masked), 20000)
    # A match may begin on the newline that ends the previous statement; that
    # newline must not be read as this chain's terminator.
    i = 0
    while i < limit and masked[i].isspace():
        i += 1
    while i < limit:
        ch = masked[i]
        if ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "{":
            depth_brace += 1
        elif ch == "}":
            depth_brace -= 1
        elif ch == "[":
            depth_bracket += 1
        elif ch == "]":
            depth_bracket -= 1
        elif ch == "\n" and depth_paren <= 0 and depth_brace <= 0 and depth_bracket <= 0:
            before = masked[:i].rstrip()
            # A line ending in `+`, `=` or `.` is continued on the next one.
            if before.endswith(("+", "=", ".", ",", "(", "{")):
                i += 1
                continue
            rest = masked[i + 1:]
            stripped = rest.lstrip()
            if stripped.startswith(".") and not stripped.startswith(".."):
                i += 1
                continue
            return start + i
        i += 1
    return start + limit


def split_top_level_args(arg_text):
    """Split a call's argument text on top-level commas."""
    masked = strip_comments_and_strings(arg_text)
    args = []
    depth = 0
    current_start = 0
    for i, ch in enumerate(masked):
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        elif ch == "," and depth == 0:
            args.append(arg_text[current_start:i].strip())
            current_start = i + 1
    tail = arg_text[current_start:].strip()
    if tail:
        args.append(tail)
    return args


def extract_call_args(text, call_start):
    """Given an index at the ``(`` of a call, return (args_text, index_past_close)."""
    masked = strip_comments_and_strings(text)
    if call_start >= len(text) or text[call_start] != "(":
        return None, call_start
    depth = 0
    for i in range(call_start, len(masked)):
        if masked[i] == "(":
            depth += 1
        elif masked[i] == ")":
            depth -= 1
            if depth == 0:
                return text[call_start + 1:i], i + 1
    return None, call_start


# --------------------------------------------------------------------------
# Java string-literal evaluation
# --------------------------------------------------------------------------

_JAVA_ESCAPES = {
    "n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f",
    '"': '"', "'": "'", "\\": "\\", "0": "\0",
}

STRING_LITERAL_RE = re.compile(r'"((?:[^"\\]|\\.)*)"', re.DOTALL)


def unescape_java(raw):
    out = []
    i = 0
    while i < len(raw):
        if raw[i] == "\\" and i + 1 < len(raw):
            nxt = raw[i + 1]
            if nxt == "u":
                try:
                    out.append(chr(int(raw[i + 2:i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            out.append(_JAVA_ESCAPES.get(nxt, nxt))
            i += 2
        else:
            out.append(raw[i])
            i += 1
    return "".join(out)


IDENT_PATH_RE = re.compile(r"[A-Za-z_$][A-Za-z0-9_$]*(?:\s*\.\s*[A-Za-z_$][A-Za-z0-9_$]*)*")


CONFIG_KEY_CALL_RE = re.compile(
    r"([A-Za-z_$][A-Za-z0-9_$]*(?:\s*\.\s*[A-Za-z_$][A-Za-z0-9_$]*)*)\s*\.\s*key\s*\(\s*\)")


def concatenated_string_literal(expr, string_constants=None, owner_class=None, _depth=0,
                                key_resolver=None):
    """Evaluate a Java expression that is a concatenation of string literals.

    With ``string_constants`` supplied, identifiers that name a ``static final
    String`` constant are substituted too, which is how the many
    ``SOME_PREFIX + "suffix"`` config keys get resolved. Returns None when the
    expression contains anything else, so that callers can distinguish "a
    literal we read exactly" from "something computed".

    ``key_resolver`` additionally substitutes ``OTHER_CONFIG.key()``. Several
    configs name another config inside their own documentation ("Required when
    `" + QUERY_TYPE.key() + "` is set to ..."), and without this the whole
    documentation string is discarded as computed.
    """
    expr = expr.strip()
    if not expr or _depth > 8:
        return None
    pieces = []
    pos = 0
    saw_literal = False
    while pos < len(expr):
        if key_resolver is not None:
            key_call = CONFIG_KEY_CALL_RE.match(expr, pos)
            if key_call:
                resolved_key = key_resolver(key_call.group(1).replace(" ", ""))
                if resolved_key is not None:
                    pieces.append(resolved_key)
                    saw_literal = True
                    pos = key_call.end()
                    continue
                return None
        match = STRING_LITERAL_RE.match(expr, pos)
        if match:
            pieces.append(unescape_java(match.group(1)))
            saw_literal = True
            pos = match.end()
            continue
        ch = expr[pos]
        if ch.isspace() or ch == "+":
            pos += 1
            continue
        if string_constants is not None:
            ident = IDENT_PATH_RE.match(expr, pos)
            if ident:
                resolved = lookup_string_constant(
                    ident.group(0), string_constants, owner_class, _depth)
                if resolved is not None:
                    pieces.append(resolved)
                    saw_literal = True
                    pos = ident.end()
                    continue
        return None
    return "".join(pieces) if saw_literal else None


def lookup_string_constant(path, string_constants, owner_class, depth):
    """Resolve ``PREFIX`` / ``Holder.PREFIX`` to its literal value, recursively."""
    parts = [p.strip() for p in path.split(".")]
    const = parts[-1]
    qualifier = parts[-2] if len(parts) >= 2 else None
    for candidate in ((qualifier, const), (owner_class, const), (None, const)):
        expression = string_constants.get(candidate)
        if expression is None:
            continue
        resolved = concatenated_string_literal(
            expression, string_constants, candidate[0] or owner_class, depth + 1)
        if resolved is not None:
            return resolved
    return None


ENUM_DESCRIPTION_RE = re.compile(r"@EnumDescription\s*\(")
ENUM_FIELD_DESCRIPTION_RE = re.compile(r"@EnumFieldDescription\s*\(")
ENUM_DECL_RE = re.compile(r"\benum\s+([A-Za-z_$][A-Za-z0-9_$]*)")
# The last constant in an enum body is followed by `}` rather than `,` or `;`.
ENUM_CONSTANT_RE = re.compile(r"\A\s*([A-Z][A-Z0-9_]*)\s*[,;(){}]")


def collect_enum_documentation(repo_root):
    """Pass 0b. Enum name -> its ``@EnumDescription`` and per-constant descriptions.

    ``withDocumentation(SomeEnum.class)`` means the user-facing text for that
    config lives on the enum, not at the declaration. Without this the catalog
    would report "no documentation" for exactly the configs whose valid values
    matter most.
    """
    documented = {}
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "@EnumDescription" not in original:
            continue
        masked = strip_comments_and_strings(original, keep_string_bodies=True)
        for match in ENUM_DESCRIPTION_RE.finditer(masked):
            args, after = extract_call_args(masked, match.end() - 1)
            if args is None:
                continue
            enum_match = ENUM_DECL_RE.search(masked, after)
            if not enum_match:
                continue
            name = enum_match.group(1)
            body_start = masked.find("{", enum_match.end())
            if body_start < 0:
                continue
            depth = 0
            body_end = len(masked)
            for i in range(body_start, len(masked)):
                if masked[i] == "{":
                    depth += 1
                elif masked[i] == "}":
                    depth -= 1
                    if depth == 0:
                        body_end = i
                        break
            values = []
            for field in ENUM_FIELD_DESCRIPTION_RE.finditer(masked, body_start, body_end):
                field_args, field_after = extract_call_args(masked, field.end() - 1)
                if field_args is None:
                    continue
                tail = masked[field_after:field_after + 160]
                constant = ENUM_CONSTANT_RE.match(tail)
                if not constant:
                    continue
                values.append(OrderedDict([
                    ("value", constant.group(1)),
                    ("description", concatenated_string_literal(field_args) or ""),
                ]))
            documented[name] = OrderedDict([
                ("enum", name),
                ("file", os.path.relpath(file_path, repo_root)),
                ("description", concatenated_string_literal(args) or ""),
                ("values", values),
            ])
    return documented


STRING_CONSTANT_RE = re.compile(
    r"\bstatic\s+final\s+String\s+(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*(?P<value>[^;]*);")

# `val QUERY_TYPE_SNAPSHOT_OPT_VAL = "snapshot"`, with the type ascription
# optional. Scala values spelled across several lines are left alone: the value
# stops at the newline, and an unresolved name is reported rather than guessed.
SCALA_STRING_CONSTANT_RE = re.compile(
    r"\b(?:lazy\s+)?val\s+(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*"
    r"(?::\s*String\s*)?=\s*(?P<value>[^\n]*?)\s*(?:\n|$)")


def collect_string_constants(repo_root):
    """Pass 0. (class, constant) -> its initializer expression, for key prefixes.

    Keyed both by owning class and by bare name. A bare name claimed by two
    different expressions is dropped, so an ambiguous prefix never silently
    resolves to the wrong value.
    """
    by_qualified = {}
    bare = {}
    bare_conflicts = set()
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "static final String" not in original:
            continue
        masked = strip_comments_and_strings(original, keep_string_bodies=True)
        for match in STRING_CONSTANT_RE.finditer(masked):
            name = match.group("name")
            value = match.group("value").strip()
            if not value or len(value) > 400:
                continue
            owner = find_enclosing_class(masked, match.start())
            by_qualified[(owner, name)] = value
            if name in bare and bare[name] != value:
                bare_conflicts.add(name)
            bare[name] = value

    # Scala `val` string constants. Spark's datasource configs take their
    # defaults and valid values from these, so without them a Spark user is told
    # the default of hoodie.datasource.query.type is the literal text
    # "QUERY_TYPE_SNAPSHOT_OPT_VAL" rather than "snapshot".
    for file_path in walk_source_files(repo_root, (".scala",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "val " not in original:
            continue
        masked = strip_comments_and_strings(original, keep_string_bodies=True, scala=True)
        for match in SCALA_STRING_CONSTANT_RE.finditer(masked):
            name = match.group("name")
            value = match.group("value").strip()
            # Only plain string expressions; a chain or a call is not a constant.
            if not value or len(value) > 400 or '"' not in value:
                continue
            if value.endswith(("+", "(", "{", ",", "=")):
                continue
            owner = find_enclosing_scala_object(masked, match.start())
            by_qualified[(owner, name)] = value
            if name in bare and bare[name] != value:
                bare_conflicts.add(name)
            bare[name] = value

    for name in bare_conflicts:
        bare.pop(name, None)
    constants = {(None, name): value for name, value in bare.items()}
    constants.update(by_qualified)
    return constants


ENUM_CONSTANT_CALL_RE = re.compile(
    r"\A(?:[A-Za-z_$][A-Za-z0-9_$]*\s*\.\s*)*?([A-Z][A-Z0-9_]*)\s*\.\s*"
    r"(?:name|toString)\s*\(\s*\)\Z")


def enum_constant_name(expr):
    """`IndexType.SIMPLE.name()` -> `SIMPLE`, else None.

    The value such an expression produces is the constant's own name, so this
    is a resolution rather than a guess. Valid-value lists are written this way
    throughout, and leaving them unresolved would answer "what can I set this
    to?" with `SIMPLE.name()`.
    """
    match = ENUM_CONSTANT_CALL_RE.match(" ".join(expr.split()))
    return match.group(1) if match else None


def literal_or_expression(expr, string_constants=None, owner_class=None):
    """Return (value, is_literal) for a default-value expression.

    With ``string_constants``, a default written as a named constant resolves to
    the string it holds. That is the difference between telling a user the
    default is "snapshot" and telling them it is QUERY_TYPE_SNAPSHOT_OPT_VAL.
    """
    literal = concatenated_string_literal(expr)
    if literal is not None:
        return literal, True
    if string_constants is not None:
        resolved = concatenated_string_literal(expr, string_constants, owner_class)
        if resolved is not None:
            return resolved, True
    enum_member = enum_constant_name(expr)
    if enum_member is not None:
        return enum_member, True
    collapsed = " ".join(expr.split())
    if re.fullmatch(r"(true|false)", collapsed):
        return collapsed, True
    if re.fullmatch(r"-?\d+[LlFfDd]?", collapsed):
        return collapsed.rstrip("LlFfDd"), True
    if re.fullmatch(r"-?\d*\.\d+[FfDd]?", collapsed):
        return collapsed.rstrip("FfDd"), True
    return collapsed, False


# --------------------------------------------------------------------------
# Pass 1 -- config declarations
# --------------------------------------------------------------------------

# Matches both `ConfigProperty<String> FOO = ConfigProperty` and the Flink
# `ConfigOption<String> FOO = ConfigOptions` forms, plus the rarer single-line
# `= ConfigProperty.key(...)`.
DECLARATION_RE = re.compile(
    r"(?P<decl>(?:public|protected|private)?\s*(?:static\s+)?(?:final\s+)?"
    r"(?P<holder>ConfigProperty|ConfigOption)\s*<\s*(?P<type>[^>]*(?:<[^>]*>)?[^>]*)\s*>\s+"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*)"
    r"(?P<builder>ConfigProperty|ConfigOptions)\b"
)

CLASS_DECL_RE = re.compile(
    r"\b(?:public|protected|private)?\s*(?:static\s+)?(?:final\s+)?(?:abstract\s+)?"
    r"(?:class|interface|enum)\s+([A-Za-z_$][A-Za-z0-9_$]*)"
)

# Scala: `val QUERY_TYPE: ConfigProperty[String] = ConfigProperty`. The type
# ascription is optional, and `lazy val`/`var`/`private` may precede it.
SCALA_DECLARATION_RE = re.compile(
    r"(?P<decl>\b(?:private|protected)?\s*(?:\[[A-Za-z0-9_$]*\]\s*)?(?:lazy\s+)?(?:val|var)\s+"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*"
    r"(?::\s*(?P<holder>ConfigProperty|ConfigOption)\s*\[\s*(?P<type>[^\]]*(?:\[[^\]]*\])?[^\]]*)\s*\]\s*)?"
    r"=\s*)"
    r"(?P<builder>ConfigProperty|ConfigOptions)\s*\n?\s*\.\s*key\s*\("
)

# Scala objects, classes and traits all hold configs.
SCALA_CLASS_DECL_RE = re.compile(
    r"\b(?:case\s+)?(?:object|class|trait)\s+([A-Za-z_$][A-Za-z0-9_$]*)")

# `val REALTIME_MERGE: ConfigProperty[String] = HoodieReaderConfig.MERGE_TYPE`
# re-exports a Java-declared config under a Scala name. It declares no new key,
# but a read site naming the Scala constant must still resolve to the real key.
SCALA_ALIAS_RE = re.compile(
    r"\b(?:private|protected)?\s*(?:lazy\s+)?(?:val|var)\s+"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*"
    r":\s*(?:ConfigProperty|ConfigOption)\s*\[[^\]]*\]\s*=\s*"
    r"(?P<target>[A-Za-z_$][A-Za-z0-9_$]*(?:\s*\.\s*[A-Za-z_$][A-Za-z0-9_$]*)+)\s*(?:\n|$)")


def find_enclosing_scala_object(text, index):
    """Innermost-looking object/class/trait name declared before ``index``."""
    best = None
    for match in SCALA_CLASS_DECL_RE.finditer(text, 0, index):
        best = match.group(1)
    return best

# Builder steps we care about, applied to the chain text after the key.
CHAIN_STEP_RE = re.compile(r"\.\s*([A-Za-z_$][A-Za-z0-9_$]*)\s*\(")


def find_enclosing_class(text, index):
    """Innermost-looking class name declared before ``index``."""
    best = None
    for match in CLASS_DECL_RE.finditer(text, 0, index):
        best = match.group(1)
    return best


def parse_chain_steps(chain_text, scala=False):
    """Return an ordered list of (method_name, args_text) from a builder chain.

    Comments are blanked first -- authors do put explanatory comments between a
    builder call's arguments, and those must not end up inside an extracted
    value. String bodies are kept so literals survive.
    """
    chain_text = strip_comments_and_strings(
        chain_text, keep_string_bodies=True, scala=scala)
    steps = []
    pos = 0
    while True:
        match = CHAIN_STEP_RE.search(chain_text, pos)
        if not match:
            break
        args, after = extract_call_args(chain_text, match.end() - 1)
        if args is None:
            pos = match.end()
            continue
        steps.append((match.group(1), args))
        pos = after
    return steps


DERIVED_KEY_RE = re.compile(
    r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*key\(\s*\)\s*(?:\+\s*(.+))?")


def derived_from_config_key(expr, string_constants, owner_class, base_key_resolver):
    """Resolve `OTHER_CONFIG.key()` and `OTHER_CONFIG.key() + ".suffix"`.

    Several configs name their own key, and several name an alternative key,
    relative to another config's. Returns None when the base config is unknown.
    """
    match = DERIVED_KEY_RE.fullmatch(" ".join(expr.split()))
    if not match:
        return None
    base = base_key_resolver(match.group(1))
    if base is None:
        return None
    if match.group(2) is None:
        return base
    suffix = concatenated_string_literal(match.group(2), string_constants, owner_class)
    return None if suffix is None else base + suffix


def parse_declaration(masked_for_scan, original, match, file_path, repo_root,
                      string_constants, base_key_resolver=lambda path: None,
                      enum_docs=None, scala=False):
    """Build one catalog entry from a declaration match. Never raises."""
    warnings = []
    start = match.start()
    end = (find_scala_statement_end(original, start) if scala
           else find_statement_end(original, start))
    if end < 0:
        end = min(len(original), start + 4000)
        warnings.append("statement terminator not found; chain truncated at 4000 chars")
    chain_text = original[start:end]

    steps = parse_chain_steps(chain_text, scala=scala)
    step_names = [name for name, _ in steps]

    enclosing_class = (find_enclosing_scala_object(masked_for_scan, start) if scala
                       else find_enclosing_class(masked_for_scan, start))

    key = None
    key_expr = None
    key_resolution = None
    for name, args in steps:
        if name == "key":
            key_expr = args.strip()
            key = concatenated_string_literal(args)
            if key is not None:
                key_resolution = "literal"
            else:
                key = concatenated_string_literal(
                    args, string_constants, enclosing_class)
                if key is not None:
                    key_resolution = "resolvedFromConstants"
            break

    if key is None and key_expr:
        # Flink re-exports read the key off another ConfigProperty, and some
        # Java configs build the key from a shared prefix constant.
        reference = re.fullmatch(
            r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*key\(\s*\)", key_expr.strip())
        if reference:
            return None, "alias:" + reference.group(1)
        # `OTHER_CONFIG.key() + ".suffix"` -- a key derived from another config's.
        key = derived_from_config_key(
            key_expr, string_constants, enclosing_class, base_key_resolver)
        if key is not None:
            key_resolution = "derivedFromConfigKey"
        else:
            warnings.append(
                "key is a computed expression: " + " ".join(key_expr.split())[:160])

    entry = OrderedDict()
    entry["key"] = key
    entry["keyResolution"] = key_resolution
    entry["keyExpression"] = (" ".join(key_expr.split())
                              if key_expr and key_resolution != "literal" else None)
    # Scala makes the type ascription optional, so there may be no type to read.
    declared_type = match.groupdict().get("type")
    entry["type"] = " ".join(declared_type.split()) if declared_type else None
    entry["language"] = "scala" if scala else "java"
    entry["declaredIn"] = OrderedDict([
        ("class", enclosing_class),
        ("constant", match.group("name")),
        ("file", os.path.relpath(file_path, repo_root)),
        ("line", line_of(original, start)),
    ])
    entry["configGroup"] = entry["declaredIn"]["class"]
    entry["builderStyle"] = "ConfigProperty" if match.group("builder") == "ConfigProperty" else "FlinkConfigOptions"

    # Default value.
    default_value = None
    default_is_literal = False
    has_default = None
    for name, args in steps:
        if name == "defaultValue":
            parts = split_top_level_args(args)
            if parts:
                default_value, default_is_literal = literal_or_expression(
                    parts[0], string_constants, enclosing_class)
                has_default = True
                if len(parts) > 1:
                    doc_on_default = concatenated_string_literal(parts[1])
                    if doc_on_default:
                        entry["docOnDefaultValue"] = doc_on_default
            break
        if name == "noDefaultValue":
            has_default = False
            break
    if has_default is None:
        has_default = False
        if entry["builderStyle"] == "ConfigProperty":
            warnings.append("neither defaultValue() nor noDefaultValue() found in chain")
    entry["hasDefaultValue"] = has_default
    entry["defaultValue"] = default_value
    entry["defaultValueIsLiteral"] = default_is_literal
    if has_default and not default_is_literal:
        entry["defaultValueNote"] = "computed at class-init time; shown as the source expression"

    # Documentation: a string, or a class reference whose text lives elsewhere.
    documentation = None
    documentation_source = None
    enum_documentation = None
    for name, args in steps:
        if name not in ("withDocumentation", "withDescription"):
            continue
        parts = split_top_level_args(args)
        if not parts:
            continue
        class_ref = re.fullmatch(r"([A-Za-z_$][A-Za-z0-9_$.]*)\s*\.\s*class", parts[0].strip())
        if class_ref:
            enum_name = class_ref.group(1).split(".")[-1]
            documentation_source = "enumClass:" + enum_name
            extra = concatenated_string_literal(parts[1]) if len(parts) > 1 else None
            described = (enum_docs or {}).get(enum_name)
            if described:
                entry_enum = described
                pieces = [p for p in (extra, described["description"]) if p]
                documentation = "\n".join(pieces)
                enum_documentation = entry_enum
            else:
                documentation = extra
                warnings.append(
                    "documentation points at enum %s, whose @EnumDescription was not found"
                    % enum_name)
        else:
            documentation = concatenated_string_literal(parts[0])
            documentation_source = "literal"
            if documentation is None:
                documentation = concatenated_string_literal(
                    parts[0], string_constants, enclosing_class)
                documentation_source = "resolvedFromConstants"
            if documentation is None:
                # Documentation that cites another config by `OTHER.key()`.
                documentation = concatenated_string_literal(
                    parts[0], string_constants, enclosing_class,
                    key_resolver=base_key_resolver)
                documentation_source = "resolvedFromConfigKeys"
            if documentation is None:
                documentation_source = None
                warnings.append("documentation is a computed expression")
        break
    entry["documentation"] = documentation
    entry["documentationSource"] = documentation_source
    entry["enumDocumentation"] = enum_documentation

    def collect_string_args(step_name):
        for name, args in steps:
            if name == step_name:
                values = []
                for part in split_top_level_args(args):
                    literal = concatenated_string_literal(part)
                    if literal is None:
                        literal = concatenated_string_literal(
                            part, string_constants, enclosing_class)
                    if literal is None:
                        literal = derived_from_config_key(
                            part, string_constants, enclosing_class, base_key_resolver)
                    if literal is None:
                        literal = enum_constant_name(part)
                    values.append(literal if literal is not None
                                  else " ".join(part.split()))
                return values
        return []

    entry["validValues"] = collect_string_args("withValidValues")
    # A config whose documentation is an enum class declares its permitted values on the enum
    # rather than via withValidValues(...). Fall back to the enum's constants so that
    # "what can I set this to?" is answerable for strategy/policy configs, which are exactly
    # the ones where it matters most.
    if not entry["validValues"] and enum_documentation:
        entry["validValues"] = [v["value"] for v in enum_documentation.get("values", []) if v.get("value")]
    entry["alternatives"] = collect_string_args("withAlternatives")
    since = collect_string_args("sinceVersion")
    entry["sinceVersion"] = since[0] if since else None
    deprecated = collect_string_args("deprecatedAfter")
    entry["deprecatedAfter"] = deprecated[0] if deprecated else None
    entry["supportedVersions"] = collect_string_args("supportedVersions")
    entry["advanced"] = "markAdvanced" in step_names
    entry["hasInferFunction"] = "withInferFunction" in step_names
    entry["builderSteps"] = step_names

    if key is None:
        warnings.append("key could not be resolved to a string literal")
    if documentation is None and documentation_source != "enumClass":
        warnings.append("no documentation text resolved")

    entry["parseWarnings"] = warnings
    return entry, None


def collect_declarations(repo_root, verbose=False):
    """Pass 1. Returns (entries, constant_index, stats)."""
    entries = []
    # (file-local class, constant name) -> config key, so later passes can map a
    # constant reference back to the config it names.
    constant_index = {}
    alias_pending = []
    alias_pending_scala = []
    stats = defaultdict(int)

    string_constants = collect_string_constants(repo_root)
    stats["stringConstants"] = len(string_constants)
    enum_docs = collect_enum_documentation(repo_root)
    stats["documentedEnums"] = len(enum_docs)
    if verbose:
        print("  string constants indexed: %d" % len(string_constants), file=sys.stderr)
        print("  documented enums indexed: %d" % len(enum_docs), file=sys.stderr)

    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            stats["unreadableFiles"] += 1
            continue
        if "ConfigProperty" not in original and "ConfigOptions" not in original:
            continue

        masked = strip_comments_and_strings(original)

        def resolve_base_key(path, _index=constant_index):
            parts = path.split(".")
            const = parts[-1]
            qualifier = parts[-2] if len(parts) >= 2 else None
            return _index.get((qualifier, const)) or _index.get((None, const))

        for match in DECLARATION_RE.finditer(masked):
            stats["declarationsSeen"] += 1
            entry, alias_target = parse_declaration(
                masked, original, match, file_path, repo_root, string_constants,
                resolve_base_key, enum_docs)
            if alias_target is not None:
                alias_pending.append((
                    find_enclosing_class(masked, match.start()),
                    match.group("name"),
                    alias_target.split(":", 1)[1],
                    os.path.relpath(file_path, repo_root),
                    line_of(original, match.start()),
                ))
                continue
            if entry is None:
                stats["unparseable"] += 1
                continue
            # Flink ConfigOptions that do not name a hoodie.* key belong to the
            # Flink option namespace, not the Hudi config namespace.
            if entry["builderStyle"] == "FlinkConfigOptions" and not (
                    entry["key"] or "").startswith("hoodie."):
                stats["flinkOptionsSkipped"] += 1
                continue
            entries.append(entry)
            owner = entry["declaredIn"]["class"]
            if entry["key"]:
                constant_index[(owner, entry["declaredIn"]["constant"])] = entry["key"]
                constant_index[(None, entry["declaredIn"]["constant"])] = entry["key"]

    # Scala declarations come second so that a Scala config deriving its key
    # from a Java config's (`OTHER.key() + ".suffix"`) finds it already indexed.
    for file_path in walk_source_files(repo_root, (".scala",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            stats["unreadableFiles"] += 1
            continue
        if "ConfigProperty" not in original and "ConfigOptions" not in original:
            continue

        masked = strip_comments_and_strings(original, scala=True)

        def resolve_base_key(path, _index=constant_index):
            parts = path.split(".")
            const = parts[-1]
            qualifier = parts[-2] if len(parts) >= 2 else None
            return _index.get((qualifier, const)) or _index.get((None, const))

        for match in SCALA_DECLARATION_RE.finditer(masked):
            stats["scalaDeclarationsSeen"] += 1
            stats["declarationsSeen"] += 1
            entry, alias_target = parse_declaration(
                masked, original, match, file_path, repo_root, string_constants,
                resolve_base_key, enum_docs, scala=True)
            if entry is None:
                stats["unparseable"] += 1
                continue
            # A Scala config whose key resolves outside the hoodie namespace is
            # not a Hudi table config: `ConfigProperty.key(prop.key())` inside a
            # generic type-converting wrapper declares no key of its own.
            if not (entry["key"] or "").startswith("hoodie."):
                stats["scalaNonHoodieSkipped"] += 1
                continue
            entries.append(entry)
            owner = entry["declaredIn"]["class"]
            constant_index[(owner, entry["declaredIn"]["constant"])] = entry["key"]
            constant_index[(None, entry["declaredIn"]["constant"])] = entry["key"]
            stats["scalaDeclarationsParsed"] += 1

        # Scala re-exports of Java configs: index the Scala name against the
        # Java key so read sites naming the Scala constant still attribute.
        for match in SCALA_ALIAS_RE.finditer(masked):
            target = match.group("target")
            target_parts = [p.strip() for p in target.split(".")]
            key = (constant_index.get((target_parts[-2] if len(target_parts) >= 2 else None,
                                       target_parts[-1]))
                   or constant_index.get((None, target_parts[-1])))
            if not key:
                stats["scalaAliasesUnresolved"] += 1
                continue
            owner = find_enclosing_scala_object(masked, match.start())
            constant_index[(owner, match.group("name"))] = key
            alias_pending_scala.append(OrderedDict([
                ("key", key), ("aliasClass", owner), ("aliasConstant", match.group("name")),
                ("file", os.path.relpath(file_path, repo_root)),
                ("line", line_of(original, match.start()))]))
            stats["scalaAliasesResolved"] += 1

    # Resolve re-export aliases (`FOO = OtherHolder.FOO`) to the real key so a
    # read site that uses the alias still lands on the right config.
    aliases = []
    for owner, name, target, rel_file, line in alias_pending:
        target_parts = target.split(".")
        target_const = target_parts[-1]
        target_class = target_parts[-2] if len(target_parts) >= 2 else None
        key = constant_index.get((target_class, target_const)) or \
            constant_index.get((None, target_const))
        if key:
            constant_index[(owner, name)] = key
            aliases.append(OrderedDict([
                ("key", key), ("aliasClass", owner), ("aliasConstant", name),
                ("file", rel_file), ("line", line)]))
            stats["aliasesResolved"] += 1
        else:
            stats["aliasesUnresolved"] += 1

    aliases.extend(alias_pending_scala)

    return entries, constant_index, aliases, stats


# --------------------------------------------------------------------------
# Pass 2 -- accessor methods
# --------------------------------------------------------------------------

# Methods and constructors, including package-private ones. The modifier is
# optional, so NON_METHOD_KEYWORDS below keeps `if (...) {` and friends out.
METHOD_SIGNATURE_RE = re.compile(
    r"(?:(?:public|protected|private)\s+)?(?:static\s+|final\s+|synchronized\s+|abstract\s+)*"
    r"(?:(?P<ret>[A-Za-z_$][A-Za-z0-9_$<>,.\[\]\s?]*?)\s+)?"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*\((?P<params>[^;{()]*(?:\([^()]*\)[^;{()]*)*)\)\s*"
    r"(?:throws\s+[A-Za-z0-9_$.,\s]+)?\{"
)

NON_METHOD_KEYWORDS = frozenset((
    "if", "else", "for", "while", "switch", "catch", "try", "do", "synchronized",
    "return", "new", "case", "assert", "super", "this",
))

GETTER_CALL_RE = re.compile(
    r"\bget(?:String|Int|Integer|Long|Boolean|Double|Float|StringOrDefault|"
    r"IntOrDefault|BooleanOrDefault|LongOrDefault|Enum|Class)?\s*\(\s*"
    r"(?P<arg>[A-Za-z_$][A-Za-z0-9_$.]*)\s*[,)]"
)

ALT_KEYS_CALL_RE = re.compile(
    r"\b(?:get|contains)(?:String|Int|Integer|Long|Boolean|Double|Raw)?WithAltKeys\s*\([^)]*?"
    r"\b(?P<arg>[A-Za-z_$][A-Za-z0-9_$.]*)\s*[,)]"
)


def find_method_bodies(masked, original):
    """Yield (name, body_start, body_end, signature_line) for each method.

    ``body_start`` indexes the ``{``; ``body_end`` the matching ``}``.
    """
    for match in METHOD_SIGNATURE_RE.finditer(masked):
        if match.group("name") in NON_METHOD_KEYWORDS:
            continue
        if (match.group("ret") or "").split()[-1:] and \
                (match.group("ret") or "").split()[-1] in NON_METHOD_KEYWORDS:
            continue
        brace_index = masked.find("{", match.end() - 1)
        if brace_index < 0:
            continue
        depth = 0
        end = -1
        for i in range(brace_index, len(masked)):
            if masked[i] == "{":
                depth += 1
            elif masked[i] == "}":
                depth -= 1
                if depth == 0:
                    end = i
                    break
        if end < 0:
            continue
        yield match.group("name"), brace_index, end, line_of(original, match.start())


def resolve_constant(expr, owner_class, constant_index):
    """Map a (possibly qualified) constant reference to a config key."""
    parts = expr.split(".")
    const = parts[-1]
    qualifier = parts[-2] if len(parts) >= 2 else None
    return (constant_index.get((qualifier, const))
            or (constant_index.get((owner_class, const)) if qualifier is None else None)
            or constant_index.get((None, const)))


def collect_accessors(repo_root, constant_index):
    """Pass 2. accessor method name -> set of config keys it reads.

    Deliberately narrow: only methods whose body is short and mentions exactly
    one config constant, so that a call site of the method is unambiguous.
    """
    accessors = defaultdict(set)
    accessor_sites = {}
    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "get" not in original or "Config" not in original:
            continue
        masked = strip_comments_and_strings(original)
        owner_class = None
        class_match = CLASS_DECL_RE.search(masked)
        if class_match:
            owner_class = class_match.group(1)

        for name, body_start, body_end, sig_line in find_method_bodies(masked, original):
            body = masked[body_start:body_end]
            if len(body) > 600:
                continue
            keys = set()
            for pattern in (GETTER_CALL_RE, ALT_KEYS_CALL_RE):
                for call in pattern.finditer(body):
                    key = resolve_constant(call.group("arg"), owner_class, constant_index)
                    if key:
                        keys.add(key)
            if len(keys) == 1:
                only = next(iter(keys))
                accessors[name].add(only)
                accessor_sites.setdefault(name, (owner_class, os.path.relpath(file_path, repo_root), sig_line))
    # An accessor name that maps to more than one key across classes is
    # ambiguous at a call site; drop it rather than mis-attribute.
    return ({name: next(iter(keys)) for name, keys in accessors.items() if len(keys) == 1},
            accessor_sites)


# --------------------------------------------------------------------------
# Pass 2b -- engine-specific effective defaults
# --------------------------------------------------------------------------
#
# `ConfigProperty.defaultValue()` is the *declared* default, and for a handful of
# configs it is not the one that takes effect. A builder's `build()` calls
#
#     config.setDefaultValue(PROP, getDefaultXxx(engineType))
#
# and `HoodieConfig.setDefaultValue(prop, value)` writes that value whenever the
# user has not set the key -- so for those configs the engine's value *is* the
# effective default. Reporting the declared one tells a Spark user that
# hoodie.metadata.index.column.stats.enable defaults to false when on Spark it is
# true, which is the kind of confidently wrong answer this catalog exists to
# prevent.
#
# This pass finds the two-argument setDefaultValue calls, follows the named
# helper to its `switch (engineType)`, and reads one value per engine. Some of
# those helpers do not return a fixed value per engine: they branch further on
# another config (`isConsistentHashingBucketIndex()`) or on the runtime
# (`getSparkRuntimeVersion()`). Those are recorded as conditional rather than
# flattened into a value that would be wrong half the time.

ENGINE_NAMES = ("SPARK", "FLINK", "JAVA")

SET_DEFAULT_VALUE_RE = re.compile(
    r"\.\s*setDefaultValue\s*\(")

# `getDefaultColStatsEnable(engineType)` -- a bare call passing the engine.
ENGINE_HELPER_CALL_RE = re.compile(
    r"\A(?P<helper>[A-Za-z_$][A-Za-z0-9_$]*)\s*\(\s*engineType\s*\)\Z")

CASE_LABEL_RE = re.compile(
    r"\b(?:case\s+([A-Za-z_$][A-Za-z0-9_$]*)|(default))\s*:")
RETURN_RE = re.compile(r"\breturn\b")
TERNARY_RE = re.compile(r"\A(?P<cond>.+?)\?(?P<yes>.+):(?P<no>.+)\Z", re.DOTALL)


def _if_else_returns_as_ternary(region):
    """Rewrite ``if (c) { return a; } else { return b; }`` as ``c ? a : b``.

    Only the shape where both arms return is handled; anything else returns None
    so the caller falls back to reading the first return it finds.
    """
    match = IF_RE.search(region)
    if not match:
        return None
    condition, after = extract_call_args(region, match.end() - 1)
    if condition is None:
        return None

    then_value, after_then = _first_returned_expression(region, after)
    if then_value is None:
        return None

    else_index = region.find("else", after_then)
    if else_index < 0:
        return None
    else_value, _ = _first_returned_expression(region, else_index + 4)
    if else_value is None:
        return None

    return "%s ? %s : %s" % (" ".join(condition.split()), then_value, else_value)


def _first_returned_expression(region, start):
    """The expression of the first ``return`` at or after ``start``."""
    match = RETURN_RE.search(region, start)
    if not match:
        return None, start
    semicolon = region.find(";", match.end())
    if semicolon < 0:
        return None, start
    return " ".join(region[match.end():semicolon].split()), semicolon + 1


def _switch_case_returns(body):
    """Map each ``case LABEL:`` in a switch body to the expression it returns.

    Labels that fall through to a later ``return`` share that return, which is
    how SPARK and JAVA come to share one value in several of these helpers.
    """
    labels = [(m.group(1) or "default", m.start(), m.end())
              for m in CASE_LABEL_RE.finditer(body)]
    if not labels:
        return {}
    returns = {}
    for index, (label, _, label_end) in enumerate(labels):
        # The region running to the next case label (or the end of the body).
        region_end = labels[index + 1][1] if index + 1 < len(labels) else len(body)
        region = body[label_end:region_end]

        # An engine arm may itself branch -- Spark's marker type depends on
        # whether the embedded timeline server is on. Rewriting that if/else as
        # the equivalent ternary lets the caller record it as a condition rather
        # than silently reporting only the first branch's value.
        branched = _if_else_returns_as_ternary(region)
        if branched:
            returns[label] = branched
            continue

        match = RETURN_RE.search(region)
        if match:
            expression = region[match.end():]
            semicolon = expression.find(";")
            returns[label] = expression[:semicolon if semicolon >= 0 else len(expression)].strip()
        else:
            # Fall-through: this label shares whatever the next one returns.
            returns[label] = None

    # Resolve fall-through by walking forward to the next label that returns.
    ordered = [label for label, _, _ in labels]
    for position, label in enumerate(ordered):
        if returns.get(label) is not None:
            continue
        for later in ordered[position + 1:]:
            if returns.get(later) is not None:
                returns[label] = returns[later]
                break
    return returns


def _resolve_engine_expression(expression, string_constants, owner_class):
    """Turn one engine's return expression into (value, conditional_or_None).

    Returns ``(value, None)`` for a fixed value, or ``(None, description)`` when
    the helper branches on something other than the engine, so the caller can
    record the condition instead of inventing a value.
    """
    collapsed = " ".join(expression.split())
    if not collapsed:
        return None, None

    ternary = TERNARY_RE.match(collapsed)
    if ternary:
        condition = " ".join(ternary.group("cond").split())
        yes, _ = _resolve_engine_expression(
            ternary.group("yes"), string_constants, owner_class)
        no, _ = _resolve_engine_expression(
            ternary.group("no"), string_constants, owner_class)
        return None, OrderedDict([
            ("condition", condition),
            ("whenTrue", yes),
            ("whenFalse", no),
        ])

    literal, is_literal = literal_or_expression(
        collapsed, string_constants, owner_class)
    if is_literal:
        return literal, None

    # `MarkerType.DIRECT.toString()` -- an enum constant, whose own name is the
    # value the config receives.
    enum_member = enum_constant_name(collapsed)
    if enum_member:
        return enum_member, None

    # `ENABLE.defaultValue()` -- explicitly the declared default.
    if re.fullmatch(r"[A-Za-z_$][A-Za-z0-9_$.]*\s*\.\s*defaultValue\s*\(\s*\)", collapsed):
        return "(declared default)", None

    return None, OrderedDict([("expression", collapsed), ("resolved", False)])


def _resolve_config_constant_strictly(expr, owner_class, constant_index):
    """Resolve a config constant without the ambiguous bare-name fallback.

    Getting this wrong here is worse than returning nothing: an engine default
    attached to the wrong config is a confident lie about a config nobody
    overrode. A qualified name must match its qualifier, and an unqualified one
    must belong to the file's own config class.
    """
    parts = [part.strip() for part in expr.split(".")]
    const = parts[-1]
    if len(parts) >= 2:
        return constant_index.get((parts[-2], const))
    return constant_index.get((owner_class, const))


def collect_engine_defaults(repo_root, constant_index):
    """Pass 2b. config key -> per-engine effective default, with provenance."""
    string_constants = collect_string_constants(repo_root)
    by_key = {}
    stats = defaultdict(int)

    for file_path in walk_source_files(repo_root, (".java",)):
        if SOURCE_DIR_MARKER not in file_path:
            continue
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        if "setDefaultValue" not in original or "engineType" not in original:
            continue

        masked = strip_comments_and_strings(original, keep_string_bodies=True)
        relative = os.path.relpath(file_path, repo_root)
        # These calls sit inside a nested Builder, but the constants they name
        # belong to the enclosing config class. Resolving against the innermost
        # class would miss and fall back to a bare-name lookup, which happily
        # matches a same-named constant in an unrelated config class --
        # HoodieMetadataConfig.ENABLE would come back as ConsistencyGuardConfig's.
        top_level_match = CLASS_DECL_RE.search(masked)
        owner_class = top_level_match.group(1) if top_level_match else None

        # Index every method body in the file once, so a helper can be found
        # wherever it is declared: some sit on the Builder, others (the Parquet
        # codec one) are static on the config class itself.
        bodies = {}
        for name, body_start, body_end, _ in find_method_bodies(masked, original):
            bodies.setdefault(name, masked[body_start:body_end])

        for call in SET_DEFAULT_VALUE_RE.finditer(masked):
            args, _ = extract_call_args(masked, call.end() - 1)
            if args is None:
                continue
            parts = split_top_level_args(args)
            if len(parts) != 2:
                continue
            key = _resolve_config_constant_strictly(
                parts[0].strip(), owner_class, constant_index)
            if not key:
                stats["constantUnresolved"] += 1
                continue
            helper_call = ENGINE_HELPER_CALL_RE.match(" ".join(parts[1].split()))
            if not helper_call:
                continue
            helper = helper_call.group("helper")
            body = bodies.get(helper)
            if body is None:
                stats["helperBodyNotFound"] += 1
                continue

            returns = _switch_case_returns(body)
            if not returns:
                stats["helperWithoutSwitch"] += 1
                continue

            # Most helpers end `default: throw new HoodieNotSupportedException`,
            # which returns nothing and must not become any engine's value. Where
            # `default:` does return (the Parquet codec helper puts Java there),
            # it is the value for every engine without its own case.
            fallback = returns.get("default")

            engine_defaults = OrderedDict()
            conditional = OrderedDict()
            for engine in ENGINE_NAMES:
                expression = returns.get(engine)
                if expression is None:
                    expression = fallback
                if expression is None:
                    continue
                value, condition = _resolve_engine_expression(
                    expression, string_constants, owner_class)
                if condition is not None:
                    conditional[engine.lower()] = condition
                    stats["conditionalEngineDefaults"] += 1
                else:
                    engine_defaults[engine.lower()] = value

            if not engine_defaults and not conditional:
                continue

            record = OrderedDict()
            record["engineDefaults"] = engine_defaults
            if conditional:
                record["conditionalEngineDefaults"] = conditional
                record["defaultIsConfigDependent"] = True
            else:
                record["defaultIsConfigDependent"] = False
            record["resolvedFrom"] = OrderedDict([
                ("helper", helper),
                ("class", owner_class),
                ("file", relative),
                ("line", line_of(original, call.start())),
            ])
            by_key[key] = record
            stats["configsWithEngineDefaults"] += 1

    return by_key, stats


# --------------------------------------------------------------------------
# Pass 3 -- read sites, co-configs, gating
# --------------------------------------------------------------------------

IDENTIFIER_RE = re.compile(r"\b([A-Za-z_$][A-Za-z0-9_$]*)\s*(?:\.\s*([A-Za-z_$][A-Za-z0-9_$]*))?")

IF_RE = re.compile(r"\bif\s*\(")
SWITCH_RE = re.compile(r"\bswitch\s*\(")
CASE_RE = re.compile(r"\bcase\s+([A-Za-z_$][A-Za-z0-9_$.]*)\s*:")

# Strings that mean "this read only happens under a condition", used to decide
# whether a gate is worth recording at all.
CONDITION_NOISE_RE = re.compile(r"^\s*(true|false|1|0)\s*$")


def summarize_condition(condition):
    collapsed = " ".join(condition.split())
    if len(collapsed) > 160:
        collapsed = collapsed[:157] + "..."
    return collapsed


def enclosing_gates(masked, body_start, site_index):
    """Best-effort: conditions of the if/switch/case blocks containing ``site_index``.

    Textual, not an AST walk. It finds each ``if (...)`` / ``switch (...)``
    before the site, locates the block it opens, and keeps the condition when
    the site falls inside that block. ``case`` labels are matched by scanning
    backwards to the nearest label within the enclosing switch.
    """
    gates = []

    for pattern, kind in ((IF_RE, "if"), (SWITCH_RE, "switch")):
        for match in pattern.finditer(masked, body_start, site_index):
            condition, after = extract_call_args(masked, match.end() - 1)
            if condition is None:
                continue
            brace = masked.find("{", after)
            if brace < 0 or brace > site_index:
                continue
            # Nothing but whitespace may sit between ")" and "{" for the brace
            # to be this construct's own block.
            if masked[after:brace].strip():
                continue
            depth = 0
            block_end = -1
            for i in range(brace, len(masked)):
                if masked[i] == "{":
                    depth += 1
                elif masked[i] == "}":
                    depth -= 1
                    if depth == 0:
                        block_end = i
                        break
            if block_end < 0 or not (brace < site_index < block_end):
                continue
            text = summarize_condition(condition)
            if CONDITION_NOISE_RE.match(text):
                continue
            if kind == "switch":
                label = None
                for case_match in CASE_RE.finditer(masked, brace, site_index):
                    label = case_match.group(1)
                if label:
                    gates.append("switch (%s) case %s" % (text, label))
                else:
                    gates.append("switch (%s)" % text)
            else:
                gates.append("if (%s)" % text)

    # Innermost conditions are the informative ones.
    return gates[-MAX_GATES_PER_SITE:]


def collect_read_sites(repo_root, constant_index, accessors, config_keys, verbose=False):
    """Pass 3. Returns key -> {"readSites": [...], "coConfigs": {key: count}}."""
    result = defaultdict(lambda: {"readSites": [], "coConfigs": defaultdict(int)})
    # Constant names that are unambiguous enough to attribute without a
    # qualifier: a bare name used by exactly one config.
    bare_constants = defaultdict(set)
    for (qualifier, const), key in constant_index.items():
        bare_constants[const].add(key)
    unambiguous_bare = {const: next(iter(keys))
                        for const, keys in bare_constants.items() if len(keys) == 1}

    stats = defaultdict(int)

    for file_path in walk_source_files(repo_root, READ_SITE_EXTENSIONS):
        try:
            with open(file_path, "r", encoding="utf-8", errors="replace") as handle:
                original = handle.read()
        except OSError:
            continue
        is_scala = file_path.endswith(".scala")
        masked = strip_comments_and_strings(original, scala=is_scala)
        relative = os.path.relpath(file_path, repo_root)
        owner_class = None
        class_match = (SCALA_CLASS_DECL_RE if is_scala else CLASS_DECL_RE).search(masked)
        if class_match:
            owner_class = class_match.group(1)

        if is_scala:
            # There is no Scala method-body parser here. A Scala mention is
            # recorded as a location only: no enclosing method, no co-configs and
            # no gating, because inferring any of those would mean guessing.
            # `scalaReadContextResolved: false` on the config says so outright,
            # so an empty gate list is never mistaken for "this read is
            # unconditional".
            for match in IDENTIFIER_RE.finditer(masked):
                key, via = _identifier_to_key(match, owner_class, constant_index,
                                              unambiguous_bare, accessors)
                if not key or key not in config_keys:
                    continue
                bucket = result[key]
                bucket["scalaReferences"] = bucket.get("scalaReferences", 0) + 1
                if len(bucket["readSites"]) < MAX_READ_SITES:
                    bucket["readSites"].append(OrderedDict([
                        ("class", owner_class),
                        ("method", None),
                        ("file", relative),
                        ("line", line_of(original, match.start())),
                        ("via", "scalaReference"),
                        ("gatesResolved", False),
                        ("gates", []),
                    ]))
                    stats["readSites"] += 1
                    stats["scalaReadSites"] += 1
            continue

        declaring_file = relative

        for method_name, body_start, body_end, sig_line in find_method_bodies(masked, original):
            body = masked[body_start:body_end]
            # Which configs does this method touch at all? Needed for co-configs.
            hits = []
            for match in IDENTIFIER_RE.finditer(body):
                key, via = _identifier_to_key(match, owner_class, constant_index,
                                              unambiguous_bare, accessors)
                if key and key in config_keys:
                    hits.append((key, body_start + match.start(), via))
            if not hits:
                continue

            touched = OrderedDict()
            for key, _, _ in hits:
                touched[key] = True

            # A value read into a local is almost always *used* further down,
            # often inside the branch that decides whether it matters at all.
            # Following the local is what surfaces the real gate.
            local_uses = local_alias_uses(body, body_start, hits)

            seen_in_method = set()
            for key, absolute_index, via in hits:
                bucket = result[key]
                # One read site per (method, key); the first mention wins.
                if key in seen_in_method:
                    continue
                seen_in_method.add(key)

                for other in touched:
                    if other != key:
                        bucket["coConfigs"][other] += 1

                if len(bucket["readSites"]) >= MAX_READ_SITES:
                    stats["readSitesCapped"] += 1
                    continue
                # Reading the declaration itself is not a read site.
                if declaring_file.endswith(".java") and via == "constant" \
                        and _is_declaration_site(masked, absolute_index):
                    continue

                gates = list(enclosing_gates(masked, body_start, absolute_index))
                for use_index in local_uses.get(absolute_index, ()):
                    for gate in enclosing_gates(masked, body_start, use_index):
                        if gate not in gates:
                            gates.append(gate)

                bucket["readSites"].append(OrderedDict([
                    ("class", owner_class),
                    ("method", method_name),
                    ("file", declaring_file),
                    ("line", line_of(original, absolute_index)),
                    ("via", via),
                    ("gatesResolved", True),
                    ("gates", gates[:MAX_GATES_PER_SITE * 2]),
                ]))
                stats["readSites"] += 1

    return result, stats


LOCAL_ASSIGNMENT_RE = re.compile(
    r"(?:\b(?:final\s+)?[A-Za-z_$][A-Za-z0-9_$<>,.\[\]\s?]*?\s+)?"
    r"(?P<name>[A-Za-z_$][A-Za-z0-9_$]*)\s*=\s*$")


def local_alias_uses(body, body_start, hits):
    """Map a read site to later uses of the local variable it is assigned to.

    ``int inlineCompactDeltaCommitMax = config.getInlineCompactDeltaCommitMax();``
    puts the value in a local, and the branch that decides whether the config
    matters tests that local, not the accessor. Without this the gate for such a
    config would always come back empty.
    """
    uses = defaultdict(list)
    for _, absolute_index, _ in hits:
        relative = absolute_index - body_start
        line_start = body.rfind("\n", 0, relative) + 1
        prefix = body[line_start:relative]
        match = LOCAL_ASSIGNMENT_RE.search(prefix)
        if not match:
            continue
        name = match.group("name")
        if len(name) < 3:
            continue
        for occurrence in re.finditer(r"\b%s\b" % re.escape(name), body):
            if occurrence.start() <= relative:
                continue
            uses[absolute_index].append(body_start + occurrence.start())
            if len(uses[absolute_index]) >= 12:
                break
    return uses


def _is_declaration_site(masked, index):
    line_start = masked.rfind("\n", 0, index) + 1
    line_end = masked.find("\n", index)
    line = masked[line_start:line_end if line_end > 0 else len(masked)]
    return "ConfigProperty" in line and "=" in line


def _identifier_to_key(match, owner_class, constant_index, unambiguous_bare, accessors):
    """Map one identifier occurrence to a config key, or None.

    Returns (key, via). ``via`` records how the attribution was made so a reader
    can tell a direct constant reference from a call through an accessor.
    """
    first, second = match.group(1), match.group(2)
    if second:
        # `HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS`
        key = constant_index.get((first, second))
        if key:
            return key, "constant"
        # `config.getInlineCompactDeltaCommitMax()` -- the receiver is a value,
        # so only the method name carries the attribution.
        if second in accessors:
            return accessors[second], "accessor:" + second
        if _looks_like_constant(second):
            key = unambiguous_bare.get(second)
            if key:
                return key, "constant"
        return None, None
    if first in accessors:
        return accessors[first], "accessor:" + first
    if _looks_like_constant(first):
        key = unambiguous_bare.get(first)
        if key:
            return key, "constant"
    return None, None


def _looks_like_constant(name):
    return name.upper() == name and any(ch.isalpha() for ch in name)


# --------------------------------------------------------------------------
# Assembly and output
# --------------------------------------------------------------------------

def read_project_version(repo_root):
    pom = os.path.join(repo_root, "pom.xml")
    try:
        with open(pom, "r", encoding="utf-8") as handle:
            text = handle.read()
    except OSError:
        return None
    # The project's own version is the first <version> after the hudi artifactId.
    anchor = text.find("<artifactId>hudi</artifactId>")
    if anchor < 0:
        anchor = 0
    match = re.search(r"<version>([^<]+)</version>", text[anchor:])
    return match.group(1).strip() if match else None


def read_git_sha(repo_root):
    try:
        output = subprocess.run(
            ["git", "-C", repo_root, "rev-parse", "HEAD"],
            capture_output=True, text=True, timeout=30, check=False)
        return output.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def module_of(relative_path):
    return relative_path.split(os.sep, 1)[0] if os.sep in relative_path else relative_path


def build_catalog(repo_root, verbose=False):
    entries, constant_index, aliases, decl_stats = collect_declarations(repo_root, verbose)
    if verbose:
        print("  declarations parsed: %d" % len(entries), file=sys.stderr)

    accessors, accessor_sites = collect_accessors(repo_root, constant_index)
    if verbose:
        print("  accessor methods resolved: %d" % len(accessors), file=sys.stderr)

    engine_defaults, engine_stats = collect_engine_defaults(repo_root, constant_index)
    if verbose:
        print("  configs with engine-specific defaults: %d" % len(engine_defaults),
              file=sys.stderr)

    # Merge duplicate keys (same key declared in several classes -- re-exports
    # that the alias pass could not fold, or genuinely duplicated declarations).
    by_key = OrderedDict()
    keyless = []
    for entry in entries:
        key = entry["key"]
        if not key:
            keyless.append(entry)
            continue
        if key in by_key:
            existing = by_key[key]
            # A key can be declared twice for real: Flink declares a handful of
            # shared keys as ConfigOptions while Spark declares them as a
            # ConfigProperty. The ConfigProperty form is the engine-agnostic one
            # and carries alternatives and valid values, so let it own the entry
            # and demote the other to alsoDeclaredIn.
            if (existing["builderStyle"] == "FlinkConfigOptions"
                    and entry["builderStyle"] == "ConfigProperty"):
                entry.setdefault("alsoDeclaredIn", []).extend(
                    existing.get("alsoDeclaredIn", []))
                entry["alsoDeclaredIn"].append(existing["declaredIn"])
                if not entry.get("documentation") and existing.get("documentation"):
                    entry["documentation"] = existing["documentation"]
                    entry["documentationSource"] = existing["documentationSource"]
                by_key[key] = entry
                continue
            existing.setdefault("alsoDeclaredIn", []).append(entry["declaredIn"])
            # Prefer whichever declaration carries documentation.
            if not existing.get("documentation") and entry.get("documentation"):
                existing["documentation"] = entry["documentation"]
                existing["documentationSource"] = entry["documentationSource"]
            continue
        by_key[key] = entry

    config_keys = set(by_key)
    read_data, read_stats = collect_read_sites(
        repo_root, constant_index, accessors, config_keys, verbose)

    alias_by_key = defaultdict(list)
    for alias in aliases:
        alias_by_key[alias["key"]].append(alias)

    accessor_by_key = defaultdict(list)
    for name, key in accessors.items():
        owner, rel_file, line = accessor_sites.get(name, (None, None, None))
        accessor_by_key[key].append(OrderedDict([
            ("method", name), ("class", owner), ("file", rel_file), ("line", line)]))

    configs = []
    for key in sorted(by_key):
        entry = by_key[key]
        entry["module"] = module_of(entry["declaredIn"]["file"])
        entry["accessors"] = sorted(accessor_by_key.get(key, []),
                                    key=lambda a: (a["class"] or "", a["method"]))
        entry["aliasConstants"] = alias_by_key.get(key, [])

        # Engine-specific effective defaults, alongside the declared one rather
        # than replacing it: both are facts a user may need, and which applies
        # depends on the engine they are writing with.
        override = engine_defaults.get(key)
        if override:
            resolved = OrderedDict()
            for engine, value in override["engineDefaults"].items():
                resolved[engine] = (entry["defaultValue"]
                                    if value == "(declared default)" else value)
            entry["engineDefaults"] = resolved
            if override.get("conditionalEngineDefaults"):
                entry["conditionalEngineDefaults"] = override["conditionalEngineDefaults"]
            entry["defaultIsConfigDependent"] = override["defaultIsConfigDependent"]
            entry["engineDefaultsResolvedFrom"] = override["resolvedFrom"]
            entry["engineDefaultNote"] = (
                "The builder overrides this config's declared default per engine via "
                "setDefaultValue(), which applies whenever the key is not set explicitly. "
                "For a user on a given engine the engineDefaults value is the effective "
                "default; declaredValue is not.")
        else:
            entry["engineDefaults"] = None

        data = read_data.get(key)
        if data:
            entry["readSites"] = data["readSites"]
            co_configs = sorted(data["coConfigs"].items(), key=lambda kv: (-kv[1], kv[0]))
            entry["coConfigs"] = [OrderedDict([("key", k), ("sharedMethods", c)])
                                  for k, c in co_configs[:MAX_CO_CONFIGS]]
            # Roll the per-site gates up, keeping provenance. A gate on its own
            # is close to useless -- "if (compactable)" means nothing without
            # the method it came from.
            gates = OrderedDict()
            for site in data["readSites"]:
                for gate in site["gates"]:
                    where = "%s.%s (%s:%d)" % (
                        site["class"] or "?", site["method"] or "?",
                        site["file"], site["line"])
                    gates.setdefault(gate, where)
            entry["gatingConditions"] = [
                OrderedDict([("condition", gate), ("at", where)])
                for gate, where in list(gates.items())[:12]]
        else:
            entry["readSites"] = []
            entry["coConfigs"] = []
            entry["gatingConditions"] = []
            entry["parseWarnings"].append("no read site resolved for this config")

        # Say plainly how much of this config's read context was actually
        # recovered. A Scala-only config has locations but no method, no
        # co-configs and no gates -- which must not read as "nothing gates it".
        scala_sites = sum(1 for site in entry["readSites"]
                          if site.get("via") == "scalaReference")
        entry["scalaReadSites"] = scala_sites
        entry["readContextResolved"] = bool(entry["readSites"]) and scala_sites == 0
        if scala_sites:
            entry["readContextNote"] = (
                "%d of %d recorded read sites are in Scala, where this generator resolves "
                "the file and line but not the enclosing method, co-configs or gating "
                "conditions. Absence of a gate here is not evidence the read is "
                "unconditional; check the cited line."
                % (scala_sites, len(entry["readSites"])))
        entry["gatingHeuristic"] = (
            "Conditions are recovered by text matching on enclosing if/switch blocks. "
            "They indicate where a read is conditional; they are not a complete or "
            "verified account of when the config takes effect. Confirm at the cited line.")
        configs.append(entry)

    for entry in keyless:
        entry["module"] = module_of(entry["declaredIn"]["file"])
        entry["readSites"] = []
        entry["coConfigs"] = []
        entry["gatingConditions"] = []
        entry["scalaReadSites"] = 0
        entry["readContextResolved"] = False
        entry["accessors"] = []
        entry["aliasConstants"] = []
        configs.append(entry)

    stats = {
        "declarationsSeen": decl_stats["declarationsSeen"],
        "distinctKeys": len(by_key),
        "keylessDeclarations": len(keyless),
        "aliasesResolved": decl_stats["aliasesResolved"],
        "aliasesUnresolved": decl_stats["aliasesUnresolved"],
        "flinkOptionsSkipped": decl_stats["flinkOptionsSkipped"],
        "accessorsResolved": len(accessors),
        "readSites": read_stats["readSites"],
        "readSitesCapped": read_stats["readSitesCapped"],
        "scalaDeclarations": decl_stats["scalaDeclarationsParsed"],
        "scalaReadSites": read_stats["scalaReadSites"],
        "configsWithEngineDefaults": len(engine_defaults),
        "configsWithConfigDependentDefaults": sum(
            1 for record in engine_defaults.values()
            if record.get("defaultIsConfigDependent")),
    }
    return configs, stats


def write_summary(configs, stats, catalog, out_path):
    by_group = defaultdict(int)
    by_module = defaultdict(int)
    advanced = 0
    deprecated = 0
    documented = 0
    with_reads = 0
    with_gates = 0
    scala_declared = 0
    with_engine_defaults = 0
    config_dependent = 0
    for entry in configs:
        by_group[entry.get("configGroup") or "(unknown)"] += 1
        by_module[entry.get("module") or "(unknown)"] += 1
        advanced += 1 if entry.get("advanced") else 0
        deprecated += 1 if entry.get("deprecatedAfter") else 0
        documented += 1 if entry.get("documentation") else 0
        with_reads += 1 if entry.get("readSites") else 0
        with_gates += 1 if entry.get("gatingConditions") else 0
        scala_declared += 1 if entry.get("language") == "scala" else 0
        with_engine_defaults += 1 if entry.get("engineDefaults") else 0
        config_dependent += 1 if entry.get("defaultIsConfigDependent") else 0

    lines = []
    lines.append("<!--")
    lines.append("Licensed to the Apache Software Foundation (ASF) under one")
    lines.append("or more contributor license agreements.  See the NOTICE file")
    lines.append("distributed with this work for additional information")
    lines.append("regarding copyright ownership.  The ASF licenses this file")
    lines.append("to you under the Apache License, Version 2.0 (the")
    lines.append('"License"); you may not use this file except in compliance')
    lines.append("with the License.  You may obtain a copy of the License at")
    lines.append("")
    lines.append("   http://www.apache.org/licenses/LICENSE-2.0")
    lines.append("")
    lines.append("Unless required by applicable law or agreed to in writing, software")
    lines.append('distributed under the License is distributed on an "AS IS" BASIS,')
    lines.append("WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.")
    lines.append("See the License for the specific language governing permissions and")
    lines.append("limitations under the License.")
    lines.append("-->")
    lines.append("")
    lines.append("# Config catalog summary")
    lines.append("")
    lines.append("Generated by `scripts/generate_config_catalog.py`. Do not edit by hand.")
    lines.append("")
    lines.append("| | |")
    lines.append("|---|---|")
    lines.append("| Hudi version | `%s` |" % catalog["hudiVersion"])
    lines.append("| Source commit | `%s` |" % catalog["generatedFrom"])
    lines.append("| Distinct config keys | %d |" % len(configs))
    lines.append("| Declared in Scala | %d |" % scala_declared)
    lines.append("| With engine-specific defaults | %d |" % with_engine_defaults)
    lines.append("| Whose default is also config-dependent | %d |" % config_dependent)
    lines.append("| Advanced | %d |" % advanced)
    lines.append("| Deprecated | %d |" % deprecated)
    lines.append("| With documentation text | %d |" % documented)
    lines.append("| With at least one read site | %d |" % with_reads)
    lines.append("| With at least one gating condition | %d |" % with_gates)
    lines.append("| Read sites recorded | %d |" % stats["readSites"])
    lines.append("| Accessor methods resolved | %d |" % stats["accessorsResolved"])
    lines.append("")
    engine_rows = [entry for entry in configs if entry.get("engineDefaults")]
    if engine_rows:
        lines.append("## Engine-specific effective defaults")
        lines.append("")
        lines.append("For these configs the builder overrides the declared default per engine, so "
                     "`defaultValue` alone is not what a user gets. `(conditional)` means that "
                     "engine's default branches on another config or on the runtime; see "
                     "`conditionalEngineDefaults` in the catalog.")
        lines.append("")
        lines.append("| Config | Declared | Spark | Flink | Java |")
        lines.append("|---|---|---|---|---|")
        for entry in engine_rows:
            conditional = entry.get("conditionalEngineDefaults") or {}
            cells = []
            for engine in ("spark", "flink", "java"):
                if engine in conditional:
                    cells.append("_(conditional)_")
                else:
                    value = entry["engineDefaults"].get(engine)
                    cells.append("`%s`" % value if value is not None else "_(unresolved)_")
            declared = entry.get("defaultValue")
            lines.append("| `%s` | %s | %s | %s | %s |" % (
                entry["key"],
                "`%s`" % declared if declared is not None else "_(none)_",
                cells[0], cells[1], cells[2]))
        lines.append("")

    lines.append("## By module")
    lines.append("")
    lines.append("| Module | Configs |")
    lines.append("|---|---|")
    for module, count in sorted(by_module.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append("| `%s` | %d |" % (module, count))
    lines.append("")
    lines.append("## By config group")
    lines.append("")
    lines.append("| Group | Configs |")
    lines.append("|---|---|")
    for group, count in sorted(by_group.items(), key=lambda kv: (-kv[1], kv[0])):
        lines.append("| `%s` | %d |" % (group, count))
    lines.append("")

    with open(out_path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--repo-root",
        default=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        help="Hudi checkout to scan (default: the repo this script lives in)")
    parser.add_argument(
        "--out-dir", default=None,
        help="Where to write config-catalog.json and config-catalog-summary.md "
             "(default: hudi-ai-operator/skills/hudi-config-consultant)")
    parser.add_argument("--verbose", action="store_true", help="Per-pass progress on stderr")
    args = parser.parse_args(argv)

    repo_root = os.path.abspath(args.repo_root)
    out_dir = args.out_dir or os.path.join(
        repo_root, "hudi-ai-operator", "skills", "hudi-config-consultant")
    out_dir = os.path.abspath(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    if args.verbose:
        print("Scanning %s" % repo_root, file=sys.stderr)
    configs, stats = build_catalog(repo_root, args.verbose)

    catalog = OrderedDict([
        # 2: adds engineDefaults / conditionalEngineDefaults, and Scala-declared
        # configs with their readContextResolved honesty flag.
        ("schemaVersion", 2),
        ("hudiVersion", read_project_version(repo_root)),
        ("generatedFrom", read_git_sha(repo_root)),
        ("configs", configs),
    ])

    json_path = os.path.join(out_dir, "config-catalog.json")
    with open(json_path, "w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=2, sort_keys=False, ensure_ascii=False)
        handle.write("\n")

    summary_path = os.path.join(out_dir, "config-catalog-summary.md")
    write_summary(configs, stats, catalog, summary_path)

    fully = sum(1 for c in configs if not c["parseWarnings"])
    partially = len(configs) - fully
    with_reads = sum(1 for c in configs if c["readSites"])
    with_co = sum(1 for c in configs if c["coConfigs"])
    with_gates = sum(1 for c in configs if c["gatingConditions"])

    print("Hudi version      : %s" % catalog["hudiVersion"])
    print("Source commit     : %s" % catalog["generatedFrom"])
    print("Declarations seen : %d" % stats["declarationsSeen"])
    print("Distinct configs  : %d" % len(configs))
    print("  fully parsed    : %d" % fully)
    print("  with warnings   : %d" % partially)
    print("  declared in Scala: %d" % stats["scalaDeclarations"])
    print("Engine defaults   : %d configs (%d also config-dependent)"
          % (stats["configsWithEngineDefaults"],
             stats["configsWithConfigDependentDefaults"]))
    print("Aliases resolved  : %d (unresolved %d)"
          % (stats["aliasesResolved"], stats["aliasesUnresolved"]))
    print("Accessors         : %d" % stats["accessorsResolved"])
    print("Read sites        : %d across %d configs (%d configs capped at %d)"
          % (stats["readSites"], with_reads, stats["readSitesCapped"], MAX_READ_SITES))
    print("Co-configs        : %d configs have at least one" % with_co)
    print("Gating conditions : %d configs have at least one" % with_gates)
    print("Wrote %s" % json_path)
    print("Wrote %s" % summary_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
