#!/usr/bin/env python3
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

"""Report version drift between hudi-trino's classpath and the Trino plugin's.

hudi-trino's unit tests resolve dependency versions from Hudi's root pom, but
the plugin that ships is assembled under trino-root and bundles Trino's
versions. This script compares two `mvn dependency:list -DoutputFile=...`
outputs and prints a Markdown table of the libraries whose versions differ.

Only dependencies present on both classpaths are compared: a library on one
side alone cannot run with a different version at runtime. org.apache.hudi and
io.trino artifacts are skipped because both sides take them from the same
source of truth. Artifacts are keyed by groupId:artifactId, plus the classifier
when there is one, so an artifact and its tests/shaded sibling stay separate
rows.

Exit codes: 0 no drift, 1 drift found, 2 usage or parse error.
"""

import argparse
import re
import sys

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
SKIPPED_GROUPS = ("org.apache.hudi", "io.trino")
RESOLVED_HEADER = "The following files have been resolved:"
# What dependency:list writes under the header for a scope with no dependencies.
EMPTY_MARKER = "none"


class DependencyListError(Exception):
    """A dependency:list file did not parse as a dependency listing."""


def parse_dependency_list(path):
    """Returns {groupId:artifactId[:classifier]: set(versions)} from a dependency:list file.

    Parsing is strict: once the resolved-files header is seen, every non-blank line
    must be a dependency coordinate. A format change that silently dropped most
    lines would otherwise be reported as "0 version mismatch(es)", i.e. as no drift.
    """
    deps = {}
    started = False
    with open(path, encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            line = ANSI_ESCAPE.sub("", raw).strip()
            if not line:
                continue
            if line == RESOLVED_HEADER:
                # The reference listing concatenates one file per scope, so the
                # header can show up more than once.
                started = True
                continue
            if not started or line == EMPTY_MARKER:
                # Whatever Maven printed ahead of the first header, and empty scopes.
                continue
            # Drop the JPMS " -- module ..." suffix and markers such as " (optional)".
            coordinate = line.split(" -- ")[0].split()[0]
            fields = coordinate.split(":")
            # groupId:artifactId:type:version:scope or
            # groupId:artifactId:type:classifier:version:scope
            if len(fields) not in (5, 6) or not all(fields):
                raise DependencyListError(
                    f"{path} line {number} is not a dependency coordinate: {line}")
            classifier = fields[3] if len(fields) == 6 else ""
            key = f"{fields[0]}:{fields[1]}"
            if classifier:
                key = f"{key}:{classifier}"
            deps.setdefault(key, set()).add(fields[-2])
    return deps


def format_versions(versions):
    return ", ".join(sorted(versions))


def compare(ours, reference):
    """Returns (shared key count, sorted list of (key, our versions, reference versions))."""
    shared = sorted(
        key for key in ours.keys() & reference.keys()
        if not key.startswith(tuple(group + ":" for group in SKIPPED_GROUPS)))
    mismatches = [(key, ours[key], reference[key]) for key in shared
                  if ours[key] != reference[key]]
    return len(shared), mismatches


def render_markdown(shared_count, mismatches, ours_label, reference_label):
    lines = []
    if mismatches:
        lines.append(f"| Dependency | {ours_label} | {reference_label} |")
        lines.append("| --- | --- | --- |")
        for key, ours_versions, reference_versions in mismatches:
            lines.append(f"| `{key}` | {format_versions(ours_versions)} "
                         f"| {format_versions(reference_versions)} |")
        lines.append("")
    lines.append(f"{len(mismatches)} version mismatch(es) across {shared_count} "
                 f"shared dependencies ({ours_label} vs {reference_label}).")
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--ours", required=True, help="dependency:list output for hudi-trino")
    parser.add_argument("--reference", required=True, help="dependency:list output for the plugin")
    parser.add_argument("--ours-label", default="hudi-trino")
    parser.add_argument("--reference-label", default="plugin")
    parser.add_argument("--markdown", help="also write the report to this path")
    args = parser.parse_args(argv)

    parsed = []
    for label, path in ((args.ours_label, args.ours), (args.reference_label, args.reference)):
        try:
            deps = parse_dependency_list(path)
        except OSError as error:
            print(f"ERROR: cannot read {path}: {error}", file=sys.stderr)
            return 2
        except DependencyListError as error:
            # A partial parse must fail loudly, never read as "no drift".
            print(f"ERROR: {label} listing: {error}", file=sys.stderr)
            return 2
        if not deps:
            # An empty list means the Maven step produced nothing; never report "no drift".
            print(f"ERROR: no dependencies parsed from {path} ({label})", file=sys.stderr)
            return 2
        parsed.append(deps)

    shared_count, mismatches = compare(*parsed)
    report = render_markdown(shared_count, mismatches, args.ours_label, args.reference_label)
    sys.stdout.write(report)
    if args.markdown:
        with open(args.markdown, "w", encoding="utf-8") as handle:
            handle.write(report)
    return 1 if mismatches else 0


if __name__ == "__main__":
    sys.exit(main())
