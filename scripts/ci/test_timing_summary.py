#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Summarise test timings from surefire and scalatest XML reports.

Scans every target/surefire-reports/TEST-*.xml under the given root (both surefire and the
scalatest-maven-plugin write that layout) and prints, as Markdown, the slowest test classes and
the slowest individual tests. CI appends the output to the job's step summary so the long tail
of a job is visible on the run page without opening logs; run it locally after `mvn test` to
see the same for a module.

Usage: test_timing_summary.py [--root DIR] [--classes N] [--tests N] [--title TEXT]
"""
import argparse
import collections
import os
import sys
import xml.etree.ElementTree as ET


def scan(root):
    classes = collections.defaultdict(lambda: [0.0, 0, 0])  # time, tests, failed
    tests = []
    files = 0
    for dirpath, dirnames, filenames in os.walk(root):
        # Reports live at <module>/target/surefire-reports. Prune everything else so the walk
        # stays cheap on a built checkout, where target/ holds thousands of class directories.
        base = os.path.basename(dirpath)
        if base == "target":
            dirnames[:] = [d for d in dirnames if d == "surefire-reports"]
        elif base != "surefire-reports":
            dirnames[:] = [d for d in dirnames if d not in (".git", "node_modules", "src")]
        if base != "surefire-reports":
            continue
        for name in filenames:
            if not (name.startswith("TEST-") and name.endswith(".xml")):
                continue
            try:
                suite = ET.parse(os.path.join(dirpath, name)).getroot()
            except ET.ParseError:
                continue
            files += 1
            for case in suite.iter("testcase"):
                cls = case.get("classname") or suite.get("name") or "?"
                if cls.startswith("org.scalatest.tools."):
                    continue
                t = float(case.get("time") or 0)
                failed = any(child.tag in ("failure", "error") for child in case)
                entry = classes[cls]
                entry[0] += t
                entry[1] += 1
                entry[2] += failed
                tests.append((t, cls, case.get("name") or "?", failed))
    return files, classes, tests


def short(cls):
    return cls.rsplit(".", 1)[-1]


def fmt(seconds):
    return f"{seconds / 60:.1f} min" if seconds >= 120 else f"{seconds:.1f} s"


def render(title, files, classes, tests, n_classes, n_tests):
    total = sum(v[0] for v in classes.values())
    count = sum(v[1] for v in classes.values())
    out = [f"### {title}", "",
           f"{count} tests in {len(classes)} classes from {files} report files, "
           f"{fmt(total)} of test time.", ""]
    if not classes:
        return "\n".join(out)
    out += [f"Slowest {min(n_classes, len(classes))} classes:", "",
            "| class | time | tests | share |", "|---|---|---|---|"]
    for cls, (t, n, failed) in sorted(classes.items(), key=lambda kv: -kv[1][0])[:n_classes]:
        flag = f" ({failed} failed)" if failed else ""
        out.append(f"| {short(cls)}{flag} | {fmt(t)} | {n} | {t / total * 100:.0f}% |")
    out += ["", f"Slowest {min(n_tests, len(tests))} tests:", "",
            "| test | time |", "|---|---|"]
    for t, cls, name, failed in sorted(tests, key=lambda x: -x[0])[:n_tests]:
        flag = " (failed)" if failed else ""
        out.append(f"| {short(cls)}#{name}{flag} | {fmt(t)} |")
    return "\n".join(out)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    p.add_argument("--root", default=".")
    p.add_argument("--classes", type=int, default=15)
    p.add_argument("--tests", type=int, default=15)
    p.add_argument("--title", default="Test timing")
    args = p.parse_args()
    files, classes, tests = scan(args.root)
    text = render(args.title, files, classes, tests, args.classes, args.tests)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a") as fh:
            fh.write(text + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
