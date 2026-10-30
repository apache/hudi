---
name: quiet-build
description: Use whenever composing a shell command that can print more than ~50 lines — any mvn build, a test run, pip/npm install, docker build, git log/diff, find, or ps — and when a previous command flooded the conversation and you need to re-run it quietly or mine its log instead.
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# quiet-build — low-token command output

Always shape verbose commands so only the signal reaches the conversation. Two levers — use both:

1. **Quiet flags** — cut noise at the source.
2. **Log + tail** — send full output to a file, show only the tail; grep the file on failure instead of re-running verbosely.

## Universal pattern

```bash
<cmd> > /tmp/out.log 2>&1; tail -n 40 /tmp/out.log
# on failure, mine the log — don't re-run louder:
grep -nE "ERROR|FAIL|Exception|error:" /tmp/out.log | head -30
```

## Maven

- **Always add `-B -ntp`** (batch mode + no transfer progress) — kills dependency-download spam and ANSI control characters, the two biggest offenders.
- **Compile/package only:** add `-q -DskipTests` — errors only; silence on success *is* success.
  ```bash
  mvn -B -ntp -q clean package -pl <module> -am -DskipTests
  ```
- **Running tests:** keep the surefire summary but bury per-test stdout in report files:
  ```bash
  mvn -B -ntp test -Dmaven.test.redirectTestOutputToFile=true > /tmp/mvn.log 2>&1; tail -n 60 /tmp/mvn.log
  ```
  On failure, read only `target/surefire-reports/<FailingTest>.txt` — never the whole log.
- **Scope down:** `-pl <module> -am` for one module, `-Dtest='Class#method'` for one test.
- If ANSI still leaks: `-Dstyle.color=never`.

## Other verbose commands

| Instead of | Use |
|---|---|
| `ps aux` | `pgrep -fl <pattern>` |
| `find /path` | `rg --files -g '<glob>'`, or `find` with `-maxdepth` |
| `git diff` | `git diff --stat` first, then targeted paths |
| `git log` / `git log --all` | `git log --oneline -n 20` |
| `git branch -a` | `git branch -a --list '<pattern>'` |
| `gh pr diff` | `gh pr diff --name-only` first, then per-file |
| `kubectl get ... -o yaml/json` | `-o name`, `-o jsonpath=...`, `--no-headers` |
| `docker build .` | `docker build -q .` (or `--progress=plain` + log/tail) |
| `npm install` | `npm install --no-fund --no-audit --loglevel=error` |
| `npm run build` | `npm run build --silent` |
| `pip install` | `pip install -q` |
| `pytest` | `pytest -q --tb=short -x` |
| `make` | `make -s` |
| `curl <url>` | `curl -sS -o /tmp/resp.json <url>`, then inspect the file |
