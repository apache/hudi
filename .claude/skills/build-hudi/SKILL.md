---
name: build-hudi
description: >-
  Use when compiling, packaging, running tests, or running checkstyle in apache/hudi (`master` is
  1.x / 1.3.0-SNAPSHOT, `branch-0.x` is 0.x / 0.16.0-SNAPSHOT), when deciding which JDK or Maven
  profiles a branch needs, or when a build fails on `invalid source release: 11`, `DecimalWrapper
  cannot find symbol`, `NoSuchMethodError: java.nio.ByteBuffer.flip()`, `AppClassLoader cannot be
  cast to URLClassLoader`, a class file v55 error, or a dependency-resolution failure after a
  branch switch.
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

# build-hudi — building & testing apache/hudi

`master` is the 1.x line (`1.3.0-SNAPSHOT`); `branch-0.x` is the 0.x line (`0.16.0-SNAPSHOT`).
Both build the same way. The build is finicky about JDK and Maven profiles — follow these steps
exactly. Verify a fact with `git show <ref>:pom.xml` before trusting a stale memory.

## The bases

| Base | pom `java.version` (compile target) | Local JDK | CI JDK | Engine profiles |
|---|---|---|---|---|
| `master` | `11` | **11** | Temurin 11 for the main build (other jobs run 17 or 25) | `-Dspark3.3` … `-Dspark3.5`; `-Dspark4.0`+ needs JDK 17. `-Dflink1.18` … `-Dflink2.2` (default `flink2.2`) |
| `branch-0.x` | `1.8` | **11** | Temurin 8 (some jobs 17) | `-Dspark2.4` … `-Dspark3.5`; `-Dflink1.14` … `-Dflink1.18` |

Defaults are Spark 3.5 / Scala 2.12. `-Dscala-2.13` applies to Spark bundles only, never Flink. On
`master` the metaserver is off by default: it needs `-Phudi-platform-service`, and
`-Pthrift-gen-source` to regenerate the Thrift sources.

**Repo layout / worktrees.** Keep the clone on `master`. Do actual work in sibling `-wt-N`
worktrees (`hudi-wt-1`, …), never nested under the repo; see `develop-changes` for the worktree
convention.

## Never run `mvn install` unless it is genuinely required

Default to `mvn package` (or the `test` phase scoped with `-pl <module> -am`); do not run
`mvn install`. `-am` builds the sibling snapshots in-reactor and never writes to `~/.m2`. Only
use `install` when a task truly needs a locally published artifact (rare); confirm first. Two
reasons this matters: `install` pollutes the shared `~/.m2` and can corrupt concurrent reactor
builds running in other worktrees; and `~/.m2` reaches tens of GB on a working laptop, so do
not grow it needlessly.

## 1. Pick the JDK

Use **Java 11** locally on both bases. Set `JAVA_HOME` explicitly — on macOS
`export JAVA_HOME=$(/usr/libexec/java_home -v 11)` — and don't mix JDKs across compile and run.
Use a real Temurin/Corretto JDK (native arm64 on Apple silicon), not a Homebrew `openjdk@*`.

`master` *requires* source level 11: its root pom sets `<java.version>11</java.version>`;
`hudi-io` overrides only `<target>8</target>` / `<release>8</release>` for old-Hive compatibility
and still inherits source 11. For `branch-0.x` the three numbers differ: the pom's compile target
is `1.8` (bytecode must stay Java-8 compatible), CI builds on Temurin 8, and the JDK you run
locally is 11. A construct that only exists in Java 9+ will compile locally there and then break
CI.

Symptoms that identify a JDK mismatch:

- `invalid source release: 11` — building `master` on JDK 8. It has no JDK-8 path. On an
  incremental build over up-to-date classes this stays hidden ("Nothing to compile"), so only a
  clean build shows it.
- `NoSuchMethodError: java.nio.ByteBuffer.flip()` — a local Java 8 run against a `~/.m2`
  `spark-core_2.12-3.5.x` jar that was installed from a JDK 17 build. Clear and reinstall that
  artifact.
- `AppClassLoader cannot be cast to URLClassLoader` — JDK 11 running the 0.x Hive 2.x test setup.
  Run that module's tests on JDK 8, which is what `branch-0.x` CI uses.
- class file v55 errors — Flink 2.x jars (or other JDK 11+ artifacts) pulled into a JDK 8 build.
- JDK 17+/23 break independently (`ByteBuffer.limit` bytecode, Mockito inline mocking).

## 2. Pass the engine profiles you mean

A profile id the base does not define is silently ignored, and you get the default instead. Check
with `grep -oE "<id>flink[^<]*</id>" pom.xml | sort -u` if unsure.

## 3. Full build (skip tests)

```bash
mvn -B -ntp -q clean package -DskipTests -Dspark3.5 -Dscala-2.12 -T0.5C
```

Use `-T0.5C` (half a thread per core), **not** `-T 1C` or higher — on a laptop, fuller
parallelism overwhelms the processor, especially with other builds or worktrees active. Bundles
land in `packaging/*/target/`.

## 4. `clean` after switching branches between 0.x and 1.x

Switching between `master` and `branch-0.x` leaves stale `target/generated-sources` Avro classes
from the other line that no longer compile (`DecimalWrapper` / `TimeMicrosWrapper`
`cannot find symbol`, `SpecificRecordBuilderBase` constructor mismatch). Always `mvn clean`
after switching lines. A killed build can also leave corrupt incremental state that only
`mvn clean` on the affected modules fixes.

## 5. Run tests

Tests are JUnit 5 and split by tag: untagged = unit, `@Tag("functional")` / `functional-b` /
`functional-c` = functional, `IT*.java` = integration (failsafe). `-Punit-tests` or
`-Pfunctional-tests` selects one set; without either, surefire runs everything in scope.

Sibling snapshots are **not** installed in `~/.m2`, so build the dependency modules in-reactor
with `-am` (a bare `mvn test -pl <module>` fails dependency resolution). Upstream `-am` modules
have no matching test, so add `-DfailIfNoTests=false`.

```bash
export SPARK_LOCAL_IP=127.0.0.1   # required for Spark tests on macOS
mvn -B -ntp clean test -pl <module> -am -Dspark3.5 -Dscala-2.12 -T0.5C \
  -Dtest='TestClass#method' -DfailIfNoTests=false -Dmaven.test.redirectTestOutputToFile=true
```

If the `-am` chain pulls in Scala modules, also add `-DskipUTs=true`: the scalatest-maven-plugin
ignores `-Dtest` and would otherwise run the whole Scala suite; surefire still runs the
`-Dtest` match. Do not combine it with `-Punit-tests`: that profile sets surefire's `skip` to
`${skipUTs}`, so the run silently executes nothing. (Not needed for pure-Java module chains, e.g. `hudi-client/hudi-client-common`.)

On failure read `<module>/target/surefire-reports/<Test>.txt`, not the build log.

## 5b. Manual benchmarks, gated real-data tests, and large-file heap

For performance work (see the `prove-performance` skill) the invocation has three extra traps:

- **A manual benchmark named `*Benchmark`** (not `Test*`/`*Test`) does not match the surefire
  include patterns, so it never runs in CI and a bare `-Dtest` finds nothing. Run it with
  `-Dtest=MyBenchmark -Dsurefire.failIfNoSpecifiedTests=false`. Name manual benchmarks this way on
  purpose so they stay out of CI. A gated real-data test (`Test*`) should `assumeTrue(System.getProperty(...) != null)`
  so it skips in CI and only runs when you pass `-D<prop>=<path>`.
- **Maven skips recompiling a new/edited test file** with "Skipping compilation because target
  directory newer than sources", so the STALE class runs and your edits look like no-ops. Always run
  benchmarks with `-Dmaven.compiler.useIncrementalCompilation=false` (and `touch` the source file).
  This is the first thing to suspect when a benchmark "doesn't change" after an edit.
- **Large real files need heap:** the fork already runs with `-Xmx4g` from the root pom's `argLine`
  property, and `-DargLine` on the command line never reaches it (see `port-fixes`). For more, raise
  that property locally and do not commit it. Prefer streaming the file from storage over reading it into a `byte[]`.

## 6. Checkstyle

```bash
mvn -B -ntp checkstyle:check -pl <module>
```

Java style is `style/checkstyle.xml`; Scala style is `style/scalastyle.xml`. Spotless is not
enabled, so match the surrounding formatting by hand.

## Pitfalls

- **Check for a concurrent `mvn` before launching** (`pgrep -fl '[m]vn'`). An engineer often has
  their own reactor build running; two reactors writing the same `target/` dirs produce
  `bad class file ... (No such file or directory)` on `hudi-io`/`hudi-common` mid-build. Wait
  until clear, or build in a separate git worktree.
- **Don't pipe `mvn` through `tail`/`head`** — the pipeline exit code comes from `tail` and
  masks `BUILD FAILURE`. Redirect to a file and echo the exit status, then grep the file; see
  `quiet-build` for the pattern.
- **Adding a dependency to a module** usually means updating the shade config of every bundle in
  `packaging/` that includes that module.

## Related

- `quiet-build` — the log-and-tail pattern and the quiet flags to pair with every command here.
- `develop-changes` — worktree conventions these commands assume.
- `port-fixes` — landing one fix on both bases.
- `create-pr` — PR title, template, and push conventions.
- `author-changes` / `review-pr` — what to do with the change once it builds.
