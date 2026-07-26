<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# hudi-trino

Hudi connector for Trino (RFC-105). Published as `org.apache.hudi:hudi-trino` -- a regular non-shaded JAR. The Trino-side `trino-hudi` plugin module depends on this artifact and Trino's URLClassLoader isolates the plugin's transitive deps from the rest of the server, so no shading is required.

## Build

Excluded from default builds. Activate the `hudi-trino` Maven profile:

```
# tests need Trino test-jars not on Maven Central (see Running tests); skip them in the default build
mvn -Phudi-trino -pl hudi-trino install -Dmaven.test.skip=true
```

Requires JDK 25 (enforced via `maven-enforcer-plugin`).

## Running tests

Tests depend on Trino test-jars (`trino-spi`, `trino-filesystem`, `trino-hive`, `trino-main` at the `tests` classifier). Trino does not publish three of those to Maven Central, so the test deps live behind the `hudi-trino-tests` profile, off by default.

To run the tests:

1. Build the matching Trino version locally so its `*-tests.jar` artifacts land in your `~/.m2` (see `trino.version` in the root pom for the version to build).
2. Activate both profiles:

```
mvn -Phudi-trino,hudi-trino-tests -pl hudi-trino test
```

CI follows the same two steps: `.github/workflows/hudi_trino_ci.yml` installs the test-jars from a source checkout of the pinned Trino tag, then runs with both profiles enabled.

## IDE setup

Only this module needs JDK 25. Leave the rest of Hudi on its native JDK (11 or 17) so you are not toggling the project default.

1. Activate the `hudi-trino` Maven profile so the IDE picks up the module. Tick `hudi-trino-tests` too if you want the test classpath to resolve.
   - IntelliJ: Maven tool window, Profiles, tick both `hudi-trino` and `hudi-trino-tests`.
2. Override the SDK for the `hudi-trino` module only, to Temurin 25 with Language level 25.
   - IntelliJ: `File > Project Structure > Modules > hudi-trino > Dependencies > Module SDK`.

The enforcer rule only runs during `mvn`, not during the IDE's incremental compile.
