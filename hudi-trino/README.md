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

## Bootstrap Trino (do this first)

On master no `io.trino` artifact resolves from Maven Central: master tracks `trinodb/trino` master at the commit pinned by `trino.sha` in the root pom, and Trino publishes no SNAPSHOT artifacts. Pin ownership: the nightly `Hudi Trino SPI Compatibility` job verifies trino HEAD and pushes a `bot/trino-pin` branch (also refreshing `trino.e2e.version`); a committer opens and merges its PR, so the pin advances roughly nightly-to-weekly. One time per pin advance, clone `trinodb/trino` (or reuse a checkout) and build it under JDK 25:

```
scripts/trino/bootstrap_trino.sh /path/to/trino
```

Takes roughly 10-30 minutes and installs everything the connector needs, including the four test-jars and the `trino-root` pom.

On release branches `trino.version` is a released number, so compile deps resolve from Central and bootstrap is only needed for running tests.

## Build

Excluded from default builds. Activate the `hudi-trino` Maven profile (after bootstrap):

```
# tests need Trino test-jars not on Maven Central (see Running tests); skip them in the default build
mvn -Phudi-trino -pl hudi-trino install -Dmaven.test.skip=true
```

Requires JDK 25 (enforced via `maven-enforcer-plugin`).

## Running tests

Tests depend on Trino test-jars (`trino-spi`, `trino-filesystem`, `trino-hive`, `trino-main` at the `tests` classifier). Trino publishes none of those, so the test deps live behind the `hudi-trino-tests` profile, off by default.

After bootstrap, activate both profiles:

```
mvn -Phudi-trino,hudi-trino-tests -pl hudi-trino test
```

CI follows the same two steps: `.github/workflows/hudi_trino_ci.yml` runs `bootstrap_trino.sh` against the pinned commit (cached per `trino.sha`), then runs with both profiles enabled.

The tests resolve dependency versions from Hudi's root pom, while the shipped plugin bundles Trino's. The nightly `.github/workflows/hudi_trino_dependency_drift.yml` compares the two classpaths with `scripts/trino/check_dependency_drift.py` and files or updates an issue when versions differ.

## End-to-end tests (docker)

The testcontainers E2E suite (`hudi-integ-test`, classes `ITTestTrino*` under
`org.apache.hudi.integ2.testcontainers.trino`) runs Trino queries against a real
HDFS + Hive metastore + Spark stack. The Trino container image bakes in a plugin
directory assembled by the in-repo shim at `docker/trino/shim/` -- a standalone Maven
project mirroring the upstream `trinodb/trino` `plugin/trino-hudi` shim planned by
RFC-105 (not yet released upstream). CI runs the same flow via
`.github/workflows/hudi_trino_e2e.yml`.

CI builds the Trino server image from the pinned `trino.sha`
(`docker/trino/build_trino_server_image.sh`), so the plugin and the server always come from
the same commit.

Local flow (after bootstrap):

```
# 1. JDK 17: full reactor incl. the integ-test bundles the containers mount
mvn clean install -T 2 -Dscala-2.13 -Dscala.binary.version=2.13 -Dspark4.0 -Dflink1.20 \
    -Pintegration-tests -DskipTests=true -Ddocker.compose.skip=true

# 2. JDK 25: the connector
mvn -Phudi-trino -pl hudi-trino install -Dmaven.test.skip=true

# 3. JDK 25: assemble the plugin dir (package, NOT install -- installing would
#    shadow the real io.trino:trino-hudi release coordinates in the local m2).
#    dep.hudi.version comes from the reactor pom: the shim sits outside the
#    reactor, so cut_release_branch.sh cannot bump its literal default.
#    The unzip lives here, not in step 4: the fast-iteration loop below repeats
#    steps 2-3 only, and `clean` wipes the previously exploded dir.
HUDI_VERSION=$(mvn -q -ntp help:evaluate -Dexpression=project.version -DforceStdout)
TRINO_VERSION=$(sed -n 's|.*<trino.version>\(.*\)</trino.version>.*|\1|p' pom.xml)
mvn -f docker/trino/shim/pom.xml clean package -DskipTests -Ddep.hudi.version="$HUDI_VERSION"
unzip -o -q "docker/trino/shim/target/trino-hudi-$TRINO_VERSION.zip" -d docker/trino/shim/target  # trino-maven-plugin 24 emits only the zip

# 3a. Optional, JDK 25: build the server image from a trinodb/trino checkout at trino.sha
#     (the same one bootstrap used). Builds the whole trino repo, so it takes a while.
docker/trino/build_trino_server_image.sh /path/to/trino
TRINO_SHA=$(sed -n 's|.*<trino.sha>\(.*\)</trino.sha>.*|\1|p' pom.xml)

# 4. Build the Trino image (locally tagged; never published) on the server from 3a.
#    Without --base-image it falls back to the released trinodb/trino:<trino.e2e.version>
#    (or --trino-version), which only boots when the pin's SPI matches that release.
docker/trino/build_image.sh --plugin-dir "docker/trino/shim/target/trino-hudi-$TRINO_VERSION" \
    --base-image "hudi-trino-server:$TRINO_SHA"

# 5. JDK 17: run the suite (only the spark402 compose pair has the trino service)
mvn verify -pl hudi-integ-test -Dscala-2.13 -Dscala.binary.version=2.13 -Dspark4.0 \
    -Pintegration-tests -DskipITs=false -Ddocker.compose.skip=true \
    -Dit.test='ITTestTrino*' -Dcompose.profiles=trino \
    -Dspark.docker.compose.prefix=docker-compose_hadoop340_hive2310_spark402
```

Fast iteration loop: after changing connector code, redo steps 2-3, then add
`-Dtrino.plugin.dir=$PWD/docker/trino/shim/target/trino-hudi-$TRINO_VERSION` to step 5. The
container's overlay entrypoint swaps the freshly built plugin dir in at start, so the
image rebuild (step 4) is skipped.

## IDE setup

Only this module needs JDK 25. Leave the rest of Hudi on its native JDK (11 or 17) so you are not toggling the project default.

1. Activate the `hudi-trino` Maven profile so the IDE picks up the module. Tick `hudi-trino-tests` too if you want the test classpath to resolve.
   - IntelliJ: Maven tool window, Profiles, tick both `hudi-trino` and `hudi-trino-tests`.
2. Override the SDK for the `hudi-trino` module only, to Temurin 25 with Language level 25.
   - IntelliJ: `File > Project Structure > Modules > hudi-trino > Dependencies > Module SDK`.

The enforcer rule only runs during `mvn`, not during the IDE's incremental compile.
