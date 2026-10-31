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

# Hudi-built Apache DataFusion Comet

This directory builds Comet from a pinned upstream commit with a small set of
Hudi-maintained source changes, producing the artifact
`hudi-spark-native-bundle` consumes when the native Hudi scan is enabled. The
changes add a native Hudi table scan to Comet: the Spark side hands each task
its file slices, and the native side reads them (Parquet decode plus
merge-on-read log merging) through [hudi-rs](https://github.com/apache/hudi-rs),
statically linked into `libcomet`.

## Layout

- `COMET_PIN` - the upstream Comet repository and commit, and the version the
  modified build installs as (upstream version plus a `-hudi-<n>` suffix, so the
  unmodified Central artifact can never satisfy the dependency by accident).
  The hudi-rs revision is pinned in `overlay/native/Cargo.toml` and locked in
  `overlay/native/Cargo.lock`.
- `overlay/` - full copies of every Comet file the integration changes or adds,
  mirroring Comet's tree. Applied by copying over a clean checkout of the
  pinned commit.
- `OVERLAY_MANIFEST` - for each overlaid file, the upstream blob SHA it was
  written against (or `new` for added files). `build_comet.sh` verifies the
  manifest before overlaying, so bumping `COMET_COMMIT` fails loudly on any
  file where upstream moved instead of silently discarding the upstream change.
- `build_comet.sh` - clone at the pin, verify, overlay, build (cargo release
  plus Maven), and install to the local Maven repository.

## Building

```bash
export JAVA_HOME=<jdk-17>
./build_comet.sh
```

Requires git, JDK 17, and a Rust toolchain (rustup picks the version from
Comet's `rust-toolchain.toml`). The checkout and build happen under
`target/comet-build` by default (`COMET_BUILD_DIR` overrides; point it at a
cached location in CI). `COMET_SPARK_PROFILE` selects the Comet Spark profile
(default `spark-3.5`); the default `release` make target compiles for the build
machine's CPU, so portable artifacts should use Comet's per-platform release
targets via `COMET_MAKE_TARGET`.

The bundle then builds against the result with:

```bash
mvn clean package -DskipTests -Dspark3.5 \
  -Dcomet.hudi.build -Ddatafusion.comet.version=1.1.0-hudi-1 \
  -pl packaging/hudi-spark-native-bundle -am
```

`-Dcomet.hudi.build` adds the `hudi-spark-comet` glue module (the Comet scan
provider that extracts Hudi file slices at plan time) to the reactor and to the
bundle. At runtime the native scan is off until
`spark.comet.scan.hudi.enabled=true` is set; without it the bundle behaves
exactly like one built against the published Comet.

## Changing the integration

Edit a checkout of Comet at the pinned commit (or let `build_comet.sh` create
one), then copy the changed files back into `overlay/` and refresh
`OVERLAY_MANIFEST`:

```bash
git -C <checkout> rev-parse <commit>:<path>   # blob SHA for a modified file
```

Bumping `COMET_COMMIT` or the hudi-rs revision requires re-running the native
bundle validation with `NATIVE_HUDI_SCAN=1` (see
`packaging/bundle-validation/spark_native/`).
