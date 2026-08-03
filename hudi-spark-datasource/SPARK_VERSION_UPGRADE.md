<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
-->

# Adding Support for a New Spark Version

This guide describes how to add support for a new Spark version to Hudi. It is written for Hudi
contributors, not for users upgrading the Spark version of an existing deployment.

The work is spread across the build, the datasource modules, CI and the release tooling. Most of it
is mechanical, but the parts outside `hudi-spark-datasource` are easy to miss and are what usually
turns up later as a red CI job or a missing bundle at release time.

## How Spark support is structured

Hudi isolates version-specific Spark code behind a `SparkAdapter`, so that shared code can be
written once:

```
hudi-spark-common      code shared by every Spark version
hudi-spark3-common     shared by Spark 3.x, holds BaseSpark3Adapter
hudi-spark4-common     shared by Spark 4.x, holds BaseSpark4Adapter
hudi-spark3.3.x        Spark 3.3-specific adapter and overrides
hudi-spark3.4.x
hudi-spark3.5.x        default profile
hudi-spark4.0.x
hudi-spark4.1.x
hudi-spark4.2.x
hudi-spark              session extensions, procedures, SQL parser, logical plans
```

One version module is active per build, selected by a Maven profile. `SparkAdapterSupport` picks the
matching adapter at runtime from the Spark version actually on the classpath, so a bundle built for
one version fails fast rather than misbehaving on another.

There is no per-version bundle module: `packaging/hudi-spark-bundle` derives its artifact id from
the `sparkbundle.version` property, so setting that property in the new profile is what produces
`hudi-spark<X.Y>-bundle_<scala>`.

## Checklist

Every file below was touched by both the Spark 4.1 (`20a01051ca6d`) and Spark 4.2 (`77f5851a5d53`)
additions, so treat it as the minimum set.

**Build**

- [ ] `pom.xml` — add the `sparkXY.version` property, a `spark<X.Y>` profile, and the modules it activates
- [ ] `hudi-spark-datasource/hudi-spark<X.Y>.x/pom.xml` — the new module

**Version module** (`hudi-spark-datasource/hudi-spark<X.Y>.x/`)

- [ ] `adapter/Spark<X>_<Y>Adapter.scala` extending `BaseSpark3Adapter` or `BaseSpark4Adapter`
- [ ] `HoodieSpark<XY>CatalystExpressionUtils`, `HoodieSpark<XY>CatalystPlanUtils`, `HoodieSpark<XY>SchemaUtils`
- [ ] Avro serializer and deserializer, plus the copies of Spark's own `AvroSerializer`/`AvroDeserializer`
- [ ] Parquet reader and legacy file format
- [ ] `parser/HoodieSpark<X>_<Y>ExtendedSqlAstBuilder.scala` and `...ExtendedSqlParser.scala`
- [ ] `antlr4/imports/SqlBase.g4` and `antlr4/.../HoodieSqlBase.g4`, copied from the matching Spark release
- [ ] Partition mapping and `HoodieInternalRow` implementations

**Wiring**

- [ ] `HoodieSparkUtils.scala` — `isSpark<X>_<Y>` and `gteqSpark<X>_<Y>`
- [ ] `SparkAdapterSupport.scala` — add the new version to the dispatch chain, newest first

**CI**

- [ ] `.github/workflows/bot.yml` — matrix entries for the java tests, scala tests, bundle validation and docker jobs
- [ ] `.asf.yaml` — the same jobs as required status checks, otherwise they do not block merges

**Packaging and release**

- [ ] `packaging/bundle-validation/base/build_<flink><hive><spark><scala>.sh` — base image for the new combination
- [ ] `packaging/bundle-validation/ci_run.sh` — component versions for the new `SPARK_RUNTIME`, and
      the bundle artifact names for the new profile
- [ ] `packaging/bundle-validation/run_docker_java17.sh`
- [ ] `scripts/release/deploy_staging_jars_java17.sh` and `scripts/release/validate_staged_bundles.sh`

**Docs**

- [ ] `README.md` — the build-profile table
- [ ] `hudi-spark-datasource/README.md` — the module table and supported versions

## Worked example: adding Spark 4.2

What follows is the shape of `77f5851a5d53 feat(spark): add Spark 4.2 support (#18621)`, which
touched 47 files. Snippets show the current state of those files rather than the exact original
commit, since Spark 4.2 has moved from a preview build to `4.2.0` since. Substitute your own
versions, and read the real files before copying: they drift.

### 1. Create the version module

Copy the closest existing module, which for 4.2 was `hudi-spark4.1.x`, and rename the package
objects. The naming convention is inconsistent by design, so follow it rather than tidying it:

| Kind | Convention | Example |
| --- | --- | --- |
| Adapter | `Spark<X>_<Y>Adapter` | `Spark4_2Adapter` |
| Catalyst utils | `HoodieSpark<XY>...` | `HoodieSpark42CatalystPlanUtils` |
| Avro serde | `HoodieSpark<X>_<Y>Avro...` | `HoodieSpark4_2AvroSerializer` |
| Parquet | `Spark<XY>...` | `Spark42ParquetReader` |
| Parser | `HoodieSpark<X>_<Y>Extended...` | `HoodieSpark4_2ExtendedSqlAstBuilder` |

Copy `SqlBase.g4` from the Spark release you are targeting, not from the previous Hudi module. The
grammar changes between Spark versions, and a stale copy produces parse failures that only appear
for specific SQL statements.

### 2. Add the Maven profile

In the root `pom.xml`, add the version property alongside the others and a profile that sets what
the build needs:

```xml
<spark42.version>4.2.0</spark42.version>
```

```xml
<profile>
  <id>spark4.2</id>
  <properties>
    <spark4.version>${spark42.version}</spark4.version>
    <spark.version>${spark4.version}</spark.version>
    <sparkbundle.version>4.2</sparkbundle.version>
    <scala.binary.version>2.13</scala.binary.version>
    <hudi.spark.module>hudi-spark4.2.x</hudi.spark.module>
    <hudi.spark.common.module>hudi-spark4-common</hudi.spark.common.module>
    ...
  </properties>
  <modules>
    <module>hudi-spark-datasource/hudi-spark4.2.x</module>
    <module>hudi-spark-datasource/hudi-spark4-common</module>
  </modules>
</profile>
```

The profile is also where transitive dependency versions get pinned to whatever the new Spark
release ships, so that Hudi modules using Parquet, Orc, Avro or Jackson directly stay on the same
versions as Spark and do not create classpath ambiguity. Spark 4.2 needed `parquet.version`,
`orc.spark.version`, `avro.version`, `antlr.version`, the `fasterxml.*` set, `log4j2.version`,
`slf4j.version`, `hadoop.version` and `kafka.version`.

It is also where genuinely awkward differences are handled. Spark 4.2 relocated `lz4-java` from
`org.lz4` to `at.yawk.lz4` while keeping the same `net.jpountz.lz4` package, so the profile
overrides `lz4.groupId` to take Spark's copy rather than end up with both on the classpath.

### 3. Wire up version detection

`HoodieSparkUtils` carries one predicate pair per version:

```scala
def isSpark4_2: Boolean = getSparkVersion.startsWith("4.2")
def gteqSpark4_2: Boolean = getSparkVersion >= "4.2"
```

`SparkAdapterSupport` selects the adapter from those, newest first, so the new branch goes at the
top of the chain:

```scala
val adapterClass = if (HoodieSparkUtils.isSpark4_2) {
  "org.apache.spark.sql.adapter.Spark4_2Adapter"
} else if (HoodieSparkUtils.isSpark4_1) {
  ...
```

Getting the order wrong is silent: a `gteq` branch placed above an `is` branch will swallow the
newer version and load the older adapter.

### 4. Add CI coverage

`bot.yml` needs the new profile in each relevant matrix. For Spark 4.2 that was five test jobs plus
bundle validation and docker:

```yaml
- scalaProfile: "scala-2.13"
  sparkProfile: "spark4.2"
  sparkModules: "hudi-spark-datasource/hudi-spark4.2.x"
```

Then add the same jobs to `.asf.yaml` under the required status checks. A job that runs but is not
listed there does not block a merge, so this step is what makes the coverage real:

```yaml
- test-spark-java17-java-tests-part1 (scala-2.13, spark4.2, hudi-spark-datasource/hudi-spark4.2.x)
- validate-bundle-spark4 (scala-2.13, flink1.20, 1.11.4, 1.13.1, spark4.2, spark4.2.0)
```

Spark 4.x requires Java 17, so the new entries belong in the `java17` job families.

### 5. Packaging and release

Bundle validation keys off a `SPARK_RUNTIME` string rather than the Maven profile, so
`packaging/bundle-validation/ci_run.sh` needs a branch giving the component versions for the new
combination:

```bash
elif [[ ${SPARK_RUNTIME} == 'spark4.2.0' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HADOOP_VERSION=3.4.3
  SPARK_VERSION=4.2.0
  IMAGE_TAG=flink1200hive313spark420scala213
  ...
```

The same file also maps the profile to the bundle artifact names:

```bash
elif [[ ${SPARK_PROFILE} == 'spark4.2' && ${SCALA_PROFILE} == 'scala-2.13' ]]; then
  HUDI_SPARK_BUNDLE_NAME=hudi-spark4.2-bundle_2.13
  ...
```

`validate.sh` usually needs nothing: it branches on `[[ "$SPARK_VERSION" == 4.* ]]` rather than on
individual versions, so a new 4.x release is picked up automatically. It only needs attention if the
new version needs handling the existing globs do not cover. Earlier versions were listed explicitly
there and had to be edited each time, which is worth knowing if you are reading an older commit as a
template.

The release scripts under `scripts/release/` need the profile added so the staged bundles are built
and checked.

### 6. Update the docs

The build-profile table in the root `README.md` and the module table in
`hudi-spark-datasource/README.md` both enumerate supported versions.

## Verifying the change

Build and test the new module on its own first:

```shell
mvn clean install -Dscala-2.13 -Dspark4.2 -DskipTests -pl hudi-spark-datasource/hudi-spark4.2.x -am
mvn test -Dscala-2.13 -Dspark4.2 -pl hudi-spark-datasource/hudi-spark4.2.x
```

Then confirm the versions you did not touch still build, since shared-code changes made for the new
version are the usual way older ones break:

```shell
mvn clean install -Dspark3.5 -DskipTests
mvn clean install -Dscala-2.13 -Dspark4.0 -DskipTests
```

Finally build the bundle the profile is meant to produce and check its name:

```shell
mvn clean package -Dscala-2.13 -Dspark4.2 -DskipTests -pl packaging/hudi-spark-bundle -am
# expect hudi-spark4.2-bundle_2.13-<version>.jar
```

## Things that are easy to get wrong

- **Adapter dispatch order.** Newest version first; an `is` check below a `gteq` check never runs.
- **A stale ANTLR grammar.** Copy `SqlBase.g4` from the target Spark release. A grammar from the
  previous version compiles cleanly and fails only on specific SQL.
- **`.asf.yaml` left behind.** The job runs, goes red, and merges anyway.
- **Older versions left unbuilt.** Shared code in `hudi-spark-common`, `hudi-spark3-common` and
  `hudi-spark4-common` is compiled against every version, so adding a method to a shared interface
  means implementing it in every existing version module. Both the 4.1 and 4.2 changes had to touch
  the older modules for exactly this reason.
- **Preview releases.** If you are adding support against a preview build, the version string ends
  up in the bundle-validation image tag and the release scripts as well as the pom. Spark 4.2 was
  added as `4.2.0-preview4` and every one of those places had to be revisited when `4.2.0` landed.
