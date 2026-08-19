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

# hudi-native-spark-bundle

Everything in `hudi-spark-bundle`, plus [Apache DataFusion Comet](https://datafusion.apache.org/comet/)
for native vectorized execution. Use it in place of `hudi-spark-bundle`, not alongside it.

## Supported Spark versions

Comet releases one artifact per Spark minor version, and does not cover everything Hudi builds
against. The bundle is produced only where Comet has a matching release:

| Spark | Scala      | Bundle                              |
|-------|------------|-------------------------------------|
| 3.4   | 2.12       | `hudi-native-spark3.4-bundle_2.12`  |
| 3.5   | 2.12, 2.13 | `hudi-native-spark3.5-bundle_<scala>` |
| 4.0   | 2.13       | `hudi-native-spark4.0-bundle_2.13`  |
| 4.1   | 2.13       | `hudi-native-spark4.1-bundle_2.13`  |

Building with `-Dspark3.3` or `-Dspark4.2` produces no bundle at all: Comet dropped Spark 3.3
after 0.7.0 and has no Spark 4.2 release. Those builds succeed and simply skip this module.

## Runtime requirements

**Java 17 or later.** Comet carries Java 17 bytecode, so loading the bundle on Java 11 fails with
`UnsupportedClassVersionError ... class file version 61.0`, including on Spark 3.4 and 3.5, which
Hudi otherwise supports on Java 11.

**Linux only.** Comet ships pre-built native libraries for `linux/aarch64` and `linux/amd64`, both
carried in every bundle. The released Comet artifact has no macOS or Windows library, so loading it
elsewhere fails with `Unsupported OS/arch`. On macOS, run inside a Linux container
(`--platform linux/arm64` on Apple Silicon runs natively, with no emulation) or build Comet from
source to get a `darwin` library.

## What Comet accelerates

Comet does not read Hudi tables natively. Its scan rule recognizes Spark's own
`ParquetFileFormat`, not Hudi's `HoodieFileGroupReaderBasedFileFormat`, so the scan itself always
stays on Spark.

Everything above the scan can still run natively. With `spark.comet.convert.parquet.enabled=true`
Comet bridges the scan's output into Arrow and takes over the rest of the plan. A join of two
copy-on-write tables:

```
CometHashAggregate [Final] ...
+- CometHashAggregate [Partial] ...
   +- CometProject ...
      +- CometSortMergeJoin [partitionpath], Inner
         :- CometSort ...
         :  +- CometExchange hashpartitioning(...), CometNativeShuffle
         :     +- CometSparkColumnarToColumnar
         :        +- FileScan HudiFileGroup [...] Format: HoodieFileGroupReaderBasedFileFormat
```

Without that config nothing is accelerated at all, not even the join. Note that
`spark.comet.sparkToColumnar.enabled` is not the switch that does this; only
`spark.comet.convert.parquet.enabled` bridges the Hudi scan.

How the bridge is done depends on the table, and the difference is not free:

| Table | Scan | Bridge |
|---|---|---|
| Copy-on-write | `Batched: true`, Spark's vectorized Parquet reader | `CometSparkColumnarToColumnar` |
| Merge-on-read | `Batched: false`, row by row | `CometSparkRowToColumnar` |

Merge-on-read reads row by row because file-group merging is row-level, so its rows are assembled
on heap and then converted to Arrow cell by cell. Benchmark before assuming a merge-on-read
workload comes out ahead.

A scan projecting no data columns (`ReadSchema: struct<>`, for example an aggregate over only the
partition column) is not bridged at all, and that branch stays on Spark.

Making the Hudi scan itself native is separate work and is not attempted here.

## Usage

Comet is not enabled by the bundle being on the classpath. These settings turn it on:

```
spark-shell --jars hudi-native-spark3.5-bundle_2.12-<version>.jar \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.comet.CometSparkSessionExtensions \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.enabled=true \
  --conf spark.comet.convert.parquet.enabled=true
```

`spark.comet.convert.parquet.enabled` is what lets Comet accelerate anything over a Hudi table;
see below.

Comet declines operators it cannot accelerate and hands them back to Spark, so correct query
results do not imply native execution. To check what it actually accelerated, look for `Comet`
operators in `df.queryExecution.executedPlan`, and set
`--conf spark.comet.explain.fallback.enabled=true` to log the reason for every operator it
declined.

## Shading

Comet is deliberately **not** relocated. Its native library is bound through JNI symbols named
`Java_org_apache_comet_Native_*` and the shared objects are resolved as classpath resources under
`org/apache/comet/<os>/<arch>/`, so relocating `org.apache.comet` builds cleanly and then fails on
the first native call. The same applies to `org.apache.arrow.c`, which Comet carries unshaded for
the Arrow C data interface JNI bindings.
