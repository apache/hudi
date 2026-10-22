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

# hudi-spark-native-bundle

`hudi-spark-bundle` plus [Apache DataFusion Comet](https://datafusion.apache.org/comet/) for native
vectorized execution. Use it instead of `hudi-spark-bundle`, and remove any other Comet jar from the
classpath: Comet cannot be relocated, so two copies resolve class by class while `libcomet.so` is
picked up as a resource, and the mix ends in `UnsatisfiedLinkError` or `NoSuchMethodError`.

## Supported Spark versions

Comet publishes one artifact per Spark minor version, so the bundle exists only where Comet does:

| Spark | Scala      | Bundle                                |
|-------|------------|---------------------------------------|
| 3.4   | 2.12       | `hudi-spark3.4-native-bundle_2.12`    |
| 3.5   | 2.12, 2.13 | `hudi-spark3.5-native-bundle_<scala>` |
| 4.0   | 2.13       | `hudi-spark4.0-native-bundle_2.13`    |
| 4.1   | 2.13       | `hudi-spark4.1-native-bundle_2.13`    |

`-Dspark3.3` and `-Dspark4.2` build fine and produce no bundle (Comet dropped 3.3 after 0.7.0 and
supports 4.2 only experimentally, with no published artifact). Comet 1.1.0 deprecates Spark 3.4, so that bundle goes away once Comet drops it.

## Runtime requirements

- **Java 17 or later**, also on Spark 3.4 and 3.5: Comet ships Java 17 bytecode, and Java 11 fails
  with `UnsupportedClassVersionError ... class file version 61.0`.
- **Linux with glibc**, `amd64` or `aarch64`: those are the only `libcomet.so` builds Comet ships.
  A musl JVM (Alpine) fails to load it, and macOS or Windows fail with `Unsupported OS/arch`. On a Mac,
  run in a Linux container (`--platform linux/arm64` on Apple Silicon needs no emulation).

## Usage

The bundle on the classpath enables nothing by itself:

```
spark-shell --jars hudi-spark3.5-native-bundle_2.12-<version>.jar \
  --conf spark.plugins=org.apache.spark.CometPlugin \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension,org.apache.comet.CometSparkSessionExtensions \
  --conf spark.shuffle.manager=org.apache.spark.sql.comet.execution.shuffle.CometShuffleManager \
  --conf spark.memory.offHeap.enabled=true \
  --conf spark.memory.offHeap.size=1g \
  --conf spark.comet.enabled=true \
  --conf spark.comet.exec.enabled=true \
  --conf spark.comet.nativeLoadRequired=true \
  --conf spark.comet.convert.parquet.enabled=true
```

- Comet's plugin is not enabled without off-heap memory, so size `spark.memory.offHeap.size` for the job.
- `spark.comet.nativeLoadRequired=true` fails the session when `libcomet.so` cannot load, instead of
  silently running everything on Spark.
- `spark.comet.convert.parquet.enabled=true` is the switch that lets Comet accelerate anything over a
  Hudi table (see below); `spark.comet.sparkToColumnar.enabled` does not do it.

Comet hands back whatever it cannot accelerate, so correct results do not mean native execution.
Look for `Comet` operators in `df.queryExecution.executedPlan`, and set
`spark.comet.explain.fallback.enabled=true` to log why each declined operator fell back.

## What Comet accelerates

Not the Hudi scan: Comet's scan rule recognizes Spark's `ParquetFileFormat`, not Hudi's
`HoodieFileGroupReaderBasedFileFormat`, so the scan stays on Spark. With
`spark.comet.convert.parquet.enabled=true` Comet bridges the scan output into Arrow and runs the rest
of the plan natively, for example a join of two copy-on-write tables:

```
CometHashAggregate [Final]
+- CometHashAggregate [Partial]
   +- CometProject
      +- CometSortMergeJoin [partitionpath], Inner
         :- CometSort
         :  +- CometExchange hashpartitioning(...), CometNativeShuffle
         :     +- CometSparkColumnarToColumnar
         :        +- FileScan HudiFileGroup [...] Format: HoodieFileGroupReaderBasedFileFormat
```

The bridge differs by table type, and the difference is not free:

| Table         | Scan                                        | Bridge                         |
|---------------|---------------------------------------------|--------------------------------|
| Copy-on-write | `Batched: true`, vectorized Parquet reader  | `CometSparkColumnarToColumnar` |
| Merge-on-read | `Batched: false`, row by row (log merging)  | `CometSparkRowToColumnar`      |

Merge-on-read rows are assembled on heap and converted to Arrow cell by cell, so benchmark before
assuming such a workload comes out ahead. A scan that projects no data column (`ReadSchema: struct<>`)
is not bridged and stays on Spark.

## Shading

The bundle is the shaded `hudi-spark-bundle` jar plus the Comet jar, with no relocation of its own.
Hudi's relocated third-party classes (dropwizard metrics, jetty, commons-io, ...) arrive already
relocated, which keeps the bundle binary compatible with `hudi-utilities-slim-bundle`, and Comet keeps
the names Spark resolves it by.

Comet must not be relocated: `libcomet.so` binds through JNI symbols named
`Java_org_apache_comet_Native_*` and is loaded from the `org/apache/comet/<os>/<arch>/` resource path,
so a relocated `org.apache.comet` builds and then fails on the first native call. The same holds for
`org.apache.arrow.c` (the Arrow C data interface JNI bindings) and for `org.apache.spark.CometSource`,
whose `metricRegistry()` must keep returning the `com.codahale.metrics.MetricRegistry` that Spark's
`Source` declares.

Anything else shaded in next to Comet is therefore unrelocated too, and must not call a Hudi API
whose signature exposes a relocated type (`Metrics.getRegistry()`, for example): it compiles and fails
at runtime with `NoSuchMethodError`.
