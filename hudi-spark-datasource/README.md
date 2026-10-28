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

# `hudi-spark-datasource` module

This module contains the Spark integration for Hudi, providing a DataSource API for reading and writing Hudi tables using Spark SQL and DataFrames.

## Overview

The `hudi-spark-datasource` aggregates multiple sub-modules that together provide comprehensive Spark support for Hudi. 
The modules are organized in a layered architecture to maximize code reuse across different Spark versions while maintaining version-specific optimizations.

## Module Descriptions

| Module | Description |
|--------|-------------|
| `hudi-spark-common` | Core Spark integration code shared across all Spark versions. Contains DataSource V1/V2 implementations, file indexing, SQL writers, and incremental read support. |
| `hudi-spark3-common` | Code shared across Spark 3.x versions. Contains Spark 3 adapter interface, DML commands, and partition mapping. |
| `hudi-spark4-common` | Code shared across Spark 4.x versions. Contains Spark 4 adapter interface and 4.x-specific implementations. |
| `hudi-spark3.3.x` | Spark 3.3.x-specific adapter implementation with version-specific SQL parser and file readers. |
| `hudi-spark3.4.x` | Spark 3.4.x-specific adapter implementation. |
| `hudi-spark3.5.x` | Spark 3.5.x-specific adapter implementation (default). |
| `hudi-spark4.0.x` | Spark 4.0.x-specific adapter implementation. |
| `hudi-spark4.1.x` | Spark 4.1.x-specific adapter implementation. |
| `hudi-spark4.2.x` | Spark 4.2.x-specific adapter implementation. |
| `hudi-spark` | Main Spark datasource module containing Spark Session extensions, stored procedures, SQL parser, and logical plans. |

## Spark Version Support

| Spark Version | Module | Scala Version | Java Version | Build Profile |
|---------------|--------|---------------|--------------|---------------|
| 3.3.x | `hudi-spark3.3.x` | 2.12 | 11+ | `-Dspark3.3` |
| 3.4.x | `hudi-spark3.4.x` | 2.12 | 11+ | `-Dspark3.4` |
| 3.5.x (default) | `hudi-spark3.5.x` | 2.12, 2.13 | 11+ | `-Dspark3.5` |
| 4.0.x | `hudi-spark4.0.x` | 2.13 | 17+ | `-Dspark4.0` |
| 4.1.x | `hudi-spark4.1.x` | 2.13 | 17+ | `-Dspark4.1` |
| 4.2.x | `hudi-spark4.2.x` | 2.13 | 17+ | `-Dspark4.2` |

## Parquet Compression Compatibility

Hudi defaults `hoodie.parquet.compression.codec` to ZSTD on Spark 3.5 and newer and to GZIP on
Spark 3.3 and 3.4. The non-vectorized Hudi file-group reader in Spark 3.3 and 3.4 uses parquet-java
1.12.x, which is affected by [PARQUET-2160](https://issues.apache.org/jira/browse/PARQUET-2160)
and can leak off-heap memory while reading ZSTD files. Upgrade to Spark 3.5 or newer before using
ZSTD. The GZIP write default on older Spark versions does not eliminate the risk of reading ZSTD
files produced by another engine. Explicitly setting `hoodie.parquet.compression.codec` overrides
the version-specific default.

## Streaming session configuration compatibility

Starting with this change, Hudi Structured Streaming writes honor the active session's
`spark.hoodie.*` settings, just like DataFrame batch writes. For example,
`SET spark.hoodie.datasource.write.operation=insert` changes the operation of subsequently
started streaming sinks to `insert`. Previously the streaming sink ignored these session settings.
Audit ambient `spark.hoodie.*` configuration when upgrading an existing `writeStream` application.
To retain its previous behavior, unset those settings before starting the query or supply explicit
writer options, such as `.option("hoodie.datasource.write.operation", "upsert")`.

The sink captures settings once at creation, strips the `spark.` prefix, and uses the resulting
options for multi-writer validation, checkpoint deduplication and every micro-batch write.
Later `SET` statements do not change an already running sink. Explicit writer/storage options
override session settings; session settings override defaults. Explicit options using either
`hoodie.*` or `spark.hoodie.*` are supported; when both forms occur in the explicit options,
the `hoodie.*` form wins. Bare `hoodie.*` session keys are not forwarded.

Table identity must agree with the persisted Hudi table, even if an explicit option overrides
a session value. Conflicting record keys, ordering fields, partition fields, key generators or
merge settings fail validation when a sink opens an existing table, including on checkpoint replay.
Settings required for multi-writer operation, including a distinct streaming checkpoint identifier,
must also be present in the effective options.

For Spark Declarative Pipelines (RFC-112), keep operation and writer tuning in flow-scoped
`SET spark.hoodie.*` statements, not shared table properties. SDP captures the active settings
at each flow definition. A `SET` remains active for following definitions until changed.
New table properties should describe table identity. Legacy storage options still take precedence
over flow settings; the sink logs the conflicting keys and identifies the writer/storage option
as the winner. Remove the legacy behavior option from the table's storage options before expecting
a flow `SET` to control it. Configuration values are not included in this diagnostic.

The [flow settings integration scenario](../hudi-integ-test/spark-pipelines/flow_settings.py)
uses the real Spark 4.1.1 CLI/Connect harness from issue #20402 to check two SQL flow definitions
with different operations across repeated runs. Run it with the candidate Spark 4.1 bundle and
the harness directory (from the #20402 branch until it lands):

```bash
python hudi-integ-test/spark-pipelines/flow_settings.py \
  --harness-dir /path/to/harness/checkout/hudi-integ-test/spark-pipelines \
  --bundle /path/to/hudi-spark4.1-bundle_2.13.jar \
  --logs /tmp/hudi-flow-settings-logs
```

## Key Features

- **DataSource V1 Support**: Full integration with Spark's DataSource API
- **Spark SQL Integration**: Native SQL support for Hudi tables via Spark Session extensions
- **Stored Procedures**: Built-in procedures for table management and operations
- **Time Travel**: Query historical versions of tables
- **Incremental Queries**: Efficient change data capture reads
- **Index Support**: Bloom filters, column statistics, record-level index, and partition stats
- **Streaming Support**: Structured Streaming source for continuous data ingestion
- **CDC Support**: Change Data Capture for tracking row-level changes
