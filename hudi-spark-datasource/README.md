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
| `hudi-spark` | Main Spark datasource module containing Spark Session extensions, stored procedures, SQL parser, and logical plans. |

## Spark Version Support

| Spark Version | Module | Scala Version | Java Version | Build Profile |
|---------------|--------|---------------|--------------|---------------|
| 3.3.x | `hudi-spark3.3.x` | 2.12 | 11+ | `-Dspark3.3` |
| 3.4.x | `hudi-spark3.4.x` | 2.12 | 11+ | `-Dspark3.4` |
| 3.5.x (default) | `hudi-spark3.5.x` | 2.12, 2.13 | 11+ | `-Dspark3.5` |
| 4.0.x | `hudi-spark4.0.x` | 2.13 | 17+ | `-Dspark4.0` |

## Key Features

- **DataSource V1 Support**: Full integration with Spark's DataSource API
- **Spark SQL Integration**: Native SQL support for Hudi tables via Spark Session extensions
- **Stored Procedures**: Built-in procedures for table management and operations
- **Time Travel**: Query historical versions of tables
- **Incremental Queries**: Efficient change data capture reads
- **Index Support**: Bloom filters, column statistics, record-level index, and partition stats
- **Streaming Support**: Structured Streaming source for continuous data ingestion
- **CDC Support**: Change Data Capture for tracking row-level changes
