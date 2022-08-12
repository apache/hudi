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

# RFCs

The RFC process is documented on our [site](https://hudi.apache.org/contribute/rfc-process). Please familiarize yourself
with it, before working a new RFC.

Status can be one of these values.

| Status | Meaning |
| -------|-------------------------------------------------------|
| `UNDER REVIEW` |  RFC has been proposed and community is actively debating the design/proposal.        |
| `IN PROGRESS` |  The initial phase of implementation is underway.        |
| `ONGOING` |  Some or most work has landed; community continues to improve or build follow on phases.         |
| `ABANDONED` | The proposal was not implemented, due to various reasons.         |
| `COMPLETED` |  All work is deemed complete.        |

The list of all RFCs can be found here.

> Older RFC content is still [here](https://cwiki.apache.org/confluence/display/HUDI/RFC+Process).

| RFC Number | Title                                                                                                                                                                                                                | Status |
| --|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| --- |
| 1 | [CSV Source Support for Delta Streamer](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+01+%3A+CSV+Source+Support+for+Delta+Streamer)                                                                         | `COMPLETED` |
| 2 | [ORC Storage in Hudi](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708439)                                                                                                                    | `ONGOING` |
| 3 | [Timeline Service with Incremental File System View Syncing](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708965)                                                                             | `COMPLETED` |
| 4 | [Faster Hive incremental pull queries](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=115513622)                                                                                                   | `COMPLETED` |
| 5 | [HUI (Hudi WebUI)](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=130027233)                                                                                                                       | `ABANDONED` |
| 6 | [Add indexing support to the log file](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+06+%3A+Add+indexing+support+to+the+log+file)                                                                           | `ABANDONED` |
| 7 | [Point in time Time-Travel queries on Hudi table](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+07+%3A+Point+in+time+Time-Travel+queries+on+Hudi+table)                                                     | `COMPLETED` |
| 8 | [Record level indexing mechanisms for Hudi datasets](https://cwiki.apache.org/confluence/display/HUDI/RFC-08++Record+level+indexing+mechanisms+for+Hudi+datasets)                                                    | `ONGOING` |
| 9 | [Hudi Dataset Snapshot Exporter](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+09+%3A+Hudi+Dataset+Snapshot+Exporter)                                                                                       | `COMPLETED` |
| 10 | [Restructuring and auto-generation of docs](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+10+%3A+Restructuring+and+auto-generation+of+docs)                                                                 | `COMPLETED` |
| 11 | [Refactor of the configuration framework of hudi project](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+11+%3A+Refactor+of+the+configuration+framework+of+hudi+project)                                     | `ABANDONED` |
| 12 | [Efficient Migration of Large Parquet Tables to Apache Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+12+%3A+Efficient+Migration+of+Large+Parquet+Tables+to+Apache+Hudi)                               | `COMPLETED` |
| 13 | [Integrate Hudi with Flink](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=141724520)                                                                                                              | `COMPLETED` |
| 14 | [JDBC incremental puller](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+14+%3A+JDBC+incremental+puller)                                                                                                     | `COMPLETED` |
| 15 | [HUDI File Listing Improvements](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+Improvements)                                                                                        | `COMPLETED` |
| 16 | [Abstraction for HoodieInputFormat and RecordReader](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+16+Abstraction+for+HoodieInputFormat+and+RecordReader)                                                   | `COMPLETED` |
| 17 | [Abstract common meta sync module support multiple meta service](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+17+Abstract+common+meta+sync+module+support+multiple+meta+service)                           | `COMPLETED` |
| 18 | [Insert Overwrite API](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+18+Insert+Overwrite+API)                                                                                                               | `COMPLETED` |
| 19 | [Clustering data for freshness and query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance)                                                 | `COMPLETED` |
| 20 | [handle failed records](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+20+%3A+handle+failed+records)                                                                                                         | `IN PROGRESS` |
| 21 | [Allow HoodieRecordKey to be Virtual](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+21+%3A+Allow+HoodieRecordKey+to+be+Virtual)                                                                             | `COMPLETED` |
| 22 | [Snapshot Isolation using Optimistic Concurrency Control for multi-writers](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers) | `COMPLETED` |
| 23 | [Hudi Observability metrics collection](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+23+%3A+Hudi+Observability+metrics+collection)                                                                         | `ABANDONED` | 
| 24 | [Hoodie Flink Writer Proposal](https://cwiki.apache.org/confluence/display/HUDI/RFC-24%3A+Hoodie+Flink+Writer+Proposal)                                                                                              | `COMPLETED` | 
| 25 | [Spark SQL Extension For Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)                                                                                            | `COMPLETED` | 
| 26 | [Optimization For Hudi Table Query](https://cwiki.apache.org/confluence/display/HUDI/RFC-26+Optimization+For+Hudi+Table+Query)                                                                                       | `ONGOING` | 
| 27 | [Data skipping index to improve query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance)                                                         | `ONGOING` | 
| 28 | [Support Z-order curve](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181307144)                                                                                                                  | `COMPLETED` |
| 29 | [Hash Index](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+29%3A+Hash+Index)                                                                                                                                | `ONGOING` | 
| 30 | [Batch operation](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+30%3A+Batch+operation)                                                                                                                      | `UNDER REVIEW` | 
| 31 | [Hive integration Improvement](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+31%3A+Hive+integration+Improvment)                                                                                             | `ONGOING` | 
| 32 | [Kafka Connect Sink for Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC-32+Kafka+Connect+Sink+for+Hudi)                                                                                                   | `ONGOING` | 
| 33 | [Hudi supports more comprehensive Schema Evolution](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+33++Hudi+supports+more+comprehensive+Schema+Evolution)                                                    | `ONGOING` | 
| 34 | [Hudi BigQuery Integration](./rfc-34/rfc-34.md)                                                                                                                                                                      | `COMPLETED` | 
| 35 | [Make Flink MOR table writing streaming friendly](https://cwiki.apache.org/confluence/display/HUDI/RFC-35%3A+Make+Flink+MOR+table+writing+streaming+friendly)                                                        | `UNDER REVIEW` | 
| 36 | [HUDI Metastore Server](https://cwiki.apache.org/confluence/display/HUDI/%5BWIP%5D+RFC-36%3A+HUDI+Metastore+Server)                                                                                                  | `IN PROGRESS` | 
| 37 | [Hudi Metadata based Bloom Index](rfc-37/rfc-37.md)                                                                                                                                                                  | `ONGOING` | 
| 38 | [Spark Datasource V2 Integration](./rfc-38/rfc-38.md)                                                                                                                                                                | `IN PROGRESS` | 
| 39 | [Incremental source for Debezium](./rfc-39/rfc-39.md)                                                                                                                                                                | `ONGOING` | 
| 40 | [Hudi Connector for Trino](./rfc-40/rfc-40.md)                                                                                                                                                                       | `IN PROGRESS` | 
| 41 | [Hudi Snowflake Integration]                                                                                                                                                                                         | `UNDER REVIEW`| 
| 42 | [Consistent Hashing Index](./rfc-42/rfc-42.md)                                                                                                                                                                       | `IN PROGRESS` | 
| 43 | [Compaction / Clustering Service](./rfc-43/rfc-43.md)                                                                                                                                                                | `UNDER REVIEW` | 
| 44 | [Hudi Connector for Presto](./rfc-44/rfc-44.md)                                                                                                                                                                      | `ONGOING` | 
| 45 | [Asynchronous Metadata Indexing](./rfc-45/rfc-45.md)                                                                                                                                                                 | `ONGOING` | 
| 46 | [Optimizing Record Payload Handling](./rfc-46/rfc-46.md)                                                                                                                                                             | `IN PROGRESS` | 
| 47 | [Add Call Produce Command for Spark SQL](./rfc-47/rfc-47.md)                                                                                                                                                         | `ONGOING` | 
| 48 | [LogCompaction for MOR tables](./rfc-48/rfc-48.md)                                                                                                                                                                   | `UNDER REVIEW` | 
| 49 | [Support sync with DataHub](./rfc-49/rfc-49.md)                                                                                                                                                                      | `ONGOING` |
| 50 | [Improve Timeline Server](./rfc-50/rfc-50.md)                                                                                                                                                                        | `IN PROGRESS` | 
| 51 | [Change Data Capture](./rfc-51/rfc-51.md)                                                                                                                                                                            | `UNDER REVIEW` |
| 52 | [Introduce Secondary Index to Improve HUDI Query Performance](./rfc-52/rfc-52.md)                                                                                                                                    | `UNDER REVIEW` |
| 53 | [Use Lock-Free Message Queue Improving Hoodie Writing Efficiency](./rfc-53/rfc-53.md)                                                                                                                                | `IN PROGRESS` | 
| 54 | [New Table APIs and Streamline Hudi Configs](./rfc-54/rfc-54.md)                                                                                                                                                     | `UNDER REVIEW` | 
| 55 | [Improve Hive/Meta sync class design and hierachies](./rfc-55/rfc-55.md)                                                                                                                                             | `ONGOING` | 
| 56 | [Early Conflict Detection For Multi-Writer](./rfc-56/rfc-56.md)                                                                                                                                                      | `UNDER REVIEW` | 
| 57 | [DeltaStreamer Protobuf Support](./rfc-57/rfc-57.md)                                                                                                                                                                 | `UNDER REVIEW` | 
| 58 | [Integrate column stats index with all query engines](./rfc-58/rfc-58.md)                                                                                                                                            | `UNDER REVIEW` |
| 59 | [Multiple event_time Fields Latest Verification in a Single Table](./rfc-59/rfc-59.md)                                                                                                                               | `UNDER REVIEW` |
