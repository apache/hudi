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

| Status         | Meaning                                                                                 |
|----------------|-----------------------------------------------------------------------------------------|
| `UNDER REVIEW` | RFC has been proposed and community is actively debating the design/proposal.           |
| `IN PROGRESS`  | The initial phase of implementation is underway.                                        |
| `ONGOING`      | Some or most work has landed; community continues to improve or build follow on phases. |
| `ABANDONED`    | The proposal was not implemented, due to various reasons.                               |
| `COMPLETED`    | All work is deemed complete.                                                            |

The list of all RFCs can be found here.

> Older RFC content is still [here](https://cwiki.apache.org/confluence/display/HUDI/RFC+Process).

| RFC Number | Title                                                                                                                                                                                                                | Status         |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------|
| 1          | [CSV Source Support for Delta Streamer](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+01+%3A+CSV+Source+Support+for+Delta+Streamer)                                                                         | `COMPLETED`    |
| 2          | [ORC Storage in Hudi](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708439)                                                                                                                    | `COMPLETED`    |
| 3          | [Timeline Service with Incremental File System View Syncing](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=113708965)                                                                             | `COMPLETED`    |
| 4          | [Faster Hive incremental pull queries](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=115513622)                                                                                                   | `COMPLETED`    |
| 5          | [HUI (Hudi WebUI)](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=130027233)                                                                                                                       | `ABANDONED`    |
| 6          | [Add indexing support to the log file](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+06+%3A+Add+indexing+support+to+the+log+file)                                                                           | `ABANDONED`    |
| 7          | [Point in time Time-Travel queries on Hudi table](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+07+%3A+Point+in+time+Time-Travel+queries+on+Hudi+table)                                                     | `COMPLETED`    |
| 8          | [Metadata based Record Index](./rfc-8/rfc-8.md)                                                                                                                                                                      | `COMPLETED`    |
| 9          | [Hudi Dataset Snapshot Exporter](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+09+%3A+Hudi+Dataset+Snapshot+Exporter)                                                                                       | `COMPLETED`    |
| 10         | [Restructuring and auto-generation of docs](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+10+%3A+Restructuring+and+auto-generation+of+docs)                                                                 | `COMPLETED`    |
| 11         | [Refactor of the configuration framework of hudi project](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+11+%3A+Refactor+of+the+configuration+framework+of+hudi+project)                                     | `ABANDONED`    |
| 12         | [Efficient Migration of Large Parquet Tables to Apache Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+12+%3A+Efficient+Migration+of+Large+Parquet+Tables+to+Apache+Hudi)                               | `COMPLETED`    |
| 13         | [Integrate Hudi with Flink](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=141724520)                                                                                                              | `COMPLETED`    |
| 14         | [JDBC incremental puller](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+14+%3A+JDBC+incremental+puller)                                                                                                     | `COMPLETED`    |
| 15         | [HUDI File Listing Improvements](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+15%3A+HUDI+File+Listing+Improvements)                                                                                        | `COMPLETED`    |
| 16         | [Abstraction for HoodieInputFormat and RecordReader](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+16+Abstraction+for+HoodieInputFormat+and+RecordReader)                                                   | `COMPLETED`    |
| 17         | [Abstract common meta sync module support multiple meta service](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+17+Abstract+common+meta+sync+module+support+multiple+meta+service)                           | `COMPLETED`    |
| 18         | [Insert Overwrite API](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+18+Insert+Overwrite+API)                                                                                                               | `COMPLETED`    |
| 19         | [Clustering data for freshness and query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+19+Clustering+data+for+freshness+and+query+performance)                                                 | `COMPLETED`    |
| 20         | [handle failed records](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+20+%3A+handle+failed+records)                                                                                                         | `ONGOING`      |
| 21         | [Allow HoodieRecordKey to be Virtual](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+21+%3A+Allow+HoodieRecordKey+to+be+Virtual)                                                                             | `COMPLETED`    |
| 22         | [Snapshot Isolation using Optimistic Concurrency Control for multi-writers](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+22+%3A+Snapshot+Isolation+using+Optimistic+Concurrency+Control+for+multi-writers) | `COMPLETED`    |
| 23         | [Hudi Observability metrics collection](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+23+%3A+Hudi+Observability+metrics+collection)                                                                         | `ABANDONED`    | 
| 24         | [Hoodie Flink Writer Proposal](https://cwiki.apache.org/confluence/display/HUDI/RFC-24%3A+Hoodie+Flink+Writer+Proposal)                                                                                              | `COMPLETED`    | 
| 25         | [Spark SQL Extension For Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+25%3A+Spark+SQL+Extension+For+Hudi)                                                                                            | `COMPLETED`    | 
| 26         | [Optimization For Hudi Table Query](https://cwiki.apache.org/confluence/display/HUDI/RFC-26+Optimization+For+Hudi+Table+Query)                                                                                       | `COMPLETED`    | 
| 27         | [Data skipping index to improve query performance](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance)                                                         | `COMPLETED`    | 
| 28         | [Support Z-order curve](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=181307144)                                                                                                                  | `COMPLETED`    |
| 29         | [Hash Index](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+29%3A+Hash+Index)                                                                                                                                | `COMPLETED`    | 
| 30         | [Batch operation](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+30%3A+Batch+operation)                                                                                                                      | `ABANDONED`    | 
| 31         | [Hive integration Improvement](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+31%3A+Hive+integration+Improvment)                                                                                             | `ONGOING`      | 
| 32         | [Kafka Connect Sink for Hudi](https://cwiki.apache.org/confluence/display/HUDI/RFC-32+Kafka+Connect+Sink+for+Hudi)                                                                                                   | `ONGOING`      | 
| 33         | [Hudi supports more comprehensive Schema Evolution](https://cwiki.apache.org/confluence/display/HUDI/RFC+-+33++Hudi+supports+more+comprehensive+Schema+Evolution)                                                    | `COMPLETED`    | 
| 34         | [Hudi BigQuery Integration](./rfc-34/rfc-34.md)                                                                                                                                                                      | `COMPLETED`    | 
| 35         | [Make Flink MOR table writing streaming friendly](https://cwiki.apache.org/confluence/display/HUDI/RFC-35%3A+Make+Flink+MOR+table+writing+streaming+friendly)                                                        | `COMPLETED`    | 
| 36         | [HUDI Metastore Server](https://cwiki.apache.org/confluence/display/HUDI/%5BWIP%5D+RFC-36%3A+HUDI+Metastore+Server)                                                                                                  | `ONGOING`      | 
| 37         | [Hudi Metadata based Bloom Index](rfc-37/rfc-37.md)                                                                                                                                                                  | `COMPLETED`    | 
| 38         | [Spark Datasource V2 Integration](./rfc-38/rfc-38.md)                                                                                                                                                                | `COMPLETED`    | 
| 39         | [Incremental source for Debezium](./rfc-39/rfc-39.md)                                                                                                                                                                | `COMPLETED`    | 
| 40         | [Connector for Trino](./rfc-40/rfc-40.md)                                                                                                                                                                            | `COMPLETED`    | 
| 41         | [Snowflake Integration](./rfc-41/rfc-41.md), supported via [Apache XTable (Incubating)](https://xtable.apache.org/)                                                                                                  | `ABANDONED`    | 
| 42         | [Consistent Hashing Index](./rfc-42/rfc-42.md)                                                                                                                                                                       | `ONGOING`      | 
| 43         | [Table Management Service](./rfc-43/rfc-43.md)                                                                                                                                                                       | `ONGOING`      | 
| 44         | [Hudi Connector for Presto](./rfc-44/rfc-44.md)                                                                                                                                                                      | `COMPLETED`    | 
| 45         | [Asynchronous Metadata Indexing](./rfc-45/rfc-45.md)                                                                                                                                                                 | `COMPLETED`    | 
| 46         | [Optimizing Record Payload Handling](./rfc-46/rfc-46.md)                                                                                                                                                             | `COMPLETED`    | 
| 47         | [Add Call Produce Command for Spark SQL](./rfc-47/rfc-47.md)                                                                                                                                                         | `COMPLETED`    | 
| 48         | [LogCompaction for MOR tables](./rfc-48/rfc-48.md)                                                                                                                                                                   | `COMPLETED`    | 
| 49         | [Support sync with DataHub](./rfc-49/rfc-49.md)                                                                                                                                                                      | `COMPLETED`    |
| 50         | [Improve Timeline Server](./rfc-50/rfc-50.md)                                                                                                                                                                        | `IN PROGRESS`  | 
| 51         | [Change Data Capture](./rfc-51/rfc-51.md)                                                                                                                                                                            | `ONGOING`      |
| 52         | [Introduce Secondary Index to Improve HUDI Query Performance](./rfc-52/rfc-52.md)                                                                                                                                    | `ABANDONED`    |
| 53         | [Use Lock-Free Message Queue Improving Hoodie Writing Efficiency](./rfc-53/rfc-53.md)                                                                                                                                | `COMPLETED`    | 
| 54         | [New Table APIs and Streamline Hudi Configs](./rfc-54/rfc-54.md)                                                                                                                                                     | `UNDER REVIEW` | 
| 55         | [Improve Hive/Meta sync class design and hierarchies](./rfc-55/rfc-55.md)                                                                                                                                            | `COMPLETED`    | 
| 56         | [Early Conflict Detection For Multi-Writer](./rfc-56/rfc-56.md)                                                                                                                                                      | `COMPLETED`    | 
| 57         | [DeltaStreamer Protobuf Support](./rfc-57/rfc-57.md)                                                                                                                                                                 | `COMPLETED`    | 
| 58         | [Integrate column stats index with all query engines](./rfc-58/rfc-58.md)                                                                                                                                            | `UNDER REVIEW` |
| 59         | [Multiple event_time Fields Latest Verification in a Single Table](./rfc-59/rfc-59.md)                                                                                                                               | `UNDER REVIEW` |
| 60         | [Federated Storage Layer](./rfc-60/rfc-60.md)                                                                                                                                                                        | `UNDER REVIEW` |
| 61         | [Snapshot view management](./rfc-61/rfc-61.md)                                                                                                                                                                       | `UNDER REVIEW` |
| 62         | [Diagnostic Reporter](./rfc-62/rfc-62.md)                                                                                                                                                                            | `UNDER REVIEW` |
| 63         | [Expression Indexes](./rfc-63/rfc-63.md)                                                                                                                                                                             | `ONGOING`      |
| 64         | [New Hudi Table Spec API for Query Integrations](./rfc-64/rfc-64.md)                                                                                                                                                 | `UNDER REVIEW` |
| 65         | [Partition TTL Management](./rfc-65/rfc-65.md)                                                                                                                                                                       | `UNDER REVIEW` |
| 66         | [Non Blocking Concurrency Control](./rfc-66/rfc-66.md)                                                                                                                                                               | `UNDER REVIEW` |
| 67         | [Hudi Bundle Standards](./rfc-67/rfc-67.md)                                                                                                                                                                          | `UNDER REVIEW` |
| 68         | [A More Effective HoodieMergeHandler for COW Table with Parquet](./rfc-68/rfc-68.md)                                                                                                                                 | `UNDER REVIEW` |
| 69         | [Hudi 1.x](./rfc-69/rfc-69.md)                                                                                                                                                                                       | `COMPLETED`    |
| 70         | [Hudi Reverse Streamer](./rfc/rfc-70/rfc-70.md)                                                                                                                                                                      | `UNDER REVIEW` |
| 71         | [Enhance OCC conflict detection](./rfc/rfc-71/rfc-71.md)                                                                                                                                                             | `UNDER REVIEW` |
| 72         | [Redesign Hudi-Spark Integration](./rfc/rfc-72/rfc-72.md)                                                                                                                                                            | `ONGOING`      |
| 73         | [Multi-Table Transactions](./rfc-73/rfc-73.md)                                                                                                                                                                       | `UNDER REVIEW` |
| 74         | [`HoodieStorage`: Hudi Storage Abstraction and APIs](./rfc-74/rfc-74.md)                                                                                                                                             | `ONGOING`      |
| 75         | [Hudi-Native HFile Reader and Writer](./rfc-75/rfc-75.md)                                                                                                                                                            | `IN PROGRESS`  |
| 76         | [Auto Record key generation](./rfc-76/rfc-76.md)                                                                                                                                                                     | `IN PROGRESS`  |
| 77         | [Secondary Index](./rfc-77/rfc-77.md)                                                                                                                                                                                | `ONGOING`      |
| 78         | [1.0 Migration](./rfc-78/rfc-78.md)                                                                                                                                                                                  | `IN PROGRESS`  |
| 79         | [Robust handling of spark task retries and failures](./rfc-79/rfc-79.md)                                                                                                                                             | `IN PROGRESS`  |
| 80         | [Column Families](./rfc-80/rfc-80.md)                                                                                                                                                                                | `UNDER REVIEW` |
| 81         | [Log Compaction with Merge Sort](./rfc-81/rfc-81.md)                                                                                                                                                                 | `UNDER REVIEW` |
| 82         | [Concurrent schema evolution detection](./rfc-82/rfc-82.md)                                                                                                                                                          | `UNDER REVIEW` |
| 83         | [Incremental Table Service](./rfc-83/rfc-83.md)                                                                                                                                                                      | `COMPLETED`    |
| 84         | [Optimized SerDe of `DataStream` in Flink operators](./rfc-84/rfc-84.md)                                                                                                                                             | `COMPLETED`    |
| 85         | [Hudi Issue and Sprint Management in Jira](./rfc-85/rfc-85.md)                                                                                                                                                       | `UNDER REVIEW` |
| 86         | [DataFrame Implementation of HUDI write path](./rfc-86/rfc-86.md)                                                                                                                                                    | `UNDER REVIEW` |
| 87         | [Avro elimination for Flink writer](./rfc-87/rfc-87.md)                                                                                                                                                              | `UNDER REVIEW` |
| 88         | [New Schema/DataType/Expression Abstractions](./rfc-88/rfc-88.md)                                                                                                                                                    | `UNDER REVIEW` |
| 89         | [Dynamic Partition Level Bucket Index](./rfc-89/rfc-89.md)                                                                                                                                                           | `UNDER REVIEW` |
| 90         | Add support for cancellable clustering table service plans                                                                                                                                                           | `UNDER REVIEW` |
| 91         | Storage-based lock provider using conditional writes                                                                                                                                                                 | `UNDER REVIEW` |
| 92         | Support Bitmap Index                                                                                                                                                                                                 | `UNDER REVIEW` |
| 93         | [Pluggable Table Formats in Hudi](./rfc-93/rfc-93.md)                                                                                                                                                                | `IN PROGRESS`  |
| 94         | Hudi Timeline User Interface (UI)                                                                                                                                                                                    | `UNDER REVIEW` |
| 95         | Hudi Flink Source                                                                                                                                                                                                    | `UNDER REVIEW` |
| 96         | Introduce Unified Bucket Index                                                                                                                                                                                       | `UNDER REVIEW` |
