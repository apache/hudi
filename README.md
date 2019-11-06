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

# Hudi
Apache Hudi (Incubating) (pronounced Hoodie) stands for `Hadoop Upsert Delete and Incremental`. 
Hudi manages the storage of large analytical datasets on DFS (Cloud stores, HDFS or any Hadoop FileSystem compatible storage).

### Features
* Upsert support with fast, pluggable indexing
* Atomically publish data with rollback support
* Snapshot isolation between writer & queries 
* Savepoints for data recovery
* Manages file sizes, layout using statistics
* Async compaction of row & columnar data
* Timeline metadata to track lineage
 
Hudi provides the ability to query via three types of views:
 * **Read Optimized View** - Provides excellent snapshot query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/))
 * **Incremental View** - Provides a change stream with records inserted or updated after a point in time.
 * **Real-time View** - Provides snapshot queries on real-time data, using a combination of columnar & row-based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))

Learn more about Hudi at [https://hudi.apache.org](https://hudi.apache.org)

### Building Apache Hudi from source {#building-hudi}

Hudi requires Java 8 to be installed on a *nix system. Check out [code](https://github.com/apache/incubator-hudi) and 
normally build the maven project, from command line:

```
# Checkout code and build
git clone https://github.com/apache/incubator-hudi.git && cd incubator-hudi
mvn clean package -DskipTests -DskipITs
```

### Quickstart

Try [https://hudi.apache.org/quickstart.html](https://hudi.apache.org/quickstart.html) to quickly explore Hudi's capabilities using spark-shell. 
