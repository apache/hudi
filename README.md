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
Apache Hudi (incubating) (pronounced Hoodie) stands for `Hadoop Upserts anD Incrementals`. Hudi manages the storage of large analytical datasets on DFS (Cloud stores, HDFS or any Hadoop FileSystem compatible storage) and provides the ability to query them  via three types of views

 * **Read Optimized View** - Provides excellent query performance via purely columnar storage (e.g. [Parquet](https://parquet.apache.org/))
 * **Incremental View** - Provides a change stream with records inserted or updated after a point in time.
 * **Real-time View** - Provides queries on real-time data, using a combination of columnar & row-based storage (e.g Parquet + [Avro](http://avro.apache.org/docs/current/mr.html))

For more, head over [here](https://hudi.apache.org)
