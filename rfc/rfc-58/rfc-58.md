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
# RFC-58: Integrate column stats index with all query engines



## Proposers

- @pratyakshsharma

## Approvers
- @bhavanisudha
- @danny0405
- @codope

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-4552

> Please keep the status updated in `rfc/README.md`.

## Abstract

Query engines like hive or presto typically scan a large amount of data for query planning and execution. Proper indexing can help reduce this scan to a great extent. Parquet files are the most commonly used file format for storing columnar data with various lakehouse techniques mainly because of their strong support with spark and
the kind of indexing that they employ at different levels. Parquet files maintain indexes at file level, row group level and page level. Till some time back, Hudi used to make use of these indexes for fast querying via the parquet reader libraries. The problem with this approach was every file object had to be opened once to read the index stored in parquet footer to be able to do file pruning. This could potentially become a bottleneck in case of a large number of
files. With the introduction of [multi-modal index](https://www.onehouse.ai/blog/introducing-multi-modal-index-for-the-lakehouse-in-apache-hudi) in Hudi, this problem has been solved to a great extent. Currently the data skipping support using this multi-modal index is available for spark and [flink](https://issues.apache.org/jira/browse/HUDI-4353) engines. We intend to extend this support for other query engines like presto, trino and hive in this RFC. 

## Background
[RFC-27](https://github.com/apache/hudi/blob/master/rfc/rfc-27/rfc-27.md) added a new partition corresponding to column_stats index in metadata table of Hudi. We plan to use the information stored in this partition for pruning the files. 

## Implementation
Describe the new thing you want to do in appropriate detail, how it fits into the project architecture.
Provide a detailed description of how you intend to implement this feature.This may be fairly extensive and have large subsections of its own.
Or it may be a few sentences. Use judgement based on the scope of the change.

We propose two different approaches for integrating column stats index with different query engines and discuss the pros and cons for the same below.
1. **Using domains** - Presto and Trino have the concept of column domains. Domain is actually the set of possible values that need to be returned for a particular column. Domains get created at the time of creating splits for processing. Domains basically contain a map of column to possible values where the possible values are populated after doing the necessary pre work of combining all the different filter predicates supplied as part of the query. [This draft PR](https://github.com/apache/hudi/pull/6087) shows the use of these domains for integrating data skipping index with presto engine. 
This basically involves exposing a new api in HoodieTableMetadata.java as below - 

```java
FileStatus[] getFilesToQueryUsingCSI(List<String> columns, ColumnDomain<ColumnHandle> columnDomain) throws IOException;
```

This makes the integration with presto and trino a fairly easy task. However some work needs to be done to get the column domains from Hive as explained [here](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance#RFC27Dataskippingindextoimprovequeryperformance-Hive). Once we have the query predicates in Hive, an adapter needs to be written for converting them to column domains. 

2. **Using generic HudiExpression** - This approach follows the general pattern of creating a tree like structure for making of any query predicates. More details about this approach can be found [here](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance#RFC27Dataskippingindextoimprovequeryperformance-HowtoapplyquerypredicatesinHudi?).
One of the main benefits of using this approach is that it is very generic and solves the long term purpose of implementing filter pushdowns using various kind of filters. However, this involves more work in terms of creating these expressions and implementing the corresponding support for the different operators involved therein. 

## Rollout/Adoption Plan

This will be updated once we decide on one of the approaches proposed above. 

## Test Plan

This will be updated once we decide on one of the approaches proposed above. 