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

We propose two different approaches for integrating column stats index with different query engines and discuss the pros and cons for the same below.
1. **Using domains** - Presto and Trino have the concept of column domains. Domain is actually the set of possible values that need to be returned for a particular column. Domains get created at the time of creating splits for processing. Domains basically contain a map of column to possible values where the possible values are populated after doing the necessary pre work of combining all the different filter predicates supplied as part of the query. [This draft PR](https://github.com/apache/hudi/pull/6087) shows the use of these domains for integrating data skipping index with presto engine. 
This basically involves exposing a new api in HoodieTableMetadata.java as below - 

```java
FileStatus[] getFilesToQueryUsingCSI(List<String> columns, ColumnDomain<ColumnHandle> columnDomain) throws IOException;
```

ColumnHandle object only defines a column in the hudi table and comprises the name and type attributes. The possible values of TYPE includes Boolean, Long, Double, Integer, Biginteger and varchar.
ColumnDomain contains a map which contains the possible values for every column getting filtered. This is how it looks like - 

```java
public class ColumnDomain<T> {

    //This contains the range of values per column, which needs to be checked for overlap against the file stats for that particular column
    private final Option<Map<T, Domain>> domains;
}
```

Domain consists of a NavigableMap<Marker, Range>. Range is defined using Markers. Marker can take care of below cases: 

1. `>` (greater than bound, e.g col1 > 34)
2. `>=` (greater than or equal to bound, e.g col1 >= 34)
3. `<` (lower than bound, e.g col1 < 34)
4. `<=` (lower than or equal to bound, e.g col1 <= 34)
5. `=` (exact bound, e.g col1 == 34)

For cases such as IN operator, We can have a list of Ranges defined for that column. Similarly cases for NOT IN operator can also be handled.

Marker class looks like below: 

```java
public class Marker implements Comparable<Marker> {

    public enum Bound {
        BELOW,
        EXACTLY,
        ABOVE
    }

    private final Type type;
    private final Option<Object> value;
    private final Bound bound;
}
```

Any possible Range can be defined with the combination of 2 markers, low and high.

```java
class Range {
    private final Marker low;
    private final Marker high;
    
    //utility methods to check if two ranges overlap each other
}
```

Let us try to define the bounds given above one by one in the format Range{lower_marker, higher_marker}.
1. col1 > 34 

    Range{Marker(Integer, Option.of(34), ABOVE), Marker(Integer, Option.empty(), BELOW)}

2. col1 >= 34
   
    Range{Marker(Integer, Option.of(34), EXACT), Marker(Integer, Option.empty(), BELOW)}

3. col1 < 34
   
    Range{Marker(Integer, Option.empty(), ABOVE), Marker(Integer, Option.of(34), BELOW)}

4. col1 <= 34
   
    Range{Marker(Integer, Option.empty(), ABOVE), Marker(Integer, Option.of(34), EXACT)}

5. col1 = 34
   
    Range{Marker(Integer, Option.of(34), EXACT), Marker(Integer, Option.of(34), EXACT)}


The process of file pruning can be depicted on a high level as below:

The flow needs to make use of below API introduced as part of this RFC:
```java
FileStatus[] getFilesToQueryUsingCSI(List<String> columns, ColumnDomain<ColumnHandle> columnDomain) throws IOException;
```

Query engines need to prepare the list of columns and the columnDomain objects to call the above API. This API performs the below tasks - 
> Create encoded ColumnID objects to be able to search Metadata table using getRecordsByKeyPrefixes() method. 
> For every column C in the list of columns, do the following:
- get the actual Domain object from columnDomain for C, say the range is R1.
- prepare a range object R2 using the min and max values in every file for C
- compare to see if R1 and R2 overlap each other using utility methods in Range class.
- If yes, add this file to the list of filtered files per column, else reject it.

> Finally get the intersection of all column wise filtered files. This is the list of pruned files to be scanned further.


This makes the integration with presto and trino a fairly easy task. However, some work needs to be done to get the column domains from Hive as explained [here](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance#RFC27Dataskippingindextoimprovequeryperformance-Hive). Once we have the query predicates in Hive, an adapter needs to be written for converting them to column domains. 

2. **Using generic HudiExpression** - This approach follows the general pattern of creating a tree like structure for making of any query predicates. More details about this approach can be found [here](https://cwiki.apache.org/confluence/display/HUDI/RFC-27+Data+skipping+index+to+improve+query+performance#RFC27Dataskippingindextoimprovequeryperformance-HowtoapplyquerypredicatesinHudi?).
One of the main benefits of using this approach is that it is very generic and solves the long term purpose of implementing filter pushdowns using various kind of filters. However, this involves more work in terms of creating these expressions and implementing the corresponding support for the different operators involved therein. 

## Rollout/Adoption Plan

This will be updated once we decide on one of the approaches proposed above. 

## Test Plan

This will be updated once we decide on one of the approaches proposed above. 