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
# RFC-89: Partition Level Bucket Index

## Proposers
- @zhangyue19921010

## Approvers
- @danny0405
- @codope
- @xiarixiaoyao

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-8990

## Abstract

As we know, Hudi proposed and introduced Bucket Index in RFC-29. Bucket Index can well unify the indexes of Flink and
Spark, that is, Spark and Flink could upsert the same Hudi table using bucket index.

However, Bucket Index has a limit of fixed number of buckets. In order to solve this problem, RFC-42 proposed the ability
of consistent hashing achieving bucket resizing by splitting or merging several local buckets dynamically.

But from PRD experience, sometimes a Partition-Level Bucket Index and a offline way to do bucket rescale is good enough
without introducing additional efforts (multiple writes, clustering, automatic resizing,etc.). Because the more complex
the Architecture, the more error-prone it is and the greater operation and maintenance pressure.

In this regard, we could upgrade the traditional Bucket Index to implement a Partition-Level Bucket Index, so that users
can set a specific number of buckets for different partitions through a rule engine (such as regular expression matching).
On the other hand, for a certain existing partitions, an off-line command is provided to reorganize the data using insert
overwrite(need to stop the data writing of the current partition).

More importantly, the existing Bucket Index table can be upgraded to Partition-Level Bucket Index smoothly and seamlessly.

## Background
The following is the core read-write process of the Flink/Spark engine based on Simple Bucket Index
### Flink Write Using Simple Bucket Index
**Step 1**: re-partition input records based on `BucketIndexPartitioner`, BucketIndexPartitioner has **a fixed bucketNumber** for all partition path.
For each record key, compute a fixed data partition number and dispatch the record to its corresponding partition.

```java
/**
 * Bucket index input partitioner.
 * The fields to hash can be a subset of the primary key fields.
 *
 * @param <T> The type of obj to hash
 */
public class BucketIndexPartitioner<T extends HoodieKey> implements Partitioner<T> {

  private final int bucketNum;
  private final String indexKeyFields;

  private Functions.Function2<String, Integer, Integer> partitionIndexFunc;

  public BucketIndexPartitioner(int bucketNum, String indexKeyFields) {
    this.bucketNum = bucketNum;
    this.indexKeyFields = indexKeyFields;
  }

  @Override
  public int partition(HoodieKey key, int numPartitions) {
    if (this.partitionIndexFunc == null) {
      this.partitionIndexFunc = BucketIndexUtil.getPartitionIndexFunc(bucketNum, numPartitions);
    }
    int curBucket = BucketIdentifier.getBucketId(key.getRecordKey(), indexKeyFields, bucketNum);
    return this.partitionIndexFunc.apply(key.getPartitionPath(), curBucket);
  }
}
```
**Step 2**: Using `BucketStreamWriteFunction` upsert records into hoodie
- Bootstrap and cache `partition_bucket -> fileID` mapping from the existing hudi table
- Tagging: compute `bucketNum` and tag `fileID` based on record key and bucketNumber config through `BucketIdentifier`
- buffer and write records

### Flink Read Pruning Using Simple Bucket Index
**Step 1**: compute `dataBucket`
```java
  private int getDataBucket(List<ResolvedExpression> dataFilters) {
    if (!OptionsResolver.isBucketIndexType(conf) || dataFilters.isEmpty()) {
      return PrimaryKeyPruners.BUCKET_ID_NO_PRUNING;
    }
    Set<String> indexKeyFields = Arrays.stream(OptionsResolver.getIndexKeys(conf)).collect(Collectors.toSet());
    List<ResolvedExpression> indexKeyFilters = dataFilters.stream().filter(expr -> ExpressionUtils.isEqualsLitExpr(expr, indexKeyFields)).collect(Collectors.toList());
    if (!ExpressionUtils.isFilteringByAllFields(indexKeyFilters, indexKeyFields)) {
      return PrimaryKeyPruners.BUCKET_ID_NO_PRUNING;
    }
    return PrimaryKeyPruners.getBucketId(indexKeyFilters, conf);
  }
```
**Step 2**: Do partition pruning and get all files in given partitions
**Step 3**: do bucket pruning for all files from step2
```java
  /**
   * Returns all the file statuses under the table base path.
   */
  public List<StoragePathInfo> getFilesInPartitions() {
    ...
    // Partition pruning
    String[] partitions =
        getOrBuildPartitionPaths().stream().map(p -> fullPartitionPath(path, p)).toArray(String[]::new);
    if (partitions.length < 1) {
      return Collections.emptyList();
    }
    List<StoragePathInfo> allFiles = ...
    
    // bucket pruning
    if (this.dataBucket >= 0) {
      String bucketIdStr = BucketIdentifier.bucketIdStr(this.dataBucket);
      List<StoragePathInfo> filesAfterBucketPruning = allFiles.stream()
          .filter(fileInfo -> fileInfo.getPath().getName().contains(bucketIdStr))
          .collect(Collectors.toList());
      logPruningMsg(allFiles.size(), filesAfterBucketPruning.size(), "bucket pruning");
      allFiles = filesAfterBucketPruning;
    }
    ...
  }

```

### Spark Write/Read Using Simple Bucket Index
The read-write process of Spark based on Bucket Index is also similar.
- Use `HoodieSimpleBucketIndex` to tag location.
- Use `SparkBucketIndexPartitioner` to packs incoming records to be inserted into buckets (1 bucket = 1 RDD partition).
- Use `BucketIndexSupport` to Bucket Index pruning during reading.

## Design
### Workflow
The core process of writing to the Partition Level Bucket Index is shown in the figure below. First of all, the Bucket Identifier 
loads the partition bucket count information from the metadata as a cache. During the Partitioner and Writer stages, when 
using the Identifier for shuffling and tagging, it will firstly attempt to find the bucketNumber of the corresponding 
partition from the cache. If the cache was missed, it will calculate the corresponding value based on the user's expression, 
and then submit it to the metadata during the commit stage.
![workflow.jpg](workflow.jpg)
The design details of each part are as follows.

### Config
Add new Config `hoodie.bucket.index.partition.rule.type`

Add new config `hoodie.bucket.index.partition.expressions`

Re-use existed config `hoodie.bucket.index.num.buckets` as default partition bucket number

The above parameters are table-level parameters and need to be declared in the DDL. Users can't change this config through
runtime options like Flink /*+ OPTIONS(...) */

| Config                                 | type   | Description | Description                                                                                                                                                                                                                                                     |
|----------------------------------------|--------|-------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hoodie.bucket.index.partition.rule.type | string | regex       | Set rule parser for expressions.                                                                                                                                                                                                                                |
| hoodie.bucket.index.partition.expressions | string | null        | Users can use this parameter to specify expression and the corresponding bucket numbers (separated by commas).Multiple rules are separated by semicolons like `hoodie.bucket.index.partition.expressions=expression1,bucket-number1;expression2,bucket-number2` |
| hoodie.bucket.index.num.buckets        | int    | 4           | Hudi bucket number per partition.                                                                                                                                                                                                                               |
| hoodie.bucket.index.cache.maxSize             | int    | -1          | The maximum size of the partitionToBucketNumber cache, -1 for no limit                                                                                                                                                                                          |

Note: In the future, more parsers will be provided, such as range expression parsers and hybrid parsers.
Here, the parser is designed to be extensible, allowing users to customize parsers.

#### Demo
```sql
CREATE TABLE hudi_table(
  id BIGINT,
  name STRING,
  price DOUBLE
)
WITH (
'connector' = 'hudi',
'path' = 'file:///tmp/hudi_table',
'table.type' = 'MERGE_ON_READ',
'index.type' = 'BUCKET',
'hoodie.bucket.index.partition.expressions' = '\\d{4}-(06-(01|17|18)|11-(01|10|11)),256',
'hoodie.bucket.index.num.buckets' = '10'
);
```

For the dates of 06-01, 06-17, 06-18 in June and 01-11, 10-11, 11-11 in
November of each year (in the format of yyyy-MM-dd), the corresponding bucket number for the partition is 256

For common partitions use 10 as partition bucket number


#### Config Management

The expression config will be persisted as files named as `<instant>.hashing_config` for current table, it stores in
`.hoodie/.simple_hashing/config` directory and contains the following information in json format

```json
{
    "<instant1>": {
       "expression": "expression1,bucket-number1;expression2,bucket-number2",
       "rule": "rule-engine",
       "default": "default-bucket-number"
    },
    "<instant2>": {}
}
```
Expressions has newer instant has higher priority

For a specific expression like instant1 ==> Expression1 has a higher priority than expression2.

Here, the config is designed to support multiple versions. During the DDL phase, hoodie will create a `00000000000000000.hashing_config` for the first time. 
Subsequently, when modifying the expression through the provided `call command`(details refer to Tool ), 
a new version of the config will be generated. The new-version config contains all historical change records. During 
loading stage, the latest completed instant related config will be loaded based on the active timeline.

```text
.hoodie/.simple_hashing/config/00000000000000000.hashing_config
.hoodie/.simple_hashing/config/20250303095546020.hashing_config
```

Using Archive Service to clean up expired config versions, keeping up to 5 historical versions

### Hashing Metadata
Here, the Hashing Metadata is designed to support multiple versions, new versions contains all the info in old versions.

The hashing metadata will be persisted as files named as `<instant>.hashing_meta` for current table, it stores in
`.hoodie/.simple_hashing/meta` directory and contains the following information

```avro schema
{
   "namespace":"org.apache.hudi.avro.model",
   "type":"record",
   "doc": "hashing meta for current table using partition level simple bucket index",
   "name":"SimpleHashMeta",
   "fields":[
     {
       "name":"version",
       "type":["int", "null"],
       "default": 1
     },
     {
       "name": "partitionMeta",
       "type": [
         "null", {
           "type":"map",
           "values": {
             "type": "record",
             "name": "PartitionHashInfo",
             "fields": [
               {
                 "name": "bucketNumber",
                 "type": ["null", "int"],
                 "default": null
               },
               {
                 "name": "instant",
                 "type":["null","string"],
                 "default": null
               }
             ]
           }}],
       "default" : null
     }
   ]
}
```
We will write <instant>.hashing_meta during commit action (at pre-commit stage):
1. Get latest complete T1.hashing_meta
2. Merging T1.hashing_meta with new written partitions as T2.hashing_meta also do partition conflict detection
   1. For example, current write write partition1 with 20 buckets but there is already defined partition1 with 10 bucket numbers, which means there is 
another writer that uses the latest expression to pre-determine the bucket number of the current partition. In order to ensure data consistency, it is necessary to abort the current writer.
3. If conflict detection is passed AND **new partitions are added** then write T2.hashing_meta into .hoodie folder
   1. Here, a new hashing_meta will only be written when a new partition is added to avoid unnecessary I/O overhead.
4. Do commit action

Using Archive Service to clean up expired hashing meta, keeping up to 10 historical hashing meta

### SimpleBucketIdentifier
```java
public class SimpleBucketIdentifier extends BucketIdentifier {
  // use expressionEvaluator to compute bucket number for given partition path
  private BucketCalculator calculator;
  // get previous partition and bucket number from meta under .hoodie folder
  private Map<String, Integer> partition2BucketNumber;

  @Override
  public int getBucketId(String recordKey, String indexKeyFields, String partitionPath) {
    int numBuckets = getBucketNumber(partitionPath);
    return getBucketId(getHashKeys(recordKey, indexKeyFields), numBuckets);
  }
  ...
  
  public int getBucketNumber(String partitionPath) {
    return calculator.calculateBucketNumber(partitionPath);
  }
}
```
**WorkFlow for SimpleBucketIdentifier**
1. Get and Cache partition2BucketNumber 
2. For the partition of the given data, try to get its corresponding bucket number from the cache. If the cache missed, 
   1. Calculate the expression through the Bucket Calculator to obtain the corresponding Bucket Number.
   2. Put Calculated bucket number into cache

Using SimpleBucketIdentifier to get `BucketNumber` or `BucketID` based on given partitionPath and corresponding expression

For new Partitioner:
`SparkBucketIndexPartitioner`-Spark and `BucketIndexPartitioner`-Flink use this new SimpleBucketIdentifier to do data repartition

For Writer:
Writer like `BucketStreamWriteFunction` in Flink also use this new `SimpleBucketIdentifier` to compute bucketID

NOTE：We need to get hashing_meta instant and hashing_config `instant` at the beginning(JM or Driver) and pass it to each operator, 
ensure the consistency of loading and avoid unnecessary listing action.

#### Eviction Strategy For This Map Cache
If the user sets the parameter `hoodie.bucket.index.cache.maxSize`, we would use `org.apache.commons.collections.map.LRUMap` to cache partitionToBucketNumber
which could take good care of cache eviction.

By default use HashMap directly.

### BucketCalculator
The BucketCalculator calculates the bucket numbers for different partitions based on the different rules set by users.
Here, the types of supported Rules are extensible. In the first phase, regular expressions are mainly supported.

### Bucket Rescale Tool
We provide a Spark call command `run_bucket_rescale` to perform the bucket scale operation. Internally, it encapsulates 
an `insert overwrite` operation, which is a replace commit. This bucket rescale is divided into metadata modification and metadata + data modification.
Use `--type` 

1. `--type meta-only`, Only modify the metadata, that is, update the `.hoodie/.simple_hashing/config/` to generate 
a new version. At this time, an empty replace commit (insert-overwrite) will be generated. Modifications at the metadata
level do not affect the normal writing of jobs, but the latest configuration can only be loaded by restarting the jobs.
2. `--type data`, It will identify and rewrite the data of the corresponding partitions according to the new rules. 
Similarly, a new version of the config will also be generated. It should be noted that all write operations need to be stopped for this operation.
3. `--type hashing_config` show the latest hashing_config for users
4. `--type hashing_meta` show the latest hashing_meta for users
5. `--type rollback, --instant instant-time` rollback instant which is a bucket rescale action, if the scaling size does not meet the expectations, a rollback is required.

## Rollout/Adoption Plan
- First, support writing with Flink, and perform the Bucket rescale operation with Spark.
- Then, support writing with Spark.
- Finally, support the push-down of Bucket Index filtering at the partition level.

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.