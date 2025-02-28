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
On the other hand, for a certain existing partitions, an off-line command is provided to reorganized the data using insert
overwrite(need to stop the data writing of the current partition).

More importantly, the existing Bucket Index table can be upgraded to Partition-Level Bucket Index smoothly and seamlessly.

## Background
The following is the core read-write process of the Flink/Spark engine based on Simple Bucket Index
### Flink Write Using Simple Bucket Index
**Step 1**: re-partition input records based on `BucketIndexPartitioner`, BucketIndexPartitioner has **a fixed bucketNumber** for all partition path.
For each record key, compute a fixed data partition number, doing re-partition works.

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
### Overview
#### Demo
The following is a Demo of using the partition-level bucket index through Flink.

```text
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
'hoodie.bucket.index.partition.expressions' = '{"expressions":[{"expression":"\\d{4}-(06-(01|17|18)|11-(01|10|11))","bucketNumber":256,"rule":"regex"}],"defaultBucketNumber":10}'
);
```

This `hoodie.bucket.index.partition.expressions` config is at table level, Users can't change this config through
runtime options like Flink /*+ OPTIONS(...) */

We can use `ALTER TABLE table_name SET ('hoodie.bucket.index.partition.expressions' = 'new-expression'')` to change the table bucket expression

When users need to modify or add rules in multi-write scenarios, they should refer to the following steps:
1. Call "alter table" command to modify the configuration, and then restart all jobs.
2. Alternatively, stop all jobs first, then modify the configuration, and finally start the jobs.

Should not modify the configuration during the process of starting or stopping jobs. Otherwise, the jobs will be failed because of inner conflict detection

#### workflow
The core process of writing to the Partition Level Bucket Index is shown in the figure below. First, the Bucket Identifier 
loads the partition bucket count information from the metadata as a cache. During the Partitioner and Writer stages, when 
using the Identifier for shuffling and tagging, it will first attempt to find the bucketNumber of the corresponding 
partition from the cache. If the cache was missed, it will calculate the corresponding value based on the user's expression, 
and then submit it to the metadata during the commit stage.
![workflow.jpg](workflow.jpg)
The design details of each part are as follows.

### Config
Add a new config named `hoodie.bucket.index.partition.expressions` default null. Users can specify a computation rules of bucket-numbers for different
partitions by configuring a JSON expression. For example
```json
{
    "expressions": [
        {
            "expression": "\\d{4}-(06-(01|17|18)|11-(01|10|11))",
            "bucketNumber": 256,
            "rule": "regex"
        }
    ],
  "defaultBucketNumber": 10
}
```

When the user sets the above expression, for the dates of 06-01, 06-17, 06-18 in June and 01-11, 10-11, 11-11 in
November of each year (in the format of yyyy-MM-dd), the corresponding bucket number for the partition is 256.
For the ordinary "dt" partitions, the bucket number is 10.

Also `<timestamp>.hash_config` will be create during table DDL under `.hoodie/.hashing_meta/simple/`.

Users can use `ALTER TABLE table_name SET ('hoodie.bucket.index.partition.expressions' = 'new-expression')` to modify this `<timestamp>.hash_config` and it will
create a new version of hashing_config

please note that `alter table` command won't block read and write but only by restarting the job can the new config work.
### Hashing Metadata
The hashing metadata will be persisted as files named as `<instant>.hashing_meta` for current table, it stores in
`.hoodie/.hashing_meta/simple/` directory and contains the following information in a readable encoding

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
     },
     {
       "name":"defaultBucketNumber",
       "type":["int", "null"],
       "default": 256
     },
   ]
}
```
We will write <instant>.simple_hashing_meta during commit action (at pre-commit stage):
1. Get last complete T1.simple_hashing_meta
2. Merging T1.simple_hashing_meta with new written partitions as T2.simple_hashing_meta also do partition conflict detection
   1. For example, current write write partition1 with 20 buckets but there is already defined partition1 with 10 bucket numbers, which means there is 
another writer that uses the latest expression to pre-determine the bucket number of the current partition. In order to ensure data consistency, it is necessary to abort the current writer.
3. If conflict detection is passed, then write T2.simple_hashing_meta into .hoodie folder
4. Do commit action

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
2. For the partition of the given data, try to get its corresponding bucket number from the cache.
   1. If the cache missed, calculate the expression through the Bucket Calculator to obtain the corresponding Bucket Number.
   2. If the cache hit, it is still necessary to calculate the expression through the Bucket Calculator to 
obtain the corresponding Bucket Number and verify its consistency. This step is mandatory because there may be a situation where 
a new expression is loaded but a historical partition is operated on. Here, a pre - conflict check is performed to achieve fast failure.

Using SimpleBucketIdentifier to get `BucketNumber` or `BucketID` based on given partitionPath and corresponding expression

For new Partitioner:
`SparkBucketIndexPartitioner`-Spark and `BucketIndexPartitioner`-Flink use this new SimpleBucketIdentifier to do data repartition

For Writer:
Writer like `BucketStreamWriteFunction` in Flink also use this new `SimpleBucketIdentifier` to compute bucketID

NOTE：We need to get hashing_meta instant and hashing_config timestamp at the beginning and pass it to each operator, 
ensure the consistency of config loading.

### BucketCalculator
The BucketCalculator calculates the bucket numbers for different partitions based on the different rules set by users.
Here, the types of supported Rules are extensible. In the first phase, regular expressions are mainly supported.

```java
public class BucketCalculator {

    private ExpressionConfig loadConfig(String configJson) {
      // ... 
    }

    public int calculateBucketNumber(String partitionPath) {
      // ...
    }

    private static Rule getRule(String ruleType) {
        switch (ruleType.toLowerCase()) {
            case "regex":
                return new RegexRule();
            case "range":
                return new RangeRule();
            default:
                return null;
        }
    }
}
```

### Bucket re-scale
use Spark `insert overwrite` sql to re-scale bucket which is a replace commit, also updated bucket meta for related partitions
NOTE: When re-scaling, the data ingestion needs to be stopped to ensure the consistency of the bucket number.

### Spark call command to rollback bucket rescale
For the Bucket-rescale operation, rollback is supported. For example, if the scaling size does not meet the expectations, a rollback is required.

## Rollout/Adoption Plan
- First, support writing with Flink, and perform the Bucket rescale operation with Spark.
- Then, support writing with Spark.
- Finally, support the push-down of Bucket Index filtering at the partition level.


## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.