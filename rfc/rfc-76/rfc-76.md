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
# RFC-76: [Auto record key generation]

## Proposers

- @nsivabalan

## Approvers
 - @yihua
 - @codope

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-4699

> Please keep the status updated in `rfc/README.md`.

## Abstract

One of the prerequisites to create an Apache Hudi table is to configure record keys(a.k.a primary keys). Since Hudi’s 
origin at Uber revolved around supporting mutable workloads at large scale, these were deemed mandatory. As we started 
supporting myriad of use-cases and workloads, we realized that defining a record key may not be natural in all cases 
like immutable workloads, log ingestion etc. So, this RFC aims at supporting Hudi tables without configuring record 
keys by the users.

## Background
At present ingesting data into Hudi has a few unavoidable prerequisites one of which is specifying record key configuration (with record key serving as primary key). Necessity to specify primary key is one of the core assumptions built into Hudi model centered around being able to update the target table efficiently. However, some types of data/workloads actually don't have a naturally present record key: for ex, when ingesting some kind of "logs" into Hudi there might be no unique identifier held in every record that could serve the purpose of being record key, while meeting global uniqueness requirements of the primary key. There could be other immutable workloads, where the user does not have much insights into the data schema, but prefers to ingest as Hudi table to do some aggregation down the line. In all such scenarios, we want to ensure Users are able to create Hudi table, while still providing for Hudi's core strength with clustering, table services, file size management, incremental queries etc.

## Implementation

### Requirements
Let’s take a look at the requirements we have in order to support generating record keys automatically.

Auto-generated record keys have to provide for global uniqueness w/in the table, not just w/in the batch.
This is necessary to make sure we're able to support updating such tables.
Keys should be generated in a way that would allow for their efficient compression
This is necessary to make sure that auto-generated keys are not bringing substantial overhead (on storage and in handling)
Suggested approach should be compatible with all major execution environments (Spark, Flink, Kafka Connect, Java, etc)
Tables written using spark should be readable using flink, java and vice versa.

### Synthetic Key
Efficient way to associate an opaque record with an identifying record key or identity value, that is independent of the record content itself, is to simply enumerate the records.
While enumeration itself doesn't present a challenge, we have to, however, make sure that our auto-generation approach is resilient in the case of present failures while persisting the dataset. Here our analysis will be focused squarely on Spark, but similar derivations could be replicated to other execution environments as well.

Let's consider following scenario: while persisting the dataset, writing one of the files to Cloud Storage fails and Spark is unable to leverage previously cached state of the RDD (and therefore retry just the failing task) and instead it will now have to recompute the whole RDD chain (and create new files).
To provide for aforementioned requirement of the records obtaining globally unique synthetic keys either of the 2 following properties have to hold true:
Key generation has to be deterministic and reproducible (so that upon Spark retries we could be certain same records will be obtaining the identity value they did during previous pass)
Records have to be getting globally unique identity value every time (such that key collisions are simply impossible)
Note that, deterministic and reproducible identity value association is only feasible for the incoming datasets represented as "determinate" RDDs. However, It's worth pointing out that other RDD classes (such as "unordered", "indeterminate") are very rare occurrences involving some inherent non-determinism (varying content, order, etc), and pose challenges in terms of their respective handling by Hudi even w/o auto-generation (for ex, for such RDDs Hudi can't provide for uniqueness guarantee even for "insert" operation in the presence of failures).
For achieving our goal of providing globally unique keys we're planning on relying on the following synthetic key format comprised of 2 components
(Reserved) Commit timestamp: Use reserved commit timestamp as prefix (to provide for global uniqueness of rows)
Row id: unique identifier of the row (record) w/in the provided batch
Combining them in a single string key as below
"${commit_timestamp}_${batch_row_id}"

For row-id generation we plan to use a combination of “spark partition id” and a row Id (sequential Id generation) to generate unique identity value for every row w/in batch (this particular component is available in Spark out-of-the-box, but could be easily implemented for any parallel execution framework like Flink, etc)
Please note, that this setup is very similar to how currently _hoodie_commit_seqno is implemented.

So, the final format is going to be:
"${commit_timestamp}_${spark partition id}, ${row Id}"

### Auto generated record key encoding
Given that we have narrowed down the record key has to be an objective function of 3 values namely, commit time, spark partitionId and row Id, let’s discuss how we can go about generating the record keys or in other words, how we can encoding these to create the record keys.

We have few options to go with to experiment:
- Original key format is a string in the format of "<instantTime>-<partitionId>-<rowId>".
- UUID6/7 key format is implemented by using code from https://github.com/f4b6a3/uuid-creator.
- Base64 encoded key format is a string encoded from a byte array which consists of: the lowest 5 bytes from instantTime (supporting millisecond level epoch), the lowest 3 bytes from partitionId (supporting 4 million # of partitions), and lowest 5 bytes from rowId (supporting 1 trillion # of records). Since the Base64 character may use more than one bytes to encode one byte in the array, the average row key size is higher than 13 ( 5 + 3 + 5) bytes in the file.
- Similarly, ASCII encoded key format does the similar algo as Base64 key; however, after generating the byte array, in order to present valid ASCII code, we distributes the 13 * 8 = 114 bits into 114/7 = 15 bytes, and encode it.

Going back to one of our key requirements wrt auto record key generation is that, our record key generation should be storage optimized and compress well. It also implicitly means that, the time to encode and decode should also be taken into consideration along with the storage space occupied.

#### Storage comparison

Based on our experiments, here is the storage comparison across different key encodings.

| Format | Uncompessed (bytes) : Size of record key column in a parquet file w/ 100k records | Compressed size (bytes) | Compression Ratio | Example |
|--------|---------|-----------|--------|-----|
|Original| 4000185 | 244373 | 11.1 |20230822185245820_8287654_2123456789 |
|UUID 6/7| 4000184 | 1451897 | 2.74 |1ee3d530-b118-61c8-9d92-1384d7a07f9b |
|Base64| 2400184 | 202095 |11.9 |zzwBAAAAAABqLPkJig== |
|ASCII| 1900185 | 176606 |10.8 |${f$A" |


### Runtime comparison to generate the record keys

| Format | Avg runtime (ms) | Ratio compared to baseline (original format) |
|--------|-----------------|----------------------------------------------|
|Original| 0.00001         | 1                                            |
|UUID 6/7| 0.0001          | 10                                           | 
|Base64| 0.004           | 400                                          |
|ASCII| 0.004          | 400                                          |


#### Analysis
Both uncompressed and compressed sizes of record key columns in UUID6/7 are much bigger than our original formats, which means we can discard them.
Compared with the base line format Original, Base64 and ASCII formats can produce better results based on the storage usage.Specifially, Base64 format can produce around 17% of storage reduction after Parquet compression, and ASCII can produce around 28% of reduction. However, to extract relevant bytes and do the bit distribution and encoding, Base64 and ASCII can definitely require more CPU powers during writings (400x).

#### Consensus
So considering the storage size and runtimes across different encoding formats we will settle with the original format ie. "${commit_timestamp}_${spark partition id}, ${row Id}" for our auto record key generation.

### Info about few dis-regarded approaches

#### Why randomId generation may not work
It is natural to think why not we simplify further and generate something like "${commit_timestamp}_${RANDOM_NUMBER}”. While this could look very simple and easier to implement, this is not really deterministic. When a subset of spark tasks failed due to executor failure, if the spark dag is re-triggered, a slice of the input data might go through record key generation and if not for being deterministic, it could lead to data inconsistency issues. Because, down the line, our upsert partitioner (file packing) relies on the hash of the record keys.

#### monotonically_increasing_id in spark
For the same reason quoted above, we can’t go w/ the ready to use id generation in spark, monotonically_increasing_id. In fact, we heard from one of the open source user they were using monotoically increasing id func to generate record keys before ingesting to hudi, and occasionally they could see some data consistency issues. It was very hard to reproduce and narrow down the issue.

### Injecting Primary Keys into the Dataset
Auto-generated record keys could be injected at different stages:

**Approach A**: Injecting prior to handling
Injecting into the incoming batch early on (before handing the batch off to the write-client)
**Pros**
Avoids the need to modify any existing Hudi code (assuming that the primary key is always present). Will work with any operation (insert/upserts/bulk-insert).

**Cons**
Auto-generated key injection have to be replicated across every supported execution environment (Flink, Java, etc)

**Approach B**: Injecting when writing to base file
Assign to a record when writing out into an actual file
**Pros**
Straightforward approach (similar to how seq-no is already implemented)
This path is shared across all execution environments making it compatible w/ all execution environments out of the box (OOB)
**Cons**
Requires special handling in Hudi code-base (though could be restricted to bulk-insert only)
Our upsert partitioner which packs/routes incoming records to write handles is dependent on the record key (hash or record key). So, if we were to take this approach, we have to introduce a new Upsert Partitioner.

Since Approach A seems natural and does not seem a lot of heavy lifting to do, we will go with it.

## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.