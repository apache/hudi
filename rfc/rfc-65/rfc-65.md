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
## Proposers

- @stream2000
- @hujincalrin
- @huberylee
- @YuweiXiao

## Approvers

## Status

JIRA: [HUDI-5823](https://issues.apache.org/jira/browse/HUDI-5823)

## Abstract

In some classic hudi use cases, users partition hudi data by time and are only interested in data from a recent period
of time. The outdated data is useless and costly, we need a TTL(Time-To-Live) management mechanism to prevent the
dataset from growing infinitely.
This proposal introduces Partition TTL Management strategies to hudi, people can config the strategies by table config
directly or by call commands. With proper configs set, Hudi can find out which partitions are outdated and delete them.

This proposal introduces Partition TTL Management service to hudi. TTL management is like other table services such as Clean/Compaction/Clustering.
The user can config their ttl strategies through write configs and Hudi will help users find expired partitions and delete them automatically.

## Background

TTL management mechanism is an important feature for databases. Hudi already provides a `delete_partition` interface to
delete outdated partitions. However, users still need to detect which partitions are outdated and
call `delete_partition` manually, which means that users need to define and implement some kind of TTL strategies, find expired partitions and call call `delete_partition` by themselves. As the scale of installations grew, it is becoming increasingly important to implement a user-friendly TTL management mechanism for hudi.

## Implementation

Our main goals are as follows:

* Providing an extensible framework for partition TTL management.
* Implement a simple KEEP_BY_TIME strategy, which can be executed through independent Spark job, synchronous or asynchronous table services.

### Strategy Definition

The TTL strategies is similar to existing table service strategies. We can define TTL strategies like defining a clustering/clean/compaction strategy: 

```properties
hoodie.partition.ttl.strategy=KEEP_BY_TIME
hoodie.partition.ttl.strategy.class=org.apache.hudi.table.action.ttl.strategy.KeepByTimePartitionTTLStrategy
hoodie.partition.ttl.days.retain=10
```

The config `hoodie.partition.ttl.strategy.class` is to provide a strategy class (subclass of `PartitionTTLStrategy`) to get expired partition paths to delete. And `hoodie.partition.ttl.days.retain` is the strategy value used by `KeepByTimePartitionTTLStrategy` which means that we will expire partitions that haven't been modified for this strategy value set. We will cover the `KeepByTimeTTLStrategy` strategy in detail in the next section.

The core definition of `PartitionTTLStrategy` looks like this: 

```java
/**
 * Strategy for partition-level TTL management.
 */
public abstract class PartitionTTLStrategy {

  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;

  public PartitionTTLStrategy(HoodieTable hoodieTable) {
    this.writeConfig = hoodieTable.getConfig();
    this.hoodieTable = hoodieTable;
  }
  
  /**
   * Get expired partition paths for a specific partition ttl management strategy.
   *
   * @return Expired partition paths.
   */
  public abstract List<String> getExpiredPartitionPaths();
}
```

Users can provide their own implementation of `PartitionTTLStrategy` and hudi will help delete the expired partitions.

### KeepByTimeTTLManagementStrategy

We will provide a strategy call `KeepByTimePartitionTTLStrategy` in the first version of partition TTL management implementation.

The `KeepByTimePartitionTTLStrategy` will calculate the `lastCommitTime` for each input partitions. If duration between now and 'lastCommitTime' for the partition is larger than what `hoodie.partition.ttl.days.retain` configured, `KeepByTimePartitionTTLStrategy` will mark this partition as an expired partition. We use day as the unit of expired time since it is very common-used for datalakes. Open to ideas for this. 

we will to use the largest commit time of committed file groups in the partition as the partition's
`lastCommitTime`. So any write (including normal DMLs, clustering etc.) with larger instant time will change the partition's `lastCommitTime`.

For file groups generated by replace commit, it may not reveal the real insert/update time for the file group. However, we can assume that we won't do clustering for a partition without new writes for a long time when using the strategy. And in the future, we may introduce a more accurate mechanism to get `lastCommitTime` of a partition, for example using metadata table. 

For 1.0.0 and later hudi version which supports efficient completion time queries on the timeline(#9565), we can get partition's `lastCommitTime` by scanning the timeline and get the last write commit for the partition. Also for efficiency, we can store the partitions' last modified time and current completion time in the replace commit metadata. The next time we need to calculate the partitions' last modified time, we can build incrementally from the replace commit metadata of the last ttl management.

### Apply different strategies for different partitions

For some specific users, they may want to apply different strategies for different partitions. For example, they may have multi partition fileds(productId, day). For partitions under `product=1` they want to keep for 30 days while for partitions under `product=2` they want to keep for 7 days only. 

For the first version of TTL management, we do not plan to implement a complicated strategy (For example, use an array to store strategies, introduce partition regex etc.). Instead, we add a new abstract method `getPartitionPathsForTTL` in `PartitionTTLStrategy` and provides a new config `hoodie.partition.ttl.partition.selected`. 

If `hoodie.partition.ttl.partition.selected` is set, `getPartitionPathsForTTL` will return partitions provided by this config. If not, `getPartitionPathsForTTL` will return all partitions in the hudi table. 

TTL strategies will only be applied for partitions return by `getPartitionPathsForTTL`. 

Thus, if users want to apply different strategies for different partitions, they can do the TTL management multiple times, selecting different partitions and apply corresponding strategies. And we may provide a batch interface in the future to simplify this. 

The `getPartitionPathsForTTL` method will look like this:

```java
/**
 * Strategy for partition-level TTL management.
 */
public abstract class PartitionTTLStrategy {
  protected final HoodieTable hoodieTable;
  protected final HoodieWriteConfig writeConfig;

  public PartitionTTLStrategy(HoodieTable hoodieTable) {
    this.writeConfig = hoodieTable.getConfig();
    this.hoodieTable = hoodieTable;
  }
  
   /**
    * Scan and list all partitions for partition TTL management.
    *
    * @return Partitions to apply TTL strategy
    */
   protected List<String> getPartitionPathsForTTL() {
      if (StringUtils.isNullOrEmpty(writeConfig.getTTLPartitionSelected())) {
        return getMatchedPartitions();
      } else {
        // Return All partition paths
        return FSUtils.getAllPartitionPaths(hoodieTable.getContext(), writeConfig.getMetadataConfig(), writeConfig.getBasePath());
      }
   }
}
``` 

### Executing TTL

Once we already have a proper `PartitionTTLStrategy` implementation, it's easy to execute the ttl management. 

```java
public class SparkTTLManagementActionExecutor <T> extends BaseSparkCommitActionExecutor<T> {
   @Override
   public HoodieWriteMetadata<HoodieData<WriteStatus>> execute() {
      // Construct PartitionTTLstrategy
      PartitionTTLManagementStrategy strategy = (PartitionTTLStrategy) ReflectionUtils.loadClass(
              PartitionTTLStrategy.checkAndGetPartitionTTLStrategy(config),
              new Class<?>[] {HoodieTable.class, HoodieEngineContext.class, HoodieWriteConfig.class}, table, context, config);

      // Get expired partition paths
      List<String> expiredPartitions = strategy.getExpiredPartitionPaths();
      
      // Delete them reusing SparkDeletePartitionCommitActionExecutor
      return new SparkDeletePartitionCommitActionExecutor<>(context, config, table, instantTime, expiredPartitions).execute();
   }
}
```

We will add a new method `managePartitionTTL` in `HoodieTable` and `HoodieSparkCopyOnWriteTable` can implement it like this:

```java
@Override
public HoodieWriteMetadata<HoodieData<WriteStatus>> managePartitionTTL(HoodieEngineContext context, String instantTime) {
  return new SparkTTActionExecutor<>(context, config, this, instantTime).execute();
}
```

We can call `hoodieTable.managePartitionTTL` in independent flink/spark job, in async/sync inline table services like clustering/compaction/clean etc.


### User interface for Partition TTL Management

We can do partition TTL management inline with streaming ingestion job or do it with a independent batch job, for both spark and flink engine.

#### Run inline with Streaming Ingestion 

Since we can run clustering inline with streaming ingestion job through the following config: 

```properties
hoodie.clustering.async.enabled=true
hoodie.clustering.async.max.commits=5
```

We can do similar thing for partition TTL management. The config for async ttl management are: 

| Config key                   | Remarks                                                                                                            | Default |
|------------------------------|--------------------------------------------------------------------------------------------------------------------|---------|
| hoodie.ttl.async.enabled     | Enable running of TTL management service, asynchronously as writes happen on the table.                            | False   |
| hoodie.ttl.async.max.commits | Control frequency of async TTL management by specifying after how many commits TTL management should be triggered. | 4       |

We can easily implement async ttl management for both spark and flink engine since we only need to call `hoodieTable.managePartitionTTL`. And we can support synchronized ttl management if we want.

#### Run by Independent Job

Deleting a large number of partitions is a heavy operation so we may want to run TTL management through a independent job. We will provide a SparkSQL Call Command to run TTL management and it may look like this: 

```sql
call managePartitionTTL(table => 'hudi_table', strategy => 'KEEP_BY_TIME', daysRetain => '10', predicate => 'productid = 1');
```

The params are as follows:

| Param name | Remarks                                                                                                | Default      |
|------------|--------------------------------------------------------------------------------------------------------|--------------|
| table      | The hoodie table to run partition TTL                                                                  | empty string |
| basePath   | The hoodie table path to run partition TTL                                                             | empty string |
| strategy   | The partition TTL strategy, corresponding to a implementation of `PartitionTTLStrategy`                | KEEP_BY_TIME |
| predicate  | Partition predicate for TTL, will only apply ttl strategy on the partitions selected by this predicate | empty string |


Besides SparkSQL call commands, we can support run TTL management with a spark jar like running clustering by `HoodieClusteringJob` and run TTL with a flink job like `HoodieFlinkClusteringJob` in the future.

### Future plan

We can do a lot of things about TTL management in the future:

* Support record level TTL management
* Move the partitions to be cleaned up to cold/cheaper storage in objects stores instead of delete them forever
* Stash the partitions to be cleaned up in .stashedForDeletion folder (at .hoodie level) and introduce some recover mechanism, for data security.
* Support advanced ttl policies, for example wild card partition spec.`
* ...

## Rollout/Adoption Plan

Hoodie Partition TTL Management V1 will support a simple KEEP_BY_TIME strategy at first, and others can implement their own `PartitionTTLStrategy`. 

Add this feature won't affect existing functions.

## Test Plan

Will add UTs and ITs to test this.
