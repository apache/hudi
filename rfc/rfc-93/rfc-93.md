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
# RFC-93: Pluggable Table Formats in Hudi

## Proposers

*   Balaji Varadarajan

## Approvers

*   Vinoth Chandar
*   Ethan Guo

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-9332 

## Abstract

This RFC proposes support for different backing table format implementations inside Hudi. For the past 4 years at-least, we have been consistently defining Hudi as a broader platform and software [stack](https://hudi.apache.org/docs/hudi_stack) that delivers much of these benefits. Hudi's table format makes choices specific to data lake workloads, allowing efficient read/write (even the recent [blog](https://bytearray.substack.com/p/computer-science-behind-lakehouse) from Vinoth), has major differences and advantages compared to other approaches. The community plans to centrally focus on the native Hudi storage format.

However, there may be benefits to allowing other storage layouts/table formats to fit under Hudi's higher level functionality. This also has non-technical benefits of insulating the project from vendor marketing wars. Most contributors (such as myself) are happily part of the global Hudi open-source community, for the sake of just building technology.

## Background

Expanding further, there are plenty of valid technical reasons on why Hudi should allow different storage layouts, under the upper layer reader/writer and table services implementations.

1. We have use-cases, for cloud-native/high performance implementations of timeline (\`HoodieTimeline\`) and metadata (\`HoodieMetadata\` interface). In our use-case, we would like to explore backing them using NoSQL datastore like DynamoDB, for ultra-low latency queries.
2. Hudi already supports [different](https://github.com/apache/hudi/blob/master/hudi-client/hudi-client-common/src/main/java/org/apache/hudi/table/storage/HoodieStorageLayout.java) storage formats/layouts. Tables can be bucketed, consistent hashed or organized by having data laid out in order of arrival (default).
3. Hudi already allows plug-ability and customization at various layers like record merger, indexes and other core write/read paths.
4. It's very standard practice in databases to allow multiple storage backends (MySQL supports myISAM, innodb/btree, myrocks/lsm). This may be crucial step towards the database northstar vision.
5. As a long time member of the Hudi community and open-source enthusiast, I think supporting other table formats, even existing ones like Apache Iceberg or Delta Lake, benefits those communities as well.
    1. For e.g. the Hudi Streamer tool being used at our data lake (and hundreds more) for ingestion/incremental ETL can also benefit other communities.
    2. Hudi provides out-of-box automatic table management that is manual in projects like Iceberg. With such an implementation, common data lake services can be reused across formats.
    3. Hudi's high performance writer path can be extended to other formats (to the extent possible, that is not dependent on features like indexing that is only in Hudi's native format)
    4. there are more such services and functionalities to be unlocked.


Some non-technical reasons:
1. Though Hudi is clearly defined as a platform over the years, there is so much vendor attention in the space for the past couple years, where Hudi is minimized to a table format and compared. This change will help highlight the value of Hudi's open software services, beyond just open formats.
2. It may be controversial to say this. But, the project has been facing a lot of vendor FUD due to different vendors supporting different table formats. It is neither in the interest nor the business of the project community to be part of vendor wars. Opening up the table format layer to different implementations avoids these distractions for regular OSS contributors with no vendor interests, and helps focus on open-source software design and development.

  
## **Implementation**

The main implementation step here is to create abstraction called `HoodieTableFormat` which handles table format operations such as 


1. Committing writes 
2. Update and read table format specific Metadata operations 
3. Timeline 
4. Conflict Resolution
5. Lock Provider
6. Rollbacks


The Hudi platform is responsible for managing the data path and can be configured with the table format plugin or by default it continues to use Hudi's native table format. 
Other table formats will have their own implementation of this abstraction.  

### Commit Protocol:

Hudi uses the creation of an action-complete file (e:g .commit, .deltacommit, .clean, ..)  in timeline folder to advertise the completion of an action on the table. With the TableFormatPlugin, this will become a two step process:

1. Creation of the action complete file in .hoodie timeline 
2. Storing the action completion time in the table format's commit metadata.

The Hudi timeline is still very much used for all internal operations and the table format's commit metadata will be an overlay on top of this.
With this, the action is only completed when both the above steps are completed. The plugin provided timeline needs to fence the timeline ensuring the definition of complete stays consistent.  This ensures the snapshot isolation is maintained.

There will be a hudi table-property "hudi.table.format" to identify the table format with default being "native". This allows consistency in plugin behaviors across all hudi writers.

### Metadata:

If a different table format is configured, Hudi's metadata operations are replaced with the table format's metadata. This is done by adding a new adapter implementation of HoodieTableMetadata.

### Timeline: 

The timeline implementation ensures the external table format's (table format that is plugged-in) metadata state (e:g commit status) is the source of truth for all operations. 

### Conflict Resolution

Hudi already provides configurable support to plugin different strategies for identifying if two concurrent operations are conflicting at the file level. The 
external pluggable formats may not have such fine-level granularity. The external pluggable formats needs to implement the strategy based on its metadata.

### Locking
Hudi has pluggable lock provider supports. External formats will need to rely on catalog or other means for providing locking semantics. The External table format implementation needs to provide custom lock provider to adapt their locking mechanisms to Hudi's LockProvider interface so that all write and table-service operations can concurrently operate. 

### Rollbacks
External Table format implementation  needs to rollback failed writes and also be able to also restore to previous commits. The external plugin implementation needs to keep enough states to be able to rollback.
 
### Layout:

The metadata corresponding to the table format (iceberg, ..) will be stored under .hoodie/ folder by default with location stored in hoodie.properties.

## Integration

The end to end integration is shown in the below diagram
![integration](flow_and_arch.jpg)

The key points here are:

1. Hudi Writers continue to use the native file system management. This means the way Hudi organizes files as file-groups and file-slices and Hudi's index for writing will continue to be the same.
2. Hudi will continue to record actions in timeline (.hoodie/timeline).
3. The external table format will be configured as part of table properties to ensure all writers can recognize it.
4. When each of Hudi's actions (write, clean, rollback,..) complete, Hudi will call ther external table format plugin. It is the plugin's responsibility to record necessary metadata for external format readers to query this table. 
5. Hudi will use the external table format plugin's timeline implementation which uses the state stored in its metadata as the source of truth. For example, an iceberg plugin needs to create a manifest file when action completes and provides a timeline that honors the state in its snapshot files. Hudi uses this timeline to determine what actions completed and what failed (for rollback). 
6. The external plugin also needs to provide LockProvider and Conflict Resolution implementations that allows the concurrency controls supported by the external table format to be exercised (for example: optimistic concurrency control).
7. The plugin also provides capability to allow its metadata to be used by the Hudi writer.
8. The external table format's metadata along with Hudi's data files combined allows the table to be read as if it is written directly in external table format.


## Limitations

1. The initial implementation will only support Copy On Write format of Hudi with Spark as the processing framework. This can be extended to support MOR tables but not in the initial scope.
2. The table needs to be written only through Hudi. No interoperability guarantee will be provided for writes happening through the external table format. 


## Codebase:

The pluggable interface and Hudi's native format implementation will reside in hudi codebase while support for other systems will be done in Apache XTable (Incubating).


The lakehouse platform interacts with this plugin and the table can be queried using the table format corresponding to the plugin. Here is the interface definition:

```java
/**
 * External Table Format needs to implement this class. Hudi's native format will be implemented in NativeTableFormat.
 * Contract: 
 *    This implementation will get the commit and completeXXX calls after the Hudi platform commit's internally. 
 *    An operation must be defined as complete only when the plugin's implementation successfully handles the corresponding callback.
 *    This implementation is responsible for providing the source of truth timeline based on what operations completed successfully.
 */
public interface HoodieTableFormat implements Serializable {

  /**
   * Callback to handle committing a write operation. This call is made after Hudi's internal commit. 
   * @param metaClient   HoodieTableMetaClient for interacting with Hudi's internal metadata.
   * @param viewManager  FileSystem Manager to fetch table's file-system view.
   * @param commitMetadata HoodieCommitMetadata  corresponding to the transaction.
   * @param instant Hoodie Instant used for this transaction.
   */
  void completeWrite(
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager,
      HoodieCommitMetadata commitMetadata,
      HoodieInstant instant
  );

  /**
   * Callback to complete finish clustering operation by the plugin.
   * @param metaClient   HoodieTableMetaClient for interacting with Hudi's internal metadata.
   * @param viewManager  FileSystem Manager to fetch table's file-system view.
   * @param replaceCommitMetadata Replace Commit Metadata corresponding to the transaction.
   * @param instant Hoodie Instant used for this transaction.
   */
  void completeClustering(
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager,
      HoodieReplaceCommitMetadata replaceCommitMetadata,
      HoodieInstant instant
  );

  /**
   * Callback to complete finish clean operation by the plugin.
   * @param metaClient   HoodieTableMetaClient for interacting with Hudi's internal metadata.
   * @param viewManager  FileSystem Manager to fetch table's file-system view.
   * @param cleanCommitMetadata Clean Commit Metadata corresponding to the clean operation.
   * @param instant Hoodie Instant used for this transaction.
   */
  void completeClean(
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager,
      HoodieCleanMetadata cleanMetadata,
      HoodieInstant instant
  );

  /**
   * Callback to complete finish rollback operation by the plugin.
   * @param metaClient   HoodieTableMetaClient for interacting with Hudi's internal metadata.
   * @param viewManager  FileSystem Manager to fetch table's file-system view.
   * @param rollbackMetadata Rollback Metadata containing the instants rolledback and files deleted.
   * @param instant Hoodie Instant used for this transaction.
   */
  void completeRollback(
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager,
      HoodieRollbackMetadata rollbackMetadata,
      HoodieInstant instant
  );


  /**
   * Callback to complete finish archive operation by the plugin.
   * @param metaClient   HoodieTableMetaClient for interacting with Hudi's internal metadata.
   * @param viewManager  FileSystem Manager to fetch table's file-system view.
   * @param archivedInstants List of Hoodie Instants archived.
   */
  void archiveInstants(
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager,
      List<HoodieInstant> archivedInstants
  );

  /**
   * Return Timeline Factory which uses the external plugin's timeline state as the source of truth. 
   * The timeline factory needs to create timelines which has instants marked complete only when the plugin's commit call succeeded.
   */
  TimelineFactory getTimelineFactory();

  /**
   * Provides MetadataFactory which returns metadata reader interface that uses the plugin format's metadata.
   */
  BaseHoodieTableMetadataFactory getMetadataFactory();

  /**
   * LockProvider corresponding to the plugin.
   */
  Option<LockProvider> getLockProvider(LockConfiguration lockConfiguration, StorageConfiguration storageConf);

  /**
   * Conflict resolution strategy that aligns with the plugin format.
   */
  Option<String> getConflictResolutionStrategyClassName(HoodieConfig config);
}

```
## **Rollout/Adoption Plan**

*   For existing tables, utility to construct the table format for the first time.
*   Configuration of plugin through reflection to turn on specific formats. Default will be native.
*   Add support in 1.x


