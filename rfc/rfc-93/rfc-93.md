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

Revised for 1.3.0. The interface section now matches the interface that shipped in 1.1 and 1.2, and the
revision extends it with notifications for every table service, table format settings in the table
config, a coexisting metadata table, per-row format metadata columns, and write configuration defaults
and validation supplied by the table format. Merge-on-read tables are covered by
[RFC-114](../rfc-114/rfc-114.md). Lock provider and conflict resolution hooks, and writers outside Hudi,
are left to a later revision.

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
7. Table services (compaction, clustering, clean, archival), savepoint and restore
8. Per-row metadata the table format keeps in data files (format meta columns)
9. Write configuration the table format needs (defaults and validation)


The Hudi platform is responsible for managing the data path and can be configured with the table format plugin or by default it continues to use Hudi's native table format. 
Other table formats will have their own implementation of this abstraction.  

### Commit Protocol:

Hudi uses the creation of an action-complete file (e:g .commit, .deltacommit, .clean, ..)  in timeline folder to advertise the completion of an action on the table. With the TableFormatPlugin, this will become a two step process:

1. Creation of the action complete file in .hoodie timeline 
2. Storing the action completion time in the table format's commit metadata.

The Hudi timeline is still very much used for all internal operations and the table format's commit metadata will be an overlay on top of this.
With this, the action is only completed when both the above steps are completed. The plugin provided timeline needs to fence the timeline ensuring the definition of complete stays consistent.  This ensures the snapshot isolation is maintained.

There will be a hudi table-property "hoodie.table.format" to identify the table format with default being "native". This allows consistency in plugin behaviors across all hudi writers.

### Table format settings

- **Settings live in the table config.** Settings of the table format (`hoodie.table.format.*`, such as
  where its metadata is published) are stored in `hoodie.properties` when the table is created, by
  every writer that can create a table. Ingestion, a separate table service job and a reader of the
  same table therefore load the same settings, and no writer has to repeat them.
- **The name resolves or the table fails to load.** The table format name is matched ignoring case.
  A name that matches no table format on the classpath fails the table load instead of falling back
  to the native format, since a writer that ignores the table format would complete instants the
  table format never records.

### Actions the table format is notified of

Every Hudi action that changes the table notifies the table format once the action is complete in
the Hudi timeline, so the table format's metadata never lags behind a file rewrite.

| Hudi action | Notification | When |
|---|---|---|
| commit, deltacommit, insert overwrite, clustering | `commit` | after the instant completes |
| compaction, log compaction | `commit`, with the compaction's commit metadata | after the instant completes |
    // ... 238 lines omitted
}
// ... 237 more lines (total: 353)## Limitations

1. The initial implementation will only support Copy On Write format of Hudi with Spark as the processing framework. This can be extended to support MOR tables but not in the initial scope.
2. The table needs to be written only through Hudi. No interoperability guarantee will be provided for writes happening through the external table format. 

Since 1.3.0, merge-on-read tables are supported through the update as delete and insert mode of
RFC-114, with the Spark and Java writers. Limitation 2 still holds.

## Codebase:

The pluggable interface and Hudi's native format implementation will reside in hudi codebase while support for other systems will be done in Apache XTable (Incubating).


The lakehouse platform interacts with this plugin and the table can be queried using the table format corresponding to the plugin. Here is the interface definition. Methods marked
`1.3.0` are added by this revision; methods marked `proposed` are designed here and not built yet.

```java
/**
 * External Table Format needs to implement this interface. Hudi's native format is NativeTableFormat.
 * Contract:
 *    Callbacks run after the corresponding action is complete in the Hudi timeline (rollback also runs before it).
 *    An action is complete only when the table format has handled its callback.
 *    The table format provides the timeline that is the source of truth for which actions completed.
 */
public interface HoodieTableFormat extends Serializable {

  /** Name of the table format, the value of hoodie.table.format. */
  String getName();

  /** Initializes the table format with the table config, including the hoodie.table.format.* settings. */
  default void init(Properties properties) {}

  /** Commit, deltacommit, insert overwrite and clustering; since 1.3.0 also compaction and log compaction. */
  default void commit(HoodieCommitMetadata commitMetadata, HoodieInstant completedInstant,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  default void clean(HoodieCleanMetadata cleanMetadata, HoodieInstant completedInstant,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  default void archive(Supplier<List<HoodieInstant>> archivedInstants,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  /** Before Hudi reverts the instant; since 1.3.0 for merge-on-read tables too. */
  default void rollback(HoodieInstant instantToRollback,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  default void completedRollback(HoodieInstant rollbackInstant,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  default void savepoint(HoodieInstant savepointInstant,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  default void restore(HoodieInstant restoreCompletedInstant,
      HoodieEngineContext engineContext, HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {}

  /** Timelines whose instants are complete only when the table format recorded them. */
  TimelineFactory getTimelineFactory();

  /** The Hudi metadata table when enabled, the table format's own HoodieTableMetadata otherwise. */
  TableMetadataFactory getMetadataFactory();

  /** 1.3.0. Per-row metadata columns that rewritten base files carry over; empty by default. */
  default List<FormatMetaColumn> getFormatMetaColumns() {
    return Collections.emptyList();
  }

  /** Proposed. Table config entries stored when a table of this format is created. */
  default Map<String, String> getTableConfigDefaults(Properties tableProperties) {
    return Collections.emptyMap();
  }

  /** Proposed. Write config defaults, applied to settings the user did not set. */
  default Map<String, String> getWriteConfigDefaults(HoodieTableConfig tableConfig) {
    return Collections.emptyMap();
  }

  /** Proposed. Refuses a write config the table format cannot support, naming the setting and the fix. */
  default void validateWriteConfig(HoodieConfig writeConfig, HoodieTableConfig tableConfig) {}
}

/** 1.3.0. A per-row metadata column declared by a table format. */
public interface FormatMetaColumn extends Serializable {

  /** Field in the base files: a nullable long or int, not named like a Hudi meta field. */
  HoodieSchemaField field();

  /** Value of a row written as new or changed by the write with this instant. */
  Object valueOnWrite(String instantTime);

  /** Value of a changed version of an existing row; the table format may keep the replaced value. */
  default Object valueOnUpdate(String instantTime, Object replacedValue) {
    return valueOnWrite(instantTime);
  }

  /** Resolves the values of rows carried over from a base file; throws if it cannot, which fails the write. */
  BaseFileColumnResolver resolverFor(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile);
}

/** 1.3.0. */
public interface BaseFileColumnResolver extends Serializable {

  /** Value for the row at this position of the base file, given the value the file stores (or null). */
  Object valueAt(long rowPosition, Object storedValue);
}
```

The lock provider and conflict resolution methods of the original proposal
(`getLockProvider(LockConfiguration, StorageConfiguration)` and
`getConflictResolutionStrategyClassName(HoodieConfig)`) are deferred to the later revision on
concurrency.

## **Rollout/Adoption Plan**

*   For existing tables, utility to construct the table format for the first time.
*   Configuration of plugin through reflection to turn on specific formats. Default will be native.
*   Add support in 1.x

**Step 1, shipped in 1.1 and 1.2.** The interface with write, clean, archive, rollback, savepoint and
restore callbacks, the timeline and metadata factories, and copy-on-write tables.

**Step 2, 1.3.0.** Compaction, log compaction and merge-on-read rollback notifications, table format
settings in the table config, the coexisting metadata table, format meta columns, and merge-on-read
tables through RFC-114. All of it is a no-op for native tables.

**Step 3.** Write configuration defaults and validation from the table format, which turns the
RFC-114 mode into a table property set by the table format.

**Step 4.** Lock provider and conflict resolution from the table format, and writers outside Hudi,
in a later revision.

**Switching an existing Hudi table.** An existing table switches in place without rewriting data:
stop writers until no instant is pending, build the table format's metadata from the latest completed
state of the table, record that state as the table format's baseline, then set `hoodie.table.format`.
The Hudi timeline is unchanged throughout, so switching back is setting the format to `native` and
dropping the table format's metadata.

**Compatibility.** Every new method has a default that keeps today's behavior, so a table format
built against 1.2 keeps working. Native tables see no change.

## **Test Plan**

The RFC is validated when:

- [ ] A test table format in Hudi's test sources receives each callback in the action table above,
  for copy-on-write and merge-on-read tables, with the Java and Spark writers.
- [ ] A table whose format name is not on the classpath fails to load, and a table created with
  format settings keeps them for every later writer.
- [ ] Format meta columns keep their values across merge, small-file packing, compaction and
  clustering, and every rewrite path that cannot carry them refuses to run.
- [ ] A table format's write defaults apply when the user leaves a setting unset, and its validation
  refuses an unsupported config before any instant is requested.
- [ ] The Apache XTable plugin's integration tests read the same rows through the table format and
  through Hudi after every action.
