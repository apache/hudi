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
and validation supplied by the table format. It also states that the table format is fixed at table
creation and records the alternatives considered. Merge-on-read tables are covered by
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
- **No secrets under the prefix.** Everything under `hoodie.table.format.` is written in plain text to
  `hoodie.properties`, which anyone who can read the table can read. A table format takes credentials
  and tokens, such as those of its catalog, from settings outside this prefix or from the environment,
  and its documentation says so.
- **The name resolves or the table fails to load.** The table format name is matched ignoring case
  (open PR #20936).
  A name that matches no table format on the classpath fails the table load instead of falling back
  to the native format, since a writer that ignores the table format would complete instants the
  table format never records. This holds for every reader and writer that loads the table, including
  catalog sync, the CLI and query engines: one without the table format's jar used to read the table
  as a native table and now fails until the jar is on its classpath. The release notes state this,
  together with the case-insensitive match, under which a name written in another case now resolves
  to the table format instead of the native format.

### Table format lifecycle

The table format is fixed when the table is created: `hoodie.table.format` is written to
`hoodie.properties` at creation and every meta client loads it from there. Switching the table format
of an existing table is not supported, in either direction. Switching to the native format would
expose instants that completed in Hudi but that the table format never recorded, and switching from
it would need the table format's metadata built from files and instants it never saw; converting an
existing table needs its own design. Today a write that names a different `hoodie.table.format` than
the table's is silently ignored; a planned change makes such a write fail fast.

### Actions the table format is notified of

Every Hudi action that changes the table notifies the table format once the action is complete in
the Hudi timeline (a rollback also before it), so the table format's metadata never lags behind a
file rewrite. The instant passed is the completed instant of the Hudi timeline, so a table service
arrives under the action it completes as. Status is **shipped** (released, or merged for 1.3.0),
**open PR** (in review), or **proposed** (designed here, not built).

| Hudi action | Callback | Instant passed | Status |
|---|---|---|---|
| Write, copy-on-write | `commit` | completed `commit` | shipped, 1.1 |
| Write, merge-on-read | `commit` | completed `deltacommit` | shipped, 1.1; RFC-114 writes: open PR #20167 |
| Insert overwrite, clustering | `commit` | completed `replacecommit` | shipped, 1.1 |
| Compaction | `commit`, operation type `COMPACT` | completed `commit` | shipped, 1.3.0 (#20877) |
| Log compaction | `commit`, operation type `LOG_COMPACT` | completed `deltacommit` | shipped, 1.3.0 (#20877) |
| Clean | `clean` | completed `clean` | shipped, 1.1 |
| Archival | `archive` | the archived instants | shipped, 1.1 |
| Rollback, copy-on-write | `rollback` before Hudi reverts, `completedRollback` after | instant rolled back; completed `rollback` | shipped, 1.1 |
| Rollback, merge-on-read | `rollback` before Hudi reverts | instant rolled back | open PR #20167 |
| Rollback, merge-on-read | `completedRollback` after | completed `rollback` | shipped, 1.1 |
| Savepoint | `savepoint` | completed `savepoint` | shipped, 1.1 |
| Restore | `rollback` per completed instant it reverts, no `completedRollback`; then `restore` | instant rolled back; completed `restore` | shipped, 1.1; per-instant `rollback` on merge-on-read: open PR #20167 |
| Table creation, client start | table config defaults, write config defaults, validation | none | proposed |

`rollback` runs only for an instant that is complete in the table format's timeline. An instant that
the table format's timeline shows as inflight while Hudi's timeline shows it completed gets no
`rollback`: Hudi finds it in its own timeline by requested time, reverts it there, and calls
`completedRollback` once the rollback completes.

### Metadata:

The Hudi metadata table and the table format's metadata coexist. The table format's metadata serves
readers of the table format; the Hudi metadata table keeps serving Hudi's own writers and readers,
including its indexes (record index, secondary index, column and partition stats, bloom filters).

- **The metadata factory decides.** The table format's metadata factory returns the Hudi metadata
  table when the writer has enabled it, as the native format does, and the table format's own
  implementation of `HoodieTableMetadata` otherwise. Replacing the metadata table outright would drop
  Hudi's indexes, which are a large part of what Hudi's writers bring to a table of another format.
- **The table format decides which files belong to the table.** A table of another format can hold
  files Hudi did not write. The file-system view of a non-native table ignores data files whose
  names Hudi did not produce. The target shape is for the file-system view to ask the table format's
  metadata which files belong to the table, so the decision sits in one place.

### Timeline: 

The timeline implementation ensures the external table format's (table format that is plugged-in) metadata state (e:g commit status) is the source of truth for all operations. 

A Hudi instant that is complete in the Hudi timeline but not recorded by the table format (the
process died between the two steps of the commit protocol) is pending in the timeline the table
format provides, and gets rolled back like any failed write.

### Conflict Resolution

Hudi already provides configurable support to plugin different strategies for identifying if two concurrent operations are conflicting at the file level. The 
external pluggable formats may not have such fine-level granularity. The external pluggable formats needs to implement the strategy based on its metadata.

### Locking
Hudi has pluggable lock provider supports. External formats will need to rely on catalog or other means for providing locking semantics. The External table format implementation needs to provide custom lock provider to adapt their locking mechanisms to Hudi's LockProvider interface so that all write and table-service operations can concurrently operate. 

The lock provider and conflict resolution hooks are not part of the interface that shipped in 1.1 and
1.2; a later revision designs them together with writers that commit to the table format outside Hudi.

### Rollbacks
External Table format implementation  needs to rollback failed writes and also be able to also restore to previous commits. The external plugin implementation needs to keep enough states to be able to rollback.

A table format whose history only moves forward can record a rollback or a restore as a new state
whose files equal those of the target instant, instead of moving its history back. Readers of the
table format then see the restored data as the latest state.

### Format meta columns

A table format may keep per-row metadata in its data files that it derives from a row's position in
the file or from the write that produced the row, such as row lineage. When Hudi rewrites a base file
(merge, small-file packing, compaction, clustering), the rows it carries over land at new positions,
so the table format loses those values unless Hudi carries them.

- **Declaration.** The table format declares its format meta columns. Each column gives its field (a
  nullable long or int), the value a row gets when it is written as new or changed, the value a
  changed version of an existing row gets (by default the same as a new row, or the table format can
  keep the replaced row's value), and a resolver that returns the value of a carried-over row from its
  position in the base file and its stored value.
- **The rule of the Hudi meta fields.** A row carried over unchanged keeps its value; a new or changed
  row gets the declared value of the write. For compaction and clustering the write is the compaction
  or clustering instant.
- **Storage.** The columns are stored in base files after the table columns. They are not columns of
  the table: they stay out of the table schema, the commit metadata and the rows a Hudi reader returns.
- **Fail closed.** A rewrite path that cannot carry the columns over refuses to run on a table whose
  format declares them, instead of silently dropping the values.
- **Native format.** The native format declares none, so nothing changes for native tables.

- **Constraints on the table format.** The declared names are reserved: a table column must not take
  one, or every rewrite fails its name check. Since a custom write handle or clustering strategy can
  bypass the refusals, a table format that declares columns should verify at commit that the base
  files the commit wrote carry them. Flink copy-on-write upserts, Flink clustering and Flink
  merge-on-read compaction refuse such a table, so it is written with the Spark or Java writer.

A row that an update moves to another file group (the update as delete and insert mode of RFC-114) is
an update of that row and keeps its value as an in-place update does, getting `valueOnUpdate` of
the value resolved at its old position. The writer will carry that value in a follow-up once the RFC-114 mode (#20167) and format meta columns (#20883) have merged. Until it lands, whichever
of the two merges second makes the RFC-114 mode refuse a table format that declares columns.

### Write configuration defaults and validation

A table of another format often needs write settings a native table does not: the update as delete
and insert mode of RFC-114 on merge-on-read tables, small-file packing off, record positions on, a
specific Parquet list layout, and refusals for features the table format cannot represent (for
example schema evolution on read, or updates on a merge-on-read table without a record key). Today a
user has to know and set each of them, and a writer that misses one can break the table format's
invariants.

The table format supplies them:

- **Table defaults at creation.** When a table is created with a non-native format, the table format
  returns table config entries to store with the table, so a second writer or a separate table
  service job cannot omit them. The RFC-114 mode stays a write config (see
  [Alternatives considered](#alternatives-considered)); a table format that needs it supplies it as a
  write default and refuses a client that turns it off.
- **Write defaults.** When a write client or a table service client starts on the table, the table
  format returns write config defaults. They apply to settings the user did not set.
- **Validation.** The table format validates the final write config against the table config and
  refuses an unsupported combination with a message that names the setting and the fix. Validation
  runs at table creation and at every client start, before any instant is requested.

The native format supplies no defaults and accepts every config, so native tables are unchanged.

### Layout:

The metadata corresponding to the table format (iceberg, ..) will be stored under .hoodie/ folder by default with location stored in hoodie.properties.

### Alternatives considered

- **Resolve format meta column values by record key instead of by position.** The values a table
  format derives, such as row lineage, follow from a row's position in the file it was written to, and
  the table format keeps no index from record key to value. A rewrite already reads the base file in
  order and knows each row's position, so resolving by position needs no extra lookup.
- **Let the table format fix up the values at commit instead of the write handles carrying them.** At
  commit the table format sees file-level write stats only; recovering per-row values would mean
  reading every rewritten base file and its predecessor again and joining them, and the values live in
  the data files, which would then have to be rewritten.
- **Keep the RFC-114 mode as a table property instead of a write config.** A table property makes every
  writer and compaction scheduler agree. It stays a write config so that it can be turned on or off for
  a table, including a native one, without a table config change. A scheduler without the mode is
  covered by a commit-time check (#20167): a delta commit in the mode fails when a compaction requested after it
  covers a file group it wrote positional deletes into.

## Integration

The end to end integration is shown in the below diagram
![integration](flow_and_arch.jpg)

The key points here are:

1. Hudi Writers continue to use the native file system management. This means the way Hudi organizes files as file-groups and file-slices and Hudi's index for writing will continue to be the same.
2. Hudi will continue to record actions in timeline (.hoodie/timeline).
3. The external table format will be configured as part of table properties to ensure all writers can recognize it.
4. When each of Hudi's actions (write, clean, rollback,..) complete, Hudi will call the external table format plugin. It is the plugin's responsibility to record necessary metadata for external format readers to query this table. 
5. Hudi will use the external table format plugin's timeline implementation which uses the state stored in its metadata as the source of truth. For example, an iceberg plugin needs to create a manifest file when action completes and provides a timeline that honors the state in its snapshot files. Hudi uses this timeline to determine what actions completed and what failed (for rollback). 
6. The external plugin also needs to provide LockProvider and Conflict Resolution implementations that allows the concurrency controls supported by the external table format to be exercised (for example: optimistic concurrency control).
7. The plugin also provides capability to allow its metadata to be used by the Hudi writer.
8. The external table format's metadata along with Hudi's data files combined allows the table to be read as if it is written directly in external table format.


## Limitations

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

**Step 3.** Write configuration defaults and validation from the table format, so a table format that
needs the RFC-114 mode supplies it to every writer and refuses a writer that turns it off.

**Step 4.** Lock provider and conflict resolution from the table format, and writers outside Hudi,
in a later revision.

**Existing tables.** Switching the table format of an existing table is not supported (see
[Table format lifecycle](#table-format-lifecycle)).

**Compatibility.** Every new method has a default that keeps today's behavior, so a table format
built against 1.2 compiles and loads. It does receive callbacks it did not get before: `commit` for
compaction and log compaction, told apart from writes by the commit metadata's operation type, and
`rollback` for merge-on-read rollbacks. Native tables see no change.

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
