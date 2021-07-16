Configurations
==============
This page covers the different ways of configuring your job to write/read Hudi tables. 
At a high level, you can control behaviour at few levels.

## Consistency Guard Configurations {#consistency-guard-configurations}

The consistency guard relevant config options.

### hoodie.optimistic.consistency.guard.sleep_time_ms {#hoodie.optimistic.consistency.guard.sleep_time_ms}

Default Value: 500
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.consistency.check.max_interval_ms {#hoodie.consistency.check.max_interval_ms}

Default Value: 20000
Since Version: 0.5.0
Type of Value: class java.lang.Number


### hoodie.consistency.check.enabled {#hoodie.consistency.check.enabled}
Enabled to handle S3 eventual consistency issue. This property is no longer required since S3 is now strongly consistent. Will be removed in the future releases.
Default Value: false
Since Version: 0.5.0
Type of Value: class java.lang.Object


### hoodie.consistency.check.initial_interval_ms {#hoodie.consistency.check.initial_interval_ms}

Default Value: 400
Since Version: 0.5.0
Type of Value: class java.lang.Number


### hoodie.consistency.check.max_checks {#hoodie.consistency.check.max_checks}

Default Value: 6
Since Version: 0.5.0
Type of Value: class java.lang.Number


### _hoodie.optimistic.consistency.guard.enable {#_hoodie.optimistic.consistency.guard.enable}

Default Value: true
Since Version: 0.6.0
Type of Value: class java.lang.Object


## Write Configurations {#write-configurations}

The datasource can be configured by passing the below options. These options are useful when writing tables.

### hoodie.embed.timeline.server.reuse.enabled {#hoodie.embed.timeline.server.reuse.enabled}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.datasource.write.keygenerator.class {#hoodie.datasource.write.keygenerator.class}
Key generator class, that implements will extract the key out of incoming Row object
Default Value: _


### hoodie.bulkinsert.shuffle.parallelism {#hoodie.bulkinsert.shuffle.parallelism}
Bulk insert is meant to be used for large initial imports and this parallelism determines the initial number of files in your table. Tune this to achieve a desired optimal size during initial import.
Default Value: 1500
Type of Value: class java.lang.Object


### hoodie.rollback.parallelism {#hoodie.rollback.parallelism}
Determines the parallelism for rollback of commits.
Default Value: 100
Type of Value: class java.lang.Object


### hoodie.write.schema {#hoodie.write.schema}

Default Value: _


### hoodie.timeline.layout.version {#hoodie.timeline.layout.version}

Default Value: _


### hoodie.avro.schema.validate {#hoodie.avro.schema.validate}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.bulkinsert.sort.mode {#hoodie.bulkinsert.sort.mode}
Sorting modes to use for sorting records for bulk insert. This is leveraged when user defined partitioner is not configured. Default is GLOBAL_SORT. Available values are - GLOBAL_SORT: this ensures best file sizes, with lowest memory overhead at cost of sorting. PARTITION_SORT: Strikes a balance by only sorting within a partition, still keeping the memory overhead of writing lowest and best effort file sizing. NONE: No sorting. Fastest and matches spark.write.parquet() in terms of number of files, overheads
Default Value: GLOBAL_SORT
Type of Value: class java.lang.Object


### hoodie.upsert.shuffle.parallelism {#hoodie.upsert.shuffle.parallelism}
Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data
Default Value: 1500
Type of Value: class java.lang.Object


### hoodie.embed.timeline.server.gzip {#hoodie.embed.timeline.server.gzip}

Default Value: true
Type of Value: class java.lang.Object


### hoodie.fail.on.timeline.archiving {#hoodie.fail.on.timeline.archiving}

Default Value: true
Type of Value: class java.lang.Object


### hoodie.write.status.storage.level {#hoodie.write.status.storage.level}
HoodieWriteClient.insert and HoodieWriteClient.upsert returns a persisted RDD[WriteStatus], this is because the Client can choose to inspect the WriteStatus and choose and commit or not based on the failures. This is a configuration for the storage level for this RDD
Default Value: MEMORY_AND_DISK_SER
Type of Value: class java.lang.Object


### _.hoodie.allow.multi.write.on.same.instant {#_.hoodie.allow.multi.write.on.same.instant}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.embed.timeline.server.port {#hoodie.embed.timeline.server.port}

Default Value: 0
Type of Value: class java.lang.Object


### hoodie.consistency.check.max_interval_ms {#hoodie.consistency.check.max_interval_ms}
Max interval time for consistency check
Default Value: 300000
Type of Value: class java.lang.Number


### hoodie.write.concurrency.mode {#hoodie.write.concurrency.mode}
Enable different concurrency support
Default Value: SINGLE_WRITER
Type of Value: class java.lang.Object


### hoodie.bulkinsert.schema.ddl {#hoodie.bulkinsert.schema.ddl}

Default Value: _


### hoodie.delete.shuffle.parallelism {#hoodie.delete.shuffle.parallelism}
This parallelism is Used for “delete” operation while deduping or repartioning.
Default Value: 1500
Type of Value: class java.lang.Object


### hoodie.embed.timeline.server {#hoodie.embed.timeline.server}

Default Value: true
Type of Value: class java.lang.Object


### hoodie.base.path {#hoodie.base.path}
Base DFS path under which all the data partitions are created. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under the base directory.
Default Value: _


### hoodie.avro.schema {#hoodie.avro.schema}
This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update.
Default Value: _


### hoodie.table.name {#hoodie.table.name}
Table name that will be used for registering with Hive. Needs to be same across runs.
Default Value: _


### hoodie.embed.timeline.server.async {#hoodie.embed.timeline.server.async}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.combine.before.upsert {#hoodie.combine.before.upsert}
Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS
Default Value: true
Type of Value: class java.lang.Object


### hoodie.embed.timeline.server.threads {#hoodie.embed.timeline.server.threads}

Default Value: -1
Type of Value: class java.lang.Object


### hoodie.consistency.check.max_checks {#hoodie.consistency.check.max_checks}
Maximum number of checks, for consistency of written data. Will wait upto 256 Secs
Default Value: 7
Type of Value: class java.lang.Number


### hoodie.combine.before.delete {#hoodie.combine.before.delete}
Flag which first combines the input RDD and merges multiple partial records into a single record before deleting in DFS
Default Value: true
Type of Value: class java.lang.Object


### hoodie.datasource.write.keygenerator.type {#hoodie.datasource.write.keygenerator.type}
Type of build-in key generator, currently support SIMPLE, COMPLEX, TIMESTAMP, CUSTOM, NON_PARTITION, GLOBAL_DELETE
Default Value: SIMPLE
Type of Value: class java.lang.Object


### hoodie.write.buffer.limit.bytes {#hoodie.write.buffer.limit.bytes}

Default Value: 4194304
Type of Value: class java.lang.Object


### hoodie.bulkinsert.user.defined.partitioner.class {#hoodie.bulkinsert.user.defined.partitioner.class}
If specified, this class will be used to re-partition input records before they are inserted.
Default Value: _


### hoodie.client.heartbeat.interval_in_ms {#hoodie.client.heartbeat.interval_in_ms}

Default Value: 60000
Type of Value: class java.lang.Number


### Key: 'hoodie.avro.schema' , default: null description: This is the current reader avro schema for the table. This is a string of the entire schema. HoodieWriteClient uses this schema to pass on to implementations of HoodieRecordPayload to convert from the source format to avro record. This is also used when re-writing records during an update. since version: version is not defined deprecated after: version is not defined).externalTransformation {#key:-'hoodie.avro.schema'-,-default:-null-description:-this-is-the-current-reader-avro-schema-for-the-table.-this-is-a-string-of-the-entire-schema.-hoodiewriteclient-uses-this-schema-to-pass-on-to-implementations-of-hoodierecordpayload-to-convert-from-the-source-format-to-avro-record.-this-is-also-used-when-re-writing-records-during-an-update.-since-version:-version-is-not-defined-deprecated-after:-version-is-not-defined).externaltransformation}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.client.heartbeat.tolerable.misses {#hoodie.client.heartbeat.tolerable.misses}

Default Value: 2
Type of Value: class java.lang.Number


### hoodie.auto.commit {#hoodie.auto.commit}
Should HoodieWriteClient autoCommit after insert and upsert. The client can choose to turn off auto-commit and commit on a “defined success condition”
Default Value: true
Type of Value: class java.lang.Object


### hoodie.combine.before.insert {#hoodie.combine.before.insert}
Flag which first combines the input RDD and merges multiple partial records into a single record before inserting or updating in DFS
Default Value: false
Type of Value: class java.lang.Object


### hoodie.writestatus.class {#hoodie.writestatus.class}

Default Value: org.apache.hudi.client.WriteStatus
Type of Value: class java.lang.Object


### hoodie.markers.delete.parallelism {#hoodie.markers.delete.parallelism}
Determines the parallelism for deleting marker files.
Default Value: 100
Type of Value: class java.lang.Object


### hoodie.consistency.check.initial_interval_ms {#hoodie.consistency.check.initial_interval_ms}
Time between successive attempts to ensure written data's metadata is consistent on storage
Default Value: 2000
Type of Value: class java.lang.Number


### hoodie.merge.data.validation.enabled {#hoodie.merge.data.validation.enabled}
Data validation check performed during merges before actual commits
Default Value: false
Type of Value: class java.lang.Object


### hoodie.datasource.write.payload.class {#hoodie.datasource.write.payload.class}
Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting. This will render any value set for PRECOMBINE_FIELD_OPT_VAL in-effective
Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
Type of Value: class java.lang.Object


### hoodie.insert.shuffle.parallelism {#hoodie.insert.shuffle.parallelism}
Once data has been initially imported, this parallelism controls initial parallelism for reading input records. Ensure this value is high enough say: 1 partition for 1 GB of input data
Default Value: 1500
Type of Value: class java.lang.Object


### hoodie.datasource.write.precombine.field {#hoodie.datasource.write.precombine.field}
Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)
Default Value: ts
Type of Value: class java.lang.Object


### hoodie.write.meta.key.prefixes {#hoodie.write.meta.key.prefixes}
Comma separated metadata key prefixes to override from latest commit during overlapping commits via multi writing
Default Value: 
Type of Value: class java.lang.Object


### hoodie.finalize.write.parallelism {#hoodie.finalize.write.parallelism}

Default Value: 1500
Type of Value: class java.lang.Object


### hoodie.merge.allow.duplicate.on.inserts {#hoodie.merge.allow.duplicate.on.inserts}
Allow duplicates with inserts while merging with existing records
Default Value: false
Type of Value: class java.lang.Object


### hoodie.rollback.using.markers {#hoodie.rollback.using.markers}
Enables a more efficient mechanism for rollbacks based on the marker files generated during the writes. Turned off by default.
Default Value: false
Type of Value: class java.lang.Object


## Payload Configurations {#payload-configurations}

Payload related configs. This config can be leveraged by payload implementations to determine their business logic.

### hoodie.payload.event.time.field {#hoodie.payload.event.time.field}
Property for payload event time field
Default Value: ts
Type of Value: class java.lang.Object


### hoodie.payload.ordering.field {#hoodie.payload.ordering.field}
Property to hold the payload ordering field name
Default Value: ts
Type of Value: class java.lang.Object


## HBase Index Configs {#hbase-index-configs}

Configurations that control indexing behavior (when HBase based indexing is enabled), which tags incoming records as either inserts or updates to older records.

### hoodie.index.hbase.qps.fraction {#hoodie.index.hbase.qps.fraction}
Property to set the fraction of the global share of QPS that should be allocated to this job. Let's say there are 3 jobs which have input size in terms of number of rows required for HbaseIndexing as x, 2x, 3x respectively. Then this fraction for the jobs would be (0.17) 1/6, 0.33 (2/6) and 0.5 (3/6) respectively. Default is 50%, which means a total of 2 jobs can run using HbaseIndex without overwhelming Region Servers.
Default Value: 0.5
Type of Value: class java.lang.Number


### hoodie.index.hbase.zknode.path {#hoodie.index.hbase.zknode.path}
Only applies if index type is HBASE. This is the root znode that will contain all the znodes created/used by HBase
Default Value: _


### hoodie.index.hbase.zkpath.qps_root {#hoodie.index.hbase.zkpath.qps_root}

Default Value: /QPS_ROOT
Type of Value: class java.lang.Object


### hoodie.index.hbase.put.batch.size {#hoodie.index.hbase.put.batch.size}

Default Value: 100
Type of Value: class java.lang.Number


### hoodie.index.hbase.dynamic_qps {#hoodie.index.hbase.dynamic_qps}
Property to decide if HBASE_QPS_FRACTION_PROP is dynamically calculated based on volume
Default Value: false
Type of Value: class java.lang.Object


### hoodie.index.hbase.max.qps.per.region.server {#hoodie.index.hbase.max.qps.per.region.server}
Property to set maximum QPS allowed per Region Server. This should be same across various jobs. This is intended to
 limit the aggregate QPS generated across various jobs to an Hbase Region Server. It is recommended to set this
 value based on global indexing throughput needs and most importantly, how much the HBase installation in use is
 able to tolerate without Region Servers going down.
Default Value: 1000
Type of Value: class java.lang.Number


### hoodie.index.hbase.zk.session_timeout_ms {#hoodie.index.hbase.zk.session_timeout_ms}

Default Value: 60000
Type of Value: class java.lang.Number


### hoodie.index.hbase.sleep.ms.for.get.batch {#hoodie.index.hbase.sleep.ms.for.get.batch}

Default Value: _


### hoodie.index.hbase.min.qps.fraction {#hoodie.index.hbase.min.qps.fraction}
Min for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads
Default Value: _


### hoodie.index.hbase.desired_puts_time_in_secs {#hoodie.index.hbase.desired_puts_time_in_secs}

Default Value: 600
Type of Value: class java.lang.Number


### hoodie.hbase.index.update.partition.path {#hoodie.hbase.index.update.partition.path}
Only applies if index type is HBASE. When an already existing record is upserted to a new partition compared to whats in storage, this config when set, will delete old record in old paritition and will insert it as new record in new partition.
Default Value: false
Type of Value: class java.lang.Object


### hoodie.index.hbase.qps.allocator.class {#hoodie.index.hbase.qps.allocator.class}
Property to set which implementation of HBase QPS resource allocator to be used
Default Value: org.apache.hudi.index.hbase.DefaultHBaseQPSResourceAllocator
Type of Value: class java.lang.Object


### hoodie.index.hbase.sleep.ms.for.put.batch {#hoodie.index.hbase.sleep.ms.for.put.batch}

Default Value: _


### hoodie.index.hbase.rollback.sync {#hoodie.index.hbase.rollback.sync}
When set to true, the rollback method will delete the last failed task index. The default value is false. Because deleting the index will add extra load on the Hbase cluster for each rollback
Default Value: false
Type of Value: class java.lang.Object


### hoodie.index.hbase.zkquorum {#hoodie.index.hbase.zkquorum}
Only applies if index type is HBASE. HBase ZK Quorum url to connect to
Default Value: _


### hoodie.index.hbase.get.batch.size {#hoodie.index.hbase.get.batch.size}

Default Value: 100
Type of Value: class java.lang.Number


### hoodie.index.hbase.zk.connection_timeout_ms {#hoodie.index.hbase.zk.connection_timeout_ms}

Default Value: 15000
Type of Value: class java.lang.Number


### hoodie.index.hbase.put.batch.size.autocompute {#hoodie.index.hbase.put.batch.size.autocompute}
Property to set to enable auto computation of put batch size
Default Value: false
Type of Value: class java.lang.Object


### hoodie.index.hbase.zkport {#hoodie.index.hbase.zkport}
Only applies if index type is HBASE. HBase ZK Quorum port to connect to
Default Value: _


### hoodie.index.hbase.max.qps.fraction {#hoodie.index.hbase.max.qps.fraction}
Max for HBASE_QPS_FRACTION_PROP to stabilize skewed volume workloads
Default Value: _


### hoodie.index.hbase.table {#hoodie.index.hbase.table}
Only applies if index type is HBASE. HBase Table name to use as the index. Hudi stores the row_key and [partition_path, fileID, commitTime] mapping in the table
Default Value: _


## Write commit HTTP callback configs {#write-commit-http-callback-configs}

Controls HTTP callback behavior on write commit. Exception will be thrown if user enabled the callback service and errors occurred during the process of callback.

### hoodie.write.commit.callback.http.api.key {#hoodie.write.commit.callback.http.api.key}
Http callback API key. hudi_write_commit_http_callback by default
Default Value: hudi_write_commit_http_callback
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.write.commit.callback.on {#hoodie.write.commit.callback.on}
Turn callback on/off. off by default.
Default Value: false
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.write.commit.callback.http.url {#hoodie.write.commit.callback.http.url}
Callback host to be sent along with callback messages
Default Value: _
Since Version: 0.6.0


### hoodie.write.commit.callback.http.timeout.seconds {#hoodie.write.commit.callback.http.timeout.seconds}
Callback timeout in seconds. 3 by default
Default Value: 3
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.write.commit.callback.class {#hoodie.write.commit.callback.class}
Full path of callback class and must be a subclass of HoodieWriteCommitCallback class, org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback by default
Default Value: org.apache.hudi.callback.impl.HoodieWriteCommitHttpCallback
Since Version: 0.6.0
Type of Value: class java.lang.Object


## Metrics Configurations for Datadog reporter {#metrics-configurations-for-datadog-reporter}

Enables reporting on Hudi metrics using the Datadog reporter type. Hudi publishes metrics on every commit, clean, rollback etc.

### hoodie.metrics.datadog.metric.tags {#hoodie.metrics.datadog.metric.tags}
Datadog metric tags (comma-delimited) to be sent along with metrics data.
Default Value: _
Since Version: 0.6.0


### hoodie.metrics.datadog.api.key.supplier {#hoodie.metrics.datadog.api.key.supplier}
Datadog API key supplier to supply the API key at runtime. This will take effect if hoodie.metrics.datadog.api.key is not set.
Default Value: _
Since Version: 0.6.0


### hoodie.metrics.datadog.metric.prefix {#hoodie.metrics.datadog.metric.prefix}
Datadog metric prefix to be prepended to each metric name with a dot as delimiter. For example, if it is set to foo, foo. will be prepended.
Default Value: _
Since Version: 0.6.0


### hoodie.metrics.datadog.api.timeout.seconds {#hoodie.metrics.datadog.api.timeout.seconds}
Datadog API timeout in seconds. Default to 3.
Default Value: 3
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.metrics.datadog.report.period.seconds {#hoodie.metrics.datadog.report.period.seconds}
Datadog report period in seconds. Default to 30.
Default Value: 30
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.metrics.datadog.metric.host {#hoodie.metrics.datadog.metric.host}
Datadog metric host to be sent along with metrics data.
Default Value: _
Since Version: 0.6.0


### hoodie.metrics.datadog.api.key.skip.validation {#hoodie.metrics.datadog.api.key.skip.validation}
Before sending metrics via Datadog API, whether to skip validating Datadog API key or not. Default to false.
Default Value: false
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.metrics.datadog.api.site {#hoodie.metrics.datadog.api.site}
Datadog API site: EU or US
Default Value: _
Since Version: 0.6.0


### hoodie.metrics.datadog.api.key {#hoodie.metrics.datadog.api.key}
Datadog API key
Default Value: _
Since Version: 0.6.0


## Locks Configurations {#locks-configurations}

Configs that control locking mechanisms if WRITE_CONCURRENCY_MODE_PROP is set to optimistic_concurrency_control

### hoodie.write.lock.zookeeper.url {#hoodie.write.lock.zookeeper.url}
Set the list of comma separated servers to connect to
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.max_wait_time_ms_between_retry {#hoodie.write.lock.max_wait_time_ms_between_retry}
Parameter used in the exponential backoff retry policy. Stands for the maximum amount of time to wait between retries by lock provider client
Default Value: 5000
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.filesystem.path {#hoodie.write.lock.filesystem.path}

Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.hivemetastore.uris {#hoodie.write.lock.hivemetastore.uris}

Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.wait_time_ms_between_retry {#hoodie.write.lock.wait_time_ms_between_retry}
Parameter used in the exponential backoff retry policy. Stands for the Initial amount of time to wait between retries by lock provider client
Default Value: 5000
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.wait_time_ms {#hoodie.write.lock.wait_time_ms}

Default Value: 60000
Since Version: 0.8.0
Type of Value: class java.lang.Number


### hoodie.write.lock.conflict.resolution.strategy {#hoodie.write.lock.conflict.resolution.strategy}
Lock provider class name, this should be subclass of org.apache.hudi.client.transaction.ConflictResolutionStrategy
Default Value: org.apache.hudi.client.transaction.SimpleConcurrentFileWritesConflictResolutionStrategy
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.num_retries {#hoodie.write.lock.num_retries}
Maximum number of times to retry by lock provider client
Default Value: 3
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.zookeeper.connection_timeout_ms {#hoodie.write.lock.zookeeper.connection_timeout_ms}
How long to wait when connecting to ZooKeeper before considering the connection a failure
Default Value: 15000
Since Version: 0.8.0
Type of Value: class java.lang.Number


### hoodie.write.lock.client.num_retries {#hoodie.write.lock.client.num_retries}
Maximum number of times to retry to acquire lock additionally from the hudi client
Default Value: 0
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.hivemetastore.database {#hoodie.write.lock.hivemetastore.database}
The Hive database to acquire lock against
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.zookeeper.port {#hoodie.write.lock.zookeeper.port}
The connection port to be used for Zookeeper
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.client.wait_time_ms_between_retry {#hoodie.write.lock.client.wait_time_ms_between_retry}
Amount of time to wait between retries from the hudi client
Default Value: 10000
Since Version: 0.8.0
Type of Value: class java.lang.Object


### hoodie.write.lock.zookeeper.lock_key {#hoodie.write.lock.zookeeper.lock_key}
Key name under base_path at which to create a ZNode and acquire lock. Final path on zk will look like base_path/lock_key. We recommend setting this to the table name
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.zookeeper.base_path {#hoodie.write.lock.zookeeper.base_path}
The base path on Zookeeper under which to create a ZNode to acquire the lock. This should be common for all jobs writing to the same table
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.zookeeper.session_timeout_ms {#hoodie.write.lock.zookeeper.session_timeout_ms}
How long to wait after losing a connection to ZooKeeper before the session is expired
Default Value: 60000
Since Version: 0.8.0
Type of Value: class java.lang.Number


### hoodie.write.lock.hivemetastore.table {#hoodie.write.lock.hivemetastore.table}
The Hive table under the hive database to acquire lock against
Default Value: _
Since Version: 0.8.0


### hoodie.write.lock.provider {#hoodie.write.lock.provider}
Lock provider class name, user can provide their own implementation of LockProvider which should be subclass of org.apache.hudi.common.lock.LockProvider
Default Value: org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider
Since Version: 0.8.0
Type of Value: class java.lang.Object


## Compaction Configs {#compaction-configs}

Configurations that control compaction (merging of log files onto a new parquet base file), cleaning (reclamation of older/unused file groups).

### hoodie.compact.inline.trigger.strategy {#hoodie.compact.inline.trigger.strategy}

Default Value: NUM_COMMITS
Type of Value: class java.lang.Object


### hoodie.cleaner.fileversions.retained {#hoodie.cleaner.fileversions.retained}

Default Value: 3
Type of Value: class java.lang.Object


### hoodie.cleaner.policy.failed.writes {#hoodie.cleaner.policy.failed.writes}
Cleaning policy for failed writes to be used. Hudi will delete any files written by failed writes to re-claim space. Choose to perform this rollback of failed writes eagerly before every writer starts (only supported for single writer) or lazily by the cleaner (required for multi-writers)
Default Value: EAGER
Type of Value: class java.lang.Object


### hoodie.parquet.small.file.limit {#hoodie.parquet.small.file.limit}
Upsert uses this file size to compact new data onto existing files. By default, treat any file <= 100MB as a small file.
Default Value: 104857600
Type of Value: class java.lang.Object


### hoodie.keep.max.commits {#hoodie.keep.max.commits}
Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.
Default Value: 30
Type of Value: class java.lang.Object


### hoodie.compaction.lazy.block.read {#hoodie.compaction.lazy.block.read}
When a CompactedLogScanner merges all log files, this config helps to choose whether the logblocks should be read lazily or not. Choose true to use I/O intensive lazy block reading (low memory usage) or false for Memory intensive immediate block read (high memory usage)
Default Value: false
Type of Value: class java.lang.Object


### hoodie.clean.automatic {#hoodie.clean.automatic}
Should cleanup if there is anything to cleanup immediately after the commit
Default Value: true
Type of Value: class java.lang.Object


### hoodie.commits.archival.batch {#hoodie.commits.archival.batch}
This controls the number of commit instants read in memory as a batch and archived together.
Default Value: 10
Type of Value: class java.lang.Object


### hoodie.cleaner.parallelism {#hoodie.cleaner.parallelism}
Increase this if cleaning becomes slow.
Default Value: 200
Type of Value: class java.lang.Object


### hoodie.cleaner.incremental.mode {#hoodie.cleaner.incremental.mode}

Default Value: true
Type of Value: class java.lang.Object


### hoodie.cleaner.delete.bootstrap.base.file {#hoodie.cleaner.delete.bootstrap.base.file}
Set true to clean bootstrap source files when necessary
Default Value: false
Type of Value: class java.lang.Object


### hoodie.copyonwrite.insert.split.size {#hoodie.copyonwrite.insert.split.size}
Number of inserts, that will be put each partition/bucket for writing. The rationale to pick the insert parallelism is the following. Writing out 100MB files, with at least 1kb records, means 100K records per file. we just over provision to 500K.
Default Value: 500000
Type of Value: class java.lang.Object


### hoodie.compaction.strategy {#hoodie.compaction.strategy}
Compaction strategy decides which file groups are picked up for compaction during each compaction run. By default. Hudi picks the log file with most accumulated unmerged data
Default Value: org.apache.hudi.table.action.compact.strategy.LogFileSizeBasedCompactionStrategy
Type of Value: class java.lang.Object


### hoodie.compaction.target.io {#hoodie.compaction.target.io}
Amount of MBs to spend during compaction run for the LogFileSizeBasedCompactionStrategy. This value helps bound ingestion latency while compaction is run inline mode.
Default Value: 512000
Type of Value: class java.lang.Object


### hoodie.compaction.payload.class {#hoodie.compaction.payload.class}
This needs to be same as class used during insert/upserts. Just like writing, compaction also uses the record payload class to merge records in the log against each other, merge again with the base file and produce the final record to be written after compaction.
Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
Type of Value: class java.lang.Object


### hoodie.cleaner.commits.retained {#hoodie.cleaner.commits.retained}
Number of commits to retain. So data will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much you can incrementally pull on this table
Default Value: 10
Type of Value: class java.lang.Object


### hoodie.record.size.estimation.threshold {#hoodie.record.size.estimation.threshold}
Hudi will use the previous commit to calculate the estimated record size by totalBytesWritten/totalRecordsWritten. If the previous commit is too small to make an accurate estimation, Hudi will search commits in the reverse order, until find a commit has totalBytesWritten larger than (PARQUET_SMALL_FILE_LIMIT_BYTES * RECORD_SIZE_ESTIMATION_THRESHOLD)
Default Value: 1.0
Type of Value: class java.lang.Object


### hoodie.compaction.daybased.target.partitions {#hoodie.compaction.daybased.target.partitions}
Used by org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy to denote the number of latest partitions to compact during a compaction run.
Default Value: 10
Type of Value: class java.lang.Object


### hoodie.keep.min.commits {#hoodie.keep.min.commits}
Each commit is a small file in the .hoodie directory. Since DFS typically does not favor lots of small files, Hudi archives older commits into a sequential log. A commit is published atomically by a rename of the commit file.
Default Value: 20
Type of Value: class java.lang.Object


### hoodie.compaction.reverse.log.read {#hoodie.compaction.reverse.log.read}
HoodieLogFormatReader reads a logfile in the forward direction starting from pos=0 to pos=file_length. If this config is set to true, the Reader reads the logfile in reverse direction, from pos=file_length to pos=0
Default Value: false
Type of Value: class java.lang.Object


### hoodie.compact.inline {#hoodie.compact.inline}
When set to true, compaction is triggered by the ingestion itself, right after a commit/deltacommit action as part of insert/upsert/bulk_insert
Default Value: false
Type of Value: class java.lang.Object


### hoodie.clean.async {#hoodie.clean.async}
Only applies when #withAutoClean is turned on. When turned on runs cleaner async with writing.
Default Value: false
Type of Value: class java.lang.Object


### hoodie.copyonwrite.insert.auto.split {#hoodie.copyonwrite.insert.auto.split}
Config to control whether we control insert split sizes automatically based on average record sizes.
Default Value: true
Type of Value: class java.lang.Object


### hoodie.copyonwrite.record.size.estimate {#hoodie.copyonwrite.record.size.estimate}
The average record size. If specified, hudi will use this and not compute dynamically based on the last 24 commit’s metadata. No value set as default. This is critical in computing the insert parallelism and bin-packing inserts into small files. See above.
Default Value: 1024
Type of Value: class java.lang.Object


### hoodie.compact.inline.max.delta.commits {#hoodie.compact.inline.max.delta.commits}
Number of max delta commits to keep before triggering an inline compaction
Default Value: 5
Type of Value: class java.lang.Object


### hoodie.compact.inline.max.delta.seconds {#hoodie.compact.inline.max.delta.seconds}
Run a compaction when time elapsed > N seconds since last compaction
Default Value: 3600
Type of Value: class java.lang.Object


### hoodie.cleaner.policy {#hoodie.cleaner.policy}
Cleaning policy to be used. Hudi will delete older versions of parquet files to re-claim space. Any Query/Computation referring to this version of the file will fail. It is good to make sure that the data is retained for more than the maximum query execution time.
Default Value: KEEP_LATEST_COMMITS
Type of Value: class java.lang.Object


## File System View Storage Configurations {#file-system-view-storage-configurations}

Configurations that control the Filesystem view

### hoodie.filesystem.view.spillable.replaced.mem.fraction {#hoodie.filesystem.view.spillable.replaced.mem.fraction}

Default Value: 0.01
Type of Value: class java.lang.Number


### hoodie.filesystem.view.incr.timeline.sync.enable {#hoodie.filesystem.view.incr.timeline.sync.enable}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.filesystem.view.secondary.type {#hoodie.filesystem.view.secondary.type}

Default Value: MEMORY
Type of Value: java.lang.Enum<org.apache.hudi.common.table.view.FileSystemViewStorageType>


### hoodie.filesystem.view.spillable.compaction.mem.fraction {#hoodie.filesystem.view.spillable.compaction.mem.fraction}

Default Value: 0.8
Type of Value: class java.lang.Number


### hoodie.filesystem.view.spillable.mem {#hoodie.filesystem.view.spillable.mem}

Default Value: 104857600
Type of Value: class java.lang.Number


### hoodie.filesystem.view.spillable.clustering.mem.fraction {#hoodie.filesystem.view.spillable.clustering.mem.fraction}

Default Value: 0.01
Type of Value: class java.lang.Number


### hoodie.filesystem.view.remote.timeout.secs {#hoodie.filesystem.view.remote.timeout.secs}

Default Value: 300
Type of Value: class java.lang.Number


### hoodie.filesystem.view.spillable.dir {#hoodie.filesystem.view.spillable.dir}

Default Value: /tmp/view_map/
Type of Value: class java.lang.Object


### hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction {#hoodie.filesystem.view.spillable.bootstrap.base.file.mem.fraction}

Default Value: 0.05
Type of Value: class java.lang.Number


### hoodie.filesystem.view.remote.port {#hoodie.filesystem.view.remote.port}

Default Value: 26754
Type of Value: class java.lang.Number


### hoodie.filesystem.view.type {#hoodie.filesystem.view.type}

Default Value: MEMORY
Type of Value: java.lang.Enum<org.apache.hudi.common.table.view.FileSystemViewStorageType>


### hoodie.filesystem.view.remote.host {#hoodie.filesystem.view.remote.host}

Default Value: localhost
Type of Value: class java.lang.Object


### hoodie.filesystem.view.rocksdb.base.path {#hoodie.filesystem.view.rocksdb.base.path}

Default Value: /tmp/hoodie_timeline_rocksdb
Type of Value: class java.lang.Object


### hoodie.filesystem.remote.backup.view.enable {#hoodie.filesystem.remote.backup.view.enable}

Default Value: true
Type of Value: class java.lang.Object


## Metrics Configurations for Prometheus {#metrics-configurations-for-prometheus}

Enables reporting on Hudi metrics using Prometheus. Hudi publishes metrics on every commit, clean, rollback etc.

### hoodie.metrics.pushgateway.host {#hoodie.metrics.pushgateway.host}

Default Value: localhost
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.metrics.pushgateway.delete.on.shutdown {#hoodie.metrics.pushgateway.delete.on.shutdown}

Default Value: true
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.metrics.prometheus.port {#hoodie.metrics.prometheus.port}

Default Value: 9090
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.metrics.pushgateway.random.job.name.suffix {#hoodie.metrics.pushgateway.random.job.name.suffix}

Default Value: true
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.metrics.pushgateway.report.period.seconds {#hoodie.metrics.pushgateway.report.period.seconds}

Default Value: 30
Since Version: 0.6.0
Type of Value: class java.lang.Number


### hoodie.metrics.pushgateway.job.name {#hoodie.metrics.pushgateway.job.name}

Default Value: 
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.metrics.pushgateway.port {#hoodie.metrics.pushgateway.port}

Default Value: 9091
Since Version: 0.6.0
Type of Value: class java.lang.Number


## Table Configurations {#table-configurations}

Configurations on the Hoodie Table like type of ingestion, storage formats, hive table name etc Configurations are loaded from hoodie.properties, these properties are usually set during
 initializing a path as hoodie base path and never changes during the lifetime of a hoodie table.

### hoodie.table.partition.columns {#hoodie.table.partition.columns}
Partition path field. Value to be used at the partitionPath component of HoodieKey. Actual value ontained by invoking .toString()
Default Value: _


### hoodie.table.recordkey.fields {#hoodie.table.recordkey.fields}

Default Value: _


### hoodie.archivelog.folder {#hoodie.archivelog.folder}

Default Value: archived
Type of Value: class java.lang.Object


### hoodie.table.create.schema {#hoodie.table.create.schema}

Default Value: _


### hoodie.table.type {#hoodie.table.type}
The table type for the underlying data, for this write. This can’t change between writes.
Default Value: COPY_ON_WRITE
Type of Value: java.lang.Enum<org.apache.hudi.common.model.HoodieTableType>


### hoodie.table.base.file.format {#hoodie.table.base.file.format}

Default Value: PARQUET
Type of Value: java.lang.Enum<org.apache.hudi.common.model.HoodieFileFormat>


### hoodie.bootstrap.base.path {#hoodie.bootstrap.base.path}
Base path of the dataset that needs to be bootstrapped as a Hudi table
Default Value: _


### hoodie.table.name {#hoodie.table.name}
Table name that will be used for registering with Hive. Needs to be same across runs.
Default Value: _


### hoodie.table.version {#hoodie.table.version}

Default Value: ZERO
Type of Value: java.lang.Enum<org.apache.hudi.common.table.HoodieTableVersion>


### hoodie.table.precombine.field {#hoodie.table.precombine.field}
Field used in preCombining before actual write. When two records have the same key value, we will pick the one with the largest value for the precombine field, determined by Object.compareTo(..)
Default Value: _


### hoodie.bootstrap.index.class {#hoodie.bootstrap.index.class}

Default Value: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex
Type of Value: class java.lang.Object


### hoodie.table.log.file.format {#hoodie.table.log.file.format}

Default Value: HOODIE_LOG
Type of Value: java.lang.Enum<org.apache.hudi.common.model.HoodieFileFormat>


### hoodie.bootstrap.index.enable {#hoodie.bootstrap.index.enable}

Default Value: _


### hoodie.timeline.layout.version {#hoodie.timeline.layout.version}

Default Value: _


### hoodie.compaction.payload.class {#hoodie.compaction.payload.class}

Default Value: org.apache.hudi.common.model.OverwriteWithLatestAvroPayload
Type of Value: class java.lang.Object


## Memory Configurations {#memory-configurations}

Controls memory usage for compaction and merges, performed internally by Hudi.

### hoodie.memory.merge.fraction {#hoodie.memory.merge.fraction}
This fraction is multiplied with the user memory fraction (1 - spark.memory.fraction) to get a final fraction of heap space to use during merge
Default Value: 0.6
Type of Value: class java.lang.Object


### hoodie.memory.compaction.max.size {#hoodie.memory.compaction.max.size}
Property to set the max memory for compaction
Default Value: _


### hoodie.memory.spillable.map.path {#hoodie.memory.spillable.map.path}
Default file path prefix for spillable file
Default Value: /tmp/
Type of Value: class java.lang.Object


### hoodie.memory.compaction.fraction {#hoodie.memory.compaction.fraction}
HoodieCompactedLogScanner reads logblocks, converts records to HoodieRecords and then merges these log blocks and records. At any point, the number of entries in a log block can be less than or equal to the number of entries in the corresponding parquet file. This can lead to OOM in the Scanner. Hence, a spillable map helps alleviate the memory pressure. Use this config to set the max allowable inMemory footprint of the spillable map
Default Value: 0.6
Type of Value: class java.lang.Object


### hoodie.memory.writestatus.failure.fraction {#hoodie.memory.writestatus.failure.fraction}
Property to control how what fraction of the failed record, exceptions we report back to driver. Default is 10%. If set to 100%, with lot of failures, this can cause memory pressure, cause OOMs and mask actual data errors.
Default Value: 0.1
Type of Value: class java.lang.Number


### hoodie.memory.merge.max.size {#hoodie.memory.merge.max.size}
Property to set the max memory for merge
Default Value: 1073741824
Type of Value: class java.lang.Number


### hoodie.memory.dfs.buffer.max.size {#hoodie.memory.dfs.buffer.max.size}
Property to set the max memory for dfs inputstream buffer size
Default Value: 16777216
Type of Value: class java.lang.Number


## Index Configs {#index-configs}

Configurations that control indexing behavior, which tags incoming records as either inserts or updates to older records.

### hoodie.index.type {#hoodie.index.type}
Type of index to use. Default is Bloom filter. Possible options are [BLOOM | GLOBAL_BLOOM |SIMPLE | GLOBAL_SIMPLE | INMEMORY | HBASE]. Bloom filters removes the dependency on a external system and is stored in the footer of the Parquet Data Files
Default Value: _


### hoodie.bloom.index.use.treebased.filter {#hoodie.bloom.index.use.treebased.filter}
Only applies if index type is BLOOM. When true, interval tree based file pruning optimization is enabled. This mode speeds-up file-pruning based on key ranges when compared with the brute-force mode
Default Value: true
Type of Value: class java.lang.Object


### hoodie.index.bloom.num_entries {#hoodie.index.bloom.num_entries}
Only applies if index type is BLOOM. This is the number of entries to be stored in the bloom filter. We assume the maxParquetFileSize is 128MB and averageRecordSize is 1024B and hence we approx a total of 130K records in a file. The default (60000) is roughly half of this approximation. HUDI-56 tracks computing this dynamically. Warning: Setting this very low, will generate a lot of false positives and index lookup will have to scan a lot more files than it has to and Setting this to a very high number will increase the size every data file linearly (roughly 4KB for every 50000 entries). This config is also used with DYNNAMIC bloom filter which determines the initial size for the bloom.
Default Value: 60000
Type of Value: class java.lang.Object


### hoodie.bloom.index.bucketized.checking {#hoodie.bloom.index.bucketized.checking}
Only applies if index type is BLOOM. When true, bucketized bloom filtering is enabled. This reduces skew seen in sort based bloom index lookup
Default Value: true
Type of Value: class java.lang.Object


### hoodie.bloom.index.update.partition.path {#hoodie.bloom.index.update.partition.path}
Only applies if index type is GLOBAL_BLOOM. When set to true, an update including the partition path of a record that already exists will result in inserting the incoming record into the new partition and deleting the original record in the old partition. When set to false, the original record will only be updated in the old partition
Default Value: false
Type of Value: class java.lang.Object


### hoodie.bloom.index.parallelism {#hoodie.bloom.index.parallelism}
Only applies if index type is BLOOM. This is the amount of parallelism for index lookup, which involves a Spark Shuffle. By default, this is auto computed based on input workload characteristics. Disable explicit bloom index parallelism setting by default - hoodie auto computes
Default Value: 0
Type of Value: class java.lang.Object


### hoodie.bloom.index.input.storage.level {#hoodie.bloom.index.input.storage.level}
Only applies when #bloomIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values
Default Value: MEMORY_AND_DISK_SER
Type of Value: class java.lang.Object


### hoodie.bloom.index.keys.per.bucket {#hoodie.bloom.index.keys.per.bucket}
Only applies if bloomIndexBucketizedChecking is enabled and index type is bloom. This configuration controls the “bucket” size which tracks the number of record-key checks made against a single file and is the unit of work allocated to each partition performing bloom filter lookup. A higher value would amortize the fixed cost of reading a bloom filter to memory.
Default Value: 10000000
Type of Value: class java.lang.Object


### hoodie.simple.index.update.partition.path {#hoodie.simple.index.update.partition.path}

Default Value: false
Type of Value: class java.lang.Object


### hoodie.simple.index.input.storage.level {#hoodie.simple.index.input.storage.level}
Only applies when #simpleIndexUseCaching is set. Determine what level of persistence is used to cache input RDDs. Refer to org.apache.spark.storage.StorageLevel for different values
Default Value: MEMORY_AND_DISK_SER
Type of Value: class java.lang.Object


### hoodie.index.bloom.fpp {#hoodie.index.bloom.fpp}
Only applies if index type is BLOOM. Error rate allowed given the number of entries. This is used to calculate how many bits should be assigned for the bloom filter and the number of hash functions. This is usually set very low (default: 0.000000001), we like to tradeoff disk space for lower false positives. If the number of entries added to bloom filter exceeds the congfigured value (hoodie.index.bloom.num_entries), then this fpp may not be honored.
Default Value: 0.000000001
Type of Value: class java.lang.Object


### hoodie.bloom.index.filter.dynamic.max.entries {#hoodie.bloom.index.filter.dynamic.max.entries}
The threshold for the maximum number of keys to record in a dynamic Bloom filter row. Only applies if filter type is BloomFilterTypeCode.DYNAMIC_V0.
Default Value: 100000
Type of Value: class java.lang.Object


### hoodie.index.class {#hoodie.index.class}
Full path of user-defined index class and must be a subclass of HoodieIndex class. It will take precedence over the hoodie.index.type configuration if specified
Default Value: 
Type of Value: class java.lang.Object


### hoodie.bloom.index.filter.type {#hoodie.bloom.index.filter.type}
Filter type used. Default is BloomFilterTypeCode.SIMPLE. Available values are [BloomFilterTypeCode.SIMPLE , BloomFilterTypeCode.DYNAMIC_V0]. Dynamic bloom filters auto size themselves based on number of keys.
Default Value: SIMPLE
Type of Value: class java.lang.Object


### hoodie.global.simple.index.parallelism {#hoodie.global.simple.index.parallelism}
Only applies if index type is GLOBAL_SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle
Default Value: 100
Type of Value: class java.lang.Object


### hoodie.bloom.index.use.caching {#hoodie.bloom.index.use.caching}
Only applies if index type is BLOOM.When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions
Default Value: true
Type of Value: class java.lang.Object


### hoodie.bloom.index.prune.by.ranges {#hoodie.bloom.index.prune.by.ranges}
Only applies if index type is BLOOM. When true, range information from files to leveraged speed up index lookups. Particularly helpful, if the key has a monotonously increasing prefix, such as timestamp. If the record key is completely random, it is better to turn this off.
Default Value: true
Type of Value: class java.lang.Object


### hoodie.simple.index.use.caching {#hoodie.simple.index.use.caching}
Only applies if index type is SIMPLE. When true, the input RDD will cached to speed up index lookup by reducing IO for computing parallelism or affected partitions
Default Value: true
Type of Value: class java.lang.Object


### hoodie.simple.index.parallelism {#hoodie.simple.index.parallelism}
Only applies if index type is SIMPLE. This is the amount of parallelism for index lookup, which involves a Spark Shuffle
Default Value: 50
Type of Value: class java.lang.Object


## Storage Configs {#storage-configs}

Configurations that control aspects around sizing parquet and log files.

### hoodie.parquet.compression.ratio {#hoodie.parquet.compression.ratio}
Expected compression of parquet data used by Hudi, when it tries to size new parquet files. Increase this value, if bulk_insert is producing smaller than expected sized files
Default Value: 0.1
Type of Value: class java.lang.Object


### hoodie.hfile.max.file.size {#hoodie.hfile.max.file.size}

Default Value: 125829120
Type of Value: class java.lang.Object


### hoodie.logfile.data.block.max.size {#hoodie.logfile.data.block.max.size}
LogFile Data block max size. This is the maximum size allowed for a single data block to be appended to a log file. This helps to make sure the data appended to the log file is broken up into sizable blocks to prevent from OOM errors. This size should be greater than the JVM memory.
Default Value: 268435456
Type of Value: class java.lang.Object


### hoodie.parquet.block.size {#hoodie.parquet.block.size}
Parquet RowGroup size. Its better this is same as the file size, so that a single column within a file is stored continuously on disk
Default Value: 125829120
Type of Value: class java.lang.Object


### hoodie.orc.stripe.size {#hoodie.orc.stripe.size}
Size of the memory buffer in bytes for writing
Default Value: 67108864
Type of Value: class java.lang.Object


### hoodie.orc.block.size {#hoodie.orc.block.size}
File system block size
Default Value: 125829120
Type of Value: class java.lang.Object


### hoodie.orc.max.file.size {#hoodie.orc.max.file.size}

Default Value: 125829120
Type of Value: class java.lang.Object


### hoodie.hfile.compression.algorithm {#hoodie.hfile.compression.algorithm}

Default Value: GZ
Type of Value: class java.lang.Object


### hoodie.parquet.max.file.size {#hoodie.parquet.max.file.size}
Target size for parquet files produced by Hudi write phases. For DFS, this needs to be aligned with the underlying filesystem block size for optimal performance.
Default Value: 125829120
Type of Value: class java.lang.Object


### hoodie.orc.compression.codec {#hoodie.orc.compression.codec}

Default Value: ZLIB
Type of Value: class java.lang.Object


### hoodie.logfile.max.size {#hoodie.logfile.max.size}
LogFile max size. This is the maximum size allowed for a log file before it is rolled over to the next version.
Default Value: 1073741824
Type of Value: class java.lang.Object


### hoodie.parquet.compression.codec {#hoodie.parquet.compression.codec}
Compression Codec for parquet files
Default Value: gzip
Type of Value: class java.lang.Object


### hoodie.logfile.to.parquet.compression.ratio {#hoodie.logfile.to.parquet.compression.ratio}
Expected additional compression as records move from log files to parquet. Used for merge_on_read table to send inserts into log files & control the size of compacted parquet file.
Default Value: 0.35
Type of Value: class java.lang.Object


### hoodie.parquet.page.size {#hoodie.parquet.page.size}
Parquet page size. Page is the unit of read within a parquet file. Within a block, pages are compressed seperately.
Default Value: 1048576
Type of Value: class java.lang.Object


### hoodie.hfile.block.size {#hoodie.hfile.block.size}

Default Value: 1048576
Type of Value: class java.lang.Object


## Clustering Configs {#clustering-configs}

Configurations that control clustering operations in hudi. Each clustering has to be configured for its strategy, and config params. This config drives the same.

### hoodie.clustering.inline {#hoodie.clustering.inline}
Turn on inline clustering - clustering will be run after write operation is complete
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.sort.columns {#hoodie.clustering.plan.strategy.sort.columns}
Columns to sort the data by when clustering
Default Value: _
Since Version: 0.7.0


### hoodie.clustering.updates.strategy {#hoodie.clustering.updates.strategy}
When file groups is in clustering, need to handle the update to these file groups. Default strategy just reject the update
Default Value: org.apache.hudi.client.clustering.update.strategy.SparkRejectUpdateStrategy
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.inline.max.commits {#hoodie.clustering.inline.max.commits}
Config to control frequency of clustering
Default Value: 4
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.small.file.limit {#hoodie.clustering.plan.strategy.small.file.limit}
Files smaller than the size specified here are candidates for clustering
Default Value: 629145600
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.target.file.max.bytes {#hoodie.clustering.plan.strategy.target.file.max.bytes}
Each group can produce 'N' (CLUSTERING_MAX_GROUP_SIZE/CLUSTERING_TARGET_FILE_SIZE) output file groups
Default Value: 1073741824
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.daybased.lookback.partitions {#hoodie.clustering.plan.strategy.daybased.lookback.partitions}
Number of partitions to list to create ClusteringPlan
Default Value: 2
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.async.enabled {#hoodie.clustering.async.enabled}
Async clustering
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.max.bytes.per.group {#hoodie.clustering.plan.strategy.max.bytes.per.group}
Each clustering operation can create multiple groups. Total amount of data processed by clustering operation is defined by below two properties (CLUSTERING_MAX_BYTES_PER_GROUP * CLUSTERING_MAX_NUM_GROUPS). Max amount of data to be included in one group
Default Value: 2147483648
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.execution.strategy.class {#hoodie.clustering.execution.strategy.class}
Config to provide a strategy class to execute a ClusteringPlan. Class has to be subclass of RunClusteringStrategy
Default Value: org.apache.hudi.client.clustering.run.strategy.SparkSortAndSizeExecutionStrategy
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.max.num.groups {#hoodie.clustering.plan.strategy.max.num.groups}
Maximum number of groups to create as part of ClusteringPlan. Increasing groups will increase parallelism
Default Value: 30
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.clustering.plan.strategy.class {#hoodie.clustering.plan.strategy.class}
Config to provide a strategy class to create ClusteringPlan. Class has to be subclass of ClusteringPlanStrategy
Default Value: org.apache.hudi.client.clustering.plan.strategy.SparkRecentDaysClusteringPlanStrategy
Since Version: 0.7.0
Type of Value: class java.lang.Object


## Metrics Configurations {#metrics-configurations}

Enables reporting on Hudi metrics. Hudi publishes metrics on every commit, clean, rollback etc. The following sections list the supported reporters.

### hoodie.metrics.jmx.host {#hoodie.metrics.jmx.host}
Jmx host to connect to
Default Value: localhost
Since Version: 0.5.1
Type of Value: class java.lang.Object


### hoodie.metrics.executor.enable {#hoodie.metrics.executor.enable}

Default Value: _
Since Version: 0.7.0


### hoodie.metrics.jmx.port {#hoodie.metrics.jmx.port}
Jmx port to connect to
Default Value: 9889
Since Version: 0.5.1
Type of Value: class java.lang.Number


### hoodie.metrics.graphite.host {#hoodie.metrics.graphite.host}
Graphite host to connect to
Default Value: localhost
Since Version: 0.5.0
Type of Value: class java.lang.Object


### hoodie.metrics.on {#hoodie.metrics.on}
Turn on/off metrics reporting. off by default.
Default Value: false
Since Version: 0.5.0
Type of Value: class java.lang.Object


### hoodie.metrics.graphite.metric.prefix {#hoodie.metrics.graphite.metric.prefix}
Standard prefix applied to all metrics. This helps to add datacenter, environment information for e.g
Default Value: _
Since Version: 0.5.1


### hoodie.metrics.graphite.port {#hoodie.metrics.graphite.port}
Graphite port to connect to
Default Value: 4756
Since Version: 0.5.0
Type of Value: class java.lang.Number


### hoodie.metrics.reporter.type {#hoodie.metrics.reporter.type}
Type of metrics reporter.
Default Value: GRAPHITE
Since Version: 0.5.0
Type of Value: java.lang.Enum<org.apache.hudi.metrics.MetricsReporterType>


### hoodie.metrics.reporter.class {#hoodie.metrics.reporter.class}

Default Value: 
Since Version: 0.6.0
Type of Value: class java.lang.Object


## Metadata Configs {#metadata-configs}

Configurations used by the HUDI Metadata Table. This table maintains the meta information stored in hudi dataset so that listing can be avoided during queries.

### hoodie.metadata.enable {#hoodie.metadata.enable}
Enable the internal Metadata Table which stores table level file listings
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.metadata.insert.parallelism {#hoodie.metadata.insert.parallelism}
Parallelism to use when writing to the metadata table
Default Value: 1
Since Version: 0.7.0
Type of Value: class java.lang.Number


### hoodie.metadata.compact.max.delta.commits {#hoodie.metadata.compact.max.delta.commits}
Controls how often the metadata table is compacted.
Default Value: 24
Since Version: 0.7.0
Type of Value: class java.lang.Number


### hoodie.metadata.cleaner.commits.retained {#hoodie.metadata.cleaner.commits.retained}

Default Value: 3
Since Version: 0.7.0
Type of Value: class java.lang.Number


### hoodie.metadata.keep.min.commits {#hoodie.metadata.keep.min.commits}
Controls the archival of the metadata table’s timeline
Default Value: 20
Since Version: 0.7.0
Type of Value: class java.lang.Number


### hoodie.metadata.metrics.enable {#hoodie.metadata.metrics.enable}

Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.assume.date.partitioning {#hoodie.assume.date.partitioning}
Should HoodieWriteClient assume the data is partitioned by dates, i.e three levels from base path. This is a stop-gap to support tables created by versions < 0.3.1. Will be removed eventually
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.metadata.keep.max.commits {#hoodie.metadata.keep.max.commits}
Controls the archival of the metadata table’s timeline
Default Value: 30
Since Version: 0.7.0
Type of Value: class java.lang.Number


### hoodie.metadata.dir.filter.regex {#hoodie.metadata.dir.filter.regex}

Default Value: 
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.metadata.validate {#hoodie.metadata.validate}
Validate contents of Metadata Table on each access against the actual listings from DFS
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.metadata.clean.async {#hoodie.metadata.clean.async}
Enable asynchronous cleaning for metadata table
Default Value: false
Since Version: 0.7.0
Type of Value: class java.lang.Object


### hoodie.file.listing.parallelism {#hoodie.file.listing.parallelism}

Default Value: 1500
Since Version: 0.7.0
Type of Value: class java.lang.Number


## Bootstrap Configs {#bootstrap-configs}

Configurations that control bootstrap related configs. If you want to bootstrap your data for the first time into hudi, this bootstrap operation will come in handy as you don’t need to wait for entire data to be loaded into hudi to start leveraging hudi.

### hoodie.bootstrap.partitionpath.translator.class {#hoodie.bootstrap.partitionpath.translator.class}
Translates the partition paths from the bootstrapped data into how is laid out as a Hudi table.
Default Value: org.apache.hudi.client.bootstrap.translator.IdentityBootstrapPartitionPathTranslator
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.keygen.class {#hoodie.bootstrap.keygen.class}
Key generator implementation to be used for generating keys from the bootstrapped dataset
Default Value: _
Since Version: 0.6.0


### hoodie.bootstrap.mode.selector {#hoodie.bootstrap.mode.selector}
Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped
Default Value: org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.mode.selector.regex.mode {#hoodie.bootstrap.mode.selector.regex.mode}
Bootstrap mode to apply for partition paths, that match regex above. METADATA_ONLY will generate just skeleton base files with keys/footers, avoiding full cost of rewriting the dataset. FULL_RECORD will perform a full copy/rewrite of the data as a Hudi table.
Default Value: METADATA_ONLY
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.index.class {#hoodie.bootstrap.index.class}

Default Value: org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.full.input.provider {#hoodie.bootstrap.full.input.provider}
Class to use for reading the bootstrap dataset partitions/files, for Bootstrap mode FULL_RECORD
Default Value: org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.parallelism {#hoodie.bootstrap.parallelism}
Parallelism value to be used to bootstrap data into hudi
Default Value: 1500
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.mode.selector.regex {#hoodie.bootstrap.mode.selector.regex}
Matches each bootstrap dataset partition against this regex and applies the mode below to it.
Default Value: .*
Since Version: 0.6.0
Type of Value: class java.lang.Object


### hoodie.bootstrap.base.path {#hoodie.bootstrap.base.path}
Base path of the dataset that needs to be bootstrapped as a Hudi table
Default Value: _
Since Version: 0.6.0


