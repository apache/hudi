Release 0.4.2
------------------------------------

### Highlights
 * Parallelize Parquet writing & input record read resulting in upto 2x performance improvement
 * Better out-of-box configs to support upto 500GB upserts, improved ROPathFilter performance 
 * Added a union mode for RT View, that supports near-real time event ingestion without update semantics
 * Added a tuning guide with suggestions for oft-encountered problems
 * New configs for configs for compression ratio, index storage levels
 
### Full PR List 

 * **@jianxu** - Use hadoopConf in HoodieTableMetaClient and related tests #343
 * **@jianxu** - Add more options in HoodieWriteConfig #341
 * **@n3nash** - Adding a tool to read/inspect a HoodieLogFile #328
 * **@ovj** - Parallelizing parquet write and spark's external read operation. #294
 * **@n3nash** - Fixing memory leak due to HoodieLogFileReader holding on to a logblock #346
 * **@kaushikd49** - DeduplicateRecords based on recordKey if global index is used #345 
 * **@jianxu** - Checking storage level before persisting preppedRecords #358
 * **@n3nash** -  Adding config for parquet compression ratio #366 
 * **@xjodoin** - Replace deprecated jackson version #367 
 * **@n3nash** - Making ExternalSpillableMap generic for any datatype #350 
 * **@bvaradar** - CodeStyle formatting to conform to basic Checkstyle rules. #360 
 * **@vinothchandar** -  Update release notes for 0.4.1 (post) #371  
 * **@bvaradar** - Issue-329 : Refactoring TestHoodieClientOnCopyOnWriteStorage and adding test-cases #372 
 * **@n3nash** - Parallelized read-write operations in Hoodie Merge phase #370 
 * **@n3nash** - Using BufferedFsInputStream to wrap FSInputStream for FSDataInputStream #373 
 * **@suniluber** -  Fix for updating duplicate records in same/different files in same pa… #380 
 * **@bvaradar** - Fixit : Add Support for ordering and limiting results in CLI show commands #383
 * **@n3nash** - Adding metrics for MOR and COW #365  
 * **@n3nash** - Adding a fix/workaround when fs.append() unable to return a valid outputstream #388  
 * **@n3nash** - Minor fixes for MergeOnRead MVP release readiness #387 
 * **@bvaradar** - Issue-257: Support union mode in HoodieRealtimeRecordReader for pure insert workloads #379 
 * **@n3nash** - Enabling global index for MOR #389 
 * **@suniluber** - Added a new filter function to filter by record keys when reading parquet file #395 
 * **@vinothchandar** - Improving out of box experience for data source #295  
 * **@xjodoin** - Fix wrong use of TemporaryFolder junit rule #411  

Release 0.4.1
------------------------------------

### Highlights

 * Good enhancements for merge-on-read write path : spillable map for merging, evolvable log format, rollback support
 * Cloud file systems should now work out-of-box for copy-on-write tables, with configs picked up from SparkContext
 * Compaction action is no more, multiple delta commits now lead to a commit upon compaction
 * API level changes include : compaction api, new prepped APIs for higher plugability for advanced clients


### Full PR List

 * **@n3nash** - Separated rollback as a table operation, implement rollback for MOR #247
 * **@n3nash** - Implementing custom payload/merge hooks abstractions for application #275
 * **@vinothchandar** - Reformat project & tighten code style guidelines #280
 * **@n3nash** - Separating out compaction() API #282
 * **@n3nash** - Enable hive sync even if there is no compaction commit #286
 * **@n3nash** - Partition compaction strategy #281
 * **@n3nash** - Removing compaction action type and associated compaction timeline operations, replace with commit action type #288
 * **@vinothchandar** - Multi/Cloud FS Support for Copy-On-Write tables #293
 * **@vinothchandar** - Update Gemfile.lock #298
 * **@n3nash** - Reducing memory footprint required in HoodieAvroDataBlock and HoodieAppendHandle #290
 * **@jianxu** - Add FinalizeWrite in HoodieCreateHandle for COW tables #285
 * **@n3nash** - Adding global indexing to HbaseIndex implementation #318
 * **@n3nash** - Small File Size correction handling for MOR table type #299
 * **@jianxu** - Use FastDateFormat for thread safety #320
 * **@vinothchandar** - Fix formatting in HoodieWriteClient #322
 * **@n3nash** - Write smaller sized multiple blocks to log file instead of a large one #317
 * **@n3nash** - Added support for Disk Spillable Compaction to prevent OOM issues #289
 * **@jianxu** - Add new APIs in HoodieReadClient and HoodieWriteClient #327
 * **@jianxu** - Handle inflight clean instants during Hoodie instants archiving #332
 * **@n3nash** - Introducing HoodieLogFormat V2 with versioning support #331
 * **@n3nash** - Re-factoring Compaction as first level API in WriteClient similar to upsert/insert #330


Release 0.4.0
------------------------------------

### Highlights 

 * [Spark datasource API](https://uber.github.io/hoodie/quickstart.html#datasource-api) now supported for Copy-On-Write datasets, across all views
 * BloomIndex can now [prune based on key ranges](https://uber.github.io/hoodie/configurations.html#bloomIndexPruneByRanges) & cut down index tagging time dramatically, for time-prefixed/ordered record keys
 * Hive sync tool registers RO and RT tables now.
 * Client application can now specify the partitioner to be used by bulkInsert(), useful for low-level control over initial record placement
 * Framework for metadata tracking inside IO handles, to implement Spark accumulator-style counters, that are consistent with the timeline
 * Bug fixes around cleaning, savepoints & upsert's partitioner.

### Full PR List

 * **@gekath** - Writes relative paths to .commit files #184
 * **@kaushikd49** - Correct clean bug that causes exception when partitionPaths are empty #202
 * **@vinothchandar** - Refactor HoodieTableFileSystemView using FileGroups & FileSlices #201
 * **@prazanna** - Savepoint should not create a hole in the commit timeline #207
 * **@jianxu** - Fix TimestampBasedKeyGenerator in HoodieDeltaStreamer when DATE_STRING is used #211
 * **@n3nash** - Sync Tool registers 2 tables, RO and RT Tables #210
 * **@n3nash** - Using FsUtils instead of Files API to extract file extension #213
 * **@vinothchandar** - Edits to documentation #219
 * **@n3nash** - Enabled deletes in merge_on_read #218
 * **@n3nash** - Use HoodieLogFormat for the commit archived log #205
 * **@n3nash** - fix for cleaning log files in master branch (mor) #228
 * **@vinothchandar** - Adding range based pruning to bloom index #232
 * **@n3nash** - Use CompletedFileSystemView instead of CompactedView considering deltacommits too #229
 * **@n3nash** - suppressing logs (under 4MB) for jenkins #240
 * **@jianxu** - Add nested fields support for MOR tables #234
 * **@n3nash** - adding new config to separate shuffle and write parallelism #230
 * **@n3nash** - adding ability to read archived files written in log format #252
 * **@ovj** - Removing randomization from UpsertPartitioner #253
 * **@ovj** - Replacing SortBy with custom partitioner #245
 * **@esmioley** - Update deprecated hash function #259
 * **@vinothchandar** - Adding canIndexLogFiles(), isImplicitWithStorage(), isGlobal() to HoodieIndex #268
 * **@kaushikd49** - Hoodie Event callbacks #251
 * **@vinothchandar** - Spark Data Source (finally) #266


Previous Releases
------------
* Refer to [github](https://github.com/uber/hoodie/releases)
