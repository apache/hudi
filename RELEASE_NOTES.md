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
