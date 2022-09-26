---
title: CLI
keywords: [hudi, cli]
last_modified_at: 2021-08-18T15:59:57-04:00
---

### Local set up
Once hudi has been built, the shell can be fired by via  `cd hudi-cli && ./hudi-cli.sh`. A hudi table resides on DFS, in a location referred to as the `basePath` and
we would need this location in order to connect to a Hudi table. Hudi library effectively manages this table internally, using `.hoodie` subfolder to track all metadata.


### Using Hudi-cli in S3
If you are using hudi that comes packaged with AWS EMR, you can find instructions to use hudi-cli [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi-cli.html).
If you are not using EMR, or would like to use latest hudi-cli from master, you can follow the below steps to access S3 dataset in your local environment (laptop).  

Build Hudi with corresponding Spark version, for eg, -Dspark3.1.x

Set the following environment variables. 
```
export AWS_REGION=us-east-2
export AWS_ACCESS_KEY_ID=<key_id>
export AWS_SECRET_ACCESS_KEY=<secret_key>
export SPARK_HOME=<spark_home>
```
Ensure you set the SPARK_HOME to your local spark home compatible to compiled hudi spark version above.

Apart from these, we might need to add aws jars to class path so that accessing S3 is feasible from local. 
We need two jars, namely, aws-java-sdk-bundle jar and hadoop-aws jar which you can find online.
For eg:
```
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -o /lib/spark-3.2.0-bin-hadoop3.2/jars/hadoop-aws-3.2.0.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar -o /lib/spark-3.2.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.11.375.jar
```

#### Note: These AWS jar versions below are specific to Spark 3.2.0
```
export CLIENT_JAR=/lib/spark-3.2.0-bin-hadoop3.2/jars/aws-java-sdk-bundle-1.12.48.jar:/lib/spark-3.2.0-bin-hadoop3.2/jars/hadoop-aws-3.3.1.jar
```
Once these are set, you are good to launch hudi-cli and access S3 dataset. 
```
./hudi-cli/hudi-cli.sh
```


## Connect to a Kerberized cluster

Before connecting to a Kerberized cluster, you can use **kerberos kinit** command. Following is the usage of this command.

```shell
hudi->help kerberos kinit
NAME
       kerberos kinit - Perform Kerberos authentication

SYNOPSIS
       kerberos kinit --krb5conf String [--principal String] [--keytab String]

OPTIONS
       --krb5conf String
       Path to krb5.conf
       [Optional, default = /etc/krb5.conf]

       --principal String
       Kerberos principal
       [Mandatory]

       --keytab String
       Path to keytab
       [Mandatory]
```

For example:

```shell
hudi->kerberos kinit --principal user/host@DOMAIN --keytab /etc/security/keytabs/user.keytab
Perform Kerberos authentication
Parameters:
--krb5conf: /etc/krb5.conf
--principal: user/host@DOMAIN
--keytab: /etc/security/keytabs/user.keytab
Kerberos current user: user/host@DOMAIN (auth:KERBEROS)
Kerberos login user: user/host@DOMAIN (auth:KERBEROS)
Kerberos authentication success
```

If you see "Kerberos authentication success" in the command output, it means Kerberos authentication has been successful.


## Using hudi-cli

To initialize a hudi table, use the following command.

```java
===================================================================
*         ___                          ___                        *
*        /\__\          ___           /\  \           ___         *
*       / /  /         /\__\         /  \  \         /\  \        *
*      / /__/         / /  /        / /\ \  \        \ \  \       *
*     /  \  \ ___    / /  /        / /  \ \__\       /  \__\      *
*    / /\ \  /\__\  / /__/  ___   / /__/ \ |__|     / /\/__/      *
*    \/  \ \/ /  /  \ \  \ /\__\  \ \  \ / /  /  /\/ /  /         *
*         \  /  /    \ \  / /  /   \ \  / /  /   \  /__/          *
*         / /  /      \ \/ /  /     \ \/ /  /     \ \__\          *
*        / /  /        \  /  /       \  /  /       \/__/          *
*        \/__/          \/__/         \/__/    Apache Hudi CLI    *
*                                                                 *
===================================================================

hudi->create --path /user/hive/warehouse/table1 --tableName hoodie_table_1 --tableType COPY_ON_WRITE
.....
```

To see the description of hudi table, use the command:

```java
hudi:hoodie_table_1->desc
18/09/06 15:57:19 INFO timeline.HoodieActiveTimeline: Loaded instants []
    _________________________________________________________
    | Property                | Value                        |
    |========================================================|
    | basePath                | ...                          |
    | metaPath                | ...                          |
    | fileSystem              | hdfs                         |
    | hoodie.table.name       | hoodie_table_1               |
    | hoodie.table.type       | COPY_ON_WRITE                |
    | hoodie.archivelog.folder|                              |
```

Following is a sample command to connect to a Hudi table contains uber trips.

```java
hudi:trips->connect --path /app/uber/trips

16/10/05 23:20:37 INFO model.HoodieTableMetadata: All commits :HoodieCommits{commitList=[20161002045850, 20161002052915, 20161002055918, 20161002065317, 20161002075932, 20161002082904, 20161002085949, 20161002092936, 20161002105903, 20161002112938, 20161002123005, 20161002133002, 20161002155940, 20161002165924, 20161002172907, 20161002175905, 20161002190016, 20161002192954, 20161002195925, 20161002205935, 20161002215928, 20161002222938, 20161002225915, 20161002232906, 20161003003028, 20161003005958, 20161003012936, 20161003022924, 20161003025859, 20161003032854, 20161003042930, 20161003052911, 20161003055907, 20161003062946, 20161003065927, 20161003075924, 20161003082926, 20161003085925, 20161003092909, 20161003100010, 20161003102913, 20161003105850, 20161003112910, 20161003115851, 20161003122929, 20161003132931, 20161003142952, 20161003145856, 20161003152953, 20161003155912, 20161003162922, 20161003165852, 20161003172923, 20161003175923, 20161003195931, 20161003210118, 20161003212919, 20161003215928, 20161003223000, 20161003225858, 20161004003042, 20161004011345, 20161004015235, 20161004022234, 20161004063001, 20161004072402, 20161004074436, 20161004080224, 20161004082928, 20161004085857, 20161004105922, 20161004122927, 20161004142929, 20161004163026, 20161004175925, 20161004194411, 20161004203202, 20161004211210, 20161004214115, 20161004220437, 20161004223020, 20161004225321, 20161004231431, 20161004233643, 20161005010227, 20161005015927, 20161005022911, 20161005032958, 20161005035939, 20161005052904, 20161005070028, 20161005074429, 20161005081318, 20161005083455, 20161005085921, 20161005092901, 20161005095936, 20161005120158, 20161005123418, 20161005125911, 20161005133107, 20161005155908, 20161005163517, 20161005165855, 20161005180127, 20161005184226, 20161005191051, 20161005193234, 20161005203112, 20161005205920, 20161005212949, 20161005223034, 20161005225920]}
Metadata for table trips loaded
```

Once connected to the table, a lot of other commands become available. The shell has contextual autocomplete help (press TAB) and below is a list of all commands, few of which are reviewed in this section

```shell
hudi:trips->help
* ! - Allows execution of operating system (OS) commands
* // - Inline comment markers (start of line only)
* ; - Inline comment markers (start of line only)
* bootstrap index showmapping - Show bootstrap index mapping
* bootstrap index showpartitions - Show bootstrap indexed partitions
* bootstrap run - Run a bootstrap action for current Hudi table
* clean showpartitions - Show partition level details of a clean
* cleans refresh - Refresh table metadata
* cleans run - run clean
* cleans show - Show the cleans
* clear - Clears the console
* cls - Clears the console
* clustering run - Run Clustering
* clustering schedule - Schedule Clustering
* clustering scheduleAndExecute - Run Clustering. Make a cluster plan first and execute that plan immediately
* commit rollback - Rollback a commit
* commits compare - Compare commits with another Hoodie table
* commit show_write_stats - Show write stats of a commit
* commit showfiles - Show file level details of a commit
* commit showpartitions - Show partition level details of a commit
* commits refresh - Refresh table metadata
* commits show - Show the commits
* commits showarchived - Show the archived commits
* commits sync - Sync commits with another Hoodie table
* compaction repair - Renames the files to make them consistent with the timeline as dictated by Hoodie metadata. Use when compaction unschedule fails partially.
* compaction run - Run Compaction for given instant time
* compaction schedule - Schedule Compaction
* compaction scheduleAndExecute - Schedule compaction plan and execute this plan
* compaction show - Shows compaction details for a specific compaction instant
* compaction showarchived - Shows compaction details for a specific compaction instant
* compactions show all - Shows all compactions that are in active timeline
* compactions showarchived - Shows compaction details for specified time window
* compaction unschedule - Unschedule Compaction
* compaction unscheduleFileId - UnSchedule Compaction for a fileId
* compaction validate - Validate Compaction
* connect - Connect to a hoodie table
* create - Create a hoodie table if not present
* date - Displays the local date and time
* desc - Describe Hoodie Table properties
* downgrade table - Downgrades a table
* exit - Exits the shell
* export instants - Export Instants and their metadata from the Timeline
* fetch table schema - Fetches latest table schema
* hdfsparquetimport - Imports Parquet table to a hoodie table
* help - List all commands usage
* marker delete - Delete the marker
* metadata create - Create the Metadata Table if it does not exist
* metadata delete - Remove the Metadata Table
* metadata init - Update the metadata table from commits since the creation
* metadata list-files - Print a list of all files in a partition from the metadata
* metadata list-partitions - List all partitions from metadata
* metadata refresh - Refresh table metadata
* metadata set - Set options for Metadata Table
* metadata stats - Print stats about the metadata
* metadata validate-files - Validate all files in all partitions from the metadata
* quit - Exits the shell
* refresh - Refresh table metadata
* repair addpartitionmeta - Add partition metadata to a table, if not present
* repair corrupted clean files - repair corrupted clean files
* repair deduplicate - De-duplicate a partition path contains duplicates & produce repaired files to replace with
* repair migrate-partition-meta - Migrate all partition meta file currently stored in text format to be stored in base file format. See HoodieTableConfig#PARTITION_METAFILE_USE_DATA_FORMAT.
* repair overwrite-hoodie-props - Overwrite hoodie.properties with provided file. Risky operation. Proceed with caution!
* savepoint create - Savepoint a commit
* savepoint delete - Delete the savepoint
* savepoint rollback - Savepoint a commit
* savepoints refresh - Refresh table metadata
* savepoints show - Show the savepoints
* script - Parses the specified resource file and executes its commands
* set - Set spark launcher env to cli
* show archived commits - Read commits from archived files and show details
* show archived commit stats - Read commits from archived files and show details
* show env - Show spark launcher env by key
* show envs all - Show spark launcher envs
* show fsview all - Show entire file-system view
* show fsview latest - Show latest file-system view
* show logfile metadata - Read commit metadata from log files
* show logfile records - Read records from log files
* show rollback - Show details of a rollback instant
* show rollbacks - List all rollback instants
* stats filesizes - File Sizes. Display summary stats on sizes of files
* stats wa - Write Amplification. Ratio of how many records were upserted to how many records were actually written
* sync validate - Validate the sync by counting the number of records
* system properties - Shows the shell's properties
* table delete-configs - Delete the supplied table configs from the table.
* table recover-configs - Recover table configs, from update/delete that failed midway.
* table update-configs - Update the table configs with configs with provided file.
* temp_delete - Delete view name
* temp_query - query against created temp view
* temp delete - Delete view name
* temp query - query against created temp view
* temps_show - Show all views name
* temps show - Show all views name
* upgrade table - Upgrades a table
* utils loadClass - Load a class
* version - Displays shell version

hudi:trips->
```


### Inspecting Commits

The task of upserting or inserting a batch of incoming records is known as a **commit** in Hudi. A commit provides basic atomicity guarantees such that only committed data is available for querying.
Each commit has a monotonically increasing string/number called the **commit number**. Typically, this is the time at which we started the commit.

To view some basic information about the last 10 commits,


```java
hudi:trips->commits show --sortBy "Total Bytes Written" --desc true --limit 10
    ________________________________________________________________________________________________________________________________________________________________________
    | CommitTime    | Total Bytes Written| Total Files Added| Total Files Updated| Total Partitions Written| Total Records Written| Total Update Records Written| Total Errors|
    |=======================================================================================================================================================================|
    ....
    ....
    ....
```

At the start of each write, Hudi also writes a .inflight commit to the .hoodie folder. You can use the timestamp there to estimate how long the commit has been inflight


```java
$ hdfs dfs -ls /app/uber/trips/.hoodie/*.inflight
-rw-r--r--   3 vinoth supergroup     321984 2016-10-05 23:18 /app/uber/trips/.hoodie/20161005225920.inflight
```


### Drilling Down to a specific Commit

To understand how the writes spread across specific partiions,


```java
hudi:trips->commit showpartitions --commit 20161005165855 --sortBy "Total Bytes Written" --desc true --limit 10
    __________________________________________________________________________________________________________________________________________
    | Partition Path| Total Files Added| Total Files Updated| Total Records Inserted| Total Records Updated| Total Bytes Written| Total Errors|
    |=========================================================================================================================================|
     ....
     ....
```

If you need file level granularity , we can do the following


```java
hudi:trips->commit showfiles --commit 20161005165855 --sortBy "Partition Path"
    ________________________________________________________________________________________________________________________________________________________
    | Partition Path| File ID                             | Previous Commit| Total Records Updated| Total Records Written| Total Bytes Written| Total Errors|
    |=======================================================================================================================================================|
    ....
    ....
```


### FileSystem View

Hudi views each partition as a collection of file-groups with each file-group containing a list of file-slices in commit order (See concepts).
The below commands allow users to view the file-slices for a data-set.

```java
hudi:stock_ticks_mor->show fsview all
 ....
  _______________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
 | Partition | FileId | Base-Instant | Data-File | Data-File Size| Num Delta Files| Total Delta File Size| Delta Files |
 |==============================================================================================================================================================================================================================================================================================================================================================================================================|
 | 2018/08/31| 111415c3-f26d-4639-86c8-f9956f245ac3| 20181002180759| hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/111415c3-f26d-4639-86c8-f9956f245ac3_0_20181002180759.parquet| 432.5 KB | 1 | 20.8 KB | [HoodieLogFile {hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/.111415c3-f26d-4639-86c8-f9956f245ac3_20181002180759.log.1}]|



hudi:stock_ticks_mor->show fsview latest --partitionPath "2018/08/31"
 ......
 __________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
 | Partition | FileId | Base-Instant | Data-File | Data-File Size| Num Delta Files| Total Delta Size| Delta Size - compaction scheduled| Delta Size - compaction unscheduled| Delta To Base Ratio - compaction scheduled| Delta To Base Ratio - compaction unscheduled| Delta Files - compaction scheduled | Delta Files - compaction unscheduled|
 |=================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================================|
 | 2018/08/31| 111415c3-f26d-4639-86c8-f9956f245ac3| 20181002180759| hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/111415c3-f26d-4639-86c8-f9956f245ac3_0_20181002180759.parquet| 432.5 KB | 1 | 20.8 KB | 20.8 KB | 0.0 B | 0.0 B | 0.0 B | [HoodieLogFile {hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/.111415c3-f26d-4639-86c8-f9956f245ac3_20181002180759.log.1}]| [] |

```


### Statistics

Since Hudi directly manages file sizes for DFS table, it might be good to get an overall picture


```java
hudi:trips->stats filesizes --partitionPath 2016/09/01 --sortBy "95th" --desc true --limit 10
    ________________________________________________________________________________________________
    | CommitTime    | Min     | 10th    | 50th    | avg     | 95th    | Max     | NumFiles| StdDev  |
    |===============================================================================================|
    | <COMMIT_ID>   | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 93.9 MB | 2       | 2.3 KB  |
    ....
    ....
```

In case of Hudi write taking much longer, it might be good to see the write amplification for any sudden increases


```java
hudi:trips->stats wa
    __________________________________________________________________________
    | CommitTime    | Total Upserted| Total Written| Write Amplifiation Factor|
    |=========================================================================|
    ....
    ....
```


### Archived Commits

In order to limit the amount of growth of .commit files on DFS, Hudi archives older .commit files (with due respect to the cleaner policy) into a commits.archived file.
This is a sequence file that contains a mapping from commitNumber => json with raw information about the commit (same that is nicely rolled up above).


### Compactions

To get an idea of the lag between compaction and writer applications, use the below command to list down all
pending compactions.

```java
hudi:trips->compactions show all
     ___________________________________________________________________
    | Compaction Instant Time| State    | Total FileIds to be Compacted|
    |==================================================================|
    | <INSTANT_1>            | REQUESTED| 35                           |
    | <INSTANT_2>            | INFLIGHT | 27                           |
```

To inspect a specific compaction plan, use

```java
hudi:trips->compaction show --instant <INSTANT_1>
    _________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | Partition Path| File Id | Base Instant  | Data File Path                                    | Total Delta Files| getMetrics                                                                                                                    |
    |================================================================================================================================================================================================================================================
    | 2018/07/17    | <UUID>  | <INSTANT_1>   | viewfs://ns-default/.../../UUID_<INSTANT>.parquet | 1                | {TOTAL_LOG_FILES=1.0, TOTAL_IO_READ_MB=1230.0, TOTAL_LOG_FILES_SIZE=2.51255751E8, TOTAL_IO_WRITE_MB=991.0, TOTAL_IO_MB=2221.0}|

```

To manually schedule or run a compaction, use the below command. This command uses spark launcher to perform compaction
operations.

**NOTE:** Make sure no other application is scheduling compaction for this table concurrently
{: .notice--info}

```java
hudi:trips->help compaction schedule
Keyword:                   compaction schedule
Description:               Schedule Compaction
 Keyword:                  sparkMemory
   Help:                   Spark executor memory
   Mandatory:              false
   Default if specified:   '__NULL__'
   Default if unspecified: '1G'

* compaction schedule - Schedule Compaction
```

```java
hudi:trips->help compaction run
Keyword:                   compaction run
Description:               Run Compaction for given instant time
 Keyword:                  tableName
   Help:                   Table name
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  parallelism
   Help:                   Parallelism for hoodie compaction
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  schemaFilePath
   Help:                   Path for Avro schema file
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  sparkMemory
   Help:                   Spark executor memory
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  retry
   Help:                   Number of retries
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

 Keyword:                  compactionInstant
   Help:                   Base path for the target hoodie table
   Mandatory:              true
   Default if specified:   '__NULL__'
   Default if unspecified: '__NULL__'

* compaction run - Run Compaction for given instant time
```

### Validate Compaction

Validating a compaction plan : Check if all the files necessary for compactions are present and are valid

```java
hudi:stock_ticks_mor->compaction validate --instant 20181005222611
...

   COMPACTION PLAN VALID

    ___________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | File Id                             | Base Instant Time| Base Data File                                                                                                                   | Num Delta Files| Valid| Error|
    |==========================================================================================================================================================================================================================|
    | 05320e98-9a57-4c38-b809-a6beaaeb36bd| 20181005222445   | hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/05320e98-9a57-4c38-b809-a6beaaeb36bd_0_20181005222445.parquet| 1              | true |      |



hudi:stock_ticks_mor->compaction validate --instant 20181005222601

   COMPACTION PLAN INVALID

    _______________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________________
    | File Id                             | Base Instant Time| Base Data File                                                                                                                   | Num Delta Files| Valid| Error                                                                           |
    |=====================================================================================================================================================================================================================================================================================================|
    | 05320e98-9a57-4c38-b809-a6beaaeb36bd| 20181005222445   | hdfs://namenode:8020/user/hive/warehouse/stock_ticks_mor/2018/08/31/05320e98-9a57-4c38-b809-a6beaaeb36bd_0_20181005222445.parquet| 1              | false| All log files specified in compaction operation is not present. Missing ....    |
```

**NOTE:** The following commands must be executed without any other writer/ingestion application running.
{: .notice--warning}

Sometimes, it becomes necessary to remove a fileId from a compaction-plan inorder to speed-up or unblock compaction
operation. Any new log-files that happened on this file after the compaction got scheduled will be safely renamed
so that are preserved. Hudi provides the following CLI to support it


### Unscheduling Compaction

```java
hudi:trips->compaction unscheduleFileId --fileId <FileUUID>
....
No File renames needed to unschedule file from pending compaction. Operation successful.
```

In other cases, an entire compaction plan needs to be reverted. This is supported by the following CLI

```java
hudi:trips->compaction unschedule --compactionInstant <compactionInstant>
.....
No File renames needed to unschedule pending compaction. Operation successful.
```

### Repair Compaction

The above compaction unscheduling operations could sometimes fail partially (e:g -> DFS temporarily unavailable). With
partial failures, the compaction operation could become inconsistent with the state of file-slices. When you run
`compaction validate`, you can notice invalid compaction operations if there is one.  In these cases, the repair
command comes to the rescue, it will rearrange the file-slices so that there is no loss and the file-slices are
consistent with the compaction plan

```java
hudi:stock_ticks_mor->compaction repair --instant 20181005222611
......
Compaction successfully repaired
.....
```

### Savepoint and Restore 
As the name suggest, "savepoint" saves the table as of the commit time, so that it lets you restore the table to this 
savepoint at a later point in time if need be. You can read more about savepoints and restore [here](/docs/next/disaster_recovery)

To trigger savepoint for a hudi table
```java
connect --path /tmp/hudi_trips_cow/
commits show
set --conf SPARK_HOME=<SPARK_HOME>
savepoint create --commit 20220128160245447 --sparkMaster local[2]
```

To restore the table to one of the savepointed commit:

```java
connect --path /tmp/hudi_trips_cow/
commits show
set --conf SPARK_HOME=<SPARK_HOME>
savepoints show
╔═══════════════════╗
║ SavepointTime     ║
╠═══════════════════╣
║ 20220128160245447 ║
╚═══════════════════╝
savepoint rollback --savepoint 20220128160245447 --sparkMaster local[2]
```

### Upgrade and Downgrade Table
In case the user needs to downgrade the version of Hudi library used, the Hudi table needs to be manually downgraded
on the newer version of Hudi CLI before library downgrade.  To downgrade a Hudi table through CLI, user needs to specify
the target Hudi table version as follows:

```shell
connect --path <table_path>
downgrade table --toVersion <target_version>
```

The following table shows the Hudi table versions corresponding to the Hudi release versions:

| Hudi Table Version | Hudi Release Version(s) |
|:-------------------|:------------------------|
| `FIVE` or `5`      | 0.12.0 and above        |
| `FOUR` or `4`      | 0.11.x                  |
| `THREE` or `3`     | 0.10.x                  |
| `TWO` or `2`       | 0.9.x                   |
| `ONE` or `1`       | 0.6.x - 0.8.x           |
| `ZERO` or `0`      | 0.5.x and below         |

For example, to downgrade a table from version `FIVE`(`5`) (current version) to `TWO`(`2`), you should run (use proper Spark master based
on your environment)

```shell
downgrade table --toVersion TWO --sparkMaster local[2]
```

or

```shell
downgrade table --toVersion 2 --sparkMaster local[2]
```

You can verify the table version by looking at the `hoodie.table.version` property in `.hoodie/hoodie.properties` under
the table path:

```properties
hoodie.table.version=2
```

Hudi CLI also provides the ability to manually upgrade a Hudi table.  To upgrade a Hudi table through CLI:

```shell
upgrade table --toVersion <target_version>
```

:::note
Table upgrade is automatically handled by the Hudi write client in different deployment modes such as DeltaStreamer
after upgrading the Hudi library so that the user does not have to do manual upgrade.  Such automatic table upgrade
is the **recommended** way in general, instead of using `upgrade` CLI command.

Table upgrade from table version ONE to TWO requires key generator related configs such as
"hoodie.datasource.write.recordkey.field", which is only available when user configures the write job. So the table
upgrade from version ONE to TWO through CLI is not supported, and user should rely on the automatic upgrade in the write
client instead.
:::

You may also run the upgrade command without specifying the target version.  In such a case, the latest table version
corresponding to the library release version is used:

```shell
upgrade table
```