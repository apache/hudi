---
title: CLI
keywords: [hudi, cli]
last_modified_at: 2026-05-27T00:00:00-00:00
---

### Local set up
Once hudi has been built, the shell can be fired by via  `cd packaging/hudi-cli-bundle && hudi-cli-with-bundle.sh` or `packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh`.

### Hudi CLI setup
In release `0.13.0` we have now added new way of launching the `hudi cli`, which is using the `hudi-cli-bundle` script.

#### Note: The traditional `hudi-cli.sh` script has been deprecated and replaced with `hudi-cli-with-bundle.sh` from `1.0.2` release onwards. Users should migrate to the new bundled CLI script `hudi-cli-with-bundle.sh` for better compatibility and ease of use.

There are a couple of requirements such as having `spark` installed locally on your machine. 
It is required to use a spark distribution with hadoop dependencies packaged such as `spark-3.5.4-bin-hadoop3.tgz` from https://archive.apache.org/dist/spark/.
We also recommend you set an env variable `$SPARK_HOME` to the path of where spark is installed on your machine. 
One important thing to note is that the `hudi-spark-bundle` should also be present when using the `hudi-cli-bundle`.  
To provide the locations of these bundle jars you can set them in your shell like so:
`export CLI_BUNDLE_JAR=<path-to-cli-bundle-jar-to-use>` , `export SPARK_BUNDLE_JAR=<path-to-spark-bundle-jar-to-use>`.

For steps see below if you are not compiling the project and downloading the jars: 

1. Create an empty folder as a new directory
2. Copy the hudi-cli-bundle jars and hudi-spark*-bundle jars to this directory
3. Copy the following script and folder to this directory
```
packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
packaging/hudi-cli-bundle/conf .  the `conf` folder should be in this directory.
```

4. Start Hudi CLI shell with environment variables set
```
export SPARK_HOME=<spark-home-folder>
export CLI_BUNDLE_JAR=<cli-bundle-jar-to-use>
export SPARK_BUNDLE_JAR=<spark-bundle-jar-to-use>

./hudi-cli-with-bundle.sh

```

### Base path
A hudi table resides on DFS, in a location referred to as the `basePath` and
we would need this location in order to connect to a Hudi table. Hudi library effectively manages this table internally, using `.hoodie` subfolder to track all metadata.




### Using Hudi-cli in S3
If you are using hudi that comes packaged with AWS EMR, you can find instructions to use hudi-cli [here](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hudi-cli.html).
If you are not using EMR, or would like to use latest hudi-cli from master, you can follow the below steps to access S3 dataset in your local environment (laptop).  

Build Hudi with corresponding Spark version, for eg, -Dspark3.5

Set the following environment variables. 
```
export AWS_REGION=us-east-2
export AWS_ACCESS_KEY_ID=<key_id>
export AWS_SECRET_ACCESS_KEY=<secret_key>

export SPARK_HOME=<spark_home>
export CLI_BUNDLE_JAR=<cli-bundle-jar-to-use>
export SPARK_BUNDLE_JAR=<spark-bundle-jar-to-use>
```
Ensure you set the SPARK_HOME to your local spark home compatible to compiled hudi spark version above. One important thing to note is that the `hudi-spark-bundle` should also be present when using the `hudi-cli-bundle`.

Apart from these, we might need to add aws jars to class path so that accessing S3 is feasible from local. 
We need two jars, namely, aws-java-sdk-bundle jar and hadoop-aws jar which you can find online.
For eg:
```
wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar -o /lib/spark-3.5.4-bin-hadoop3/jars/hadoop-aws-3.3.4.jar
wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar -o /lib/spark-3.5.4-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar
```

#### Note: These AWS jar versions below are specific to Spark 3.5.4 and Hadoop 3.3.4
```
export CLIENT_JAR=/lib/spark-3.5.4-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.262.jar:/lib/spark-3.5.4-bin-hadoop3/jars/hadoop-aws-3.3.4.jar
```
Once these are set, you are good to launch hudi-cli and access S3 dataset. 
```
./packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
```
### Using hudi-cli on Google Dataproc
[Dataproc](https://cloud.google.com/dataproc) is Google's managed service for running Apache Hadoop, Apache Spark, 
Apache Flink, Presto and many other frameworks, including Hudi. If you want to run the Hudi CLI on a Dataproc node 
which has not been launched with Hudi support enabled, you can use the steps below:  

These steps use Hudi version 1.1.1. If you want to use a different version you will have to edit the below commands 
appropriately:  
1. Once you've started the Dataproc cluster, you can ssh into it as follows:
```
$ gcloud compute ssh --zone "YOUR_ZONE" "HOSTNAME_OF_MASTER_NODE"  --project "YOUR_PROJECT"
```  

2. Download the Hudi CLI bundle
```
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-cli-bundle_2.12/1.1.1/hudi-cli-bundle_2.12-1.1.1.jar  
```

3. Download the Hudi Spark bundle
```
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.1.1/hudi-spark3.5-bundle_2.12-1.1.1.jar
```     

4. Download the shell script that launches Hudi CLI bundle
```
wget https://raw.githubusercontent.com/apache/hudi/release-1.1.1/packaging/hudi-cli-bundle/hudi-cli-with-bundle.sh
```    

5. Launch Hudi CLI bundle with appropriate environment variables as follows:
``` 
CLIENT_JAR=$DATAPROC_DIR/lib/gcs-connector.jar CLI_BUNDLE_JAR=hudi-cli-bundle_2.12-1.1.1.jar SPARK_BUNDLE_JAR=hudi-spark3.5-bundle_2.12-1.1.1.jar ./hudi-cli-with-bundle.sh  
```

6. hudi->connect --path gs://path_to_some_table  
Metadata for table some_table loaded  

7. hudi:some_table->commits show --limit 5  
This command should show the recent commits, if the above steps work correctly.  

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

**kerberos kdestroy** is the counterpart: it logs the current user out of the keytab and resets the cached
`UserGroupInformation`, which is what you want before authenticating as a different principal in the same session.

```shell
hudi->kerberos kdestroy --krb5conf /etc/krb5.conf
Destroy Kerberos authentication
Parameters:
--krb5conf: /etc/krb5.conf
Current user: user (auth:SIMPLE)
Login user: user (auth:SIMPLE)
Destroy Kerberos authentication success
```

`--krb5conf` defaults to `/etc/krb5.conf`. If no user is currently logged in with Kerberos, the command prints
`Currently, no user login with kerberos, do nothing` and still reports success, so it is safe to run twice.


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
* diff file - Check how file differs across range of commits
* diff partition - Check how file differs across range of commits. It is meant to be used only for partitioned tables.
* downgrade table - Downgrades a table
* exit - Exits the shell
* export instants - Export Instants and their metadata from the Timeline
* fetch table schema - Fetches latest table schema
* hdfsparquetimport - Imports Parquet table to a hoodie table
* help - List all commands usage
* kerberos kdestroy - Destroy Kerberos authentication
* locks audit cleanup - Clean up old audit lock files
* locks audit disable - Disable storage lock audit service for the current table
* locks audit enable - Enable storage lock audit service for the current table
* locks audit status - Show the current status of lock audit service
* locks audit validate - Validate audit lock files for consistency and integrity
* marker delete - Delete the marker
* metadata create - Create the Metadata Table if it does not exist
* metadata delete - Remove the Metadata Table
* metadata delete-record-index - Delete the record index from Metadata Table
* metadata init - Update the metadata table from commits since the creation
* metadata list-files - Print a list of all files in a partition from the metadata
* metadata list-partitions - List all partitions from metadata
* metadata refresh - Refresh table metadata
* metadata set - Set options for Metadata Table
* metadata stats - Print stats about the metadata
* metadata timeline show active - List all instants in active timeline of metadata table
* metadata timeline show incomplete - List all incomplete instants in active timeline of metadata table
* metadata validate-files - Validate all files in all partitions from the metadata
* quit - Exits the shell
* refresh - Refresh table metadata
* rename partition - Rename partition. Usage: rename partition --oldPartition <oldPartition> --newPartition <newPartition>
* repair addpartitionmeta - Add partition metadata to a table, if not present
* repair corrupted clean files - repair corrupted clean files
* repair deduplicate - De-duplicate a partition path contains duplicates & produce repaired files to replace with
* repair deprecated partition - Repair deprecated partition ("default"). Re-writes data from the deprecated partition into __HIVE_DEFAULT_PARTITION__
* repair migrate-partition-meta - Migrate all partition meta file currently stored in text format to be stored in base file format. See HoodieTableConfig#PARTITION_METAFILE_USE_DATA_FORMAT.
* repair overwrite-hoodie-props - Overwrite hoodie.properties with provided file. Risky operation. Proceed with caution!
* repair show empty commit metadata - show failed commits
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
* show restore - Show details of a restore instant
* show restores - List all restore instants
* show rollback - Show details of a rollback instant
* show rollbacks - List all rollback instants
* stats filesizes - File Sizes. Display summary stats on sizes of files
* stats wa - Write Amplification. Ratio of how many records were upserted to how many records were actually written
* sync validate - Validate the sync by counting the number of records
* system properties - Shows the shell's properties
* table delete-configs - Delete the supplied table configs from the table.
* table recover-configs - Recover table configs, from update/delete that failed midway.
* table set-meta-fields-mode - Set hoodie.meta.fields.mode on an existing table.
* table update-configs - Update the table configs with configs with provided file.
* temp_delete - Delete view name
* temp_query - query against created temp view
* temp delete - Delete view name
* temp query - query against created temp view
* temps_show - Show all views name
* temps show - Show all views name
* timeline show active - List all instants in active timeline
* timeline show incomplete - List all incomplete instants in active timeline
* trigger archival - trigger archival
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

To list all inflight and requested instants that have been running longer than a specified number of minutes, use `commits show_inflights`:

```shell
hudi:trips->commits show_inflights --lookbackInMins 30
```

This lists every inflight or requested instant whose requested timestamp is older than 30 minutes, showing the commit time, action type, and current state. This is useful for detecting hung or stuck writes. The `--lookbackInMins` option defaults to `0` (returns all inflight/requested instants).

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

Archival normally runs inline with writes. `trigger archival` runs it on demand, as a Spark job, for the table you are
connected to. The retention options mirror the write configs of the same name, so passing nothing here archives with
Hudi's defaults rather than with whatever your writer is configured to use.

```java
hudi:trips->trigger archival --minCommits 20 --maxCommits 30 --commitsRetainedByCleaner 10 --enableMetadata true
Archival successfully triggered
```

| Option | Default | Description |
| --- | --- | --- |
| `--minCommits` | `20` | Minimum number of instants to retain in the active timeline. Mirrors `hoodie.keep.min.commits`. |
| `--maxCommits` | `30` | Maximum number of instants to retain in the active timeline. Mirrors `hoodie.keep.max.commits`. |
| `--commitsRetainedByCleaner` | `10` | Number of commits to retain without cleaning. |
| `--enableMetadata` | `true` | Whether the metadata table is enabled for this run. |
| `--sparkMemory` | `1G` | Spark executor memory. |
| `--sparkMaster` | `local` | Spark master. |

The command reports `Archival successfully triggered` on a zero exit code from the Spark job, and
`Failed to trigger archival` otherwise. Check the Spark logs for why.


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
\{: .notice--info}

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
\{: .notice--warning}

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
hudi:trips->compaction unschedule --instant <compactionInstant>
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
savepoint at a later point in time if need be. You can read more about savepoints and restore [here](disaster_recovery.md)

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

A rollback to a savepoint writes a `restore` instant on the timeline. `show restores` lists them, and `show restore`
expands a single one into the commits it reverted, which is how you confirm after the fact what a restore actually
undid.

```java
hudi:trips->show restores --limit 10 --includeInflights false
hudi:trips->show restore --instant 20220128170512331
```

`show restores` accepts `--limit` (default `10`), `--sortBy` (default unset), `--desc` (default `false`),
`--headeronly` (default `false`), and `--includeInflights` (default `false`, so only completed restores are listed).
`show restore` takes the instant to expand via `--instant` and the same `--limit` / `--sortBy` / `--desc` /
`--headeronly` display options; it has no `--includeInflights`.

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
| `NINE` or `9`      | 1.1.x - 1.2.x           |
| `EIGHT` or `8`     | 1.0.x                   |
| `SIX` or `6`       | 0.14.x - 0.15.x         |
| `FIVE` or `5`      | 0.12.x - 0.13.x         |
| `FOUR` or `4`      | 0.11.x                  |
| `THREE` or `3`     | 0.10.x                  |
| `TWO` or `2`       | 0.9.x                   |
| `ONE` or `1`       | 0.6.x - 0.8.x           |
| `ZERO` or `0`      | 0.5.x and below         |

For example, to downgrade a table from version `EIGHT`(`8`) (current version) to `SIX`(`6`), you should run (use proper Spark master based
on your environment)

```shell
downgrade table --toVersion SIX --sparkMaster local[2]
```

or

```shell
downgrade table --toVersion 6 --sparkMaster local[2]
```

You can verify the table version by looking at the `hoodie.table.version` property in `.hoodie/hoodie.properties` under
the table path:

```properties
hoodie.table.version=6
```

Hudi CLI also provides the ability to manually upgrade a Hudi table.  To upgrade a Hudi table through CLI:

```shell
upgrade table --toVersion <target_version>
```

:::note
Table upgrade is automatically handled by the Hudi write client in different deployment modes such as Hudi Streamer
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

### Record Index Lookup

To look up a record's file location via the Record Level Index (RLI) stored in the Metadata Table:

```shell
hudi:trips->metadata lookup-record-index --record_key <key>
```

For a partitioned (non-global) RLI, the partition path is required:

```shell
hudi:trips->metadata lookup-record-index --record_key <key> --partition_path <partition>
```

The `--partition_path` argument is optional for a global RLI (where record keys are unique across all partitions) and required for a partitioned RLI. If `--partition_path` is omitted for a partitioned RLI, the command will return an error. The output columns are `Record key`, `Partition path`, `File Id`, and `Instant time`.

To drop the record index partition from the Metadata Table entirely, for example before rebuilding it:

```shell
hudi:trips->metadata delete-record-index --backup true
Record Index has been deleted from the Metadata Table and backed up to /user/hive/warehouse/table1/.hoodie/.metadata_record_index_20260831090412345
```

`--backup` defaults to `true`. The backup is a rename rather than a copy, so it is cheap: the
`.hoodie/metadata/record_index` partition is moved to `.hoodie/.metadata_record_index_<instantTime>`, where
`<instantTime>` is the current instant in `yyyyMMddHHmmssSSS` form. Pass `--backup false` to delete the partition
outright, in which case the output is just `Record Index has been deleted from the Metadata Table` and the only way
back is to rebuild the index.

Either way the command first flips the partition off in the table config, so readers stop consulting the index before
the files go away. If the record index partition does not exist, nothing is deleted and the message still prints, with
`null` in place of the backup path.

### Change Hudi Table Type
There are cases we want to change the hudi table type. For example, change COW table to MOR for more efficient and 
lower latency ingestion; change MOR to COW for better read performance and compatibility with downstream engines.
So we offer the table command to perform this modification conveniently. 

Changing **COW to MOR**, we can simply modify the `hoodie.table.type` in `hoodie.properties` to MERGE_ON_READ.

While changing **MOR to COW**, we must make sure all the log files are compacted before modifying the table type, 
or it will cause data loss.

```shell
connect --path <table_path>
table change-table-type <target_table_type>
```

The parameter `target_table_type` candidates are below:

| target table type | comment                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
|:------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| MOR               | Change COW table to MERGE_ON_READ.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| COW               | Change MOR table to COPY_ON_WRITE. <br/>By default, changing to COW will **execute all pending compactions** and **perform a full compaction** if any log file left. Set `--enable-compaction=false` will disable the default compaction. <br/>There are params can be set for the compaction operation:<br/>`--parallelism`: Default `3`. Parallelism for hoodie compaction<br/>`--sparkMaster`: Default `local`. Spark Master<br/>`--sparkMemory`: Default `4G`. Spark executor memory<br/>`--retry`: Default `1`. Number of retries<br/>`--propsFilePath`: Default ` `. path to properties file on localfs or dfs with configurations for hoodie client for compacting<br/>`--hoodieConfigs`: Default ` `. Any configuration that can be set in the properties file can be passed here in the form of an array |


Example below is changing MOR table to COW:
```shell
connect --path /var/dataset/test_table_mor2cow
desc
╔════════════════════════════════════════════════╤═════════════════════════════════════════╗
║ Property                                       │ Value                                   ║
╠════════════════════════════════════════════════╪═════════════════════════════════════════╣
║ basePath                                       │ /var/dataset/test_table_mor2cow         ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ metaPath                                       │ /var/dataset/test_table_mor2cow/.hoodie ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ fileSystem                                     │ file                                    ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.name                              │ test_table                              ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.compaction.record.merger.strategy       │ eeb8d96f-b1e4-49fd-bbf8-28ac514178e5    ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.metadata.partitions               │ files                                   ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.type                              │ MERGE_ON_READ                           ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.metadata.partitions.inflight      │                                         ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.archivelog.folder                       │ archived                                ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.timeline.layout.version                 │ 1                                       ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.checksum                          │ 2702201862                              ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.compaction.payload.type                 │ HOODIE_AVRO                             ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.table.version                           │ 6                                       ║
╟────────────────────────────────────────────────┼─────────────────────────────────────────╢
║ hoodie.datasource.write.drop.partition.columns │ false                                   ║
╚════════════════════════════════════════════════╧═════════════════════════════════════════╝

table change-table-type COW
╔════════════════════════════════════════════════╤══════════════════════════════════════╤══════════════════════════════════════╗
║ Property                                       │ Old Value                            │ New Value                            ║
╠════════════════════════════════════════════════╪══════════════════════════════════════╪══════════════════════════════════════╣
║ hoodie.archivelog.folder                       │ archived                             │ archived                             ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.compaction.payload.type                 │ HOODIE_AVRO                          │ HOODIE_AVRO                          ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.compaction.record.merger.strategy       │ eeb8d96f-b1e4-49fd-bbf8-28ac514178e5 │ eeb8d96f-b1e4-49fd-bbf8-28ac514178e5 ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.datasource.write.drop.partition.columns │ false                                │ false                                ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.checksum                          │ 2702201862                           │ 2702201862                           ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.metadata.partitions               │ files                                │ files                                ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.metadata.partitions.inflight      │                                      │                                      ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.name                              │ test_table                           │ test_table                           ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.type                              │ MERGE_ON_READ                        │ COPY_ON_WRITE                        ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.table.version                           │ 6                                    │ 6                                    ║
╟────────────────────────────────────────────────┼──────────────────────────────────────┼──────────────────────────────────────╢
║ hoodie.timeline.layout.version                 │ 1                                    │ 1                                    ║
╚════════════════════════════════════════════════╧══════════════════════════════════════╧══════════════════════════════════════╝
```

### Changing the Meta Fields Mode

`hoodie.meta.fields.mode` decides which of Hudi's meta columns are physically written into base files. It is a
storage-layout decision baked in at write time, so a write can never change it on an existing table.
`table set-meta-fields-mode` is the sanctioned way to change it.

```java
hudi:trips->table set-meta-fields-mode --target-mode COMMIT_TIME_ONLY
```

`--target-mode` accepts `ALL`, `NONE`, `COMMIT_TIME_ONLY`, `FILE_NAME_ONLY` or `COMMIT_TIME_AND_FILE_NAME`. The value
is resolved case-insensitively and trimmed, so `commit_time_only` is accepted too. Setting the mode the table is
already in is a no-op and reports as much.

On a table that already has commits, two guards apply, because this command changes the table property without
rewriting a single existing file:

- **Widening is refused outright**, and `--force` does not override it. Widening means the target mode populates a
  meta column the current mode does not. Since earlier files are not rewritten, the table would advertise a column
  that is null for every row written so far, and incremental queries and file-name lookups silently skip exactly
  those rows. To widen, recreate the table. The CLI uses the same predicate as the write path
  (`BaseHoodieWriteClient#validateAgainstTableProperties`), so the two cannot disagree about which transitions are
  legal.
- **Narrowing needs `--force`** (default `false`). It leaves mixed-mode files: old commits keep the old layout, new
  commits use the new one, and incremental and file-pruning semantics differ between the two sets. Passing `--force`
  logs a warning recording the transition and the commit count.

Neither guard applies to a table with no commits, where the mode can be set freely.

### Inspecting the Timeline

`commits show` lists completed commits. The timeline commands show every instant regardless of action and state, which
is what you want when diagnosing a stuck table: a compaction sitting in `REQUESTED`, or a rollback that never
completed, never appears in `commits show`.

```java
hudi:trips->timeline show active --limit 10
hudi:trips->timeline show incomplete
```

Both print `Instant`, `Action`, `State`, and the `Requested` / `Inflight` / `Completed` file modification times.
`timeline show incomplete` restricts the listing to instants that are not yet completed.

| Option | Default | Applies to | Description |
| --- | --- | --- | --- |
| `--limit` | `10` | both | Number of rows to display. |
| `--sortBy` | unset | both | Field to sort by. |
| `--desc` | `false` | both | Reverse the ordering. |
| `--headeronly` | `false` | both | Print the header only. |
| `--show-rollback-info` | `false` | both | For rollback instants, also show the instant being rolled back. |
| `--show-time-seconds` | `false` | both | Include seconds in the instant file modification times. |
| `--with-metadata-table` | `false` | `timeline show active` only | Show the metadata table timeline alongside the data table, adding `MT Action`, `MT State` and the three matching MT time columns. |

The metadata table has its own timeline, and the two can disagree when a metadata commit fails. To read it directly:

```java
hudi:trips->metadata timeline show active --limit 10
hudi:trips->metadata timeline show incomplete
```

These accept `--limit`, `--sortBy`, `--desc`, `--headeronly` and `--show-time-seconds`. They have no
`--show-rollback-info` and no `--with-metadata-table`, since they are already scoped to the metadata table.

### Diffing a File or Partition

`diff file` and `diff partition` replay the timeline and show every commit that touched a given file group or
partition, which is the quickest way to answer "what has been writing to this file". Both report the standard commit
columns plus the write statistics for the matching entries only.

```java
hudi:trips->diff file --fileId 5f8a1e0b-1b4b-4a3f-9b1a-2c7d6e5f4a3b-0 --limit 10
hudi:trips->diff partition --partitionPath 2026/08/26 --includeArchivedTimeline true
```

`diff file` takes `--fileId` and `diff partition` takes `--partitionPath` as a partition path relative to the table
base path. `diff partition` is only meaningful on a partitioned table. Both then share these options:

| Option | Default | Description |
| --- | --- | --- |
| `--includeArchivedTimeline` | `false` | Also scan archived instants, not just the active timeline. |
| `--startTs` | unset, meaning now minus 10 days | Start of the instant range. Only applied when `--includeArchivedTimeline` is `true`. |
| `--endTs` | unset, meaning now minus 1 day | End of the instant range. Only applied when `--includeArchivedTimeline` is `true`. |
| `--limit` | `-1`, meaning no limit | Number of rows to display. |
| `--sortBy` | unset | Field to sort by. |
| `--desc` | `false` | Reverse the ordering. |
| `--headeronly` | `false` | Print the header only. |

Note the interaction between the three range options, which is easy to get wrong. `--startTs` and `--endTs` are used
to select archived instants only. With the default `--includeArchivedTimeline false` the whole active timeline is
scanned and both bounds are ignored, so passing a narrow range does not restrict the output. Set
`--includeArchivedTimeline true` for the bounds to take effect, and note that the archived range is then merged with
the full active timeline rather than replacing it.

### Repairing a Table

`repair show empty commit metadata` scans completed instants on the active timeline and reports the ones whose
metadata file is empty, which is what a commit interrupted between file creation and metadata write leaves behind.

```java
hudi:trips->repair show empty commit metadata
```

Note that this command writes its findings to the CLI log at `WARN` level rather than returning a table, so with the
default logging configuration you will see the `Empty Commit: ...` lines in the console log rather than in a rendered
result. It only reports; it does not modify the timeline.

`rename partition` rewrites the data under one partition value into another, as a Spark job, and deletes the old
partition on success.

```java
hudi:trips->set --conf SPARK_HOME=<SPARK_HOME>
hudi:trips->rename partition --oldPartition 2026/08/26 --newPartition 2026-08-26 --sparkMaster local[2]
```

`repair deprecated partition` is the special case of that rename for tables written before Hudi settled on its
placeholder for the null partition value: it rewrites data from the deprecated `default` partition into
`__HIVE_DEFAULT_PARTITION__`.

```java
hudi:trips->repair deprecated partition --sparkMaster local[2]
```

Both take `--sparkProperties` (a Spark properties file path, empty by default), `--sparkMaster` (empty by default) and
`--sparkMemory` (`4G` by default). Both read the old partition, rewrite those records under the new partition value,
and then issue a `delete_partition` write against the old one, so the change goes through the timeline rather than
behind it. Both are a no-op when the old partition holds no records.

They differ in one respect worth knowing: `rename partition` additionally removes the old partition directory from
storage after the delete write, logging a warning if that removal fails, whereas `repair deprecated partition` leaves
the emptied `default` directory in place. Either way these rewrite data, so take a savepoint first if the table
matters.

### Auditing Storage Locks

When a table uses a storage-based lock provider, the lock provider can record every lock transition to a set of JSONL
files, so that a suspected concurrency violation can be reconstructed after the fact. The audit is off by default and
is controlled by a config file next to the locks themselves, at
`<basePath>/.hoodie/.locks/audit_enabled.json`; the audit records land in `<basePath>/.hoodie/.locks/audit/`.

```java
hudi:trips->locks audit enable
Lock audit enabled successfully.
Audit config written to: /user/hive/warehouse/table1/.hoodie/.locks/audit_enabled.json
Audit files will be stored at: /user/hive/warehouse/table1/.hoodie/.locks/audit
```

`locks audit status` reports whether auditing is on, and where both the config and the records live. A table that has
never had auditing enabled reports `DISABLED` with the config file marked `(not found)`.

```java
hudi:trips->locks audit status
Lock Audit Status: ENABLED
Table: /user/hive/warehouse/table1
Config file: /user/hive/warehouse/table1/.hoodie/.locks/audit_enabled.json
Audit files location: /user/hive/warehouse/table1/.hoodie/.locks/audit
```

`locks audit validate` is the reason to collect the records. It parses every `.jsonl` file in the audit folder into
transaction windows and checks them against each other. Overlapping windows are reported as errors, since two writers
holding the lock at once is exactly the violation the lock provider exists to prevent. A transaction that never
released its lock is reported as a warning, which usually means a driver OOM or a non-graceful shutdown rather than a
correctness problem. The verdict is `PASSED`, `WARNING` when only warnings were found, or `FAILED` when any error was.

```java
hudi:trips->locks audit validate
Validation Result: PASSED
Audit Files: 12 total, 12 parsed successfully, 0 failed to parse
Transactions Validated: 12
Issues Found: 0
Details: All audit lock transactions validated successfully
```

With no audit folder or no audit files the command reports `PASSED` with zero transactions validated, so a `PASSED`
verdict on its own does not prove that auditing was ever on. Check `locks audit status` first.

`locks audit cleanup` prunes old records. `--ageDays` defaults to `7` and `--dryRun` defaults to `false`, so run it
with `--dryRun true` first to see what it would remove.

```java
hudi:trips->locks audit cleanup --dryRun true --ageDays 30
```

`locks audit disable` turns auditing off. It keeps the existing records by default; pass `--keepAuditFiles false` to
delete them at the same time, which internally runs the same cleanup with no age threshold.

```java
hudi:trips->locks audit disable --keepAuditFiles true
```

All five commands require a table to be connected, and report `No Hudi table loaded. Please connect to a table first.`
otherwise.

## Command reference

Every command `hudi-cli` exposes, grouped by area, with its options and their defaults. An option marked
`(required)` has no default and must be supplied; a value in backticks after an option is its default.
The sections above cover the commonly used ones in more depth.

Some entries are aliases of the same command rather than distinct ones. `refresh`, `metadata refresh`,
`commits refresh`, `cleans refresh` and `savepoints refresh` are five names for one method that reloads the table
metadata, and `temp query` / `temp_query`, `temp delete` / `temp_delete` and `temps show` / `temps_show` are
underscore and space spellings of the same three commands.

### Table and session

- **`cleans refresh`** Refresh table metadata.
- **`commits refresh`** Refresh table metadata.
- **`connect`** Connect to a hoodie table.
  <br />Options: `--path` (required), `--eventuallyConsistent` (`false`), `--initialCheckIntervalMs` (`2000`), `--maxWaitIntervalMs` (`300000`), `--maxCheckIntervalMs` (`7`), `--timeGeneratorType` (`WAIT_TO_ADJUST_SKEW`), `--maxExpectedClockSkewMs` (`200`), `--useDefaultLockProvider` (`false`)
- **`create`** Create a hoodie table if not present.
  <br />Options: `--path` (required), `--tableName` (required), `--tableType` (`COPY_ON_WRITE`), `--archiveLogFolder`, `--tableVersion`, `--payloadClass` (`org.apache.hudi.common.model.HoodieAvroPayload`)
- **`desc`** Describe Hoodie Table properties.
- **`fetch table schema`** Fetches latest table schema.
  <br />Options: `--outputFilePath`
- **`kerberos kdestroy`** Destroy Kerberos authentication.
  <br />Options: `--krb5conf` (`/etc/krb5.conf`)
- **`kerberos kinit`** Perform Kerberos authentication.
  <br />Options: `--krb5conf` (`/etc/krb5.conf`), `--principal` (required), `--keytab` (required)
- **`metadata refresh`** Refresh table metadata.
- **`refresh`** Refresh table metadata.
- **`savepoints refresh`** Refresh table metadata.
- **`set`** Set spark launcher env to cli.
  <br />Options: `--conf` (required)
- **`show env`** Show spark launcher env by key.
  <br />Options: `--key` (required)
- **`show envs all`** Show spark launcher envs.
- **`table change-table-type`** Change hudi table type to target type: COW or MOR.
  <br />Options: `--target-type` (required), `--enable-compaction` (`true`), `--parallelism` (`3`), `--sparkMaster` (`local`), `--sparkMemory` (`4G`), `--retry` (`1`), `--propsFilePath`, `--hoodieConfigs`
- **`table delete-configs`** Delete the supplied table configs from the table.
  <br />Options: `--comma-separated-configs` (required)
- **`table recover-configs`** Recover table configs, from update/delete that failed midway.
- **`table set-meta-fields-mode`** Set hoodie.meta.fields.mode on an existing table. This is the sanctioned way to change.
  <br />Options: `--target-mode` (required), `--force` (`false`)
- **`table update-configs`** Update the table configs with configs with provided file.
  <br />Options: `--props-file` (required)
- **`utils loadClass`** Load a class.
  <br />Options: `--class` (required)

### Commits and the timeline

- **`commit show_write_stats`** Show write stats of a commit.
  <br />Options: `--createView`, `--commit` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeArchivedTimeline` (`false`)
- **`commit showfiles`** Show file level details of a commit.
  <br />Options: `--createView`, `--commit` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeArchivedTimeline` (`false`)
- **`commit showpartitions`** Show partition level details of a commit.
  <br />Options: `--createView`, `--commit` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeArchivedTimeline` (`false`)
- **`commits compare`** Compare commits with another Hoodie table.
  <br />Options: `--path` (required)
- **`commits show`** Show the commits.
  <br />Options: `--includeExtraMetadata` (`false`), `--createView`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--partition`, `--includeArchivedTimeline` (`false`)
- **`commits show_inflights`** Show inflight instants that are left longer than a certain duration.
  <br />Options: `--lookbackInMins` (`0`)
- **`commits showarchived`** Show the archived commits.
  <br />Options: `--includeExtraMetadata` (`false`), `--createView`, `--startTs`, `--endTs`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--partition`
- **`commits sync`** Sync commits with another Hoodie table.
  <br />Options: `--path` (required)
- **`diff file`** Check how file differs across range of commits.
  <br />Options: `--fileId` (required), `--startTs`, `--endTs`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeArchivedTimeline` (`false`)
- **`diff partition`** Check how file differs across range of commits. It is meant to be used only for partitioned tables.
  <br />Options: `--partitionPath` (required), `--startTs`, `--endTs`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeArchivedTimeline` (`false`)
- **`metadata timeline show active`** List all instants in active timeline of metadata table.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--show-time-seconds` (`false`)
- **`metadata timeline show incomplete`** List all incomplete instants in active timeline of metadata table.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--show-time-seconds` (`false`)
- **`show archived commit stats`** Read commits from archived files and show file group details.
  <br />Options: `--archiveFolderPattern`, `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show archived commits`** Read commits from archived files and show details.
  <br />Options: `--skipMetadata` (`true`), `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`timeline show active`** List all instants in active timeline.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--with-metadata-table` (`false`), `--show-rollback-info` (`false`), `--show-time-seconds` (`false`)
- **`timeline show incomplete`** List all incomplete instants in active timeline.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--show-rollback-info` (`false`), `--show-time-seconds` (`false`)
- **`trigger archival`** Trigger archival.
  <br />Options: `--minCommits` (`20`), `--maxCommits` (`30`), `--commitsRetainedByCleaner` (`10`), `--enableMetadata` (`true`), `--sparkMemory` (`1G`), `--sparkMaster` (`local`)

### Files, stats and log files

- **`show fsview all`** Show entire file-system view.
  <br />Options: `--pathRegex` (`*`), `--baseFileOnly` (`false`), `--maxInstant`, `--includeMax` (`false`), `--includeInflight` (`false`), `--excludeCompaction` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show fsview latest`** Show latest file-system view.
  <br />Options: `--partitionPath`, `--baseFileOnly` (`false`), `--maxInstant`, `--merge` (`true`), `--includeMax` (`false`), `--includeInflight` (`false`), `--excludeCompaction` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show logfile metadata`** Read commit metadata from log files.
  <br />Options: `--logFilePathPattern` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show logfile records`** Read records from log files.
  <br />Options: `--limit` (`10`), `--logFilePathPattern` (required), `--mergeRecords` (`false`)
- **`stats filesizes`** File Sizes. Display summary stats on sizes of files.
  <br />Options: `--partitionPath` (`*/*/*`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`stats wa`** Write Amplification. Ratio of how many records were upserted to how many.
  <br />Options: `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)

### Table services

- **`clean showpartitions`** Show partition level details of a clean.
  <br />Options: `--clean` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`cleans run`** Run clean.
  <br />Options: `--sparkMemory` (`4G`), `--propsFilePath`, `--hoodieConfigs`, `--sparkMaster`
- **`cleans show`** Show the cleans.
  <br />Options: `--limit` (`-1`), `--sortBy`, `--startTs`, `--endTs`, `--includeArchivedTimeline` (`false`), `--desc` (`false`), `--headeronly` (`false`)
- **`clustering run`** Run Clustering.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`), `--sparkMemory` (`4g`), `--parallelism` (`1`), `--retry` (`1`), `--clusteringInstant`, `--propsFilePath`, `--hoodieConfigs`
- **`clustering schedule`** Schedule Clustering.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`), `--sparkMemory` (`1g`), `--propsFilePath`, `--hoodieConfigs`
- **`clustering scheduleAndExecute`** Run Clustering. Make a cluster plan first and execute that plan immediately.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`), `--sparkMemory` (`4g`), `--parallelism` (`1`), `--retry` (`1`), `--propsFilePath`, `--hoodieConfigs`
- **`compaction repair`** Renames the files to make them consistent with the timeline as.
  <br />Options: `--instant` (required), `--parallelism` (`3`), `--sparkMaster` (`local`), `--sparkMemory` (`2G`), `--dryRun` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`compaction run`** Run Compaction for given instant time.
  <br />Options: `--parallelism` (`3`), `--schemaFilePath`, `--sparkMaster` (`local`), `--sparkMemory` (`4G`), `--retry` (`1`), `--compactionInstant`, `--propsFilePath`, `--hoodieConfigs`
- **`compaction schedule`** Schedule Compaction.
  <br />Options: `--sparkMemory` (`1G`), `--propsFilePath`, `--hoodieConfigs`, `--sparkMaster` (`local`)
- **`compaction scheduleAndExecute`** Schedule compaction plan and execute this plan.
  <br />Options: `--parallelism` (`3`), `--schemaFilePath`, `--sparkMaster` (`local`), `--sparkMemory` (`4G`), `--retry` (`1`), `--propsFilePath`, `--hoodieConfigs`
- **`compaction show`** Shows compaction details for a specific compaction instant.
  <br />Options: `--instant` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--partition`
- **`compaction showarchived`** Shows compaction details for a specific compaction instant.
  <br />Options: `--instant` (required), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--partition`
- **`compaction unschedule`** Unschedule Compaction.
  <br />Options: `--instant` (required), `--parallelism` (`3`), `--sparkMaster` (`local`), `--sparkMemory` (`2G`), `--skipValidation` (`false`), `--dryRun` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`compaction unscheduleFileId`** UnSchedule Compaction for a fileId.
  <br />Options: `--fileId` (required), `--partitionPath`, `--sparkMaster` (`local`), `--sparkMemory` (`2G`), `--skipValidation` (`false`), `--dryRun` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`compaction validate`** Validate Compaction.
  <br />Options: `--instant` (required), `--parallelism` (`3`), `--sparkMaster` (`local`), `--sparkMemory` (`2G`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`compactions show all`** Shows all compactions that are in active timeline.
  <br />Options: `--includeExtraMetadata` (`false`), `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`compactions showarchived`** Shows compaction details for specified time window.
  <br />Options: `--includeExtraMetadata` (`false`), `--startTs`, `--endTs`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`marker delete`** Delete the marker.
  <br />Options: `--commit` (required), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`1G`)

### Rollback, savepoint, restore and repair

- **`commit rollback`** Rollback a commit.
  <br />Options: `--commit` (required), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`), `--rollbackUsingMarkers` (`false`)
- **`downgrade table`** Downgrades a table.
  <br />Options: `--toVersion`, `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)
- **`rename partition`** Rename partition. Usage: rename partition --oldPartition &lt;oldPartition&gt; --newPartition &lt;newPartition&gt;.
  <br />Options: `--oldPartition` (required), `--newPartition` (required), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)
- **`repair addpartitionmeta`** Add partition metadata to a table, if not present.
  <br />Options: `--dryrun` (`true`)
- **`repair corrupted clean files`** Repair corrupted clean files.
- **`repair deduplicate`** De-duplicate a partition path contains duplicates & produce repaired files to replace with.
  <br />Options: `--duplicatedPartitionPath`, `--repairedOutputPath` (required), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`), `--dryrun` (`true`), `--dedupeType` (`insert_type`)
- **`repair deprecated partition`** Repair deprecated partition ("default"). Re-writes data from the deprecated partition into.
  <br />Options: `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)
- **`repair migrate-partition-meta`** Migrate all partition meta file currently stored in text format.
  <br />Options: `--dryrun` (`true`)
- **`repair overwrite-hoodie-props`** Overwrite hoodie.properties with provided file. Risky operation. Proceed with caution!.
  <br />Options: `--new-props-file` (required)
- **`repair show empty commit metadata`** Show failed commits.
- **`savepoint create`** Savepoint a commit.
  <br />Options: `--commit` (required), `--user` (`default`), `--comments` (`default`), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)
- **`savepoint delete`** Delete the savepoint.
  <br />Options: `--commit` (required), `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)
- **`savepoint rollback`** Savepoint a commit.
  <br />Options: `--savepoint` (required), `--sparkProperties`, `--sparkMaster`, `--lazyFailedWritesCleanPolicy` (`false`), `--sparkMemory` (`4G`)
- **`savepoints show`** Show the savepoints.
- **`show restore`** Show details of a restore instant.
  <br />Options: `--instant` (required), `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show restores`** List all restore instants.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`), `--includeInflights` (`false`)
- **`show rollback`** Show details of a rollback instant.
  <br />Options: `--instant` (required), `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`show rollbacks`** List all rollback instants.
  <br />Options: `--limit` (`10`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`upgrade table`** Upgrades a table.
  <br />Options: `--toVersion`, `--sparkProperties`, `--sparkMaster`, `--sparkMemory` (`4G`)

### Metadata table

- **`metadata create`** Create the Metadata Table if it does not exist.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`)
- **`metadata delete`** Remove the Metadata Table.
  <br />Options: `--backup` (`true`)
- **`metadata delete-record-index`** Delete the record index from Metadata Table.
  <br />Options: `--backup` (`true`)
- **`metadata init`** Update the metadata table from commits since the creation.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`), `--readonly` (`false`)
- **`metadata list-files`** Print a list of all files in a partition from the metadata.
  <br />Options: `--partition`
- **`metadata list-partitions`** List all partitions from metadata.
  <br />Options: `--sparkMaster` (`SparkUtil.DEFAULT_SPARK_MASTER`)
- **`metadata lookup-record-index`** Print Record index information for a record_key.
  <br />Options: `--record_key` (required), `--partition_path` (required)
- **`metadata set`** Set options for Metadata Table.
  <br />Options: `--metadataDir`
- **`metadata stats`** Print stats about the metadata.
- **`metadata validate-files`** Validate all files in all partitions from the metadata.
  <br />Options: `--verbose` (`false`)

### Bootstrap

- **`bootstrap index showmapping`** Show bootstrap index mapping.
  <br />Options: `--partitionPath`, `--fileIds`, `--limit` (`-1`), `--sortBy`, `--desc` (`false`), `--headeronly` (`false`)
- **`bootstrap index showpartitions`** Show bootstrap indexed partitions.
- **`bootstrap run`** Run a bootstrap action for current Hudi table.
  <br />Options: `--srcPath` (required), `--targetPath` (required), `--tableName` (required), `--tableType` (required), `--rowKeyField` (required), `--partitionPathField`, `--bootstrapIndexClass` (`org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex`), `--selectorClass` (`org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector`), `--keyGeneratorClass` (`org.apache.hudi.keygen.SimpleKeyGenerator`), `--fullBootstrapInputProvider` (`org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider`), `--schemaProviderClass`, `--payloadClass`, `--merge-mode`, `--merge-strategy-id`, `--merge-impl-classes`, `--parallelism` (`1500`), `--sparkMaster`, `--sparkMemory` (`4G`), `--enableHiveSync` (`false`), `--propsFilePath`, `--hoodieConfigs`

### Lock auditing

- **`locks audit cleanup`** Clean up old audit lock files.
  <br />Options: `--dryRun` (`false`), `--ageDays` (`7`)
- **`locks audit disable`** Disable storage lock audit service for the current table.
  <br />Options: `--keepAuditFiles` (`true`)
- **`locks audit enable`** Enable storage lock audit service for the current table.
- **`locks audit status`** Show the current status of lock audit service.
- **`locks audit validate`** Validate audit lock files for consistency and integrity.

### Export, temp views and sync

- **`export instants`** Export Instants and their metadata from the Timeline.
  <br />Options: `--limit` (`-1`), `--actions` (`clean,commit,deltacommit,rollback,savepoint,restore`), `--desc` (`false`), `--localFolder` (required)
- **`sync validate`** Validate the sync by counting the number of records.
  <br />Options: `--mode` (`complete`), `--sourceDb` (`rawdata`), `--targetDb` (`dwh_hoodie`), `--partitionCount` (`5`), `--hiveServerUrl` (required), `--hiveUser`, `--hivePass`
- **`temp delete`** Delete view name.
  <br />Options: `--view` (required)
- **`temp query`** Query against created temp view.
  <br />Options: `--sql` (required)
- **`temp_delete`** Delete view name.
  <br />Options: `--view` (required)
- **`temp_query`** Query against created temp view.
  <br />Options: `--sql` (required)
- **`temps show`** Show all views name.
- **`temps_show`** Show all views name.


## Related Resources

<h3>Blogs</h3>
* [Getting Started: Manage your Hudi tables with the admin Hudi-CLI tool](https://www.onehouse.ai/blog/getting-started-manage-your-hudi-tables-with-the-admin-hudi-cli-tool)
