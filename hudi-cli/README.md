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

Hudi cli tool assist in managing hudi tables.

Set up:
If you are looking to access a local table, you can just invoke hudi-cli.sh from hudi-cli/ directory.
No other additional steps are required.

But if you are looking to access a dataset in hdfs or EMR, etc, you might have to tar hudi-cli directory
and import the tar to your dfs.
1. Tar hudi-cli/* 
2. Import the tar to dfs
3. Untar
4. Set below env variables
  HADOOP_CONF_DIR // represents your hadoop conf dir. In EMR value = /etc/hadoop/conf
  SPARK_CONF_DIR // represents your spark conf dir. In EMR value = /etc/spark/conf
5. Launch hudi-cli.sh from hudi-cli directory. 

Let's go over the commands available with hudi-cli tool. 

Note: hudi-cli tool has help and auto complete feature which should help you show different options available for any
given command. for eg: `commit show_write_stats` is one of the commands available with hudi-cli. After typing out this
command, if you press `[TAB]`, it will show what are the additional options that could be passed in with this command 
typed so far. On similar lines, you can also try [TAB] after `--`. For example, `sync validate --` [TAB] will show
`sync validate --hivePass         sync validate --hiveServerUrl    sync validate --hiveUser ` as possible options. 
Now, lets go through all commands available with hudi-cli.

- `help` - List all commands usage
- `connect` - Connect to a hoodie table
   Connects hudi-cli to a hudi table. Any command with a hudi table has to be connected to get started. 
   ```
   connect --path <hudi-table-path>
   ```
- `create` - Create a hoodie table if not present.
   ```
   create --path <hudi-table-path> --tableName <tableName>
   ```
- `desc` - Describe Hoodie Table properties
Note: most of the commits/commit command has extra options with each command. Like --headeronly, --limit, --desc, 
   --sortBy. Use it as per necessity as we can't list all possible options with every command.    
- `commits show` - Lists all commits in hudi table.
- `commits showarchived` - Lists all archived commits in hudi table.
- `commits refresh` - Refreshes the commit timeline. If there are ongoing operations after launching
    hudi-cli tool, this will refresh your commits to include all latest commits too.
- `commits compare` - Compares commits with another hudi table. 
- `commits sync` - Syncs current hudi table with another table. 
- `commit show_write_stats` - Lists write stats for commit of interest. A commit id need to be provided as an argument
   ```
   commit show_write_stats --commit 20210501233331
   ```
- `commit showfiles` - Lists files for a given commit.
   ```
   commit showfiles --commit 20210501233331
   ```
- `commit showpartitions` - Shows partition information for a given commit.
   ```
   commit showpartitions --commit 20210501233331
   ```
- `commit rollback` - Rollsback a commit.
   ```
   commit rollback --commit 20210501233331
   ```
- `stats filesizes`: Display summary stats on sizes of files
   ```
   stats filesizes
   ```
   ```
   stats filesizes --partitionPath <PartitionPath>
   ```
- `stats wa`: Write Amplification. Ratio of how many records were upserted to how many records were actually written
- `show fsview all` - Show entire file-system view.
- `show fsview latest` - Show latest file-system view.
- `show rollbacks` - Lists all rollback instants.
- `show rollback` - Show details of a rollback instant
```
show rollback --instant <Instant>
```
- `show archived commits` - Read commits from archived files and show details
- `show archived commit stats` -  Read commits from archived files and show details.
- `show logfile metadata` - Read commit metadata from log files.
```
show logfile metadata --logFilePathPattern <log_file_path>
```
- `show logfile records` - Read records from log files.
```
show logfile records --logFilePathPattern <log_file_path>
```
- `show env` - Show spark launcher env by key
- `show envs all` - Show spark launcher envs
- `compactions show all` - Shows all compactions that are in active timeline
- `compaction show` - Shows compaction details for a specific compaction instant
```
compaction show --instant <Instant>
```
- `compactions showarchived` - Shows compaction details for specified time window
- `compaction showarchived` - Shows compaction details for a specific compaction instant
- `compaction run` - Run Compaction for given instant time
- `compaction schedule` - Schedule Compaction
- `compaction unschedule` - Unschedule Compaction
- `compaction unscheduleFileId` - UnSchedule Compaction for a fileId
- `compaction validate` - Validate Compaction
- `compaction repair` - Renames the files to make them consistent with the timeline as dictated by Hoodie metadata. Use when compaction unschedule fails partially.
- `repair addpartitionmeta` - Add partition metadata to a table, if not present
- `repair corrupted clean files` - repair corrupted clean files
- `repair deduplicate` - De-duplicate a partition path contains duplicates & produce repaired files to replace with
- `repair overwrite-hoodie-props` - Overwrite hoodie.properties with provided file. Risky operation. Proceed with caution!
- `savepoints show` - Show the savepoints
- `savepoint create` - Savepoint a commit
- `savepoint delete` - Delete the savepoint
- `savepoint rollback` - Savepoint a commit
- `savepoints refresh` - Refresh table metadata
- `cleans show` - Show the cleans
- `cleans run` - run clean
- `clean showpartitions` - Show partition level details of a clean
- `cleans refresh` - Refresh table metadata
- `sync validate` - validate hive sync
    ```
    sync validate --hiveUser <user> --hivePass <password> --hiveServerUrl <hive_server_url>
    ```
  There are lot of options with this command like --mode, --sourceDb, --targetDb, etc.
- `metadata create` - Create the Metadata Table if it does not exist
- `metadata delete` - Remove the Metadata Table
- `metadata init` - Update the metadata table from commits since the creation
- `metadata list-files` - Print a list of all files in a partition from the metadata
- `metadata list-partitions` - Print a list of all partitions from the metadata
- `metadata refresh` - Refresh table metadata
- `metadata set` - Set options for Metadata Table
- `metadata stats` - Print stats about the metadata
- `clustering run` - Run Clustering
- `clustering schedule` - Schedule Clustering
- `bootstrap index showmapping` - Show bootstrap index mapping
- `bootstrap index showpartitions` - Show bootstrap indexed partitions
- `bootstrap run` - Run a bootstrap action for current Hudi table
- `temps_show` - Show all views name
- `temps show` - Show all views name
- `temp_query` - query against created temp view
- `temp query` - query against created temp view
- `temp delete` - Delete view name
- `temp_delete` - Delete view name
- `export instants` - Export Instants and their metadata from the Timeline
- `hdfsparquetimport` - Imports Parquet table to a hoodie table
- `refresh` - Refresh table metadata

- `!` - Allows execution of operating system (OS) commands
- `//` - Inline comment markers (start of line only)
- `;` - Inline comment markers (start of line only)
- `clear` - Clears the console
- `cls` - Clears the console
- `date` - Displays the local date and time
- `script` - Parses the specified resource file and executes its commands
- `set` - Set spark launcher env to cli
- `system properties` - Shows the shell's properties
- `utils loadClass` - Load a class
- `version` - Displays shell version

- `exit` - Exits the shell
- `quit` - Exits the shell