---
title: Procedures
summary: "In this page, we introduce how to use procedures with Hudi."
toc: true
last_modified_at: 
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

Stored procedures available when use Hudi SparkSQL extensions in all spark's version.

## Usage
CALL supports passing arguments by name (recommended) or by position. Mixing position and named arguments is also supported.

#### Named arguments
All procedure arguments are named. When passing arguments by name, arguments can be in any order and any optional argument can be omitted.
```
CALL system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1, ... arg_name_n => arg_n);
```
#### Positional arguments
When passing arguments by position, the arguments may be omitted if they are optional.
```
CALL system.procedure_name(arg_1, arg_2, ... arg_n);
```
*note:* The system here has no practical meaning, the complete procedure name is system.procedure_name.

### Help Procedure

Show parameters and outputTypes of a procedure.

**Input**

| Parameter Name | Type   | Required | Default Value | Description         |
|----------------|--------|----------|---------------|---------------------|
| cmd            | String | N        | None          | name of a procedure |

**Output**

| Output Name  | Type   |
|--------------|--------|
| result       | String |

**Example**x

```
call help(cmd => 'show_commits');
```

| result                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                | 
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| parameters: <br/>param                           type_name                       default_value                   required <br/>table                           string                          None                            true <br/>limit                           integer                         10                              false <br/>outputType: <br/>name                            type_name                       nullable                        metadata <br/>commit_time                     string                          true                            \{} <br/>action                          string                          true                            \{} <br/>total_bytes_written             long                            true                            \{} <br/>total_files_added               long                            true                            \{} <br/>total_files_updated             long                            true                            \{} <br/>total_partitions_written        long                            true                            \{} <br/>total_records_written           long                            true                            \{} <br/>total_update_records_written    long                            true                            \{} <br/>total_errors                    long                            true                            \{} | 

## Commit management

### show_commits

Show commits' info.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name                  | Type   |
|------------------------------|--------|
| commit_time                  | String |
| total_bytes_written          | Long   |
| total_files_added            | Long   |
| total_files_updated          | Long   |
| total_partitions_written     | Long   |
| total_records_written        | Long   |
| total_update_records_written | Long   |
| total_errors                 | Long   |

**Example**

```
call show_commits(table => 'test_hudi_table', limit => 10);
```

| commit_time       | 	total_bytes_written     | total_files_added | total_files_updated | total_partitions_written | total_records_written | total_update_records_written | total_errors |
|-------------------|--------------------------|-------------------|---------------------|--------------------------|-----------------------|------------------------------|--------------|
| 20220216171049652 | 	432653                  | 0                 | 1                   | 1                        | 0                     | 0                            | 0            |
| 20220216171027021 | 	435346                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |
| 20220216171019361 | 	435349                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |

### show_commits_metadata

Show commits' metadata.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name                     | Type   |
|---------------------------------|--------|
| commit_time                     | String |
| action                          | String |
| partition                       | String |
| file_id                         | String |
| previous_commit                 | String |
| num_writes                      | Long   |
| num_inserts                     | Long   |
| num_deletes                     | Long   |
| num_update_writes               | String |
| total_errors                    | Long   |
| total_log_blocks                | Long   |
| total_corrupt_logblocks         | Long   |
| total_rollback_blocks           | Long   |
| total_log_records               | Long   |
| total_updated_records_compacted | Long   |
| total_bytes_written             | Long   |

**Example**

```
call show_commits_metadata(table => 'test_hudi_table');
```

|    commit_time   | action  | partition     | file_id                                | previous_commit   | num_writes | num_inserts | num_deletes | num_update_writes | total_errors | total_log_blocks | total_corrupt_logblocks | total_rollback_blocks | total_log_records | total_updated_records_compacted | total_bytes_written|
|----------------- |---------|---------------|----------------------------------------|-------------------|------------|-------------|-------------|-------------------|--------------|------------------|-------------------------|-----------------------|-------------------|---------------------------------|------------------- |
|20220109225319449 | commit  | dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0 	              | 0                       | 0                     | 0                 | 0                               | 435349             |
|20220109225311742 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109214830592 | 1          | 1           | 0           | 0	                | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435340             |
|20220109225301429 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109214830592 | 1          | 1           | 0           | 0                 | 0	           | 0                | 0                       | 0                     | 0                 | 0                               | 435340             |
|20220109214830592 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109191631015 | 0          | 0           | 1           | 0	                | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 432653             |
|20220109214830592 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109191648181 | 0          | 0           | 1           | 0	                | 0	           | 0                | 0                       | 0                     | 0                 | 0                               | 432653             |
|20220109191648181 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435341             |
|20220109191631015 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435341             |

### show_commit_extra_metadata

Show commits' extra metadata.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |
| instant_time   | String | N        | None          | Instant time                         |
| metadata_key   | String | N        | None          | Key of metadata                      |

**Output**

| Output Name    | Type   |
|----------------|--------|
| instant_time   | String |
| action         | String |
| metadata_key   | String |
| metadata_value | String |

**Example**

```
call show_commit_extra_metadata(table => 'test_hudi_table');
```

| instant_time      | action      | metadata_key  | metadata_value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
|-------------------|-------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 20230206174349556 | deltacommit | schema        | \{"type":"record","name":"hudi_mor_tbl","fields":[\{"name":"_hoodie_commit_time","type":["null","string"],"doc":"","default":null},\{"name":"_hoodie_commit_seqno","type":["null","string"],"doc":"","default":null},\{"name":"_hoodie_record_key","type":["null","string"],"doc":"","default":null},\{"name":"_hoodie_partition_path","type":["null","string"],"doc":"","default":null},\{"name":"_hoodie_file_name","type":["null","string"],"doc":"","default":null},\{"name":"id","type":"int"},\{"name":"ts","type":"long"}]}                                                                       | 
| 20230206174349556 | deltacommit | latest_schema | \{"max_column_id":8,"version_id":20230206174349556,"type":"record","fields":[\{"id":0,"name":"_hoodie_commit_time","optional":true,"type":"string","doc":""},\{"id":1,"name":"_hoodie_commit_seqno","optional":true,"type":"string","doc":""},\{"id":2,"name":"_hoodie_record_key","optional":true,"type":"string","doc":""},\{"id":3,"name":"_hoodie_partition_path","optional":true,"type":"string","doc":""},\{"id":4,"name":"_hoodie_file_name","optional":true,"type":"string","doc":""},\{"id":5,"name":"id","optional":false,"type":"int"},\{"id":8,"name":"ts","optional":false,"type":"long"}]} | 

### show_archived_commits

Show archived commits.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                                    |
|----------------|--------|----------|---------------|------------------------------------------------|
| table          | String | Y        | None          | Hudi table name                                |
| limit          | Int    | N        | 10            | Max number of records to be returned           |
| start_ts       | String | N        | ""            | Start time for commits, default: now - 10 days |
| end_ts         | String | N        | ""            | End time for commits, default: now - 1 day     |

**Output**

| Output Name                  | Type   |
|------------------------------|--------|
| commit_time                  | String |
| total_bytes_written          | Long   |
| total_files_added            | Long   |
| total_files_updated          | Long   |
| total_partitions_written     | Long   |
| total_records_written        | Long   |
| total_update_records_written | Long   |
| total_errors                 | Long   |

**Example**

```
call show_archived_commits(table => 'test_hudi_table');
```

| commit_time       | 	total_bytes_written     | total_files_added | total_files_updated | total_partitions_written | total_records_written | total_update_records_written | total_errors |
|-------------------|--------------------------|-------------------|---------------------|--------------------------|-----------------------|------------------------------|--------------|
| 20220216171049652 | 	432653                  | 0                 | 1                   | 1                        | 0                     | 0                            | 0            |
| 20220216171027021 | 	435346                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |
| 20220216171019361 | 	435349                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |


### show_archived_commits_metadata

Show archived commits' metadata.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                                    |
|----------------|--------|----------|---------------|------------------------------------------------|
| table          | String | Y        | None          | Hudi table name                                |
| limit          | Int    | N        | 10            | Max number of records to be returned           |
| start_ts       | String | N        | ""            | Start time for commits, default: now - 10 days |
| end_ts         | String | N        | ""            | End time for commits, default: now - 1 day     |

**Output**

| Output Name                     | Type   |
|---------------------------------|--------|
| commit_time                     | String |
| action                          | String |
| partition                       | String |
| file_id                         | String |
| previous_commit                 | String |
| num_writes                      | Long   |
| num_inserts                     | Long   |
| num_deletes                     | Long   |
| num_update_writes               | String |
| total_errors                    | Long   |
| total_log_blocks                | Long   |
| total_corrupt_logblocks         | Long   |
| total_rollback_blocks           | Long   |
| total_log_records               | Long   |
| total_updated_records_compacted | Long   |
| total_bytes_written             | Long   |

**Example**

```
call show_archived_commits_metadata(table => 'test_hudi_table');
```

|    commit_time   | action  | partition     | file_id                                | previous_commit   | num_writes | num_inserts | num_deletes | num_update_writes | total_errors | total_log_blocks | total_corrupt_logblocks | total_rollback_blocks | total_log_records | total_updated_records_compacted | total_bytes_written|
|----------------- |---------|---------------|----------------------------------------|-------------------|------------|-------------|-------------|-------------------|--------------|------------------|-------------------------|-----------------------|-------------------|---------------------------------|------------------- |
|20220109225319449 | commit  | dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0 	              | 0                       | 0                     | 0                 | 0                               | 435349             |
|20220109225311742 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109214830592 | 1          | 1           | 0           | 0	                | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435340             |
|20220109225301429 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109214830592 | 1          | 1           | 0           | 0                 | 0	           | 0                | 0                       | 0                     | 0                 | 0                               | 435340             |
|20220109214830592 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109191631015 | 0          | 0           | 1           | 0	                | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 432653             |
|20220109214830592 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109191648181 | 0          | 0           | 1           | 0	                | 0	           | 0                | 0                       | 0                     | 0                 | 0                               | 432653             |
|20220109191648181 | commit  | dt=2021-05-02 | b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435341             |
|20220109191631015 | commit  | dt=2021-05-01 | 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | null              | 1          | 1           | 0           | 0                 | 0            | 0                | 0                       | 0                     | 0                 | 0                               | 435341             |


```
call show_archived_commits(table => 'test_hudi_table');
```

| commit_time       | 	total_bytes_written     | total_files_added | total_files_updated | total_partitions_written | total_records_written | total_update_records_written | total_errors |
|-------------------|--------------------------|-------------------|---------------------|--------------------------|-----------------------|------------------------------|--------------|
| 20220216171049652 | 	432653                  | 0                 | 1                   | 1                        | 0                     | 0                            | 0            |
| 20220216171027021 | 	435346                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |
| 20220216171019361 | 	435349                  | 1                 | 0                   | 1                        | 1                     | 	0                           | 0            |

### show_commit_files

Show files of a commit.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |
| instant_time   | String | Y        | None          | Instant time                         |

**Output**

| Output Name           | Type   |
|-----------------------|--------|
| action                | String |
| partition_path        | String |
| file_id               | String |
| previous_commit       | String |
| total_records_updated | Long   |
| total_records_written | Long   |
| total_bytes_written   | Long   |
| total_errors          | Long   |
| file_size             | Long   |

**Example**

```
call show_commit_files(table => 'test_hudi_table', instant_time => '20230206174349556');
```

| action      | 	partition_path | file_id                                | previous_commit | total_records_updated | total_records_written | total_bytes_written | total_errors | file_size |
|-------------|-----------------|----------------------------------------|-----------------|-----------------------|-----------------------|---------------------|--------------|-----------|
| deltacommit | dt=2021-05-03   | 7fb52523-c7f6-41aa-84a6-629041477aeb-0 | null            | 0                     | 1                     | 434768              | 0            | 434768    |           

### show_commit_partitions

Show partitions of a commit.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |
| instant_time   | String | Y        | None          | Instant time                         |

**Output**

| Output Name            | Type   |
|------------------------|--------|
| action                 | String |
| partition_path         | String |
| total_files_added      | Long   |
| total_files_updated    | Long   |
| total_records_inserted | Long   |
| total_records_updated  | Long   |
| total_bytes_written    | Long   |
| total_errors           | Long   |

**Example**

```
call show_commit_partitions(table => 'test_hudi_table', instant_time => '20230206174349556');
```

| action      | 	partition_path | total_files_added                      | total_files_updated | total_records_inserted | total_records_updated | total_bytes_written | total_errors | 
|-------------|-----------------|----------------------------------------|---------------------|------------------------|-----------------------|---------------------|--------------|
| deltacommit | dt=2021-05-03   | 7fb52523-c7f6-41aa-84a6-629041477aeb-0 | 0                   | 1                      | 434768                | 0                   | 0            |          

### show_commit_write_stats

Show write statistics of a commit.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |
| instant_time   | String | Y        | None          | Instant time                         |

**Output**

| Output Name           | Type   |
|-----------------------|--------|
| action                | String |
| total_bytes_written   | Long   |
| total_records_written | Long   |
| avg_record_size       | Long   |

**Example**

```
call show_commit_write_stats(table => 'test_hudi_table', instant_time => '20230206174349556');
```

| action      | total_bytes_written | total_records_written | avg_record_size |
|-------------|---------------------|-----------------------|-----------------|
| deltacommit | 434768              | 1                     | 434768          |

### show_rollbacks

Show rollback commits.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name          | Type   |
|----------------------|--------|
| instant              | String |
| rollback_instant     | String |
| total_files_deleted  | Int    |
| time_taken_in_millis | Long   |
| total_partitions     | Int    |

**Example**

```
call show_rollbacks(table => 'test_hudi_table');
```

| instant     | rollback_instant | total_files_deleted | time_taken_in_millis | time_taken_in_millis |
|-------------|------------------|---------------------|----------------------|----------------------|
| deltacommit | 434768           | 1                   | 434768               | 2                    |


### show_rollback_detail

Show details of a rollback commit.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |
| instant_time   | String | Y        | None          | Instant time                         |

**Output**

| Output Name      | Type   |
|------------------|--------|
| instant          | String |
| rollback_instant | String |
| partition        | String |
| deleted_file     | String |
| succeeded        | Int    |

**Example**

```
call show_rollback_detail(table => 'test_hudi_table', instant_time => '20230206174349556');
```

| instant     | rollback_instant | partition | deleted_file | succeeded |
|-------------|------------------|-----------|--------------|-----------|
| deltacommit | 434768           | 1         | 434768       | 2         |

### commits_compare

Compare commit with another path.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| path           | String | Y        | None          | Path of table   |

**Output**

| Output Name    | Type   |
|----------------|--------|
| compare_detail | String |

**Example**

```
call commits_compare(table => 'test_hudi_table', path => 'hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table');
```

| compare_detail                                                         |
|------------------------------------------------------------------------|
| Source test_hudi_table is ahead by 0 commits. Commits to catch up - [] |       

### archive_commits

archive commits.

**Input**

| Parameter Name                                                         | Type    | Required | Default Value | Description                                                                                                                                                                                                                                            |
|------------------------------------------------------------------------|---------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                                  | String  | N        | None          | Hudi table name                                                                                                                                                                                                                                        |
| path                                                                   | String  | N        | None          | Path of table                                                                                                                                                                                                                                          |
| [min_commits](configurations.md#hoodiekeepmincommits)          | Int     | N        | 20            | Similar to hoodie.keep.max.commits, but controls the minimum number of instants to retain in the active timeline.                                                                                                                                      |
| [max_commits](configurations.md#hoodiekeepmaxcommits)          | Int     | N        | 30            | Archiving service moves older entries from timeline into an archived log after each write, to keep the metadata overhead constant, even as the table size grows. This config controls the maximum number of instants to retain in the active timeline. |
| [retain_commits](configurations.md#hoodiecommitsarchivalbatch) | Int     | N        | 10            | Archiving of instants is batched in best-effort manner, to pack more instants into a single archive log. This config controls such archival batch size.                                                                                                |
| [enable_metadata](configurations.md#hoodiemetadataenable)      | Boolean | N        | false         | Enable the internal metadata table                                                                                                                                                                                                                     |

**Output**

| Output Name | Type |
|-------------|------|
| result      | Int  |

**Example**

```
call archive_commits(table => 'test_hudi_table');
```

| result |
|--------|
| 0      |    

### export_instants

extract instants to local folder.

**Input**

| Parameter Name | Type    | Required | Default Value                                       | Description                       |
|----------------|---------|----------|-----------------------------------------------------|-----------------------------------|
| table          | String  | Y        | None                                                | Hudi table name                   |
| local_folder   | String  | Y        | None                                                | Local folder                      |
| limit          | Int     | N        | -1                                                  | Number of instants to be exported |
| actions        | String  | N        | clean,commit,deltacommit,rollback,savepoint,restore | Commit action                     |
| desc           | Boolean | N        | false                                               | Descending order                  |

**Output**

| Output Name   | Type   |
|---------------|--------|
| export_detail | String |

**Example**

```
call export_instants(table => 'test_hudi_table', local_folder => '/tmp/folder');
```

| export_detail                      |
|:-----------------------------------|
| Exported 6 Instants to /tmp/folder |

### rollback_to_instant

Rollback a table to the commit that was current at some time.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |

**Output**

| Output Name     | Type    |
|-----------------|---------|
| rollback_result | Boolean |

**Example**

Roll back test_hudi_table to one instant
```
call rollback_to_instant(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_result |
|:----------------|
| true            |

### create_savepoint

Create a savepoint to hudi's table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| commit_time    | String | Y        | None          | Commit time     |
| user           | String | N        | ""            | User name       |
| comments       | String | N        | ""            | Comments        |

**Output**

| Output Name             | Type    |
|-------------------------|---------|
| create_savepoint_result | Boolean |

**Example**

```
call create_savepoint(table => 'test_hudi_table', commit_time => '20220109225319449');
```

| create_savepoint_result |
|:------------------------|
| true                    |

### show_savepoints

Show savepoints.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |

**Output**

| Output Name    | Type   |
|----------------|--------|
| savepoint_time | String |

**Example**

```
call show_savepoints(table => 'test_hudi_table');
```

| savepoint_time    |
|:------------------|
| 20220109225319449 |
| 20220109225311742 |
| 20220109225301429 |

### delete_savepoint

Delete a savepoint to hudi's table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| instant_time   | String | Y        | None          | Instant time    |

**Output**

| Output Name             | Type    |
|-------------------------|---------|
| delete_savepoint_result | Boolean |

**Example**

Delete a savepoint to test_hudi_table
```
call delete_savepoint(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| delete_savepoint_result |
|:------------------------|
| true                    |

### rollback_to_savepoint

Rollback a table to the commit that was current at some time.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| instant_time   | String | Y        | None          | Instant time    |

**Output**

| Output Name               | Type    |
|---------------------------|---------|
| rollback_savepoint_result | Boolean |

**Example**

Rollback test_hudi_table to one savepoint
```
call rollback_to_savepoint(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_savepoint_result |
|:--------------------------|
| true                      |

### copy_to_temp_view

copy table to a temporary view.

**Input**

| Parameter Name                                                    | Type    | Required | Default Value | Description                                                                                                                                                                                                                                 |
|-------------------------------------------------------------------|---------|----------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                             | String  | Y        | None          | Hudi table name                                                                                                                                                                                                                             |
| [query_type](configurations.md#hoodiedatasourcequerytype) | String  | N        | "snapshot"    | Whether data needs to be read, in `incremental` mode (new data since an instantTime) (or) `read_optimized` mode (obtain latest view, based on base files) (or) `snapshot` mode (obtain latest view, by merging base and (if any) log files) |
| view_name                                                         | String  | Y        | None          | Name of view                                                                                                                                                                                                                                |
| begin_instance_time                                               | String  | N        | ""            | Begin instance time                                                                                                                                                                                                                         |
| end_instance_time                                                 | String  | N        | ""            | End instance time                                                                                                                                                                                                                           |
| as_of_instant                                                     | String  | N        | ""            | As of instant time                                                                                                                                                                                                                          |
| replace                                                           | Boolean | N        | false         | Replace an existed view                                                                                                                                                                                                                     |
| global                                                            | Boolean | N        | false         | Global view                                                                                                                                                                                                                                 |

**Output**

| Output Name | Type    |
|-------------|---------|
| status      | Boolean |

**Example**

```
call copy_to_temp_view(table => 'test_hudi_table', view_name => 'copy_view_test_hudi_table');
```

| status | 
|--------|
| 0      | 

### copy_to_table

copy table to a new table.

**Input**

| Parameter Name                                                    | Type   | Required | Default Value | Description                                                                                                                                                                                                                                 |
|-------------------------------------------------------------------|--------|----------|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                             | String | Y        | None          | Hudi table name                                                                                                                                                                                                                             |
| [query_type](configurations.md#hoodiedatasourcequerytype) | String | N        | "snapshot"    | Whether data needs to be read, in `incremental` mode (new data since an instantTime) (or) `read_optimized` mode (obtain latest view, based on base files) (or) `snapshot` mode (obtain latest view, by merging base and (if any) log files) |
| new_table                                                         | String | Y        | None          | Name of new table                                                                                                                                                                                                                           |
| begin_instance_time                                               | String | N        | ""            | Begin instance time                                                                                                                                                                                                                         |
| end_instance_time                                                 | String | N        | ""            | End instance time                                                                                                                                                                                                                           |
| as_of_instant                                                     | String | N        | ""            | As of instant time                                                                                                                                                                                                                          |
| save_mode                                                         | String | N        | "overwrite"   | Save mode                                                                                                                                                                                                                                   |
| columns                                                           | String | N        | ""            | Columns of source table which should copy to new table                                                                                                                                                                                      |


**Output**

| Output Name | Type    |
|-------------|---------|
| status      | Boolean |

**Example**

```
call copy_to_table(table => 'test_hudi_table', new_table => 'copy_table_test_hudi_table');
```

| status | 
|--------|
| 0      | 

## Metadata Table management

### create_metadata_table

Create metadata table of a hudi table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |

**Output**

| Output Name | Type   |
|-------------|--------|
| result      | String |

**Example**

```
call create_metadata_table(table => 'test_hudi_table');
```

| result                                                                                                            |
|:------------------------------------------------------------------------------------------------------------------|
| Created Metadata Table in hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/.hoodie/metadata (duration=2.777secs) |

### init_metadata_table

Init metadata table of a hudi table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description     |
|----------------|---------|----------|---------------|-----------------|
| table          | String  | Y        | None          | Hudi table name |
| read_only      | Boolean | N        | false         | Read only       |

**Output**

| Output Name | Type    |
|-------------|---------|
| result      | String  |

**Example**

```
call init_metadata_table(table => 'test_hudi_table');
```

| result                                                                                                               |
|:---------------------------------------------------------------------------------------------------------------------|
| Initialized Metadata Table in hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/.hoodie/metadata (duration=0.023sec) |

### delete_metadata_table

Delete metadata table of a hudi table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description     |
|----------------|---------|----------|---------------|-----------------|
| table          | String  | Y        | None          | Hudi table name |

**Output**

| Output Name | Type    |
|-------------|---------|
| result      | String  |

**Example**

```
call delete_metadata_table(table => 'test_hudi_table');
```

| result                                                                                         |
|:-----------------------------------------------------------------------------------------------|
| Removed Metadata Table from hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/.hoodie/metadata |

### show_metadata_table_partitions

Show partition of a hudi table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |

**Output**

| Output Name | Type    |
|-------------|---------|
| partition   | String  |

**Example**

```
call show_metadata_table_partitions(table => 'test_hudi_table');
```

| partition     |
|:--------------|
| dt=2021-05-01 |
| dt=2021-05-02 |
| dt=2021-05-03 |

### show_metadata_table_files

Show files of a hudi table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| partition      | String | N        | ""            | Partition name  |
| limit          | Int    | N        | 100           | Limit number    |

**Output**

| Output Name | Type    |
|-------------|---------|
| file_path   | String  |

**Example**

Show files of a hudi table under one partition.
```
call show_metadata_table_files(table => 'test_hudi_table', partition => 'dt=20230220');
```

| file_path                                                                 |
|:--------------------------------------------------------------------------|
| .d3cdf6ff-250a-4cee-9af4-ab179fdb9bfb-0_20230220190948086.log.1_0-111-123 |
| d3cdf6ff-250a-4cee-9af4-ab179fdb9bfb-0_0-78-81_20230220190948086.parquet  |

### show_metadata_table_stats

Show metadata table stats of a hudi table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |

**Output**

| Output Name | Type   |
|-------------|--------|
| stat_key    | String |
| stat_value  | String |

**Example**

```
call show_metadata_table_stats(table => 'test_hudi_table');
```

| stat_key                               | stat_value | 
|----------------------------------------|------------|
| dt=2021-05-03.totalBaseFileSizeInBytes | 23142      |

### validate_metadata_table_files

Validate metadata table files of a hudi table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description                |
|----------------|---------|----------|---------------|----------------------------|
| table          | String  | Y        | None          | Hudi table name            |
| verbose        | Boolean | N        | False         | If verbose print all files |


**Output**

| Output Name            | Type    |
|------------------------|---------|
| partition              | String  |
| file_name              | String  |
| is_present_in_fs       | Boolean |
| is_present_in_metadata | Boolean |
| fs_size                | Long    |
| metadata_size          | Long    |

**Example**

```
call validate_metadata_table_files(table => 'test_hudi_table');
```

| partition     | file_name                                                           | is_present_in_fs | is_present_in_metadata | fs_size | metadata_size |
|---------------|---------------------------------------------------------------------|------------------|------------------------|---------|---------------|
| dt=2021-05-03 | ad1e5a3f-532f-4a13-9f60-223676798bf3-0_0-4-4_00000000000002.parquet | true             | true                   | 43523   | 43523         | 

## Table information

### show_table_properties

Show hudi properties of a table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| path           | String | N        | None          | Path of table                        |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name | Type   |
|-------------|--------|
| key         | String |
| value       | String |

**Example**

```
call show_table_properties(table => 'test_hudi_table', limit => 10);
```

| key                           | value |
|-------------------------------|-------|
| hoodie.table.precombine.field | ts    |
| hoodie.table.partition.fields | dt    |

### show_fs_path_detail

Show detail of a path.

**Input**

| Parameter Name | Type    | Required | Default Value | Description            |
|----------------|---------|----------|---------------|------------------------|
| path           | String  | Y        | None          | Hudi table name        |
| is_sub         | Boolean | N        | false         | Whether to list files  |
| sort           | Boolean | N        | true          | Sorted by storage_size |
| limit          | Int     | N        | 100           | Limit number           |

**Output**

| Output Name        | Type   |
|--------------------|--------|
| path_num           | Long   |
| file_num           | Long   |
| storage_size       | Long   |
| storage_size(unit) | String |
| storage_path       | String |
| space_consumed     | Long   |
| quota              | Long   |
| space_quota        | Long   |

**Example**

```
call show_fs_path_detail(path => 'hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table');
```

| path_num | file_num | storage_size | storage_size(unit) | storage_path                                      | space_consumed | quota   | space_quota |
|----------|----------|--------------|--------------------|---------------------------------------------------|----------------|---------|-------------|
| 22       | 58       | 2065612      | 1.97MB             | hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table | -1             | 6196836 | -1          |

### stats_file_sizes

Show file sizes of a table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| partition_path | String | N        | ""            | Partition path                       |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name | Type   |
|-------------|--------|
| commit_time | String |
| min         | Long   |
| 10th        | Double |
| 50th        | Double |
| avg         | Double |
| 95th        | Double |
| max         | Long   |
| num_files   | Int    |
| std_dev     | Double |

**Example**

```
call stats_file_sizes(table => 'test_hudi_table');
```

| commit_time       | min    | 10th     | 50th     | avg      | 95th     | max    | num_files | std_dev |
|-------------------|--------|----------|----------|----------|----------|--------|-----------|---------|
| 20230205134149455 | 435000 | 435000.0 | 435000.0 | 435000.0 | 435000.0 | 435000 | 1         | 0.0     |

### stats_wa

Show write stats and amplification of a table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Hudi table name                      |
| limit          | Int    | N        | 10            | Max number of records to be returned |

**Output**

| Output Name                | Type   |
|----------------------------|--------|
| commit_time                | String |
| total_upserted             | Long   |
| total_written              | Long   |
| write_amplification_factor | String |

**Example**

```
call stats_wa(table => 'test_hudi_table');
```

| commit_time | total_upserted | total_written | write_amplification_factor | 
|-------------|----------------|---------------|----------------------------|
| Total       | 0              | 0             | 0                          |

### show_logfile_records

Show records in logfile of a table.

**Input**

| Parameter Name        | Type    | Required | Default Value | Description                          |
|-----------------------|---------|----------|---------------|--------------------------------------|
| table                 | String  | Y        | None          | Hudi table name                      |
| log_file_path_pattern | String  | Y        | 10            | Pattern of logfile                   |
| merge                 | Boolean | N        | false         | Merge results                        |
| limit                 | Int     | N        | 10            | Max number of records to be returned |

**Output**

| Output Name | Type   |
|-------------|--------|
| records     | String |

**Example**

```
call show_logfile_records(table => 'test_hudi_table', log_file_path_pattern => 'hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/*.log*');
```

| records                                                                                                                                                                                                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| \{"_hoodie_commit_time": "20230205133427059", "_hoodie_commit_seqno": "20230205133427059_0_10", "_hoodie_record_key": "1", "_hoodie_partition_path": "", "_hoodie_file_name": "3438e233-7b50-4eff-adbb-70b1cd76f518-0", "id": 1, "name": "a1", "price": 40.0, "ts": 1111} | 

### show_logfile_metadata

Show metadatas in logfile of a table.

**Input**

| Parameter Name        | Type    | Required | Default Value | Description                          |
|-----------------------|---------|----------|---------------|--------------------------------------|
| table                 | String  | Y        | None          | Hudi table name                      |
| log_file_path_pattern | String  | Y        | 10            | Pattern of logfile                   |
| merge                 | Boolean | N        | false         | Merge results                        |
| limit                 | Int     | N        | 10            | Max number of records to be returned |

**Output**

| Output Name     | Type   |
|-----------------|--------|
| instant_time    | String |
| record_count    | Int    |
| block_type      | String |
| header_metadata | String |
| footer_metadata | String |

**Example**

```
call show_logfile_metadata(table => 'hudi_mor_tbl', log_file_path_pattern => 'hdfs://ns1/hive/warehouse/hudi.db/hudi_mor_tbl/*.log*');
```

| instant_time      | record_count | block_type      | header_metadata                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | footer_metadata |
|-------------------|--------------|-----------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------|
| 20230205133427059 | 1            | AVRO_DATA_BLOCK | \{"INSTANT_TIME":"20230205133427059","SCHEMA":"\{\"type\":\"record\",\"name\":\"hudi_mor_tbl_record\",\"namespace\":\"hoodie.hudi_mor_tbl\",\"fields\":[\{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},\{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},\{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},\{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},\{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},\{\"name\":\"id\",\"type\":\"int\"},\{\"name\":\"name\",\"type\":\"string\"},\{\"name\":\"price\",\"type\":\"double\"},\{\"name\":\"ts\",\"type\":\"long\"}]}"} | {}              |

### show_invalid_parquet

Show invalid parquet files of a table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description                          |
|----------------|---------|----------|---------------|--------------------------------------|
| Path           | String  | Y        | None          | Hudi table name                      |
| limit          | Int     | N        | 100           | Limit number                         |

**Output**

| Output Name | Type   |
|-------------|--------|
| Path        | String |

**Example**

```
call show_invalid_parquet(path => 'hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table');
```

| Path                                                                                                                       | 
|----------------------------------------------------------------------------------------------------------------------------|
| hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/7fb52523-c7f6-41aa-84a6-629041477aeb-0_0-92-99_20230205133532199.parquet | 

### show_fsview_all

Show file system views of a table.

**Input**

| Parameter Name     | Type    | Required | Default Value | Description                          |
|--------------------|---------|----------|---------------|--------------------------------------|
| table              | String  | Y        | None          | Hudi table name                      |
| max_instant        | String  | N        | ""            | Max instant time                     |
| include_max        | Boolean | N        | false         | Include max instant                  |
| include_in_flight  | Boolean | N        | false         | Include in flight                    |
| exclude_compaction | Boolean | N        | false         | Exclude compaction                   |
| limit              | Int     | N        | 10            | Max number of records to be returned |
| path_regex         | String  | N        | "\*\/\*\/\*"  | Pattern of path                      |

**Output**

| Output Name           | Type   |
|-----------------------|--------|
| partition             | String |
| file_id               | String |
| base_instant          | String |
| data_file             | String |
| data_file_size        | Long   |
| num_delta_files       | Long   |
| total_delta_file_size | Long   |
| delta_files           | String |

**Example**

```
call show_fsview_all(table => 'test_hudi_table');
```

| partition     | file_id                                | base_instant      | data_file                                                                | data_file_size | num_delta_files | total_delta_file_size | delta_files                                                             |
|---------------|----------------------------------------|-------------------|--------------------------------------------------------------------------|----------------|-----------------|-----------------------|-------------------------------------------------------------------------|
| dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | 20220109225319449 | 7fb52523-c7f6-41aa-84a6-629041477aeb-0_0-92-99_20220109225319449.parquet | 5319449        | 1               | 213193                | .7fb52523-c7f6-41aa-84a6-629041477aeb-0_20230205133217210.log.1_0-60-63 |

### show_fsview_latest

Show latest file system view of a table.

**Input**

| Parameter Name     | Type    | Required | Default Value | Description                          |
|--------------------|---------|----------|---------------|--------------------------------------|
| table              | String  | Y        | None          | Hudi table name                      |
| max_instant        | String  | N        | ""            | Max instant time                     |
| include_max        | Boolean | N        | false         | Include max instant                  |
| include_in_flight  | Boolean | N        | false         | Include in flight                    |
| exclude_compaction | Boolean | N        | false         | Exclude compaction                   |
| partition_path     | String  | Y        | None          | Partition path                       |
| merge              | Boolean | N        | false         | Merge results                        |

**Output**

| Output Name                                | Type   |
|--------------------------------------------|--------|
| partition                                  | String |
| file_id                                    | String |
| base_instant                               | String |
| data_file                                  | String |
| data_file_size                             | Long   |
| num_delta_files                            | Long   |
| total_delta_file_size                      | Long   |
| delta_size_compaction_scheduled            | Long   |
| delta_size_compaction_unscheduled          | Long   |
| delta_to_base_radio_compaction_scheduled   | Double |
| delta_to_base_radio_compaction_unscheduled | Double |
| delta_files_compaction_scheduled           | String |
| delta_files_compaction_unscheduled         | String |

**Example**

```
call show_fsview_latest(table => 'test_hudi_table'， partition => 'dt=2021-05-03');
```

| partition     | file_id                                | base_instant      | data_file                                                                | data_file_size | num_delta_files | total_delta_file_size | delta_files                                                             |
|---------------|----------------------------------------|-------------------|--------------------------------------------------------------------------|----------------|-----------------|-----------------------|-------------------------------------------------------------------------|
| dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | 20220109225319449 | 7fb52523-c7f6-41aa-84a6-629041477aeb-0_0-92-99_20220109225319449.parquet | 5319449        | 1               | 213193                | .7fb52523-c7f6-41aa-84a6-629041477aeb-0_20230205133217210.log.1_0-60-63 |

## Optimization table

### run_clustering

Trigger clustering on a hoodie table. By using partition predicates, clustering table can be run 
with specified partitions, and you can also specify the order columns to sort data.

:::note
Newly clustering instant will be generated every call, or some pending clustering instants are executed.
When calling this procedure, one of parameters ``table`` and ``path`` must be specified at least. If both
parameters are given, ``table`` will take effect.

:::


**Input**

| Parameter Name          | Type    | Required | Default Value | Description                                                    |
|-------------------------|---------|----------|---------------|----------------------------------------------------------------|
| table                   | String  | N        | None          | Name of table to be clustered                                  |
| path                    | String  | N        | None          | Path of table to be clustered                                  |
| predicate               | String  | N        | None          | Predicate to filter partition                                  |
| order                   | String  | N        | None          | Order column split by `,`                                      |
| show_involved_partition | Boolean | N        | false         | Show involved partition in the output                          |
| op                      | String  | N        | None          | Operation type, `EXECUTE` or `SCHEDULE`                        |
| order_strategy          | String  | N        | None          | Records layout optimization, `linear/z-order/hilbert`          |
| options                 | String  | N        | None          | Customize hudi configs in the format "key1=value1,key2=value2` |
| instants                | String  | N        | None          | Specified instants by `,`                                      |
| selected_partitions     | String  | N        | None          | Partitions to run clustering by `,`                            |
| limit                   | Int     | N        | None          | Max number of plans to be executed                             |

**Output**

The output as follows:

| Parameter Name      | Type   | Required | Default Value | Description                              |
|---------------------|--------|----------|---------------|------------------------------------------|
| timestamp           | String | N        | None          | Instant name                             |
| input_group_size    | Int    | N        | None          | The input group sizes for each plan      |
| state               | String | N        | None          | The instant final state                  |
| involved_partitions | String | N        | *             | Show involved partitions, default is `*` |

**Example**

Clustering test_hudi_table with table name
```
call run_clustering(table => 'test_hudi_table');
```

Clustering test_hudi_table with table path
```
call run_clustering(path => '/tmp/hoodie/test_hudi_table');
```

Clustering test_hudi_table with table name, predicate and order column
```
call run_clustering(table => 'test_hudi_table', predicate => 'ts <= 20220408L', order => 'ts');
```

Clustering test_hudi_table with table name, show_involved_partition
```
call run_clustering(table => 'test_hudi_table', show_involved_partition => true);
```

Clustering test_hudi_table with table name, op
```
call run_clustering(table => 'test_hudi_table', op => 'schedule');
```

Clustering test_hudi_table with table name, order_strategy
```
call run_clustering(table => 'test_hudi_table', order_strategy => 'z-order');
```

Clustering test_hudi_table with table name, op, options
```
call run_clustering(table => 'test_hudi_table', op => 'schedule', options => '
hoodie.clustering.plan.strategy.target.file.max.bytes=1024*1024*1024,
hoodie.clustering.plan.strategy.max.bytes.per.group=2*1024*1024*1024');
```

Clustering test_hudi_table with table name, op, instants
```
call run_clustering(table => 'test_hudi_table', op => 'execute', instants => 'ts1,ts2');
```

Clustering test_hudi_table with table name, op, selected_partitions
```
call run_clustering(table => 'test_hudi_table', op => 'execute', selected_partitions => 'par1,par2');
```

Clustering test_hudi_table with table name, op, limit
```
call run_clustering(table => 'test_hudi_table', op => 'execute', limit => 10);
```
:::note
Limit parameter is valid only when op is execute.

:::

### show_clustering

Show pending clusterings on a hoodie table. 

:::note
When calling this procedure, one of parameters ``table`` and ``path`` must be specified at least. 
If both parameters are given, ``table`` will take effect.

:::


**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | N        | None          | Name of table to be clustered        |
| path           | String | N        | None          | Path of table to be clustered        |
| limit          | Int    | N        | None          | Max number of records to be returned |

**Output**

| Parameter Name | Type   | Required | Default Value | Description                           |
|----------------|--------|----------|---------------|---------------------------------------|
| timestamp      | String | N        | None          | Instant time                          |
| groups         | Int    | N        | None          | Number of file groups to be processed |

**Example**

Show pending clusterings with table name
```
call show_clustering(table => 'test_hudi_table');
```
| timestamp         | groups | 
|-------------------|--------|
| 20220408153707928 | 2      |
| 20220408153636963 | 3      |

Show pending clusterings with table path
```
call show_clustering(path => '/tmp/hoodie/test_hudi_table');
```
| timestamp         | groups | 
|-------------------|--------|
| 20220408153707928 | 2      |
| 20220408153636963 | 3      |

Show pending clusterings with table name and limit
```
call show_clustering(table => 'test_hudi_table', limit => 1);
```
| timestamp         | groups | 
|-------------------|--------|
| 20220408153707928 | 2      |

### run_compaction

Schedule or run compaction on a hoodie table. 

:::note
For scheduling compaction, if `timestamp` is specified, new scheduled compaction will use given 
timestamp as instant time. Otherwise, compaction will be scheduled by using current system time. 

For running compaction, given ``timestamp`` must be a pending compaction instant time that 
already exists, if it's not, exception will be thrown. Meanwhile, if ``timestamp``is specified 
and there are pending compactions, all pending compactions will be executed without new compaction 
instant generated. 

When calling this procedure, one of parameters ``table`` and ``path``must be specified at least. 
If both parameters are given, ``table`` will take effect.
:::

**Input**

| Parameter Name | Type   | Required | Default Value | Description                                                                                        |
|----------------|--------|----------|---------------|----------------------------------------------------------------------------------------------------|
| op             | String | N        | None          | Operation type, `RUN` or `SCHEDULE`                                                                |
| table          | String | N        | None          | Name of table to be compacted                                                                      |
| path           | String | N        | None          | Path of table to be compacted                                                                      |
| timestamp      | String | N        | None          | Instant time                                                                                       |
| options        | String | N        | None          | comma separated  list of Hudi configs for compaction in the format "config1=value1,config2=value2" |

**Output**

The output of `RUN` operation is `EMPTY`, the output of `SCHEDULE` as follow:

| Parameter Name | Type   | Required  | Default Value | Description  |
|----------------|--------|-----------|---------------|--------------|
| instant        | String | N         | None          | Instant name |

**Example**

Run compaction with table name
```
call run_compaction(op => 'run', table => 'test_hudi_table');
```

Run compaction with table path
```
call run_compaction(op => 'run', path => '/tmp/hoodie/test_hudi_table');
```

Run compaction with table path and timestamp
```
call run_compaction(op => 'run', path => '/tmp/hoodie/test_hudi_table', timestamp => '20220408153658568');
```
Run compaction with options
```
call run_compaction(op => 'run', table => 'test_hudi_table', options => hoodie.compaction.strategy=org.apache.hudi.table.action.compact.strategy.LogFileNumBasedCompactionStrategy,hoodie.compaction.logfile.num.threshold=3);
```

Schedule compaction with table name
```
call run_compaction(op => 'schedule', table => 'test_hudi_table');
```
| instant           |
|-------------------|
| 20220408153650834 |

Schedule compaction with table path
```
call run_compaction(op => 'schedule', path => '/tmp/hoodie/test_hudi_table');
```
| instant           |
|-------------------|
| 20220408153650834 |

Schedule compaction with table path and timestamp
```
call run_compaction(op => 'schedule', path => '/tmp/hoodie/test_hudi_table', timestamp => '20220408153658568');
```
| instant           |
|-------------------|
| 20220408153658568 |

### show_compaction

Show all compactions on a hoodie table, in-flight or completed compactions are included, and result will
be in reverse order according to trigger time. 

:::note
When calling this procedure, one of parameters ``table``and ``path`` must be specified at least. 
If both parameters are given, ``table`` will take effect.
:::

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | N        | None          | Name of table to show compaction     |
| path           | String | N        | None          | Path of table to show compaction     |
| limit          | Int    | N        | None          | Max number of records to be returned |

**Output**

| Parameter Name | Type   | Required | Default Value | Description                           |
|----------------|--------|----------|---------------|---------------------------------------|
| timestamp      | String | N        | None          | Instant time                          |
| action         | String | N        | None          | Action name of compaction             |
| size           | Int    | N        | None          | Number of file slices to be compacted |

**Example**

Show compactions with table name
```
call show_compaction(table => 'test_hudi_table');
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |
| 20220408153636963 | compaction | 10      |

Show compactions with table path
```
call show_compaction(path => '/tmp/hoodie/test_hudi_table');
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |
| 20220408153636963 | compaction | 10      |

Show compactions with table name and limit
```
call show_compaction(table => 'test_hudi_table', limit => 1);
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |

### run_clean

Run cleaner on a hoodie table.

**Input**

| Parameter Name                                                                        | Type    | Required | Default Value | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|---------------------------------------------------------------------------------------|---------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                                                 | String  | Y        | None          | Name of table to be cleaned                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| schedule_in_line                                                                      | Boolean | N        | true          | Set "true" if you want to schedule and run a clean. Set false if you have already scheduled a clean and want to run that.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| [clean_policy](configurations.md#hoodiecleanerpolicy)                         | String  | N        | None          | org.apache.hudi.common.model.HoodieCleaningPolicy: Cleaning policy to be used. The cleaner service deletes older file slices files to re-claim space. Long running query plans may often refer to older file slices and will break if those are cleaned, before the query has had a chance to run. So, it is good to make sure that the data is retained for more than the maximum query execution time. By default, the cleaning policy is determined based on one of the following configs explicitly set by the user (at most one of them can be set; otherwise, KEEP_LATEST_COMMITS cleaning policy is used). KEEP_LATEST_FILE_VERSIONS: keeps the last N versions of the file slices written; used when "hoodie.cleaner.fileversions.retained" is explicitly set only. KEEP_LATEST_COMMITS(default): keeps the file slices written by the last N commits; used when "hoodie.cleaner.commits.retained" is explicitly set only. KEEP_LATEST_BY_HOURS: keeps the file slices written in the last N hours based on the commit time; used when "hoodie.cleaner.hours.retained" is explicitly set only. |
| [retain_commits](configurations.md#hoodiecleanercommitsretained)              | Int     | N        | None          | When KEEP_LATEST_COMMITS cleaning policy is used, the number of commits to retain, without cleaning. This will be retained for num_of_commits * time_between_commits (scheduled). This also directly translates into how much data retention the table supports for incremental queries.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [hours_retained](configurations.md#hoodiecleanerhoursretained)                | Int     | N        | None          | When KEEP_LATEST_BY_HOURS cleaning policy is used, the number of hours for which commits need to be retained. This config provides a more flexible option as compared to number of commits retained for cleaning service. Setting this property ensures all the files, but the latest in a file group, corresponding to commits with commit times older than the configured number of hours to be retained are cleaned.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| [file_versions_retained](configurations.md#hoodiecleanerfileversionsretained) | Int     | N        | None          | When KEEP_LATEST_FILE_VERSIONS cleaning policy is used, the minimum number of file slices to retain in each file group, during cleaning.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| [trigger_strategy](configurations.md#hoodiecleantriggerstrategy)              | String  | N        | None          | org.apache.hudi.table.action.clean.CleaningTriggerStrategy: Controls when cleaning is scheduled.     NUM_COMMITS(default): Trigger the cleaning service every N commits, determined by `hoodie.clean.max.commits`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| [trigger_max_commits](configurations/#hoodiecleanmaxcommits)               | Int     | N        | None          | Number of commits after the last clean operation, before scheduling of a new clean is attempted.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| [options](configurations/#Clean-Configs)                                   | String  | N        | None          | comma separated  list of Hudi configs for cleaning in the format "config1=value1,config2=value2"                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |

**Output**

| Parameter Name            | Type   |
|---------------------------|--------|
| start_clean_time          | String |
| time_taken_in_millis      | Long   |
| total_files_deleted       | Int    |
| earliest_commit_to_retain | String |
| bootstrap_part_metadata   | String |
| version                   | Int    |

**Example**

Run clean with table name
```
call run_clean(table => 'test_hudi_table');
```

Run clean with keep latest file versions policy
```
call run_clean(table => 'test_hudi_table', trigger_max_commits => 2, clean_policy => 'KEEP_LATEST_FILE_VERSIONS', file_versions_retained => 1)
```

### delete_marker

Delete marker files of a hudi table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| instant_time   | String | Y        | None          | Instant name    |

**Output**

| Output Name          | Type    |
|----------------------|---------|
| delete_marker_result | Boolean |

**Example**

```
call delete_marker(table => 'test_hudi_table', instant_time => '20230206174349556');
```

| delete_marker_result |
|:---------------------|
| true                 |

### sync_validate

Validate sync procedure.

**Input**

| Parameter Name  | Type   | Required | Default Value | Description       |
|-----------------|--------|----------|---------------|-------------------|
| src_table       | String | Y        | None          | Source table name |
| dst_table       | String | Y        | None          | Target table name |
| mode            | String | Y        | "complete"    | Mode              |
| hive_server_url | String | Y        | None          | Hive server url   |
| hive_pass       | String | Y        | None          | Hive password     |
| src_db          | String | N        | "rawdata"     | Source database   |
| target_db       | String | N        | dwh_hoodie"   | Target database   |
| partition_cnt   | Int    | N        | 5             | Partition count   |
| hive_user       | String | N        | ""            | Hive user name    |

**Output**

| Output Name | Type   |
|-------------|--------|
| result      | String |

**Example**

```
 call sync_validate(hive_server_url=>'jdbc:hive2://localhost:10000/default', src_table => 'test_hudi_table_src', dst_table=> 'test_hudi_table_dst', mode=>'complete', hive_pass=>'', src_db=> 'default', target_db=>'default');
```

### hive_sync

Sync the table's latest schema to Hive metastore.

**Input**

| Parameter Name                                                                                            | Type   | Required | Default Value | Description                                                                                                                                                                                                                                        |
|-----------------------------------------------------------------------------------------------------------|--------|----------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                                                                     | String | Y        | None          | Hudi table name                                                                                                                                                                                                                                    |
| metastore_uri                                                                                             | String | N        | ""            | Metastore_uri                                                                                                                                                                                                                                      |
| username                                                                                                  | String | N        | ""            | User name                                                                                                                                                                                                                                          |
| password                                                                                                  | String | N        | ""            | Password                                                                                                                                                                                                                                           |
| [use_jdbc](configurations.md#hoodiedatasourcehive_syncuse_jdbc)                                   | String | N        | ""            | Use JDBC when hive synchronization is enabled                                                                                                                                                                                                      |
| [mode](configurations.md#hoodiedatasourcehive_syncmode)                                           | String | N        | ""            | Mode to choose for Hive ops. Valid values are hms, jdbc and hiveql.                                                                                                                                                                                |
| [partition_fields](configurations.md#hoodiedatasourcehive_syncpartition_fields)                   | String | N        | ""            | Field in the table to use for determining hive partition columns.                                                                                                                                                                                  |                                                       |
| [partition_extractor_class](configurations.md#hoodiedatasourcehive_syncpartition_extractor_class) | String | N        | ""            | Class which implements PartitionValueExtractor to extract the partition values, default 'org.apache.hudi.hive.MultiPartKeysValueExtractor'.                                                                                                        |
| [strategy](configurations.md#hoodiedatasourcehive_synctablestrategy)                              | String | N        | ""            | Hive table synchronization strategy. Available option: RO, RT, ALL.                                                                                                                                                                                |
| [sync_incremental](configurations.md#hoodiemetasyncincremental)                                   | String | N        | ""            | Whether to incrementally sync the partitions to the metastore, i.e., only added, changed, and deleted partitions based on the commit metadata. If set to `false`, the meta sync executes a full partition sync operation when partitions are lost. |



**Output**

| Output Name | Type   |
|-------------|--------|
| result      | String |

**Example**

```
call hive_sync(table => 'test_hudi_table');
```

| result |
|:-------|
| true   |

### hdfs_parquet_import

add parquet files to a hudi table.

**Input**

| Parameter Name   | Type   | Required | Default Value | Description                                |
|------------------|--------|----------|---------------|--------------------------------------------|
| table            | String | Y        | None          | Hudi table name                            |
| table_type       | String | Y        | ""            | Table type, MERGE_ON_READ or COPY_ON_WRITE |
| src_path         | String | Y        | ""            | Source path                                |
| target_path      | String | Y        | ""            | target path                                |
| row_key          | String | Y        | ""            | Primary key                                |
| partition_key    | String | Y        | ""            | Partition key                              |
| schema_file_path | String | Y        | ""            | Path of Schema file                        |
| format           | String | N        | "parquet"     | File format                                |
| command          | String | N        | "insert"      | Import command                             |
| retry            | Int    | N        | 0             | Retry times                                |
| parallelism      | Int    | N        | None          | Parallelism                                |
| props_file_path  | String | N        | ""            | Path of properties file                    |

**Output**

| Output Name   | Type |
|---------------|------|
| import_result | Int  |

**Example**

```
call hdfs_parquet_import(table => 'test_hudi_table', table_type => 'COPY_ON_WRITE', src_path => '', target_path => '', row_key => 'id', partition_key => 'dt', schema_file_path => '');
```

| import_result |
|:--------------|
| 0             |


### repair_add_partition_meta

Repair add partition for a hudi table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description     |
|----------------|---------|----------|---------------|-----------------|
| table          | String  | Y        | None          | Hudi table name |
| dry_run        | Boolean | N        | true          | Dry run         |

**Output**

| Output Name         | Type   |
|---------------------|--------|
| partition_path      | String |
| metadata_is_present | String |
| action              | String |

**Example**

```
call repair_add_partition_meta(table => 'test_hudi_table');
```

| partition_path | metadata_is_present | action |
|----------------|---------------------|--------|
| dt=2021-05-03  | Yes                 | None   |

### repair_corrupted_clean_files

Repair corrupted clean files for a hudi table.

**Input**

| Parameter Name      | Type    | Required | Default Value | Description        |
|---------------------|---------|----------|---------------|--------------------|
| table               | String  | Y        | None          | Hudi table name    |

**Output**

| Output Name | Type    |
|-------------|---------|
| result      | Boolean |

**Example**

```
call repair_corrupted_clean_files(table => 'test_hudi_table');
```

| result | 
|--------|
| true   | 

### repair_deduplicate

Repair deduplicate records for a hudi table. The job dedupliates the data in the duplicated_partition_path and writes it into repaired_output_path. In the end of the job, the data in repaired_output_path is copied into the original path (duplicated_partition_path).

**Input**

| Parameter Name            | Type    | Required | Default Value | Description               |
|---------------------------|---------|----------|---------------|---------------------------|
| table                     | String  | Y        | None          | Hudi table name           |
| duplicated_partition_path | String  | Y        | None          | Duplicated partition path |
| repaired_output_path      | String  | Y        | None          | Repaired output path      |
| dry_run                   | Boolean | N        | true          | Dry run                   |
| dedupe_type               | String  | N        | "insert_type" | Dedupe type               |

**Output**

| Output Name | Type   |
|-------------|--------|
| result      | String |

**Example**

```
call repair_deduplicate(table => 'test_hudi_table', duplicated_partition_path => 'dt=2021-05-03', repaired_output_path => '/tmp/repair_path/');
```

| result                                       | 
|----------------------------------------------|
| Reduplicated files placed in: /tmp/repair_path/. | 

### repair_migrate_partition_meta

downgrade a hudi table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description     |
|----------------|---------|----------|---------------|-----------------|
| table          | String  | Y        | None          | Hudi table name |
| dry_run        | Boolean | N        | true          | Dry run         |

**Output**

| Output Name           | Type   |
|-----------------------|--------|
| partition_path        | String |
| text_metafile_present | String |
| base_metafile_present | String |
| action                | String |

**Example**

```
call repair_migrate_partition_meta(table => 'test_hudi_table');
```

### repair_overwrite_hoodie_props

overwrite a hudi table properties.

**Input**

| Parameter Name      | Type   | Required | Default Value | Description            |
|---------------------|--------|----------|---------------|------------------------|
| table               | String | Y        | None          | Hudi table name        |
| new_props_file_path | String | Y        | None          | Path of new properties |

**Output**

| Output Name | Type   |
|-------------|--------|
| property    | String |
| old_value   | String |
| new_value   | String |

**Example**

```
call repair_overwrite_hoodie_props(table => 'test_hudi_table', new_props_file_path = > '/tmp/props');
```

| property                 | old_value | new_value |
|--------------------------|-----------|-----------|
| hoodie.file.index.enable | true      | false     | 

## Bootstrap

### run_bootstrap

Convert an existing table to Hudi.

**Input**

| Parameter Name                                                               | Type    | Required | Default Value                                                                 | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|------------------------------------------------------------------------------|---------|----------|-------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| table                                                                        | String  | Y        | None                                                                          | Name of table to be clustered                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| table_type                                                                   | String  | Y        | None                                                                          | Table type, MERGE_ON_READ or COPY_ON_WRITE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| [bootstrap_path](configurations.md#hoodiebootstrapbasepath)          | String  | Y        | None                                                                          | Base path of the dataset that needs to be bootstrapped as a Hudi table                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| base_path                                                                    | String  | Y        | None                                                                          | Base path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| rowKey_field                                                                 | String  | Y        | None                                                                          | Primary key field                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| base_file_format                                                             | String  | N        | "PARQUET"                                                                     | Format of base file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| partition_path_field                                                         | String  | N        | ""                                                                            | Partitioned column field                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| [bootstrap_index_class](configurations.md#hoodiebootstrapindexclass) | String  | N        | "org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex"                  | Implementation to use, for mapping a skeleton base file to a bootstrap base file.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| [selector_class](configurations.md#hoodiebootstrapmodeselector)      | String  | N        | "org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector" | Selects the mode in which each file/partition in the bootstrapped dataset gets bootstrapped                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| key_generator_class                                                          | String  | N        | "org.apache.hudi.keygen.SimpleKeyGenerator"                                   | Class of key generator                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| full_bootstrap_input_provider                                                | String  | N        | "org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider"                 | Class of full bootstrap input provider                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| schema_provider_class                                                        | String  | N        | ""                                                                            | Class of schema provider                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| payload_class                                                                | String  | N        | "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"                 | Class of payload                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| [parallelism](configurations.md#hoodiebootstrapparallelism)          | Int     | N        | 1500                                                                          | For metadata-only bootstrap, Hudi parallelizes the operation so that each table partition is handled by one Spark task. This config limits the number of parallelism. We pick the configured parallelism if the number of table partitions is larger than this configured value. The parallelism is assigned to the number of table partitions if it is smaller than the configured value. For full-record bootstrap, i.e., BULK_INSERT operation of the records, this configured value is passed as the BULK_INSERT shuffle parallelism (`hoodie.bulkinsert.shuffle.parallelism`), determining the BULK_INSERT write behavior. If you see that the bootstrap is slow due to the limited parallelism, you can increase this. |
| enable_hive_sync                                                             | Boolean | N        | false                                                                         | Whether to enable hive sync                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| props_file_path                                                              | String  | N        | ""                                                                            | Path of properties file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| bootstrap_overwrite                                                          | Boolean | N        | false                                                                         | Overwrite bootstrap path                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

**Output**

| Output Name | Type    |
|-------------|---------|
| status      | Boolean |

**Example**

```
call run_bootstrap(table => 'test_hudi_table', table_type => 'COPY_ON_WRITE', bootstrap_path => 'hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table', base_path => 'hdfs://ns1//tmp/hoodie/test_hudi_table', rowKey_field => 'id', partition_path_field => 'dt',bootstrap_overwrite => true);
```

| status | 
|--------|
| 0      | 

### show_bootstrap_mapping

Show mapping files of a bootstrap table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description                          |
|----------------|---------|----------|---------------|--------------------------------------|
| table          | String  | Y        | None          | Name of table to be clustered        |
| partition_path | String  | N        | ""            | Partition path                       |
| file_ids       | String  | N        | ""            | File ids                             |
| limit          | Int     | N        | 10            | Max number of records to be returned |
| sort_by        | String  | N        | "partition"   | Sort by columns                      |
| desc           | Boolean | N        | false         | Descending order                     |

**Output**

| Parameter Name   | Type   | 
|------------------|--------|
| partition        | String | 
| file_id          | Int    |
| source_base_path | String | 
| source_partition | Int    | 
| source_file      | String | 

**Example**

```
call show_bootstrap_mapping(table => 'test_hudi_table');
```

| partition     | file_id                                | source_base_path                                                                                                                    | source_partition | source_file                                | 
|---------------|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|------------------|--------------------------------------------|
| dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | hdfs://ns1/hive/warehouse/hudi.db/test_hudi_table/dt=2021-05-03/d0073a12-085d-4f49-83e9-402947e7e90a-0_0-2-2_00000000000002.parquet | dt=2021-05-03    | hdfs://ns1/tmp/dt=2021-05-03/00001.parquet |


### show_bootstrap_partitions

Show partitions of a bootstrap table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description                          |
|----------------|--------|----------|---------------|--------------------------------------|
| table          | String | Y        | None          | Name of table to be clustered        |

**Output**

| Parameter Name     | Type   | 
|--------------------|--------|
| indexed_partitions | String | 

**Example**

```
call show_bootstrap_partitions(table => 'test_hudi_table');
```

| indexed_partitions | 
|--------------------|
| 	dt=2021-05-03     |

## Version management

### upgrade_table

upgrade a hudi table to a specific version.

**Input**

| Parameter Name | Type   | Required | Default Value | Description             |
|----------------|--------|----------|---------------|-------------------------|
| table          | String | Y        | None          | Hudi table name         |
| to_version     | String | Y        | None          | Version of hoodie table |

**Output**

| Output Name | Type    |
|-------------|---------|
| result      | Boolean |

**Example**

```
call upgrade_table(table => 'test_hudi_table', to_version => 'FIVE');
```

| result | 
|--------|
| true   | 

### downgrade_table

downgrade a hudi table to a specific version.

**Input**

| Parameter Name | Type   | Required | Default Value | Description             |
|----------------|--------|----------|---------------|-------------------------|
| table          | String | Y        | None          | Hudi table name         |
| to_version     | String | Y        | None          | Version of hoodie table |

**Output**

| Output Name | Type    |
|-------------|---------|
| result      | Boolean |

**Example**

```
call downgrade_table(table => 'test_hudi_table', to_version => 'FOUR');
```

| result | 
|--------|
| true   |
