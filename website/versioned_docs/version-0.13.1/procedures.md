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
CALL system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1, ... arg_name_n => arg_n)
```
#### Positional arguments
When passing arguments by position, the arguments may be omitted if they are optional.
```
CALL system.procedure_name(arg_1, arg_2, ... arg_n)
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
| parameters: <br/>param                           type_name                       default_value                   required <br/>table                           string                          None                            true <br/>limit                           integer                         10                              false <br/>outputType: <br/>name                            type_name                       nullable                        metadata <br/>commit_time                     string                          true                            {} <br/>action                          string                          true                            {} <br/>total_bytes_written             long                            true                            {} <br/>total_files_added               long                            true                            {} <br/>total_files_updated             long                            true                            {} <br/>total_partitions_written        long                            true                            {} <br/>total_records_written           long                            true                            {} <br/>total_update_records_written    long                            true                            {} <br/>total_errors                    long                            true                            {} | 

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
| 20230206174349556 | deltacommit | schema        | {"type":"record","name":"hudi_mor_tbl","fields":[{"name":"_hoodie_commit_time","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_commit_seqno","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_record_key","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_partition_path","type":["null","string"],"doc":"","default":null},{"name":"_hoodie_file_name","type":["null","string"],"doc":"","default":null},{"name":"id","type":"int"},{"name":"ts","type":"long"}]}                                                                       | 
| 20230206174349556 | deltacommit | latest_schema | {"max_column_id":8,"version_id":20230206174349556,"type":"record","fields":[{"id":0,"name":"_hoodie_commit_time","optional":true,"type":"string","doc":""},{"id":1,"name":"_hoodie_commit_seqno","optional":true,"type":"string","doc":""},{"id":2,"name":"_hoodie_record_key","optional":true,"type":"string","doc":""},{"id":3,"name":"_hoodie_partition_path","optional":true,"type":"string","doc":""},{"id":4,"name":"_hoodie_file_name","optional":true,"type":"string","doc":""},{"id":5,"name":"id","optional":false,"type":"int"},{"id":8,"name":"ts","optional":false,"type":"long"}]} | 

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

| Parameter Name  | Type    | Required | Default Value | Description                                      |
|-----------------|---------|----------|---------------|--------------------------------------------------|
| table           | String  | N        | None          | Hudi table name                                  |
| path            | String  | N        | None          | Path of table                                    |
| min_commits     | Int     | N        | 20            | Configuration as 'hoodie.keep.min.commits'       |
| max_commits     | Int     | N        | 30            | Configuration as 'hoodie.keep.max.commits'       |
| retain_commits  | Int     | N        | 10            | Configuration as 'hoodie.commits.archival.batch' |
| enable_metadata | Boolean | N        | false         | Enable the internal metadata table               |

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

Roll back test_hudi_table to one instant
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

| Parameter Name      | Type    | Required | Default Value | Description                                     |
|---------------------|---------|----------|---------------|-------------------------------------------------|
| table               | String  | Y        | None          | Hudi table name                                 |
| query_type          | String  | N        | "snapshot"    | Configuration as 'hoodie.datasource.query.type' |
| view_name           | String  | Y        | None          | Name of view                                    |
| begin_instance_time | String  | N        | ""            | Begin instance time                             |
| end_instance_time   | String  | N        | ""            | End instance time                               |
| as_of_instant       | String  | N        | ""            | As of instant time                              |
| replace             | Boolean | N        | false         | Replace an existed view                         |
| global              | Boolean | N        | false         | Global view                                     |

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

| Parameter Name      | Type   | Required | Default Value | Description                                     |
|---------------------|--------|----------|---------------|-------------------------------------------------|
| table               | String | Y        | None          | Hudi table name                                 |
| query_type          | String | N        | "snapshot"    | Configuration as 'hoodie.datasource.query.type' |
| new_table           | String | Y        | None          | Name of new table                               |
| begin_instance_time | String | N        | ""            | Begin instance time                             |
| end_instance_time   | String | N        | ""            | End instance time                               |
| as_of_instant       | String | N        | ""            | As of instant time                              |
| save_mode           | String | N        | "overwrite"   | Save mode                                       |

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
| {"_hoodie_commit_time": "20230205133427059", "_hoodie_commit_seqno": "20230205133427059_0_10", "_hoodie_record_key": "1", "_hoodie_partition_path": "", "_hoodie_file_name": "3438e233-7b50-4eff-adbb-70b1cd76f518-0", "id": 1, "name": "a1", "price": 40.0, "ts": 1111} | 

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
| 20230205133427059 | 1            | AVRO_DATA_BLOCK | {"INSTANT_TIME":"20230205133427059","SCHEMA":"{\"type\":\"record\",\"name\":\"hudi_mor_tbl_record\",\"namespace\":\"hoodie.hudi_mor_tbl\",\"fields\":[{\"name\":\"_hoodie_commit_time\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},{\"name\":\"_hoodie_commit_seqno\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},{\"name\":\"_hoodie_record_key\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},{\"name\":\"_hoodie_partition_path\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},{\"name\":\"_hoodie_file_name\",\"type\":[\"null\",\"string\"],\"doc\":\"\",\"default\":null},{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"ts\",\"type\":\"long\"}]}"} | {}              |

### show_invalid_parquet

Show invalid parquet files of a table.

**Input**

| Parameter Name | Type    | Required | Default Value | Description                          |
|----------------|---------|----------|---------------|--------------------------------------|
| Path           | String  | Y        | None          | Hudi table name                      |

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
| path_regex         | String  | N        | "*/*/*"       | Pattern of path                      |

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
| partition_path     | String  | Y        | ""            | Partition path                       |
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
call show_fsview_latest(table => 'test_hudi_table');
```

| partition     | file_id                                | base_instant      | data_file                                                                | data_file_size | num_delta_files | total_delta_file_size | delta_files                                                             |
|---------------|----------------------------------------|-------------------|--------------------------------------------------------------------------|----------------|-----------------|-----------------------|-------------------------------------------------------------------------|
| dt=2021-05-03 | d0073a12-085d-4f49-83e9-402947e7e90a-0 | 20220109225319449 | 7fb52523-c7f6-41aa-84a6-629041477aeb-0_0-92-99_20220109225319449.parquet | 5319449        | 1               | 213193                | .7fb52523-c7f6-41aa-84a6-629041477aeb-0_20230205133217210.log.1_0-60-63 |

## Optimization table

### run_clustering

Trigger clustering on a hoodie table. By using partition predicates, clustering table can be run 
with specified partitions, and you can also specify the order columns to sort data.

:::note
Newly clustering instant will be generated every call, and all pending clustering instants are executed.
When calling this procedure, one of parameters ``table`` and ``path`` must be specified at least. If both 
parameters are given, ``table`` will take effect.

:::


**Input**

| Parameter Name | Type   | Required | Default Value | Description                   |
|----------------|--------|----------|---------------|-------------------------------|
| table          | String | N        | None          | Name of table to be clustered |
| path           | String | N        | None          | Path of table to be clustered |
| predicate      | String | N        | None          | Predicate to filter partition |
| order          | String | N        | None          | Order column split by `,`     |

**Output**

Empty

**Example**

Clustering test_hudi_table with table name
```
call run_clustering(table => 'test_hudi_table')
```

Clustering test_hudi_table with table path
```
call run_clustering(path => '/tmp/hoodie/test_hudi_table')
```

Clustering test_hudi_table with table name, predicate and order column
```
call run_clustering(table => 'test_hudi_table', predicate => 'ts <= 20220408L', order => 'ts')
```

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
call show_clustering(table => 'test_hudi_table')
```
| timestamp         | groups | 
|-------------------|--------|
| 20220408153707928 | 2      |
| 20220408153636963 | 3      |

Show pending clusterings with table path
```
call show_clustering(path => '/tmp/hoodie/test_hudi_table')
```
| timestamp         | groups | 
|-------------------|--------|
| 20220408153707928 | 2      |
| 20220408153636963 | 3      |

Show pending clusterings with table name and limit
```
call show_clustering(table => 'test_hudi_table', limit => 1)
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

| Parameter Name | Type   | Required | Default Value | Description                         |
|----------------|--------|----------|---------------|-------------------------------------|
| op             | String | N        | None          | Operation type, `RUN` or `SCHEDULE` |
| table          | String | N        | None          | Name of table to be compacted       |
| path           | String | N        | None          | Path of table to be compacted       |
| timestamp      | String | N        | None          | Instant time                        |

**Output**

The output of `RUN` operation is `EMPTY`, the output of `SCHEDULE` as follow:

| Parameter Name | Type   | Required  | Default Value | Description  |
|----------------|--------|-----------|---------------|--------------|
| instant        | String | N         | None          | Instant name |

**Example**

Run compaction with table name
```
call run_compaction(op => 'run', table => 'test_hudi_table')
```

Run compaction with table path
```
call run_compaction(op => 'run', path => '/tmp/hoodie/test_hudi_table')
```

Run compaction with table path and timestamp
```
call run_compaction(op => 'run', path => '/tmp/hoodie/test_hudi_table', timestamp => '20220408153658568')
```

Schedule compaction with table name
```
call run_compaction(op => 'schedule', table => 'test_hudi_table')
```
| instant           |
|-------------------|
| 20220408153650834 |

Schedule compaction with table path
```
call run_compaction(op => 'schedule', path => '/tmp/hoodie/test_hudi_table')
```
| instant           |
|-------------------|
| 20220408153650834 |

Schedule compaction with table path and timestamp
```
call run_compaction(op => 'schedule', path => '/tmp/hoodie/test_hudi_table', timestamp => '20220408153658568')
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
call show_compaction(table => 'test_hudi_table')
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |
| 20220408153636963 | compaction | 10      |

Show compactions with table path
```
call show_compaction(path => '/tmp/hoodie/test_hudi_table')
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |
| 20220408153636963 | compaction | 10      |

Show compactions with table name and limit
```
call show_compaction(table => 'test_hudi_table', limit => 1)
```
| timestamp         | action     | size    |
|-------------------|------------|---------|
| 20220408153707928 | compaction | 10      |

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

| Parameter Name            | Type   | Required | Default Value | Description                                                              |
|---------------------------|--------|----------|---------------|--------------------------------------------------------------------------|
| table                     | String | Y        | None          | Hudi table name                                                          |
| metastore_uri             | String | N        | ""            | Metastore_uri                                                            |
| username                  | String | N        | ""            | User name                                                                |
| password                  | String | N        | ""            | Password                                                                 |
| use_jdbc                  | String | N        | ""            | Configration as 'hoodie.datasource.hive_sync.use_jdbc'                   |
| mode                      | String | N        | ""            | Configuration as 'hoodie.datasource.hive_sync.mode'                      |
| partition_fields          | String | N        | ""            | Configuration as 'hoodie.datasource.hive_sync.partition_fields'          |                                                       |
| partition_extractor_class | String | N        | ""            | Configuration as 'hoodie.datasource.hive_sync.partition_extractor_class' |
| strategy                  | String | N        | ""            | Configuration as 'hoodie.datasource.hive_sync.table.strategy'            |

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

Repair deduplicate records for a hudi table.

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
call repair_deduplicate(table => 'test_hudi_table', duplicated_partition_path => 'dt=2021-05-03', repaired_output_path => 'dt=2021-05-04');
```

| result                                       | 
|----------------------------------------------|
| Reduplicated files placed in: dt=2021-05-04. | 

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

| Parameter Name                | Type    | Required | Default Value                                                                 | Description                                |
|-------------------------------|---------|----------|-------------------------------------------------------------------------------|--------------------------------------------|
| table                         | String  | Y        | None                                                                          | Name of table to be clustered              |
| table_type                    | String  | Y        | None                                                                          | Table type, MERGE_ON_READ or COPY_ON_WRITE |
| bootstrap_path                | String  | Y        | None                                                                          | Bootstrap path                             |
| base_path                     | String  | Y        | None                                                                          | Base path                                  |
| rowKey_field                  | String  | Y        | None                                                                          | Primary key field                          |
| base_file_format              | String  | N        | "PARQUET"                                                                     | Format of base file                        |
| partition_path_field          | String  | N        | ""                                                                            | Partitioned column field                   |
| bootstrap_index_class         | String  | N        | "org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex"                  | Class of bootstrap index                   |
| selector_class                | String  | N        | "org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector" | Class of selector                          |
| key_generator_class           | String  | N        | "org.apache.hudi.keygen.SimpleKeyGenerator"                                   | Class of key generator                     |
| full_bootstrap_input_provider | String  | N        | "org.apache.hudi.bootstrap.SparkParquetBootstrapDataProvider"                 | Class of full bootstrap input provider     |
| schema_provider_class         | String  | N        | ""                                                                            | Class of schema provider                   |
| payload_class                 | String  | N        | "org.apache.hudi.common.model.OverwriteWithLatestAvroPayload"                 | Class of payload                           |
| parallelism                   | Int     | N        | 1500                                                                          | Parallelism                                |
| enable_hive_sync              | Boolean | N        | false                                                                         | Whether to enable hive sync                |
| props_file_path               | String  | N        | ""                                                                            | Path of properties file                    |
| bootstrap_overwrite           | Boolean | N        | false                                                                         | Overwrite bootstrap path                   |

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
call show_bootstrap_mapping(table => 'test_hudi_table')
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
call show_bootstrap_partitions(table => 'test_hudi_table')
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