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
| :---------------|
|    true         |

### create_savepoints

Create a savepoint to hudi's table.

**Input**

| Parameter Name | Type   | Required | Default Value | Description     |
|----------------|--------|----------|---------------|-----------------|
| table          | String | Y        | None          | Hudi table name |
| commit_Time    | String | Y        | None          | Commit time     |
| user           | String | N        | ""            | User name       |
| comments       | String | N        | ""            | Comments        |

**Output**

| Output Name             | Type    |
|-------------------------|---------|
| create_savepoint_result | Boolean |

**Example**

Roll back test_hudi_table to one instant
```
call create_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| create_savepoint_result |
|:------------------------|
| true                    |

### delete_savepoints

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
call delete_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| delete_savepoint_result |
|:------------------------|
| true                    |

### rollback_savepoints

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
call rollback_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_savepoint_result |
|:--------------------------|
| true                      |

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