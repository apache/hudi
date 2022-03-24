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

:::note
Show a commit's info.
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |
| limit | Int | N | 10 | return result's limit |

Output

| Output Name | Type |
|------------|--------|
| commit_time | String |
| total_bytes_written | Long |
| total_files_added | Long |
| total_files_updated | Long |
| total_partitions_written | Long |
| total_records_written | Long |
| total_update_records_written | Long |
| total_errors | Long |

#### Example
```
call show_commits(table => 'test_hudi_table', limit => 10);
```

|    commit_time   |	total_bytes_written | total_files_added | total_files_updated | total_partitions_written | total_records_written | total_update_records_written | total_errors |
|----------------- | ------------- |-------------|-------------|------------------ |------------- |----------------  |-------------------------|
|20220216171049652 |	432653     |	0 |	1 |	1 |	0 |	0 |	0 |
|20220216171027021 |	435346     |	1 |	0 |	1 |	1 |	0 |	0 |
|20220216171019361 |	435349     |	1 |	0 |	1 |	1 |	0 |	0 |

### show_commits_metadata

:::note
Show a commits metadata.
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |
| limit | Int | N | 10 | return result's limit |

Output

| Output Name | Type |
|------------|--------|
| commit_time | String |
| action | String |
| partition | String |
| file_id | String |
| previous_commit | String |
| num_writes | Long |
| num_inserts | Long |
| num_deletes | Long |
| num_update_writes | String |
| total_errors | Long |
| total_log_blocks | Long |
| total_corrupt_logblocks | Long |
| total_rollback_blocks | Long |
| total_log_records | Long |
| total_updated_records_compacted | Long |
| total_bytes_written | Long |

#### Example
```
call show_commits_metadata(table => 'test_hudi_table');
```

|    commit_time   | action |	 partition  |               	file_id              |	previous_commit |	num_writes | num_inserts | num_deletes | num_update_writes | total_errors | total_log_blocks | total_corrupt_logblocks | total_rollback_blocks | total_log_records | total_updated_records_compacted | total_bytes_written|
|----------------- |  ----  |  ------------ |----------------------------------------| ---------------- |------------- |-------------|-------------|------------------ |------------- |----------------  |-------------------------|---------------------- |-------------------|-------------------------------  |------------------- |
|20220109225319449 | commit | dt=2021-05-03	| d0073a12-085d-4f49-83e9-402947e7e90a-0 | null             | 	1          | 	1        | 	0          | 0                 | 0            | 0				 | 0					   | 0                     | 0                 | 0                               | 435349             |
|20220109225311742 | commit	| dt=2021-05-02	| b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109214830592| 	1          | 	1        | 	0	       | 0	               | 0            | 0				 | 0					   | 0                     | 0                 | 0                               | 435340             |
|20220109225301429 | commit	| dt=2021-05-01	| 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109214830592| 	1          | 	1        | 	0          | 0                 | 0	          | 0				 | 0					   | 0                     | 0                 | 0                               | 435340             |
|20220109214830592 | commit	| dt=2021-05-01	| 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | 20220109191631015| 	0          | 	0        | 	1	       | 0	               | 0            | 0				 | 0					   | 0                     | 0                 | 0                               | 432653             |
|20220109214830592 | commit	| dt=2021-05-02	| b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | 20220109191648181| 	0          | 	0        | 	1	       | 0	               | 0	          | 0				 | 0					   | 0                     | 0                 | 0                               | 432653             |
|20220109191648181 | commit	| dt=2021-05-02	| b3b32bac-8a44-4c4d-b433-0cb1bf620f23-0 | null             | 	1          | 	1        | 	0          | 0                 | 0            | 0				 | 0					   | 0                     | 0                 | 0                               | 435341             |
|20220109191631015 | commit	| dt=2021-05-01	| 0d7298b3-6b55-4cff-8d7d-b0772358b78a-0 | null             | 	1          | 	1        | 	0          | 0                 | 0            | 0				 | 0					   | 0                     | 0                 | 0                               | 435341             |

### rollback_to_instant

:::note
Rollback a table to the commit that was current at some time.
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |

Output

| Output Name | Type |
|------------|--------|
| rollback_result | Bollean |

#### Example
Roll back test_hudi_table to one instant
```
call rollback_to_instant(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_result |
| :---------------|
|    true         |

### create_savepoints

:::note
Create a savepoint to hudi's table..
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |
| commit_Time | String | Y | None | commit time |
| user | String | N | "" | user name |
| comments | String | N | "" | comments |

Output

| Output Name | Type |
|------------|--------|
| create_savepoint_result | Bollean |

#### Example
Roll back test_hudi_table to one instant
```
call create_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| create_savepoint_result |
| :---------------|
|    true         |

### delete_savepoints

:::note
Delete a savepoint to hudi's table.
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |
| instant_time | String | Y | None | instant time |

Output

| Output Name | Type |
|------------|--------|
| delete_savepoint_result | Bollean |

#### Example
Delete a savepoint to test_hudi_table
```
call delete_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| delete_savepoint_result |
| :---------------|
|    true         |

### rollback_savepoints

:::note
Rollback a table to the commit that was current at some time.
:::

#### Usage

Iutput

| Parameter Name | Type | Required | Default Value | Description |
|------------|--------|--------|--------|--------|
| table | String | Y | None | hudi table name |
| instant_time | String | Y | None | instant time |

Output

| Output Name | Type |
|------------|--------|
| rollback_savepoint_result | Bollean |

#### Example
Rollback test_hudi_table to one savepoint
```
call rollback_savepoints(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_savepoint_result |
| :---------------|
|    true         |

## Optimization table

### RunClusteringProcedure

### ShowClusteringProcedure
