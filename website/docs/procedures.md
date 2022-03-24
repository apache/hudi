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

| Output Name | Type | Description |
|------------|--------|--------|
| rollback_result | Bollean | rollback the execution result of this action. |

#### Example
Roll back test_hudi_table to one instant
```
call rollback_to_instant(table => 'test_hudi_table', instant_time => '20220109225319449');
```

| rollback_result |
| :---------------|
|    true         |

## Optimization table

## Metadata table management

## Table migration



