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
# RFC-62: Diagnostic Reporter



## Proposers

- zhangyue19921010@163.com

## Approvers
 - @codope
 - @xushiyan

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-4707

> Please keep the status updated in `rfc/README.md`.

## Abstract

With the development of hudi, more and more users choose hudi to build their own ingestion pipelines to support real-time or batch upsert requirements.
Subsequently, some of them may ask the community for help, such as how to improve the performance of their hudi ingestion jobs? Why did their hudi jobs fail? etc.

For the volunteers in the hudi community, dealing with such issue, the volunteers may ask users to provide a list of information, including engine context, job configs,
data pattern, Spark UI, etc. Users need to spend extra effort to review their own jobs, collect metrics one buy one according to the list and give feedback to volunteers.
By the way, unexpected errors may occur at this time as users are manually collecting these information.

Obviously, there are relatively high communication costs for both volunteers and users.

On the other hand, for advanced users, they also need some way to efficiently understand the characteristics of their hudi tables, including data volume, upsert pattern, and so on.

## Background
As we know, hudi already has its own unique metrics system and metadata framework. These information are very important for hudi job tuning or troubleshooting. Fo example:

1. Hudi will record the complete timeline in the .hoodie directory, including active timeline and archive timeline. From this we can trace the historical state of the hudi job.

2. Hudi metadata table which will records all the partitions, all the data files, etc

3. Each commit of hudi records various metadata information and runtime metrics currently written, such as:
```json
{
    "partitionToWriteStats":{
        "20210623/0/20210825":[
            {
                "fileId":"4ae31921-eedd-4c56-8218-bb47849397a4-0",
                "path":"20210623/0/20210825/4ae31921-eedd-4c56-8218-bb47849397a4-0_0-27-2006_20220818134233973.parquet",
                "prevCommit":"null",
                "numWrites":123352,
                "numDeletes":0,
                "numUpdateWrites":0,
                "numInserts":123352,
                "totalWriteBytes":4675371,
                "totalWriteErrors":0,
                "tempPath":null,
                "partitionPath":"20210623/0/20210825",
                "totalLogRecords":0,
                "totalLogFilesCompacted":0,
                "totalLogSizeCompacted":0,
                "totalUpdatedRecordsCompacted":0,
                "totalLogBlocks":0,
                "totalCorruptLogBlock":0,
                "totalRollbackBlocks":0,
                "fileSizeInBytes":4675371,
                "minEventTime":null,
                "maxEventTime":null
            }
        ]
    },
    "compacted":false,
    "extraMetadata":{
        "schema":"xxxx"
    },
    "operationType":"UPSERT",
    "totalRecordsDeleted":0,
    "totalLogFilesSize":0,
    "totalScanTime":0,
    "totalCreateTime":21051,
    "totalUpsertTime":0,
    "minAndMaxEventTime":{
        "Optional.empty":{
            "val":null,
            "present":false
        }
    },
    "writePartitionPaths":[
        "20210623/0/20210825"
    ],
    "fileIdAndRelativePaths":{
        "c144908e-ca7d-401f-be1c-613de98d96a3-0":"20210623/0/20210825/c144908e-ca7d-401f-be1c-613de98d96a3-0_3-33-2009_20220818134233973.parquet"
    },
    "totalLogRecordsCompacted":0,
    "totalLogFilesCompacted":0,
    "totalCompactedRecordsUpdated":0
}
```

In order to expose hudi table context more efficiently, this RFC propose a Diagnostic Reporter Tool.
This tool can be turned on as the final stage in ingestion job after commit which will collect common troubleshooting information including engine(take spark as example here) runtime information and generate a diagnostic report json file.

Or users can trigger this diagnostic reporter tool using hudi-cli to generate this report json file.

## Implementation

This Diagnostic Reporter Tool will go through whole hudi table and generate a report json file which contains all the necessary information. Also this tool will package .hoodie folder as a zip compressed file.

Users can use this Diagnostic Reporter Tool in the following two ways：
1. Users can directly enable this diagnostic reporter in the writing jobs, at this time diagnostic reporter tool will go through current hudi table and generate report files as the last stage after commit.
2. Users can directly generate the corresponding report file for a hudi table through the hudi cli command.

Report json file location: hudi will create a new meta folder name `.hoodie/report/` for this report result, for example  `.hoodie/report/xxxx.json` and `.hoodie/report/xxxx.zip`

Naming rules for specific report json/zip file: `HudiTableName + "_" + HudiVersion + "_" + appName + "_" + applicationId + "_" + applicationAttemptId + "_" + isLocal + format`.
There may be multiple writers and writing multiple hudi tables. Using this naming rule should be sufficient to identify a writer.


AS for the json file this diagnostic reporter tool generated, it will contains three parts: data information, meta information and engine runtime information(take spark as an example here):

### Data information

Diagnostic reporter tool need to list all the partitions and all the data files for current hudi table. Then do basic analysis, build follow result (part1 of report.json)
Also we can get these information from metadata table directly if users enable it. And we can run hoodieMetadata table valuation tool after get all these information.
```json
{
    "total":{
        "partition_numbers":"100",
        "data_file_numbers":"1000",
        "data_volume":"1000GB",
        "partition_keys":"year/month/day"
    },
    "max_data_volume_per_partition":{
        "partition_name":"2022/09/05",
        "data_file_number":"10",
        "file_group_number":"5",
        "data_volume":"10GB",
        "max_data_file_size":"1GB",
        "min_data_file_size":"100MB"
    },
    "max_data_file_numbers_per_partition":{
        "partition_name":"2022/09/06",
        "data_file_number":"100",
        "file_group_number":"50",
        "data_volume":"1GB",
        "max_data_file_size":"100MB",
        "min_data_file_size":"10MB"
    },
    "sample_hoodie_key":{
        "hoodie_keys":"user_id",
        "sample_values":[
            "123",
            "456"
        ]
    }
}
```

We can quickly catch up the data distribution characteristics of the current hudi table through this part, which can be used to determine whether there is a small file problem 
or a data hotspot problem, etc.
In addition, through data distribution and sample hudi keys, we can also use this information to help users choose the most appropriate index mode, for example:
- If the hudi data is roughly evenly distributed, users can try using bucket index to improve upsert performance.
- If the hudi key is monotonically increasing then users can try the bloom index

Of course, the behavior of the sample hoodie key is false by default for privacy reasons



### Meta information
Meta information consists of two parts
The first part is `configs`, including engine-related config and hoodie-related config
The second part is the `metadata information` related to the last k active commits, sorted by time as a List

```json
{
    "configs":{
        "engine_config":{
            "spark.executor.memoryOverhead":"3072"
        },
        "hoodie_config":{
            "hoodie.datasource.write.keygenerator.class":"org.apache.hudi.keygen.ComplexKeyGenerator"
        }
    },
    "commits":[
        {
            "20220818134233973.commit":{
                "partitionToWriteStats":{
                    "20210623/0/20210825":[
                        {
                            "fileId":"4ae31921-eedd-4c56-8218-bb47849397a4-0",
                            "path":"20210623/0/20210825/4ae31921-eedd-4c56-8218-bb47849397a4-0_0-27-2006_20220818134233973.parquet",
                            "prevCommit":"null",
                            "numWrites":123352,
                            "numDeletes":0,
                            "numUpdateWrites":0,
                            "numInserts":123352,
                            "totalWriteBytes":4675371,
                            "totalWriteErrors":0,
                            "tempPath":null,
                            "partitionPath":"20210623/0/20210825",
                            "totalLogRecords":0,
                            "totalLogFilesCompacted":0,
                            "totalLogSizeCompacted":0,
                            "totalUpdatedRecordsCompacted":0,
                            "totalLogBlocks":0,
                            "totalCorruptLogBlock":0,
                            "totalRollbackBlocks":0,
                            "fileSizeInBytes":4675371,
                            "minEventTime":null,
                            "maxEventTime":null
                        }
                    ]
                },
                "compacted":false,
                "extraMetadata":{
                    "schema":"xxxx"
                },
                "operationType":"UPSERT",
                "totalRecordsDeleted":0,
                "totalLogFilesSize":0,
                "totalScanTime":0,
                "totalCreateTime":21051,
                "totalUpsertTime":0,
                "minAndMaxEventTime":{
                    "Optional.empty":{
                        "val":null,
                        "present":false
                    }
                },
                "writePartitionPaths":[
                    "20210623/0/20210825"
                ],
                "fileIdAndRelativePaths":{
                    "c144908e-ca7d-401f-be1c-613de98d96a3-0":"20210623/0/20210825/c144908e-ca7d-401f-be1c-613de98d96a3-0_3-33-2009_20220818134233973.parquet"
                },
                "totalLogRecordsCompacted":0,
                "totalLogFilesCompacted":0,
                "totalCompactedRecordsUpdated":0
            }
        }
    ]
}
```

### Engine Runtime Information

Spark allow users to get metrics through api as a json same as spark ui:
> In addition to viewing the metrics in the UI, they are also available as JSON. This gives developers an easy way to create new visualizations and monitoring tools for Spark. 
> The JSON is available for both running applications, and in the history server. The endpoints are mounted at /api/v1. 
> For example, for the history server, they would typically be accessible at http://<server-url>:18080/api/v1, and for a running application, at http://localhost:4040/api/v1.

So that we can collect any spark runtime information we want. For example collect all the stages and collect the corresponding execution time

```json
{
    "engine_runtime_info":[
        {
            "status":"COMPLETE",
            "stageId":7191,
            "attemptId":0,
            "numTasks":1,
            "numActiveTasks":0,
            "numCompleteTasks":1,
            "numFailedTasks":0,
            "numKilledTasks":0,
            "numCompletedIndices":1,
            "submissionTime":"2022-09-05T09:42:16.648GMT",
            "firstTaskLaunchedTime":"2022-09-05T09:42:16.667GMT",
            "completionTime":"2022-09-05T09:42:18.236GMT",
            "executorDeserializeTime":7,
            "executorDeserializeCpuTime":6942821,
            "executorRunTime":1559,
            "executorCpuTime":6341871,
            "resultSize":795,
            "jvmGcTime":0,
            "resultSerializationTime":0,
            "memoryBytesSpilled":0,
            "diskBytesSpilled":0,
            "peakExecutionMemory":0,
            "inputBytes":0,
            "inputRecords":0,
            "outputBytes":0,
            "outputRecords":0,
            "shuffleRemoteBlocksFetched":0,
            "shuffleLocalBlocksFetched":0,
            "shuffleFetchWaitTime":0,
            "shuffleRemoteBytesRead":0,
            "shuffleRemoteBytesReadToDisk":0,
            "shuffleLocalBytesRead":0,
            "shuffleReadBytes":0,
            "shuffleReadRecords":0,
            "shuffleWriteBytes":0,
            "shuffleWriteTime":0,
            "shuffleWriteRecords":0,
            "name":"f_sa_auction_hourly_hudi_final",
            "description":"Obtaining marker files for all created, merged paths",
            "details":"org.apache.spark.api.java.AbstractJavaRDDLike.foreach(JavaRDDLike.scala:45)\norg.apache.hudi.client.common.HoodieSparkEngineContext.foreach(HoodieSparkEngineContext.java:88)\norg.apache.hudi.table.MarkerFiles.deleteMarkerDir(MarkerFiles.java:98)\norg.apache.hudi.table.MarkerFiles.quietDeleteMarkerDir(MarkerFiles.java:75)\norg.apache.hudi.client.AbstractHoodieWriteClient.postCommit(AbstractHoodieWriteClient.java:427)\norg.apache.hudi.client.AbstractHoodieWriteClient.commitStats(AbstractHoodieWriteClient.java:186)\norg.apache.hudi.client.SparkRDDWriteClient.commit(SparkRDDWriteClient.java:122)\norg.apache.hudi.HoodieSparkSqlWriter$.commitAndPerformPostOperations(HoodieSparkSqlWriter.scala:479)\norg.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:223)\norg.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:145)\norg.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:46)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:90)\norg.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:180)\norg.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:218)\norg.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)\norg.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:215)\norg.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:176)\norg.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:127)",
            "schedulingPool":"default",
            "rddIds":[
                28873
            ],
            "accumulatorUpdates":[

            ],
            "killedTasksSummary":{

            }
        }
    ]
}
```
Through this part, we can observe all stages of the current hudi job and the corresponding execution time

### final schema demo
```json
{
    "data_info":{
        "total":{
            "partition_numbers":"100",
            "data_file_numbers":"1000",
            "data_volume":"1000GB",
            "partition_keys":"year/month/day"
        },
        "max_data_volume_per_partition":{
            "partition_name":"2022/09/05",
            "data_file_number":"10",
            "file_group_number":"5",
            "data_volume":"10GB",
            "max_data_file_size":"1GB",
            "min_data_file_size":"100MB"
        },
        "max_data_file_numbers_per_partition":{
            "partition_name":"2022/09/06",
            "data_file_number":"100",
            "file_group_number":"50",
            "data_volume":"1GB",
            "max_data_file_size":"100MB",
            "min_data_file_size":"10MB"
        },
        "sample_hoodie_key":{
            "hoodie_keys":"user_id",
            "sample_values":[
                "123",
                "456"
            ]
        }
    },
    "meta_info":{
        "configs":{
            "engine_config":{
                "spark.executor.memoryOverhead":"3072"
            },
            "hoodie_config":{
                "hoodie.datasource.write.keygenerator.class":"org.apache.hudi.keygen.ComplexKeyGenerator"
            }
        },
        "commits":[
            "xxx"
        ]
    },
    "engine_info":{
        "engine_runtime_info":[
            {
                "status":"COMPLETE",
                "stageId":7191,
                "attemptId":0,
                "numTasks":1,
                "numActiveTasks":0,
                "numCompleteTasks":1,
                "numFailedTasks":0,
                "numKilledTasks":0,
                "numCompletedIndices":1,
                "submissionTime":"2022-09-05T09:42:16.648GMT",
                "firstTaskLaunchedTime":"2022-09-05T09:42:16.667GMT",
                "completionTime":"2022-09-05T09:42:18.236GMT",
                "executorDeserializeTime":7,
                "executorDeserializeCpuTime":6942821,
                "executorRunTime":1559,
                "executorCpuTime":6341871,
                "resultSize":795,
                "jvmGcTime":0,
                "resultSerializationTime":0,
                "memoryBytesSpilled":0,
                "diskBytesSpilled":0,
                "peakExecutionMemory":0,
                "inputBytes":0,
                "inputRecords":0,
                "outputBytes":0,
                "outputRecords":0,
                "shuffleRemoteBlocksFetched":0,
                "shuffleLocalBlocksFetched":0,
                "shuffleFetchWaitTime":0,
                "shuffleRemoteBytesRead":0,
                "shuffleRemoteBytesReadToDisk":0,
                "shuffleLocalBytesRead":0,
                "shuffleReadBytes":0,
                "shuffleReadRecords":0,
                "shuffleWriteBytes":0,
                "shuffleWriteTime":0,
                "shuffleWriteRecords":0,
                "name":"f_sa_auction_hourly_hudi_final",
                "description":"Obtaining marker files for all created, merged paths",
                "details":"org.apache.spark.api.java.AbstractJavaRDDLike.foreach(JavaRDDLike.scala:45)\norg.apache.hudi.client.common.HoodieSparkEngineContext.foreach(HoodieSparkEngineContext.java:88)\norg.apache.hudi.table.MarkerFiles.deleteMarkerDir(MarkerFiles.java:98)\norg.apache.hudi.table.MarkerFiles.quietDeleteMarkerDir(MarkerFiles.java:75)\norg.apache.hudi.client.AbstractHoodieWriteClient.postCommit(AbstractHoodieWriteClient.java:427)\norg.apache.hudi.client.AbstractHoodieWriteClient.commitStats(AbstractHoodieWriteClient.java:186)\norg.apache.hudi.client.SparkRDDWriteClient.commit(SparkRDDWriteClient.java:122)\norg.apache.hudi.HoodieSparkSqlWriter$.commitAndPerformPostOperations(HoodieSparkSqlWriter.scala:479)\norg.apache.hudi.HoodieSparkSqlWriter$.write(HoodieSparkSqlWriter.scala:223)\norg.apache.hudi.DefaultSource.createRelation(DefaultSource.scala:145)\norg.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand.run(SaveIntoDataSourceCommand.scala:46)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult$lzycompute(commands.scala:70)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.sideEffectResult(commands.scala:68)\norg.apache.spark.sql.execution.command.ExecutedCommandExec.doExecute(commands.scala:90)\norg.apache.spark.sql.execution.SparkPlan.$anonfun$execute$1(SparkPlan.scala:180)\norg.apache.spark.sql.execution.SparkPlan.$anonfun$executeQuery$1(SparkPlan.scala:218)\norg.apache.spark.rdd.RDDOperationScope$.withScope(RDDOperationScope.scala:151)\norg.apache.spark.sql.execution.SparkPlan.executeQuery(SparkPlan.scala:215)\norg.apache.spark.sql.execution.SparkPlan.execute(SparkPlan.scala:176)\norg.apache.spark.sql.execution.QueryExecution.toRdd$lzycompute(QueryExecution.scala:127)",
                "schedulingPool":"default",
                "rddIds":[
                    28873
                ],
                "accumulatorUpdates":[

                ],
                "killedTasksSummary":{

                }
            }
        ]
    }
}
```


## Rollout/Adoption Plan

 - What impact (if any) will there be on existing users? 
 - If we are changing behavior how will we phase out the older behavior?
 - If we need special migration tools, describe them here.
 - When will we remove the existing behavior

## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.