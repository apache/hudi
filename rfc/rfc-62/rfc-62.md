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

- zhangyue19921010

## Approvers
 - @codope
 - @xushiyan
 - @YuweiXiao

## Status

JIRA: https://issues.apache.org/jira/browse/HUDI-4707

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

1. Hudi will record the complete timeline in the `.hoodie` directory, including active timeline and archive timeline. The timeline acts as an **event log** for the Hudi table using which one can track table snapshots.

2. Hudi metadata table which records partitions, data files, columns statistics, etc.

3. Each commit of hudi records various metadata information and runtime metrics currently written, such as: totalNumWrites, totalNumDeletes, totalNumUpdateWrites, totalNumInserts, totalWriteBytes, 
totalWriteErrors, totalLogRecords, totalLogFilesCompacted, totalLogSizeCompacted, totalUpdatedRecordsCompacted, totalScanTime, totalCreateTime, totalUpsertTime => based on `HoodieWriteStat`


In order to expose hudi table context more efficiently, this RFC proposes a Diagnostic Reporter System.
It can be turned on as the final stage in the ingestion job post-commit which will collect common troubleshooting information including engine (Spark, Flink) runtime information, and generate a diagnostic report JSON file.
Alternatively, users can trigger diagnostic reporter using `hudi-cli` to generate the JSON report.

If users turn on this diagnostic reporter, hoodie tries to generate this report under `.hoodie/report/` no matter current commit is successful or not.

## Implementation

Diagnostic Reporter will go through the whole hudi table and generate a JSON file that contains all the necessary information. Also, this tool will package `.hoodie` folder as a zip compressed file.

Users can use this Diagnostic Reporter Tool in the following two ways：
1. Users can directly enable this diagnostic reporter in the writing jobs, at this time diagnostic reporter tool will go through current hudi table and generate report files as the last stage after commit.
2. Users can directly generate the corresponding report file for a hudi table through the hudi cli command.

Report json file location: hudi will create a new meta folder name `.hoodie/report/instant-time/` for this report result, for example  `.hoodie/report/xxxx.json` and `.hoodie/report/xxxx.zip`

Naming rules for specific report json/zip file: `HudiTableName + "_" + HudiVersion + "_" + appName + "_" + applicationId + "_" + applicationAttemptId + "_" + isLocal + format`.
There may be multiple writers and writing multiple hudi tables. Using this naming rule should be sufficient to identify a writer.


AS for the json file this diagnostic reporter tool generated, it will contains three parts: data information, meta information and engine runtime information(take spark as an example here):

### Data information

Diagnostic reporter need to do file system listing. Then do basic analysis, build follow result (part1 of report.json)
- List all the partitions and all the data files for current hudi table.
- Calculate how many partition numbers in total as `total.partition_numbers`
- Calculate how many latest base files in total as `total.latest_base_file_numbers`
- Calculate total data volumes for all the latest base files as `total.latest_base_file_volume`
- Select the partition with the largest number of latest base files as `partition_max_data_file_numbers`
- Select the partition with the largest volume of latest base files as `partition_max_data_volume`

```json
{
    "total":{
        "partition_numbers":"100",
        "latest_base_file_numbers":"1000",
        "latest_base_file_volume":"1000GB",
        "partition_keys":"year/month/day"
    },
    "partition_max_data_volume":{
        "partition_name":"2022/09/05",
        "file_group_number":"5",
        "data_volume":"10GB",
        "max_data_file_size":"1GB",
        "min_data_file_size":"100MB"
    },
    "partition_max_data_file_numbers":{
        "partition_name":"2022/09/06",
        "file_group_number":"50",
        "data_volume":"1GB",
        "max_data_file_size":"100MB",
        "min_data_file_size":"10MB"
    },
    "sample_hoodie_key":{
        "hoodie_keys":"user_id",
        "sample_values":[
            "1**4",
            "4**7"
        ]
    }
}
```

We can quickly catch up the data distribution characteristics of the current hudi table through this part, which can be used to determine whether there is a small file problem 
or a data hotspot problem, etc.

In addition, through data distribution and sample hudi keys, we can also use this information to help users choose the most appropriate index mode, for example:
- If the hudi data is roughly evenly distributed, users can try using bucket index to improve upsert performance.
- If the hudi key is monotonically increasing then users can try the bloom index.

Of course, the behavior of the sample hoodie key is false by default for privacy concerns and sample values for hoodie_keys can be masked, only expose half length of them with heads and tails like `183*****832`



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
        "totalNumWrites":123352,
        "totalNumDeletes":0,
        "totalNumUpdateWrites":0,
        "totalNumInserts":123352,
        "totalWriteBytes":4675371,
        "totalWriteErrors":0,
        "totalLogRecords":0,
        "totalLogFilesCompacted":0,
        "totalLogSizeCompacted":0,
        "totalUpdatedRecordsCompacted":0,
        "totalScanTime":0,
        "totalCreateTime":21051,
        "totalUpsertTime":0
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
            "numTasks":1,
            "numFailedTasks":0,
            "submissionTime":"2022-09-05T09:42:16.648GMT",
            "completionTime":"2022-09-05T09:42:18.236GMT",
            "executorRunTime":1559,
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
            "name":"f_sa_auction_hourly_hudi_final",
            "description":"Obtaining marker files for all created, merged paths"
        }
    ]
}
```
Through this part, we can observe all stages of the current hudi job and the corresponding execution time

### final schema demo
```json
{
  "data_info": {
    "total": {
      "partition_numbers": "100",
      "latest_base_file_numbers": "1000",
      "latest_base_file_volume": "1000GB",
      "partition_keys": "year/month/day"
    },
    "partition_max_data_volume": {
      "partition_name": "2022/09/05",
      "file_group_number": "5",
      "data_volume": "10GB",
      "max_data_file_size": "1GB",
      "min_data_file_size": "100MB"
    },
    "partition_max_data_file_numbers": {
      "partition_name": "2022/09/06",
      "file_group_number": "50",
      "data_volume": "1GB",
      "max_data_file_size": "100MB",
      "min_data_file_size": "10MB"
    },
    "sample_hoodie_key": {
      "hoodie_keys": "user_id",
      "sample_values": [
        "1**4",
        "4**7"
      ]
    }
  },
  "meta_info": {
    "configs": {
      "engine_config": {
        "spark.executor.memoryOverhead": "3072"
      },
      "hoodie_config": {
        "hoodie.datasource.write.keygenerator.class": "org.apache.hudi.keygen.ComplexKeyGenerator"
      }
    },
    "commits": [
      {
        "20220818134233973.commit": {
          "totalNumWrites": 123352,
          "totalNumDeletes": 0,
          "totalNumUpdateWrites": 0,
          "totalNumInserts": 123352,
          "totalWriteBytes": 4675371,
          "totalWriteErrors": 0,
          "totalLogRecords": 0,
          "totalLogFilesCompacted": 0,
          "totalLogSizeCompacted": 0,
          "totalUpdatedRecordsCompacted": 0,
          "totalScanTime": 0,
          "totalCreateTime": 21051,
          "totalUpsertTime": 0
        }
      }
    ]
  },
  "engine_info": {
    "engine_runtime_info":[
        {
            "status":"COMPLETE",
            "stageId":7191,
            "numTasks":1,
            "numFailedTasks":0,
            "submissionTime":"2022-09-05T09:42:16.648GMT",
            "completionTime":"2022-09-05T09:42:18.236GMT",
            "executorRunTime":1559,
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
            "name":"f_sa_auction_hourly_hudi_final",
            "description":"Obtaining marker files for all created, merged paths"
        }
    ]
  }
}
```

### Config

Add several configs to control the behavior of Diagnostic Reporter

- `hoodie.write.diagnostic.reporter.enable`: default false. If users turn on this diagnostic reporter, hoodie tries to generate this report no matter current commit is successful or not.
- `hoodie.write.diagnostic.reporter.base_path`: default `.hoodie/`. Allows users to specify where the report json file and  the zip file is written.
- `hoodie.write.diagnostic.reporter.zip.meta`: default false. Set true, hoodie tries to zip `.hoodie` under `hoodie.write.diagnostic.reporter.base_path` + /report. Please pay attention here, for large hudi tables, this behavior may take pretty long time and affect write performance.
- `hoodie.write.diagnostic.reporter.zip.meta.commits.only`: default true. Takes effect when `hoodie.write.diagnostic.reporter.zip.meta` is true. Compress active timeline related files only.
- `hoodie.write.diagnostic.reporter.include.data.info`: default false. Set true, Diagnostic reporter will do file system listing and basic analysis which can reflect the data characteristics of the entire hudi table. Please pay attention here, this operation list the whole hudi table and may take pretty long time affecting write performance.
- `hoodie.write.diagnostic.reporter.include.sample.keys`: default false. Set true, Diagnostic reporter tries to sample several hoodie keys in report json file(data_info part) with masked. for example `12****56`
- `hoodie.write.diagnostic.reporter.include.meta.info`: default true. Summarize and expose the statistics of each commit: totalNumWrites, totalNumDeletes, totalNumUpdateWrites, totalNumInserts, totalWriteBytes, totalWriteErrors, totalLogRecords, totalLogFilesCompacted, totalLogSizeCompacted, totalUpdatedRecordsCompacted, totalScanTime, totalCreateTime, totalUpsertTime => based on `HoodieWriteStat`
- `hoodie.write.diagnostic.reporter.commits.number`: default -1 which means include all the active commits. Takes effect when `hoodie.write.diagnostic.reporter.include.meta.info` is true.
- `hoodie.write.diagnostic.reporter.include.engine.info`: default true. Collect runtime engine info for current job.


Please pay attention here:
When users set `hoodie.write.diagnostic.reporter.enable` true, hoodie tries to generate this json report in each commit no matter this commit is successful or not.

`hoodie.write.diagnostic.reporter.zip.meta` and `hoodie.write.diagnostic.reporter.include.data.info` operations are pretty time consuming for huge hudi tables, which may affect write performance.

## Rollout/Adoption Plan

The implementation of this Diagnostic Reporter is divided into three phases:
1. Combined with Spark hudi writer, after the commit (whether successful or not) generate a Diagnostic Reporter json file and corresponding `.hoodie` zip file.
2. Combined with Hudi CLI, users can directly generate the corresponding report file for a hudi table through the hudi cli command.
3. Develop a web app to view these metrics/json in a more user-friendly manner.
4. Combined with flink engine.


## Test Plan

Describe in few sentences how the RFC will be tested. How will we know that the implementation works as expected? How will we know nothing broke?.