<!--
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
-->

# Description of the relationship between each module

This repo contains the code that integrate Hudi with Spark. The repo is split into the following modules

`hudi-spark`
`hudi-spark2`
`hudi-spark3.1.x`
`hudi-spark3.2.x`
`hudi-spark3.3.x`
`hudi-spark2-common`
`hudi-spark3-common`
`hudi-spark-common`

* hudi-spark is the module that contains the code that both spark2 & spark3 version would share, also contains the antlr4 
file that supports spark sql on spark 2.x version.
* hudi-spark2 is the module that contains the code that compatible with spark 2.x versions.
* hudi-spark3.1.x is the module that contains the code that compatible with spark3.1.x and spark3.0.x version.
* hudi-spark3.2.x is the module that contains the code that compatible with spark 3.2.x versions.
* hudi-spark3.3.x is the module that contains the code that compatible with spark 3.3.x+ versions. 
* hudi-spark2-common is the module that contains the code that would be reused between spark2.x versions, right now the module
has no class since hudi only supports spark 2.4.4 version, and it acts as the placeholder when packaging hudi-spark-bundle module. 
* hudi-spark3-common is the module that contains the code that would be reused between spark3.x versions.
* hudi-spark-common is the module that contains the code that would be reused between spark2.x and spark3.x versions.

## Description of Time Travel
* `HoodieSpark3_2ExtendedSqlAstBuilder` have comments in the spark3.2's code fork from `org.apache.spark.sql.catalyst.parser.AstBuilder`, and additional `withTimeTravel` method.
* `SqlBase.g4` have comments in the code forked from spark3.2's parser, and add SparkSQL Syntax  `TIMESTAMP AS OF` and `VERSION AS OF`.

### Time Travel Support Spark Version:

| version | support |
| ------  | ------- |
| 2.4.x   |    No   |
| 3.0.x   |    No   |
| 3.1.2   |    No   |
| 3.2.0   |    Yes  |

### To improve:
Spark3.3 support time travel syntax link [SPARK-37219](https://issues.apache.org/jira/browse/SPARK-37219). 
Once Spark 3.3 released. The files in the following list will be removed:
* hudi-spark3.3.x's `HoodieSpark3_3ExtendedSqlAstBuilder.scala`, `HoodieSpark3_3ExtendedSqlParser.scala`, `TimeTravelRelation.scala`, `SqlBase.g4`, `HoodieSqlBase.g4`
Tracking Jira: [HUDI-4468](https://issues.apache.org/jira/browse/HUDI-4468)

Some other improvements undergoing:
* Port borrowed classes from Spark 3.3 [HUDI-4467](https://issues.apache.org/jira/browse/HUDI-4467)

