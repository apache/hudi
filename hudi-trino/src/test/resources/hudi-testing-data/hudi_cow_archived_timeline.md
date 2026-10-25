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

## Create script

Structure of table:
- COW table in table version 8 with an archived timeline
- Using Hudi 1.0.2
- Non-partitioned table

The table itself is not checked in. Its first LSM history file,
`.hoodie/timeline/history/20250918121953134_20250918122001506_0.parquet` (four commit instants, 20250918121953134
through 20250918122001506), is checked in as `src/test/resources/archived_timeline.parquet` for
`TestTrinoParquetFileReader`.

```scala
package org.apache.spark.sql.hudi.timeline

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

class TestCompactedTimelineTable extends HoodieSparkSqlTestBase {

    test("Test COW Table with Compacted LSM Timeline") {
        withRecordType()(withTempDir { tmp =>
            val tableName = generateTableName
            val tablePath = tmp.getCanonicalPath

            // Create COW table with aggressive timeline archival settings
            spark.sql(
                s"""
                   |create table $tableName (
                   |  id int,
                   |  name string,
                   |  price double,
                   |  ts long
                   |) using hudi
                   | location '$tablePath'
                   | tblproperties (
                   |  primaryKey = 'id',
                   |  type = 'cow',
                   |  preCombineField = 'ts',
                   |  'hoodie.keep.min.commits' = '3',
                   |  'hoodie.keep.max.commits' = '5',
                   |  'hoodie.cleaner.commits.retained' = '2',
                   |  'hoodie.archive.automatic' = 'true'
                   | )
       """.stripMargin)

            // Generate initial commits
            spark.sql(s"insert into $tableName values(1, 'alice', 100.0, 1000)")
            spark.sql(s"insert into $tableName values(2, 'bob', 200.0, 2000)")
            spark.sql(s"insert into $tableName values(3, 'charlie', 300.0, 3000)")

            // Update operations to create more timeline entries
            spark.sql(s"update $tableName set price = 110.0 where id = 1")
            spark.sql(s"update $tableName set name = 'robert' where id = 2")

            // More commits to trigger archival
            spark.sql(s"insert into $tableName values(4, 'david', 400.0, 4000)")
            spark.sql(s"insert into $tableName values(5, 'eve', 500.0, 5000)")

            // Delete operation
            spark.sql(s"delete from $tableName where id = 3")

            // Additional commits to exceed max commits threshold and trigger archival
            spark.sql(s"insert into $tableName values(6, 'frank', 600.0, 6000)")
            spark.sql(s"update $tableName set price = price * 1.1 where id > 4")
            spark.sql(s"insert into $tableName values(7, 'grace', 700.0, 7000)")

            // Verify data correctness after all operations
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
                Seq(1, "alice", 110.0, 1000),
                Seq(2, "robert", 200.0, 2000),
                Seq(4, "david", 400.0, 4000),
                Seq(5, "eve", 550.0, 5000),
                Seq(6, "frank", 660.0, 6000),
                Seq(7, "grace", 700.0, 7000)
            )

            // Verify timeline archival occurred
            val metaClient = HoodieTableMetaClient.builder()
                    .setConf(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
                    .setBasePath(tablePath)
                    .build()

            val timeline = metaClient.getActiveTimeline
            val archivedTimeline = metaClient.getArchivedTimeline

            // Check that archived timeline exists and has entries
            assertResult(true)(archivedTimeline.reload().countInstants() > 0)

            // Verify archived timeline files exist in the .hoodie/timeline/history directory
            val archivedDir = new File(tablePath, ".hoodie/timeline/history")
            assertResult(true)(archivedDir.exists() && archivedDir.listFiles().nonEmpty)

            // Check that archived files are parquet format
            val archivedFiles = archivedDir.listFiles().filter(_.getName.endsWith(".parquet"))
            assertResult(true)(archivedFiles.nonEmpty)

            println(s"Active timeline instants: ${timeline.countInstants()}")
            println(s"Archived timeline instants: ${archivedTimeline.reload().countInstants()}")
            println(s"Archived files: ${archivedFiles.map(_.getName).mkString(", ")}")
        })
    }
}
```
