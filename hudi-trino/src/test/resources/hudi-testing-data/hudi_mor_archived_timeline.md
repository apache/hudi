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
- MOR table in table version 8 with an archived timeline
- Using Hudi 1.0.2
- Non-partitioned table

```scala
package org.apache.spark.sql.hudi.timeline

import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase

import java.io.File

class TestCompactedTimelineTable extends HoodieSparkSqlTestBase {

    test("Test MOR Table with Compacted LSM Timeline") {
        withRecordType()(withTempDir { tmp =>
            val tableName = generateTableName
            val tablePath = tmp.getCanonicalPath

            // Create MOR table with timeline archival settings
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
                   |  type = 'mor',
                   |  preCombineField = 'ts',
                   |  'hoodie.keep.min.commits' = '2',
                   |  'hoodie.keep.max.commits' = '4',
                   |  'hoodie.cleaner.commits.retained' = '1',
                   |  'hoodie.archive.automatic' = 'true',
                   |  'hoodie.compact.inline' = 'false'
                   | )
       """.stripMargin)

            // Generate commits and updates to create both base and log files
            spark.sql(s"insert into $tableName values(1, 'user1', 10.0, 1000)")
            spark.sql(s"insert into $tableName values(2, 'user2', 20.0, 2000)")
            spark.sql(s"update $tableName set price = 15.0 where id = 1")
            spark.sql(s"insert into $tableName values(3, 'user3', 30.0, 3000)")
            spark.sql(s"update $tableName set name = 'updated_user2' where id = 2")

            // More operations to trigger archival
            spark.sql(s"insert into $tableName values(4, 'user4', 40.0, 4000)")
            spark.sql(s"delete from $tableName where id = 1")
            spark.sql(s"insert into $tableName values(5, 'user5', 50.0, 5000)")

            // Verify final data state
            checkAnswer(s"select id, name, price, ts from $tableName order by id")(
                Seq(2, "updated_user2", 20.0, 2000),
                Seq(3, "user3", 30.0, 3000),
                Seq(4, "user4", 40.0, 4000),
                Seq(5, "user5", 50.0, 5000)
            )

            // Check archived timeline creation
            val metaClient = HoodieTableMetaClient.builder()
                    .setConf(new HadoopStorageConfiguration(spark.sparkContext.hadoopConfiguration))
                    .setBasePath(tablePath)
                    .build()

            val archivedTimeline = metaClient.getArchivedTimeline
            assertResult(true)(archivedTimeline.reload().countInstants() > 0)

            println(s"MOR Archived timeline instants: ${archivedTimeline.reload().countInstants()}")
        })
    }
}
```
