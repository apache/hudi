/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hudi.dml.insert

import org.apache.hudi.sync.common.HoodieSyncTool

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.util.Utils

import java.io.File
import java.util.Properties

class TestInsertTableWithHiveSupport extends HoodieSparkSqlTestBase {

  val metastoreDerbyLocation = "/tmp/hive_metastore_db"

  override lazy val spark: SparkSession = SparkSession.builder()
    .config("spark.sql.warehouse.dir", sparkWareHouse.getCanonicalPath)
    .config("spark.sql.session.timeZone", "UTC")
    .config("hoodie.insert.shuffle.parallelism", "4")
    .config("hoodie.upsert.shuffle.parallelism", "4")
    .config("hoodie.delete.shuffle.parallelism", "4")
    .config("hoodie.datasource.hive_sync.enable", "false")
    .config("hoodie.datasource.meta.sync.enable", "false")
    .config("hoodie.meta.sync.client.tool.class", classOf[DummySyncTool].getName)
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", s"jdbc:derby:$metastoreDerbyLocation;create=true")
    .config(sparkConf())
    .enableHiveSupport()
    .getOrCreate()

  override def afterAll(): Unit = {
    // Clean up metastore derby location
    Utils.deleteRecursively(new File(metastoreDerbyLocation))
    super.afterAll()
  }

  test("Test Insert Into with multi partition") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        // Create a partitioned table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  dt string,
             |  name string,
             |  price double,
             |  ht string,
             |  ts long
             |) using hudi
             | tblproperties (primaryKey = 'id', type = '$tableType')
             | partitioned by (dt, ht)
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName partition(dt, ht)
             | select 1 as id, 'a1' as name, 10 as price,'20210101' as dt, 1000 as ts, '01' as ht
              """.stripMargin)

        spark.sql(
          s"""
             | insert into $tableName partition(dt = '20210102', ht)
             | select 2 as id, 'a2' as name, 20 as price, 2000 as ts, '02' as ht
              """.stripMargin)

        checkAnswer(s"select id, name, price, ts, dt, ht from $tableName")(
          Seq(1, "a1", 10.0, 1000, "20210101", "01"),
          Seq(2, "a2", 20.0, 2000, "20210102", "02"))
      }
    }
  }

  test("Test Insert Overwrite") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          // Create a partitioned table
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  dt string
               |) using hudi
               | tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id'
               | )
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}/$tableName'
          """.stripMargin)

          //  Insert into table
          spark.sql(
            s"""
               | insert into $tableName values
               | (1,'a1',10,1000,'2021-01-05'),
               | (2,'a2',10,1000,'2021-01-06')
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a2", 10.0, 1000, "2021-01-06")
          )


          // First respect hoodie.datasource.write.operation, if not set then respect hoodie.datasource.overwrite.mode,
          // If the previous two config both not set, then respect spark.sql.sources.partitionOverwriteMode
          spark.sql(
            s"""
               | insert overwrite table $tableName values
               | (3,'a3',10,1000,'2021-01-06'),
               | (4,'a4',10,1000,'2021-01-07')
          """.stripMargin)
          // As hoodie.datasource.write.operation and hoodie.datasource.overwrite.mode both not set, respect
          // spark.sql.sources.partitionOverwriteMode and it's default behavior is static,so insert overwrite whole table
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(3, "a3", 10.0, 1000, "2021-01-06"),
            Seq(4, "a4", 10.0, 1000, "2021-01-07")
          )

          withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "dynamic") {
            spark.sql(
              s"""
                 | insert overwrite table $tableName values
                 | (5,'a5',10,1000,'2021-01-07')
          """.stripMargin)
          }
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by id")(
            Seq(3, "a3", 10.0, 1000, "2021-01-06"),
            Seq(5, "a5", 10.0, 1000, "2021-01-07")
          )
          // Insert overwrite partitioned table with the PARTITION clause will always insert overwrite the specific
          // partition regardless of static or dynamic mode
          spark.sql(
            s"""
               | insert overwrite table $tableName partition(dt = '2021-01-06')
               | select * from (select 6 , 'a6', 10, 1000) limit 10
          """.stripMargin)
          checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
            Seq(6, "a6", 10.0, 1000, "2021-01-06"),
            Seq(5, "a5", 10.0, 1000, "2021-01-07")
          )

          withSQLConf("spark.sql.sources.partitionOverwriteMode" -> "static",
            "hoodie.datasource.overwrite.mode" -> "dynamic"
          ) {
            spark.sql(
              s"""
                 | insert overwrite table $tableName values
                 | (7,'a7',10,1000,'2021-01-07')
          """.stripMargin)
            // Config hoodie.datasource.overwrite.mode takes precedence over spark.sql.sources.partitionOverwriteMode
            checkAnswer(s"select id, name, price, ts, dt from $tableName order by dt")(
              Seq(6, "a6", 10.0, 1000, "2021-01-06"),
              Seq(7, "a7", 10.0, 1000, "2021-01-07")
            )
          }
        }
      }
    }
  }
}

class DummySyncTool(props: Properties, hadoopConf: Configuration) extends HoodieSyncTool(props, hadoopConf) {
  override def syncHoodieTable(): Unit = {
    // do nothing here
  }
}
