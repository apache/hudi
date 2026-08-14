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

import org.apache.hudi.DataSourceWriteOptions.{COW_TABLE_TYPE_OPT_VAL, MOR_TABLE_TYPE_OPT_VAL, PARTITIONPATH_FIELD, RECORDKEY_FIELD, SPARK_SQL_INSERT_INTO_OPERATION, TABLE_TYPE}
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.HoodieSparkUtils
import org.apache.hudi.client.WriteClientTestUtils
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.TableSchemaResolver
import org.apache.hudi.common.table.timeline.{HoodieInstant, HoodieTimeline}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata

import java.io.File

class TestInsertTable2 extends HoodieSparkSqlTestBase {

  test("Test Different Type of Partition Column") {
    withTempDir { tmp =>
      val typeAndValue = Seq(
        ("string", "'1000'"),
        ("int", 1000),
        ("bigint", 10000),
        ("timestamp", "TIMESTAMP'2021-05-20 00:00:00'"),
        ("date", "DATE'2021-05-20'")
      )
      typeAndValue.foreach { case (partitionType, partitionValue) =>
        val tableName = generateTableName
        validateDifferentTypesOfPartitionColumn(tmp, partitionType, partitionValue, tableName)
      }
    }
  }

  test("Test TimestampType Partition Column With Consistent Logical Timestamp Enabled") {
    withRecordType()(withTempDir { tmp =>
      val typeAndValue = Seq(
        ("timestamp", "TIMESTAMP'2021-05-20 00:00:00'"),
        ("date", "DATE'2021-05-20'")
      )
      typeAndValue.foreach { case (partitionType, partitionValue) =>
        val tableName = s"${generateTableName}_timestamp_type"
        withSQLConf("hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled" -> "true") {
          validateDifferentTypesOfPartitionColumn(tmp, partitionType, partitionValue, tableName)
        }
      }
    })
  }

  private def validateDifferentTypesOfPartitionColumn(tmp: File, partitionType: String, partitionValue: Any, tableName: String) = {
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt $partitionType
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
    // NOTE: We have to drop type-literal prefix since Spark doesn't parse type literals appropriately
    spark.sql(s"insert into $tableName partition(dt = ${dropTypeLiteralPrefix(partitionValue)}) select 1, 'a1', 10")
    // try again to trigger hoodieFileIndex
    spark.sql(s"insert overwrite $tableName partition(dt = ${dropTypeLiteralPrefix(partitionValue)}) select 1, 'a1', 10")
    spark.sql(s"insert into $tableName select 2, 'a2', 10, $partitionValue")
    checkAnswer(s"select id, name, price, cast(dt as string) from $tableName order by id")(
      Seq(1, "a1", 10, extractRawValue(partitionValue).toString),
      Seq(2, "a2", 10, extractRawValue(partitionValue).toString)
    )
  }

  test("Test insert for uppercase table name") {
    withTempDir { tmp =>
      val tableName = s"H_$generateTableName"
      val conf = if (HoodieSparkUtils.gteqSpark3_5) {
        // [SPARK-44284] Spark 3.5+ requires conf below to be case sensitive
        Map(("spark.sql.caseSensitive" -> "true"))
      } else {
        Map.empty[String, String]
      }
      withSQLConf(conf.toSeq: _*) {
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | location '${tmp.getCanonicalPath}'
       """.stripMargin)

        spark.sql(s"insert into $tableName values(1, 'a1', 10)")
        checkAnswer(s"select id, name, price from $tableName")(
          Seq(1, "a1", 10.0)
        )
        val metaClient = createMetaClient(spark, tmp.getCanonicalPath)
        assertResult(tableName)(metaClient.getTableConfig.getTableName)
      }
    }
  }

  test("Test Insert Exception") {
    val tableName = generateTableName
    spark.sql(
      s"""
         |create table $tableName (
         |  id int,
         |  name string,
         |  price double,
         |  dt string
         |) using hudi
         | tblproperties (primaryKey = 'id')
         | partitioned by (dt)
     """.stripMargin)
    val tooManyDataColumnsErrorMsg = if (HoodieSparkUtils.gteqSpark3_5) {
      s"""
         |[INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS] Cannot write to `spark_catalog`.`default`.`$tableName`, the reason is too many data columns:
         |Table columns: `id`, `name`, `price`.
         |Data columns: `1`, `a1`, `10`, `2021-06-20`.
         |""".stripMargin
    } else if (HoodieSparkUtils.gteqSpark3_4) {
      """
        |too many data columns:
        |Table columns: 'id', 'name', 'price'.
        |Data columns: '1', 'a1', '10', '2021-06-20'.
        |""".stripMargin
    } else {
      """
        |too many data columns:
        |Table columns: 'id', 'name', 'price'
        |Data columns: '1', 'a1', '10', '2021-06-20'
        |""".stripMargin
    }
    checkExceptionContain(s"insert into $tableName partition(dt = '2021-06-20') select 1, 'a1', 10, '2021-06-20'")(
      tooManyDataColumnsErrorMsg)

    val notEnoughDataColumnsErrorMsg = if (HoodieSparkUtils.gteqSpark3_5) {
      s"""
         |[INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS] Cannot write to `spark_catalog`.`default`.`$tableName`, the reason is not enough data columns:
         |Table columns: `id`, `name`, `price`, `dt`.
         |Data columns: `1`, `a1`, `10`.
         |""".stripMargin
    } else if (HoodieSparkUtils.gteqSpark3_4) {
      """
        |not enough data columns:
        |Table columns: 'id', 'name', 'price', 'dt'.
        |Data columns: '1', 'a1', '10'.
        |""".stripMargin
    } else {
      """
        |not enough data columns:
        |Table columns: 'id', 'name', 'price', 'dt'
        |Data columns: '1', 'a1', '10'
        |""".stripMargin
    }
    checkExceptionContain(s"insert into $tableName select 1, 'a1', 10")(notEnoughDataColumnsErrorMsg)
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "true", "hoodie.sql.insert.mode" -> "strict") {
      val tableName2 = generateTableName
      spark.sql(
        s"""
           |create table $tableName2 (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (
           |   primaryKey = 'id',
           |   preCombineField = 'ts'
           | )
        """.stripMargin)
      checkException(s"insert into $tableName2 values(1, 'a1', 10, 1000)")(
        "Table with primaryKey can not use bulk insert in strict mode."
      )
    }
  }

  test("Test Insert timestamp when 'spark.sql.datetime.java8API.enabled' enabled") {
    withRecordType() {
      withSQLConf("spark.sql.datetime.java8API.enabled" -> "true") {
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  dt timestamp
             |)
             |using hudi
             |partitioned by(dt)
             |options(type = 'cow', primaryKey = 'id')
             |""".stripMargin
        )

        spark.sql(s"insert into $tableName values (1, 'a1', 10, cast('2021-05-07 00:00:00' as timestamp))")
        checkAnswer(s"select id, name, price, cast(dt as string) from $tableName")(
          Seq(1, "a1", 10, "2021-05-07 00:00:00")
        )
      }
    }
  }

  test("Test bulk insert with insert into for single partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { tableName =>
            spark.sql(
              s"""
                 |create table $tableName (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)
            withSQLConf(
              "hoodie.datasource.write.insert.drop.duplicates" -> "false",
              // Enable the bulk insert
              "hoodie.sql.bulk.insert.enable" -> "true") {
              spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

              assertResult(WriteOperationType.BULK_INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
              }
              checkAnswer(s"select id, name, price, dt from $tableName")(
                Seq(1, "a1", 10.0, "2021-07-18")
              )
            }
            // Disable the bulk insert
            withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
              spark.sql(s"insert into $tableName values(2, 'a2', 10, '2021-07-18')")

              assertResult(WriteOperationType.INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
              }
              checkAnswer(s"select id, name, price, dt from $tableName order by id")(
                Seq(1, "a1", 10.0, "2021-07-18"),
                Seq(2, "a2", 10.0, "2021-07-18")
              )
            }
          }
        }
      }
    }
  }

  test("Test bulk insert with insert into for multi partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(s"${generateTableName}_multi_partition") { tableMultiPartition =>
            spark.sql(
              s"""
                 |create table $tableMultiPartition (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string,
                 |  hh string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt, hh)
                 | location '${tmp.getCanonicalPath}/$tableMultiPartition'
         """.stripMargin)

            // Enable the bulk insert
            withSQLConf("hoodie.sql.bulk.insert.enable" -> "true") {
              spark.sql(s"insert into $tableMultiPartition values(1, 'a1', 10, '2021-07-18', '12')")

              checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition")(
                Seq(1, "a1", 10.0, "2021-07-18", "12")
              )
              assertResult(WriteOperationType.BULK_INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableMultiPartition").getOperationType
              }
            }
            // Disable the bulk insert
            withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
              spark.sql(s"insert into $tableMultiPartition " +
                s"values(2, 'a2', 10, '2021-07-18','12')")

              checkAnswer(s"select id, name, price, dt, hh from $tableMultiPartition order by id")(
                Seq(1, "a1", 10.0, "2021-07-18", "12"),
                Seq(2, "a2", 10.0, "2021-07-18", "12")
              )
              assertResult(WriteOperationType.INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableMultiPartition").getOperationType
              }
            }
          }
        }
      })
    }
  }

  test("Test bulk insert with insert into for non partitioned table") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict",
      "hoodie.sql.bulk.insert.enable" -> "true") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { nonPartitionedTable =>
            spark.sql(
              s"""
                 |create table $nonPartitionedTable (
                 |  id int,
                 |  name string,
                 |  price double
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | location '${tmp.getCanonicalPath}/$nonPartitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")
            checkAnswer(s"select id, name, price from $nonPartitionedTable")(
              Seq(1, "a1", 10.0)
            )
            assertResult(WriteOperationType.BULK_INSERT) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$nonPartitionedTable").getOperationType
            }
          }
        }
      })
    }
  }

  test("Test bulk insert with CTAS") {
    withSQLConf("hoodie.sql.insert.mode" -> "non-strict",
      "hoodie.sql.bulk.insert.enable" -> "true") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { inputTable =>
            spark.sql(
              s"""
                 |create table $inputTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$inputTable'
         """.stripMargin)
            spark.sql(s"insert into $inputTable values(1, 'a1', 10, '2021-07-18')")

            withTable(generateTableName) { target =>
              spark.sql(
                s"""
                   |create table $target
                   |using hudi
                   |tblproperties(
                   | type = '$tableType',
                   | primaryKey = 'id'
                   |)
                   | location '${tmp.getCanonicalPath}/$target'
                   | as
                   | select * from $inputTable
                   |""".stripMargin)
              checkAnswer(s"select id, name, price, dt from $target order by id")(
                Seq(1, "a1", 10.0, "2021-07-18")
              )
              assertResult(WriteOperationType.BULK_INSERT) {
                getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$target").getOperationType
              }
            }
          }
        }
      }
    }
  }

  test("Test combine before bulk insert") {
    withTempDir { tmp => {
      Seq(true, false).foreach { combineConfig => {
        Seq(true, false).foreach { populateMetaConfig => {
          import spark.implicits._

          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"

          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  value double,
               |  ts long
               |) using hudi
               | location '${tablePath}'
               | tblproperties (
               |  primaryKey = 'id',
               |  preCombineField = 'ts'
               | )
               |""".stripMargin)

          val rowsWithSimilarKey = Seq((1, "a1", 10.0, 1000L), (1, "a1", 11.0, 1001L))
          rowsWithSimilarKey.toDF("id", "name", "value", "ts")
            .write.format("hudi")
            // common settings
            .option(HoodieWriteConfig.TBL_NAME.key, tableName)
            .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
            .option(RECORDKEY_FIELD.key, "id")
            .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
            .option(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key, "1")
            // test specific settings
            .option(DataSourceWriteOptions.OPERATION.key, WriteOperationType.BULK_INSERT.value)
            .option(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key, combineConfig.toString)
            .option(HoodieTableConfig.POPULATE_META_FIELDS.key, populateMetaConfig.toString)
            .mode(SaveMode.Append)
            .save(tablePath)

          val countRows = spark.sql(s"select id, name, value, ts from $tableName").count()
          if (combineConfig) {
            assertResult(1)(countRows)
          } else {
            assertResult(rowsWithSimilarKey.length)(countRows)
          }
        }
        }
      }
      }
    }
    }
  }

  test("Test bulk insert with empty dataset") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { inputTable =>
            spark.sql(
              s"""
                 |create table $inputTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$inputTable'
         """.stripMargin)

            // insert empty dataset into target table
            withTable(generateTableName) { target =>
              spark.sql(
                s"""
                   |create table $target
                   |using hudi
                   |tblproperties(
                   | type = '$tableType',
                   | primaryKey = 'id'
                   |)
                   | location '${tmp.getCanonicalPath}/$target'
                   | as
                   | select * from $inputTable where id = 2
                   |""".stripMargin)
              // check the target table is empty
              checkAnswer(s"select id, name, price, dt from $target order by id")(Seq.empty: _*)
            }
          }
        }
      }
    }
  }

  test("Test bulk insert overwrite with rollback") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "bulk_insert") {
      withTempDir { tmp =>
        withTable(generateTableName) { tableName =>
          val tableLocation = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  dt int
               |) using hudi
               | tblproperties (
               |  type = 'cow',
               |  primaryKey = 'id'
               | ) partitioned by (dt)
               | location '$tableLocation'
               | """.stripMargin)

          spark.sql(s"insert overwrite table $tableName partition (dt) values(1, 'a1', 10)")
          checkAnswer(s"select id, name, dt from $tableName order by id")(
            Seq(1, "a1", 10)
          )

          // Simulate a insert overwrite failure
          val metaClient = createMetaClient(spark, tableLocation)
          val instant = WriteClientTestUtils.createNewInstantTime()
          val timeline = HoodieTestUtils.TIMELINE_FACTORY.createActiveTimeline(metaClient)
          timeline.createNewInstant(metaClient.createNewInstant(
            HoodieInstant.State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, instant))
          timeline.createNewInstant(metaClient.createNewInstant(
            HoodieInstant.State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, instant))

          spark.sql(s"insert overwrite table $tableName partition (dt) values(1, 'a1', 10)")
          checkAnswer(s"select id, name, dt from $tableName order by id")(
            Seq(1, "a1", 10)
          )
        }
      }
    }
  }

  test("Test insert overwrite partitions with empty dataset") {
    Seq(true, false).foreach { enableBulkInsert =>
      val bulkInsertConf: Array[(String, String)] = if (enableBulkInsert) {
        Array(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value())
      } else {
        Array()
      }
      withSQLConf(bulkInsertConf: _*) {
        withTempDir { tmp =>
          Seq("cow", "mor").foreach { tableType =>
            withTable(generateTableName) { inputTable =>
              spark.sql(
                s"""
                   |create table $inputTable (
                   |  id int,
                   |  name string,
                   |  price double,
                   |  dt string
                   |) using hudi
                   | tblproperties (
                   |  type = '$tableType',
                   |  primaryKey = 'id'
                   | )
                   | partitioned by (dt)
                   | location '${tmp.getCanonicalPath}/$inputTable'
              """.stripMargin)

              withTable(generateTableName) { target =>
                spark.sql(
                  s"""
                     |create table $target (
                     |  id int,
                     |  name string,
                     |  price double,
                     |  dt string
                     |) using hudi
                     | tblproperties (
                     |  type = '$tableType',
                     |  primaryKey = 'id'
                     | )
                     | partitioned by (dt)
                     | location '${tmp.getCanonicalPath}/$target'
              """.stripMargin)
                spark.sql(s"insert into $target values(3, 'c1', 13, '2021-07-17')")
                spark.sql(s"insert into $target values(1, 'a1', 10, '2021-07-18')")

                // Insert overwrite a partition with empty record
                spark.sql(s"insert overwrite table $target partition(dt='2021-07-17') select id, name, price from $inputTable")
                checkAnswer(s"select id, name, price, dt from $target where dt='2021-07-17'")(Seq.empty: _*)
              }
            }
          }
        }
      }
    }
  }

  test("Test bulk insert with insert overwrite table") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { nonPartitionedTable =>
            spark.sql(
              s"""
                 |create table $nonPartitionedTable (
                 |  id int,
                 |  name string,
                 |  price double
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | location '${tmp.getCanonicalPath}/$nonPartitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $nonPartitionedTable values(1, 'a1', 10)")

            spark.sql(s"insert overwrite table $nonPartitionedTable values(2, 'b1', 11)")
            checkAnswer(s"select id, name, price from $nonPartitionedTable order by id")(
              Seq(2, "b1", 11.0)
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE_TABLE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$nonPartitionedTable").getOperationType
            }
          }
        }
      }
    }
  }

  test("Test bulk insert with insert overwrite partition") {
    withSQLConf(SPARK_SQL_INSERT_INTO_OPERATION.key -> WriteOperationType.BULK_INSERT.value()) {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { partitionedTable =>
            spark.sql(
              s"""
                 |create table $partitionedTable (
                 |  id int,
                 |  name string,
                 |  price double,
                 |  dt string
                 |) using hudi
                 | tblproperties (
                 |  type = '$tableType',
                 |  primaryKey = 'id'
                 | )
                 | partitioned by (dt)
                 | location '${tmp.getCanonicalPath}/$partitionedTable'
         """.stripMargin)
            spark.sql(s"insert into $partitionedTable values(3, 'c1', 13, '2021-07-17')")
            spark.sql(s"insert into $partitionedTable values(1, 'a1', 10, '2021-07-18')")

            // Insert overwrite a partition
            spark.sql(s"insert overwrite table $partitionedTable partition(dt='2021-07-17') values(2, 'b1', 11)")
            checkAnswer(s"select id, name, price, dt from $partitionedTable order by id")(
              Seq(1, "a1", 10.0, "2021-07-18"),
              Seq(2, "b1", 11.0, "2021-07-17")
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$partitionedTable").getOperationType
            }

            // Insert overwrite whole table
            spark.sql(s"insert overwrite table $partitionedTable values(4, 'd1', 14, '2021-07-19')")
            checkAnswer(s"select id, name, price, dt from $partitionedTable order by id")(
              Seq(4, "d1", 14.0, "2021-07-19")
            )
            assertResult(WriteOperationType.INSERT_OVERWRITE_TABLE) {
              getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$partitionedTable").getOperationType
            }
          }
        }
      }
    }
  }

  test("Test combine before insert") {
    Seq("cow", "mor").foreach { tableType =>
      withSQLConf("hoodie.sql.bulk.insert.enable" -> "false", "hoodie.merge.allow.duplicate.on.inserts" -> "false",
        "hoodie.combine.before.insert" -> "true") {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey = 'id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)
          spark.sql(
            s"""
               |insert overwrite table $tableName
               |select * from (
               | select 1 as id, 'a1' as name, 10 as price, 1000 as ts
               | union all
               | select 1 as id, 'a1' as name, 11 as price, 1001 as ts
               | )
               |""".stripMargin
          )
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 11.0, 1001)
          )
        })
      }
    }
  }

  test("Test insert pk-table") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    Seq("cow", "mor").foreach { tableType =>
      withSQLConf("hoodie.sql.bulk.insert.enable" -> "false", "hoodie.spark.sql.insert.into.operation" -> "upsert") {
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey = 'id',
               |  type = '$tableType',
               |  preCombineField = 'ts'
               | )
       """.stripMargin)
          spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName values(1, 'a1', 11, 1000)")
          checkAnswer(s"select id, name, price, ts from $tableName")(
            Seq(1, "a1", 11.0, 1000)
          )
        })
      }
    }
  }

  test("Test For read operation's field") {
    withRecordType()(withTempDir { tmp => {
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      import spark.implicits._
      val day = "2021-08-02"
      val df = Seq((1, "a1", 10, 1000, day, 12)).toDF("id", "name", "value", "ts", "day", "hh")
      // Write a table by spark dataframe.
      df.write.format("hudi")
        .option(HoodieWriteConfig.TBL_NAME.key, tableName)
        .option(TABLE_TYPE.key, MOR_TABLE_TYPE_OPT_VAL)
        .option(RECORDKEY_FIELD.key, "id")
        .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
        .option(PARTITIONPATH_FIELD.key, "day,hh")
        .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
        .option(HoodieWriteConfig.ALLOW_OPERATION_METADATA_FIELD.key, "true")
        .mode(SaveMode.Overwrite)
        .save(tablePath)

      val metaClient = createMetaClient(spark, tablePath)

      assertResult(true)(new TableSchemaResolver(metaClient).hasOperationField)

      spark.sql(
        s"""
           |create table $tableName using hudi
           |location '${tablePath}'
           |""".stripMargin)

      // Note: spark sql batch write currently does not write actual content to the operation field
      checkAnswer(s"select id, _hoodie_operation from $tableName")(
        Seq(1, null)
      )
    }
    })
  }

  test("Test enable hoodie.datasource.write.drop.partition.columns when write") {
    withSQLConf("hoodie.sql.bulk.insert.enable" -> "false") {
      Seq("mor", "cow").foreach { tableType =>
        withRecordType()(withTempDir { tmp =>
          val tableName = generateTableName
          spark.sql(
            s"""
               | create table $tableName (
               |  id int,
               |  name string,
               |  price double,
               |  ts long,
               |  dt string
               | ) using hudi
               | partitioned by (dt)
               | location '${tmp.getCanonicalPath}/$tableName'
               | tblproperties (
               |  primaryKey = 'id',
               |  preCombineField = 'ts',
               |  type = '$tableType',
               |  hoodie.datasource.write.drop.partition.columns = 'true'
               | )
       """.stripMargin)
          spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (1, 'a1', 10, 1000)")
          spark.sql(s"insert into $tableName partition(dt='2021-12-25') values (2, 'a2', 20, 1000)")
          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1", 10, 1000, "2021-12-25"),
            Seq(2, "a2", 20, 1000, "2021-12-25")
          )
        })
      }
    }
  }

  test("Test nested field as primaryKey and preCombineField") {
    withSQLConf("hoodie.spark.sql.insert.into.operation" -> "upsert") {
      withRecordType()(withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          val tableName = generateTableName
          // create table
          spark.sql(
            s"""
               |create table $tableName (
               |  name string,
               |  price double,
               |  ts long,
               |  nestedcol struct<a1:string, a2:struct<b1:string, b2:struct<c1:string, c2:int>>>
               |) using hudi
               | location '${tmp.getCanonicalPath}/$tableName'
               | options (
               |  type = '$tableType',
               |  primaryKey = 'nestedcol.a1',
               |  preCombineField = 'nestedcol.a2.b2.c2'
               | )
         """.stripMargin)
          // insert data to table
          spark.sql(
            s"""insert into $tableName values
               |('name_1', 10, 1000, struct('a', struct('b', struct('c', 999)))),
               |('name_2', 20, 2000, struct('a', struct('b', struct('c', 333))))
               |""".stripMargin)
          checkAnswer(s"select name, price, ts, nestedcol.a1, nestedcol.a2.b2.c2 from $tableName")(
            Seq("name_1", 10.0, 1000, "a", 999)
          )
        }
      })
    }
  }
}
