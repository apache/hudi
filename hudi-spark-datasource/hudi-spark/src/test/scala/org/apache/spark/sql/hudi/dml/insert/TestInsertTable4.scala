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

import org.apache.hudi.DataSourceWriteOptions.{DROP_INSERT_DUP_POLICY, ENABLE_ROW_WRITER, FAIL_INSERT_DUP_POLICY, INSERT_DROP_DUPS, INSERT_DUP_POLICY, NONE_INSERT_DUP_POLICY, RECORDKEY_FIELD, SPARK_SQL_INSERT_INTO_OPERATION}
import org.apache.hudi.{DataSourceWriteOptions, HoodieCLIUtils}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType
import org.apache.hudi.common.model.WriteOperationType
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.config.{HoodieClusteringConfig, HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.exception.{HoodieDuplicateKeyException, HoodieException}
import org.apache.hudi.index.HoodieIndex.IndexType

import org.apache.spark.scheduler.{SparkListener, SparkListenerStageSubmitted}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.getLastCommitMetadata

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

class TestInsertTable4 extends HoodieSparkSqlTestBase {
  test("Test Bulk Insert Into Consistent Hashing Bucket Index Table") {
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    withSQLConf("hoodie.datasource.write.operation" -> "bulk_insert") {
      Seq("false", "true").foreach { bulkInsertAsRow =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val basePath = s"${tmp.getCanonicalPath}/$tableName"
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
               | primaryKey = 'id,name',
               | type = 'mor',
               | preCombineField = 'ts',
               | hoodie.index.type = 'BUCKET',
               | hoodie.index.bucket.engine = 'CONSISTENT_HASHING',
               | hoodie.bucket.index.hash.field = 'id,name',
               | hoodie.datasource.write.row.writer.enable = '$bulkInsertAsRow'
               | )
               | partitioned by (dt)
               | location '${basePath}'
           """.stripMargin)

          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 10, 1000, "2021-01-05"),
               | (2, 'a1,2', 10, 1000, "2021-01-05"),
               | (1, 'a2,1', 20, 2000, "2021-01-06"),
               | (2, 'a2,2', 20, 2000, "2021-01-06"),
               | (1, 'a3,1', 30, 3000, "2021-01-07"),
               | (2, 'a3,2', 30, 3000, "2021-01-07")
            """.stripMargin)

          checkAnswer(s"select id, name, price, ts, dt from $tableName")(
            Seq(1, "a1,1", 10.0, 1000, "2021-01-05"),
            Seq(2, "a1,2", 10.0, 1000, "2021-01-05"),
            Seq(1, "a2,1", 20.0, 2000, "2021-01-06"),
            Seq(2, "a2,2", 20.0, 2000, "2021-01-06"),
            Seq(1, "a3,1", 30.0, 3000, "2021-01-07"),
            Seq(2, "a3,2", 30.0, 3000, "2021-01-07")
          )

          // there are six files in the table, each partition contains two files
          checkAnswer(s"select count(distinct _hoodie_file_name) from $tableName")(
            Seq(6)
          )

          val insertStatement =
            s"""
               | insert into $tableName values
               | (1, 'a1,1', 11, 1000, "2021-01-05"),
               | (2, 'a1,2', 11, 1000, "2021-01-05"),
               | (3, 'a2,1', 21, 2000, "2021-01-05"),
               | (4, 'a2,2', 21, 2000, "2021-01-05"),
               | (5, 'a3,1', 31, 3000, "2021-01-05"),
               | (6, 'a3,2', 31, 3000, "2021-01-05")
            """.stripMargin

          // We can only upsert to existing consistent hashing bucket index table
          checkExceptionContain(insertStatement)("Consistent Hashing bulk_insert only support write to new file group")

          spark.sql("set hoodie.datasource.write.operation = upsert")
          spark.sql(insertStatement)

          checkAnswer(s"select id, name, price, ts, dt from $tableName where dt = '2021-01-05'")(
            Seq(1, "a1,1", 11.0, 1000, "2021-01-05"),
            Seq(2, "a1,2", 11.0, 1000, "2021-01-05"),
            Seq(3, "a2,1", 21.0, 2000, "2021-01-05"),
            Seq(4, "a2,2", 21.0, 2000, "2021-01-05"),
            Seq(5, "a3,1", 31.0, 3000, "2021-01-05"),
            Seq(6, "a3,2", 31.0, 3000, "2021-01-05")
          )

          val clusteringOptions: Map[String, String] = Map(
            s"${RECORDKEY_FIELD.key()}" -> "id,name",
            s"${ENABLE_ROW_WRITER.key()}" -> s"${bulkInsertAsRow}",
            s"${HoodieIndexConfig.INDEX_TYPE.key()}" -> s"${IndexType.BUCKET.name()}",
            s"${HoodieIndexConfig.BUCKET_INDEX_HASH_FIELD.key()}" -> "id,name",
            s"${HoodieIndexConfig.BUCKET_INDEX_ENGINE_TYPE.key()}" -> "CONSISTENT_HASHING",
            s"${HoodieClusteringConfig.PLAN_STRATEGY_CLASS_NAME.key()}" -> "org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy",
            s"${HoodieClusteringConfig.EXECUTION_STRATEGY_CLASS_NAME.key()}" -> "org.apache.hudi.client.clustering.run.strategy.SparkConsistentBucketClusteringExecutionStrategy"
          )

          val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, clusteringOptions, Option(tableName))

          // Test bucket merge by clustering
          val instant = client.scheduleClustering(HOption.empty()).get()

          checkAnswer(s"call show_clustering(table => '$tableName')")(
            Seq(instant, 10, HoodieInstant.State.REQUESTED.name(), "*")
          )

          client.cluster(instant)

          checkAnswer(s"call show_clustering(table => '$tableName')")(
            Seq(instant, 10, HoodieInstant.State.COMPLETED.name(), "*")
          )

          spark.sql("set hoodie.datasource.write.operation = bulk_insert")
        }
      }
    }
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  /**
   * When no explicit configs are set, sql write operation is deduced as INSERT
   * which preserves duplicates by default.
   */
  test("Test sql write operation with INSERT_INTO No explicit configs") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          ingestAndValidateData(tableType, tableName, tmp, WriteOperationType.INSERT)
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override both strict mode and sql write operation") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(WriteOperationType.INSERT, WriteOperationType.BULK_INSERT, WriteOperationType.UPSERT).foreach { operation =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateData(tableType, tableName, tmp, operation,
              List("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + " = " + operation.value(), "set hoodie.sql.insert.mode = upsert"))
          }
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override only sql write operation") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        Seq(WriteOperationType.INSERT, WriteOperationType.BULK_INSERT, WriteOperationType.UPSERT).foreach { operation =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateData(tableType, tableName, tmp, operation,
              List("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + " = " + operation.value()))
          }
        }
      }
    }
  }

  test("Test sql write operation with INSERT_INTO override only strict mode") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf(DataSourceWriteOptions.INSERT_DUP_POLICY.key())
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    spark.sessionState.conf.unsetConf("hoodie.sql.bulk.insert.enable")
    withSQLConf(HoodieWriteConfig.COMBINE_BEFORE_INSERT.key() -> "true") {
      withTempDir { tmp =>
        Seq("cow", "mor").foreach { tableType =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateData(tableType, tableName, tmp, WriteOperationType.UPSERT,
              List("set hoodie.sql.insert.mode = upsert"))
          }
        }
      }
    }
  }

  def ingestAndValidateData(tableType: String, tableName: String, tmp: File,
                            expectedOperationtype: WriteOperationType,
                            setOptions: List[String] = List.empty) : Unit = {
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

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
         |  primaryKey = 'id',
         |  preCombine = 'name'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    // insert record again but w/ diff values but same primary key.
    spark.sql(
      s"""
         | insert into $tableName values
         | (1, 'a1_1', 10, "2021-07-18"),
         | (2, 'a2', 20, "2021-07-18"),
         | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    // Check if INSERT operation was explicitly set in setOptions
    val isExplicitInsertOperation = setOptions.exists(option =>
      option.toLowerCase.contains("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + " = insert")
    )

    if (expectedOperationtype == WriteOperationType.UPSERT) {
      // dedup should happen within same batch being ingested and existing records on storage should get updated
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    } else if (isExplicitInsertOperation) {
      // duplications are retained as INSERT is explicitly set
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "2021-07-18"),
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2", 20.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    } else {
      // no dedup across batches or within same batch - INSERT preserves all duplicates
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "2021-07-18"),
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2", 20.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  test("Test sql write operation with INSERT_INTO No explicit configs No Precombine") {
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataNoPrecombine(tableType, tableName, tmp, WriteOperationType.INSERT)
        }
      }
    }
  }

  var listenerCallCount: Int = 0
  var countDownLatch: CountDownLatch = _

  // add a listener for stages for parallelism checking with stage name
  class StageParallelismListener(var stageName: String) extends SparkListener {
    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
      if (stageSubmitted.stageInfo.name.contains(stageName)) {
        assertResult(1)(stageSubmitted.stageInfo.numTasks)
        listenerCallCount = listenerCallCount + 1
        countDownLatch.countDown
      }
    }
  }

  test("Test multiple partition fields pruning") {

    withTempDir { tmp =>
      val targetTable = generateTableName
      countDownLatch = new CountDownLatch(1)
      listenerCallCount = 0
      spark.sql(
        s"""
           |create table ${targetTable} (
           |  `id` string,
           |  `name` string,
           |  `dt` bigint,
           |  `day` STRING,
           |  `hour` INT
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'mor',
           |  'preCombineField'='dt',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.hash.field' = 'id',
           |  'hoodie.bucket.index.num.buckets'=512,
           |  'hoodie.metadata.enable'='false'
           | )
           |partitioned by (`day`,`hour`)
           |location '${tmp.getCanonicalPath}/$targetTable'
           |""".stripMargin)
      spark.sql(
        s"""
           |insert into ${targetTable}
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 10 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 11 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 12 as `hour`
           |""".stripMargin)
      val stageClassName = classOf[HoodieSparkEngineContext].getSimpleName
      spark.sparkContext.addSparkListener(new StageParallelismListener(stageName = stageClassName))
      val df = spark.sql(
        s"""
           |select * from ${targetTable} where day='2023-10-12' and hour=11
           |""".stripMargin)
      var rddHead = df.rdd
      while (rddHead.dependencies.size > 0) {
        assertResult(1)(rddHead.partitions.size)
        rddHead = rddHead.firstParent
      }
      assertResult(1)(rddHead.partitions.size)
      countDownLatch.await(1, TimeUnit.MINUTES)
      assert(listenerCallCount >= 1)
    }
  }

  test("Test single partiton field pruning") {

    withTempDir { tmp =>
      countDownLatch = new CountDownLatch(1)
      listenerCallCount = 0
      val targetTable = generateTableName
      spark.sql(
        s"""
           |create table ${targetTable} (
           |  `id` string,
           |  `name` string,
           |  `dt` bigint,
           |  `day` STRING,
           |  `hour` INT
           |) using hudi
           |tblproperties (
           |  'primaryKey' = 'id',
           |  'type' = 'mor',
           |  'preCombineField'='dt',
           |  'hoodie.index.type' = 'BUCKET',
           |  'hoodie.bucket.index.hash.field' = 'id',
           |  'hoodie.bucket.index.num.buckets'=512,
           |  'hoodie.metadata.enable'='false'
           | )
           |partitioned by (`day`)
           |location '${tmp.getCanonicalPath}/$targetTable'
           |""".stripMargin)
      spark.sql(
        s"""
           |insert into ${targetTable}
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 10 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 11 as `hour`
           |union
           |select '1' as id, 'aa' as name, 123 as dt, '2023-10-12' as `day`, 12 as `hour`
           |""".stripMargin)
      val stageClassName = classOf[HoodieSparkEngineContext].getSimpleName
      spark.sparkContext.addSparkListener(new StageParallelismListener(stageName = stageClassName))
      val df = spark.sql(
        s"""
           |select * from ${targetTable} where day='2023-10-12' and hour=11
           |""".stripMargin)
      var rddHead = df.rdd
      while (rddHead.dependencies.size > 0) {
        assertResult(1)(rddHead.partitions.size)
        rddHead = rddHead.firstParent
      }
      assertResult(1)(rddHead.partitions.size)
      countDownLatch.await(1, TimeUnit.MINUTES)
      assert(listenerCallCount >= 1)
    }
  }

  test("Test inaccurate index type") {
    withTempDir { tmp =>
      val targetTable = generateTableName

      assertThrows[IllegalArgumentException] {
        try {
          spark.sql(
            s"""
               |create table ${targetTable} (
               |  `id` string,
               |  `name` string,
               |  `dt` bigint,
               |  `day` STRING,
               |  `hour` INT
               |) using hudi
               |OPTIONS ('hoodie.datasource.write.hive_style_partitioning' 'false', 'hoodie.datasource.meta.sync.enable' 'false', 'hoodie.datasource.hive_sync.enable' 'false')
               |tblproperties (
               |  'primaryKey' = 'id',
               |  'type' = 'mor',
               |  'preCombineField'='dt',
               |  'hoodie.index.type' = 'BUCKET_aa',
               |  'hoodie.bucket.index.hash.field' = 'id',
               |  'hoodie.bucket.index.num.buckets'=512
               | )
               |partitioned by (`day`,`hour`)
               |location '${tmp.getCanonicalPath}'
               |""".stripMargin)
        }
      }
    }
  }

  test("Test vectorized read nested columns for LegacyHoodieParquetFileFormat") {
    withSQLConf(
      "hoodie.datasource.read.use.new.parquet.file.format" -> "false",
      "hoodie.file.group.reader.enabled" -> "false",
      "spark.sql.parquet.enableNestedColumnVectorizedReader" -> "true",
      "spark.sql.parquet.enableVectorizedReader" -> "true") {
      withTempDir { tmp =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  attributes map<string, string>,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | tblproperties (primaryKey = 'id')
             | partitioned by (dt)
             | location '${tmp.getCanonicalPath}'
                    """.stripMargin)
        spark.sql(
          s"""
             | insert into $tableName values
             | (1, 'a1', map('color', 'red', 'size', 'M'), 10, 1000, '2021-01-05'),
             | (2, 'a2', map('color', 'blue', 'size', 'L'), 20, 2000, '2021-01-06'),
             | (3, 'a3', map('color', 'green', 'size', 'S'), 30, 3000, '2021-01-07')
                    """.stripMargin)
        // Check the inserted records with map type attributes
        checkAnswer(s"select id, name, price, ts, dt from $tableName where attributes.color = 'red'")(
          Seq(1, "a1", 10.0, 1000, "2021-01-05")
        )
      }
    }
  }

  def ingestAndValidateDataNoPrecombine(tableType: String, tableName: String, tmp: File,
                                        expectedOperationtype: WriteOperationType,
                                        setOptions: List[String] = List.empty) : Unit = {
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

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

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    // insert record again but w/ diff values but same primary key.
    spark.sql(
      s"""
         | insert into $tableName values
         | (1, 'a1_1', 10, "2021-07-18"),
         | (2, 'a2', 20, "2021-07-18"),
         | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    if (expectedOperationtype == WriteOperationType.UPSERT) {
      // dedup should happen within same batch being ingested and existing records on storage should get updated
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    } else {
      // no dedup across batches
      checkAnswer(s"select id, name, price, dt from $tableName order by id")(
        Seq(1, "a1", 10.0, "2021-07-18"),
        Seq(1, "a1_1", 10.0, "2021-07-18"),
        Seq(2, "a2", 20.0, "2021-07-18"),
        Seq(2, "a2_2", 30.0, "2021-07-18")
      )
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  test("Test insert dup policy with INSERT_INTO explicit new configs INSERT operation ") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val operation = WriteOperationType.INSERT
        Seq(NONE_INSERT_DUP_POLICY, DROP_INSERT_DUP_POLICY).foreach { dupPolicy =>
          withTable(generateTableName) { tableName =>
            ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
              List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
                s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
              dupPolicy)
          }
        }
      }
    }
  }

  test("Test insert dup policy with INSERT_INTO explicit new configs BULK_INSERT operation ") {
    withTempDir { tmp =>
      Seq("cow").foreach { tableType =>
        val operation = WriteOperationType.BULK_INSERT
        val dupPolicy = NONE_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
            dupPolicy)
        }
      }
    }
  }

  test("Test DROP insert dup policy with INSERT_INTO explicit new configs BULK INSERT operation") {
    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val dupPolicy = DROP_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDropDupPolicyBulkInsert(tableType, tableName, tmp,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${WriteOperationType.BULK_INSERT.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"))
        }
      }
    })
  }

  test("Test FAIL insert dup policy with INSERT_INTO explicit new configs") {
    withRecordType(Seq(HoodieRecordType.AVRO))(withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val operation = WriteOperationType.INSERT
        val dupPolicy = FAIL_INSERT_DUP_POLICY
        withTable(generateTableName) { tableName =>
          ingestAndValidateDataDupPolicy(tableType, tableName, tmp, operation,
            List(s"set ${SPARK_SQL_INSERT_INTO_OPERATION.key}=${operation.value}",
              s"set ${DataSourceWriteOptions.INSERT_DUP_POLICY.key}=$dupPolicy"),
            dupPolicy, true)
        }
      }
    })
  }

  test("Test various data types as partition fields") {
    withRecordType()(withTempDir { tmp =>
      val tableName = generateTableName
      spark.sql(
        s"""
           |CREATE TABLE $tableName (
           |  id INT,
           |  boolean_field BOOLEAN,
           |  float_field FLOAT,
           |  byte_field BYTE,
           |  short_field SHORT,
           |  decimal_field DECIMAL(10, 5),
           |  date_field DATE,
           |  string_field STRING,
           |  timestamp_field TIMESTAMP
           |) USING hudi
           | TBLPROPERTIES (primaryKey = 'id')
           | PARTITIONED BY (boolean_field, float_field, byte_field, short_field, decimal_field, date_field, string_field, timestamp_field)
           |LOCATION '${tmp.getCanonicalPath}'
     """.stripMargin)
      // Avoid operation type modification.
      spark.sql(s"set ${INSERT_DROP_DUPS.key}=false")
      spark.sql(s"set ${INSERT_DUP_POLICY.key}=$NONE_INSERT_DUP_POLICY")

      // Insert data into partitioned table
      spark.sql(
        s"""
           |INSERT INTO $tableName VALUES
           |(1, TRUE, CAST(1.0 as FLOAT), 1, 1, 1234.56789, DATE '2021-01-05', 'partition1', TIMESTAMP '2021-01-05 10:00:00'),
           |(2, FALSE,CAST(2.0 as FLOAT), 2, 2, 6789.12345, DATE '2021-01-06', 'partition2', TIMESTAMP '2021-01-06 11:00:00')
     """.stripMargin)

      checkAnswer(s"SELECT id, boolean_field FROM $tableName ORDER BY id")(
        Seq(1, true),
        Seq(2, false)
      )
    })
  }

  test(s"Test INSERT INTO with upsert operation type") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  ts long,
             |  price int
             |) using hudi
             |partitioned by (ts)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             |)
             |location '${tmp.getCanonicalPath}/$tableName'
             |""".stripMargin
        )

        // Test insert into with upsert operation type
        spark.sql(
          s"""
             | insert into $tableName
             | values (1, 'a1', 1000, 10), (2, 'a2', 2000, 20), (3, 'a3', 3000, 30), (4, 'a4', 2000, 10), (5, 'a5', 3000, 20), (6, 'a6', 4000, 30)
             | """.stripMargin
        )
        checkAnswer(s"select id, name, price, ts from $tableName where price > 3000")(
          Seq(6, "a6", 4000, 30)
        )

        // Test update
        spark.sql(s"update $tableName set price = price + 1 where id = 6")
        checkAnswer(s"select id, name, price, ts from $tableName where price > 3000")(
          Seq(6, "a6", 4001, 30)
        )
      }
    }
  }

  test("Test Insert Into with extraMetadata") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"

      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  dt string,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | tblproperties (primaryKey = 'id')
           | partitioned by (dt)
           | location '$tablePath'
       """.stripMargin)

      spark.sql("set hoodie.datasource.write.commitmeta.key.prefix=commit_extra_meta_")

      spark.sql("set commit_extra_meta_a=valA")
      spark.sql("set commit_extra_meta_b=valB")
      spark.sql(s"insert into $tableName values (1, 'a1', 10, 1000, '2024-06-14')")

      assertResult("valA") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_a")
      }
      assertResult("valB") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_b")
      }
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10.0, "2024-06-14")
      )

      spark.sql("set commit_extra_meta_a=new_valA")
      spark.sql("set commit_extra_meta_b=new_valB")
      spark.sql(s"insert into $tableName values (2, 'a2', 20, 2000, '2024-06-14')")

      assertResult("new_valA") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_a")
      }
      assertResult("new_valB") {
        getLastCommitMetadata(spark, tablePath).getExtraMetadata.get("commit_extra_meta_b")
      }
      checkAnswer(s"select id, name, price, dt from $tableName")(
        Seq(1, "a1", 10.0, "2024-06-14"),
        Seq(2, "a2", 20.0, "2024-06-14")
      )
    }
  }

  def ingestAndValidateDataDupPolicy(tableType: String, tableName: String, tmp: File,
                                     expectedOperationtype: WriteOperationType = WriteOperationType.INSERT,
                                     setOptions: List[String] = List.empty,
                                     insertDupPolicy : String = NONE_INSERT_DUP_POLICY,
                                     expectExceptionOnSecondBatch: Boolean = false) : Unit = {

    // set additional options
    setOptions.foreach(entry => {
      spark.sql(entry)
    })

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
         |  primaryKey = 'id',
         |  preCombine = 'name'
         | )
         | partitioned by (dt)
         | location '${tmp.getCanonicalPath}/$tableName'
         """.stripMargin)

    spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")

    assertResult(expectedOperationtype) {
      getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
    }
    checkAnswer(s"select id, name, price, dt from $tableName")(
      Seq(1, "a1", 10.0, "2021-07-18")
    )

    if (expectExceptionOnSecondBatch) {
      assertThrows[HoodieDuplicateKeyException] {
        try {
          spark.sql(
            s"""
               | insert into $tableName values
               | (1, 'a1_1', 10, "2021-07-18"),
               | (2, 'a2', 20, "2021-07-18"),
               | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)
        } catch {
          case e: Exception =>
            var root: Throwable = e
            while (root.getCause != null) {
              root = root.getCause
            }
            throw root
        }
      }
    } else {

      // insert record again w/ diff values but same primary key. Since "insert" is chosen as operation type,
      // dups should be seen w/ snapshot query
      spark.sql(
        s"""
           | insert into $tableName values
           | (1, 'a1_1', 10, "2021-07-18"),
           | (2, 'a2', 20, "2021-07-18"),
           | (2, 'a2_2', 30, "2021-07-18")
              """.stripMargin)

      assertResult(expectedOperationtype) {
        getLastCommitMetadata(spark, s"${tmp.getCanonicalPath}/$tableName").getOperationType
      }

      // Check if INSERT operation was explicitly set in setOptions
      val isExplicitInsertOperation = setOptions.exists(option =>
        option.toLowerCase.contains("set " + SPARK_SQL_INSERT_INTO_OPERATION.key + "=insert")
      )

      if (expectedOperationtype == WriteOperationType.UPSERT) {
        // dedup should happen within same batch being ingested and existing records on storage should get updated
        checkAnswer(s"select id, name, price, dt from $tableName order by id")(
          Seq(1, "a1_1", 10.0, "2021-07-18"),
          Seq(2, "a2_2", 30.0, "2021-07-18")
        )
      } else if (isExplicitInsertOperation) {
        // When INSERT operation is explicitly set, duplicates should be preserved
        if (insertDupPolicy == NONE_INSERT_DUP_POLICY) {
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            Seq(1, "a1_1", 10.0, "2021-07-18"),
            Seq(2, "a2", 20.0, "2021-07-18"),
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        } else if (insertDupPolicy == DROP_INSERT_DUP_POLICY) {
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            Seq(2, "a2", 20.0, "2021-07-18"), // same-batch duplicates preserved with explicit INSERT
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        }
      } else {
        if (insertDupPolicy == NONE_INSERT_DUP_POLICY) {
          // no dedup across batches or within same batch - all duplicates preserved
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            Seq(1, "a1_1", 10.0, "2021-07-18"),
            Seq(2, "a2", 20.0, "2021-07-18"),
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        } else if (insertDupPolicy == DROP_INSERT_DUP_POLICY) {
          checkAnswer(s"select id, name, price, dt from $tableName order by id")(
            Seq(1, "a1", 10.0, "2021-07-18"),
            // Seq(2, "a2", 20.0, "2021-07-18"), // preCombine within same batch kicks in if preCombine is set
            Seq(2, "a2_2", 30.0, "2021-07-18")
          )
        }
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }

  def ingestAndValidateDropDupPolicyBulkInsert(tableType: String,
                                               tableName: String,
                                               tmp: File,
                                               setOptions: List[String] = List.empty) : Unit = {

    // set additional options
    setOptions.foreach(entry => {
      spark.sql(entry)
    })
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

    // drop dups is not supported in bulk_insert row writer path.
    assertThrows[HoodieException] {
      try {
        spark.sql(s"insert into $tableName values(1, 'a1', 10, '2021-07-18')")
      } catch {
        case e: Exception =>
          var root: Throwable = e
          while (root.getCause != null) {
            root = root.getCause
          }
          throw root
      }
    }
    spark.sessionState.conf.unsetConf(SPARK_SQL_INSERT_INTO_OPERATION.key)
    spark.sessionState.conf.unsetConf("hoodie.sql.insert.mode")
    spark.sessionState.conf.unsetConf("hoodie.datasource.insert.dup.policy")
    spark.sessionState.conf.unsetConf("hoodie.datasource.write.operation")
  }
}
