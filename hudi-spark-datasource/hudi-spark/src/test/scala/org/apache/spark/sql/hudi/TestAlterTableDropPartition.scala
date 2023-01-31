/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.{HoodieCLIUtils, HoodieSparkUtils}
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.timeline.{HoodieActiveTimeline, HoodieInstant}
import org.apache.hudi.common.util.{Option => HOption}
import org.apache.hudi.common.util.{PartitionPathEncodeUtils, StringUtils}
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue

class TestAlterTableDropPartition extends HoodieSparkSqlTestBase {

  test("Drop non-partitioned table") {
    val tableName = generateTableName
    // create table
    spark.sql(
      s"""
         | create table $tableName (
         |  id bigint,
         |  name string,
         |  ts string,
         |  dt string
         | )
         | using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01')")(
      s"$tableName is a non-partitioned table that is not allowed to drop partition")

    // show partitions
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
  }

  test("Lazy Clean drop non-partitioned table") {
    val tableName = generateTableName
    // create table
    spark.sql(
      s"""
         | create table $tableName (
         |  id bigint,
         |  name string,
         |  ts string,
         |  dt string
         | )
         | using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  hoodie.cleaner.commits.retained= '1'
         | )
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01')")(
      s"$tableName is a non-partitioned table that is not allowed to drop partition")

    // show partitions
    checkAnswer(s"show partitions $tableName")(Seq.empty: _*)
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Drop single-partition table' partitions, urlencode: $urlencode") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021/10/01"), (2, "l4", "v1", "2021/10/02"))
          .toDF("id", "name", "ts", "dt")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(URL_ENCODE_PARTITIONING.key(), urlencode)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (dt='2021/10/01')")

        val partitionPath = if (urlencode) {
          PartitionPathEncodeUtils.escapePathName("2021/10/01")
        } else {
          "2021/10/01"
        }
        checkAnswer(s"select dt from $tableName")(Seq(s"2021/10/02"))
        assertResult(true)(existsPath(s"${tmp.getCanonicalPath}/$tableName/$partitionPath"))

        // show partitions
        if (urlencode) {
          checkAnswer(s"show partitions $tableName")(Seq(PartitionPathEncodeUtils.escapePathName("2021/10/02")))
        } else {
          checkAnswer(s"show partitions $tableName")(Seq("2021/10/02"))
        }
      }
    }
  }

  Seq(false, true).foreach { urlencode =>
    test(s"Lazy Clean drop single-partition table' partitions, urlencode: $urlencode") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021/10/01")).toDF("id", "name", "ts", "dt")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "dt")
          .option(URL_ENCODE_PARTITIONING.key(), urlencode)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.cleaner.commits.retained= '1'
             | )
             |""".stripMargin)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (dt='2021/10/01')")

        spark.sql(s"""insert into $tableName values (2, "l4", "v1", "2021/10/02")""")

        val partitionPath = if (urlencode) {
          PartitionPathEncodeUtils.escapePathName("2021/10/01")
        } else {
          "2021/10/01"
        }
        checkAnswer(s"select dt from $tableName")(Seq("2021/10/02"))
        assertResult(false)(existsPath(s"${tmp.getCanonicalPath}/$tableName/$partitionPath"))

        // show partitions
        if (urlencode) {
          checkAnswer(s"show partitions $tableName")(Seq(PartitionPathEncodeUtils.escapePathName("2021/10/02")))
        } else {
          checkAnswer(s"show partitions $tableName")(Seq("2021/10/02"))
        }
      }
    }
  }

  test("Drop single-partition table' partitions created by sql") {
    val tableName = generateTableName
    // create table
    spark.sql(
      s"""
         | create table $tableName (
         |  id bigint,
         |  name string,
         |  ts string,
         |  dt string
         | )
         | using hudi
         | tblproperties (
         |  primaryKey = 'id',
         |  preCombineField = 'ts'
         | )
         | partitioned by (dt)
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    // specify duplicate partition columns
    if (HoodieSparkUtils.gteqSpark3_3) {
      checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01', dt='2021-10-02')")(
        "Found duplicate keys `dt`")
    } else {
      checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01', dt='2021-10-02')")(
        "Found duplicate keys 'dt'")
    }


    // drop 2021-10-01 partition
    spark.sql(s"alter table $tableName drop partition (dt='2021-10-01')")

    checkAnswer(s"select id, name, ts, dt from $tableName")(Seq(2, "l4", "v1", "2021-10-02"))

    // show partitions
    checkAnswer(s"show partitions $tableName")(Seq("dt=2021-10-02"))
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Drop multi-level partitioned table's partitions, isHiveStylePartitioning: $hiveStyle") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021", "10", "01"), (2, "l4", "v1", "2021", "10", "02"))
          .toDF("id", "name", "ts", "year", "month", "day")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "year,month,day")
          .option(HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             |""".stripMargin)

        // not specified all partition column
        checkExceptionContain(s"alter table $tableName drop partition (year='2021', month='10')")(
          "All partition columns need to be specified for Hoodie's partition"
        )
        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (year='2021', month='10', day='01')")

        checkAnswer(s"select id, name, ts, year, month, day from $tableName")(
          Seq(2, "l4", "v1", "2021", "10", "02")
        )

        // show partitions
        if (hiveStyle) {
          checkAnswer(s"show partitions $tableName")(Seq("year=2021/month=10/day=02"))
        } else {
          checkAnswer(s"show partitions $tableName")(Seq("2021/10/02"))
        }
      }
    }
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Lazy Clean drop multi-level partitioned table's partitions, isHiveStylePartitioning: $hiveStyle") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021", "10", "01")).toDF("id", "name", "ts", "year", "month", "day")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(TABLE_TYPE.key, COW_TABLE_TYPE_OPT_VAL)
          .option(RECORDKEY_FIELD.key, "id")
          .option(PRECOMBINE_FIELD.key, "ts")
          .option(PARTITIONPATH_FIELD.key, "year,month,day")
          .option(HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Overwrite)
          .save(tablePath)

        // register meta to spark catalog by creating table
        spark.sql(
          s"""
             |create table $tableName using hudi
             |location '$tablePath'
             | tblproperties (
             |  primaryKey = 'id',
             |  preCombineField = 'ts',
             |  hoodie.cleaner.commits.retained= '1'
             | )
             |""".stripMargin)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (year='2021', month='10', day='01')")

        // insert data
        spark.sql(s"""insert into $tableName values (2, "l4", "v1", "2021", "10", "02")""")

        checkAnswer(s"select id, name, ts, year, month, day from $tableName")(
          Seq(2, "l4", "v1", "2021", "10", "02")
        )

        assertResult(false)(existsPath(
          s"${tmp.getCanonicalPath}/$tableName/year=2021/month=10/day=01"))

        // show partitions
        if (hiveStyle) {
          checkAnswer(s"show partitions $tableName")(Seq("year=2021/month=10/day=02"))
        } else {
          checkAnswer(s"show partitions $tableName")(Seq("2021/10/02"))
        }
      }
    }
  }

  test("check instance schema") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        // create table
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  dt string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'ts'
             | )
             | partitioned by (dt)
       """.stripMargin)
        // insert data to table
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000, '01'), " +
          s"(2, 'a2', 10, 1000, '02'), (3, 'a3', 10, 1000, '03')")
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(1, "a1", 10.0, 1000, "01"),
          Seq(2, "a2", 10.0, 1000, "02"),
          Seq(3, "a3", 10.0, 1000, "03")
        )

        // drop partition
        spark.sql(s"alter table $tableName drop partition (dt = '01')")
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(2, "a2", 10.0, 1000, "02"),
          Seq(3, "a3", 10.0, 1000, "03")
        )

        // check schema
        val hadoopConf = spark.sessionState.newHadoopConf()
        val metaClient = HoodieTableMetaClient.builder().setBasePath(s"${tmp.getCanonicalPath}/$tableName")
          .setConf(hadoopConf).build()
        val lastInstant = metaClient.getActiveTimeline.lastInstant()
        val commitMetadata = HoodieCommitMetadata.fromBytes(metaClient.getActiveTimeline.getInstantDetails(
          lastInstant.get()).get(), classOf[HoodieCommitMetadata])
        val schemaStr = commitMetadata.getExtraMetadata.get(HoodieCommitMetadata.SCHEMA_KEY)
        Assertions.assertFalse(StringUtils.isNullOrEmpty(schemaStr))

        // delete
        spark.sql(s"delete from $tableName where dt = '02'")
        checkAnswer(s"select id, name, price, ts, dt from $tableName")(
          Seq(3, "a3", 10, 1000, "03")
        )
      }
    }
  }

  test("Prevent a partition from being dropped if there are pending CLUSTERING jobs") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}t/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'cow',
           |  preCombineField = 'ts'
           | )
           | partitioned by(ts)
           | location '$basePath'
           | """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
      val client = HoodieCLIUtils.createHoodieClientFromPath(spark, basePath, Map.empty)

      // Generate the first clustering plan
      val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
      client.scheduleClusteringAtInstant(firstScheduleInstant, HOption.empty())

      checkAnswer(s"call show_clustering('$tableName')")(
        Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "*")
      )

      val partition = "ts=1002"
      val errMsg = s"Failed to drop partitions. Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: [$partition]"
      checkExceptionContain(s"ALTER TABLE $tableName DROP PARTITION($partition)")(errMsg)
    }
  }

  test("Prevent a partition from being dropped if there are pending COMPACTs jobs") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}t/$tableName"
      // Using INMEMORY index type to ensure that deltacommits generate log files instead of parquet
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY'
           | )
           | partitioned by(ts)
           | location '$basePath'
           | """.stripMargin)
      // Create 5 deltacommits to ensure that it is > default `hoodie.compact.inline.max.delta.commits`
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
      spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
      val client = HoodieCLIUtils.createHoodieClientFromPath(spark, basePath, Map.empty)

      // Generate the first compaction plan
      val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
      assertTrue(client.scheduleCompactionAtInstant(firstScheduleInstant, HOption.empty()))

      checkAnswer(s"call show_compaction('$tableName')")(
        Seq(firstScheduleInstant, 5, HoodieInstant.State.REQUESTED.name())
      )

      val partition = "ts=1002"
      val errMsg = s"Failed to drop partitions. Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: [$partition]"
      checkExceptionContain(s"ALTER TABLE $tableName DROP PARTITION($partition)")(errMsg)
    }
  }

  test("Prevent a partition from being dropped if there are pending LOG_COMPACT jobs") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}t/$tableName"
      // Using INMEMORY index type to ensure that deltacommits generate log files instead of parquet
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price double,
           |  ts long
           |) using hudi
           | options (
           |  primaryKey ='id',
           |  type = 'mor',
           |  preCombineField = 'ts',
           |  hoodie.index.type = 'INMEMORY'
           | )
           | partitioned by(ts)
           | location '$basePath'
           | """.stripMargin)
      // Create 5 deltacommits to ensure that it is > default `hoodie.compact.inline.max.delta.commits`
      // Write everything into the same FileGroup but into separate blocks
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      spark.sql(s"insert into $tableName values(5, 'a5', 10, 1000)")
      val client = HoodieCLIUtils.createHoodieClientFromPath(spark, basePath, Map.empty)

      // Generate the first log_compaction plan
      val firstScheduleInstant = HoodieActiveTimeline.createNewInstantTime
      assertTrue(client.scheduleLogCompactionAtInstant(firstScheduleInstant, HOption.empty()))

      val partition = "ts=1000"
      val errMsg = s"Failed to drop partitions. Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: [$partition]"
      checkExceptionContain(s"ALTER TABLE $tableName DROP PARTITION($partition)")(errMsg)
    }
  }
}
