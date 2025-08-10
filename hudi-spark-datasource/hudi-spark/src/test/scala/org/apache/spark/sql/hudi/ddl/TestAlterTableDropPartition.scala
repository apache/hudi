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

package org.apache.spark.sql.hudi.ddl

import org.apache.hudi.{DataSourceWriteOptions, HoodieCLIUtils}
import org.apache.hudi.avro.model.{HoodieCleanMetadata, HoodieCleanPartitionMetadata}
import org.apache.hudi.common.model.{HoodieCleaningPolicy, HoodieCommitMetadata}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.table.timeline.HoodieInstant
import org.apache.hudi.common.util.{PartitionPathEncodeUtils, StringUtils, Option => HOption}
import org.apache.hudi.config.{HoodieCleanConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.{ComplexKeyGenerator, SimpleKeyGenerator}
import org.apache.hudi.testutils.HoodieClientTestUtils.createMetaClient

import org.apache.avro.Schema
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.{disableComplexKeygenValidation, enableComplexKeygenValidation, getLastCleanMetadata}
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertTrue

import scala.collection.JavaConverters._

class TestAlterTableDropPartition extends HoodieSparkSqlTestBase {
  private val schemaFields: Seq[String] = Seq("id", "name", "ts", "dt")

  private def ensureLastCommitIncludesProperSchema(path: String, expectedSchema: Seq[String] = schemaFields): Unit = {
    val metaClient = createMetaClient(spark, path)
    // A bit weird way to extract schema, but there is no way to get it exactly as is, since once `includeMetadataFields`
    // is used - it will use custom logic to forcefully add/remove fields.
    // And available public methods does not allow to specify exact instant to get schema from, only latest after some filtering
    // which may lead to false positives in test scenarios.
    val lastInstant = metaClient.getActiveTimeline.getCompletedReplaceTimeline.lastInstant().get()
    val commitMetadata = metaClient.getActiveTimeline.readCommitMetadata(lastInstant)
    val schemaStr = commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY)
    val schema = new Schema.Parser().parse(schemaStr)
    val fields = schema.getFields.asScala.map(_.name())
    assert(expectedSchema == fields, s"Commit metadata should include no meta fields, received $fields")
  }

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
         |  orderingFields = 'ts'
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
         |  orderingFields = 'ts',
         |  hoodie.clean.commits.retained= '1'
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
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "dt")
          .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key(), urlencode)
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

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "dt")
          .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key(), urlencode)
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Append)
          .save(tablePath)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (dt='2021/10/01')")
        ensureLastCommitIncludesProperSchema(tablePath)

        // trigger clean so that partition deletion kicks in.
        spark.sql(s"set ${HoodieCleanConfig.CLEANER_POLICY.key}=${HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name()}")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()
        ensureLastCommitIncludesProperSchema(tablePath)

        val cleanMetadata: HoodieCleanMetadata = getLastCleanMetadata(spark, tablePath)
        val cleanPartitionMeta = new java.util.ArrayList(cleanMetadata.getPartitionMetadata.values()).toArray()
        var totalDeletedFiles = 0
        cleanPartitionMeta.foreach(entry =>
        {
          totalDeletedFiles += entry.asInstanceOf[HoodieCleanPartitionMetadata].getSuccessDeleteFiles.size()
        })
        assertTrue(totalDeletedFiles > 0)

        val partitionPath = if (urlencode) {
          PartitionPathEncodeUtils.escapePathName("2021/10/01")
        } else {
          "2021/10/01"
        }
        checkAnswer(s"select dt from $tableName")(Seq(s"2021/10/02"))
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

  Seq(false, true).foreach { urlencode =>
    test(s"Lazy Clean drop single-partition table' partitions, urlencode: $urlencode") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021/10/01")).toDF("id", "name", "ts", "dt")

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "dt")
          .option(DataSourceWriteOptions.URL_ENCODE_PARTITIONING.key(), urlencode)
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
             |  orderingFields = 'ts',
             |  hoodie.clean.commits.retained= '1'
             | )
             |""".stripMargin)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (dt='2021/10/01')")
        ensureLastCommitIncludesProperSchema(tablePath)

        spark.sql(s"""insert into $tableName values (2, "l4", "v1", "2021/10/02")""")
        ensureLastCommitIncludesProperSchema(tablePath)

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
         |  orderingFields = 'ts'
         | )
         | partitioned by (dt)
         |""".stripMargin)
    // insert data
    spark.sql(s"""insert into $tableName values (1, "z3", "v1", "2021-10-01"), (2, "l4", "v1", "2021-10-02")""")

    // specify duplicate partition columns
    checkExceptionContain(s"alter table $tableName drop partition (dt='2021-10-01', dt='2021-10-02')")(
      "Found duplicate keys `dt`")

    // insert data
    spark.sql(s"""insert into $tableName values (3, "z5", "v1", "2021-10-01"), (4, "l5", "v1", "2021-10-02")""")

    // drop 2021-10-01 partition
    spark.sql(s"alter table $tableName drop partition (dt='2021-10-01')")
    ensureLastCommitIncludesProperSchema(getTableStoragePath(tableName))

    // trigger clean so that partition deletion kicks in.
    spark.sql(s"set ${HoodieCleanConfig.CLEANER_POLICY.key}=${HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name()}")
    spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
      .collect()

    checkAnswer(s"select id, name, ts, dt from $tableName")(
      Seq(2, "l4", "v1", "2021-10-02"),
      Seq(4, "l5", "v1", "2021-10-02")
    )

    // show partitions
    checkAnswer(s"show partitions $tableName")(Seq("dt=2021-10-02"))
  }

  Seq(false, true).foreach { hiveStyle =>
    test(s"Drop multi-level partitioned table's partitions, isHiveStylePartitioning: $hiveStyle") {
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        val schemaFields = Seq("id", "name", "ts", "year", "month", "day")

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021", "10", "01"), (2, "l4", "v1", "2021", "10", "02"))
          .toDF(schemaFields :_*)

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "year,month,day")
          .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStyle)
          // Disable complex key generator validation so that the writer can succeed
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
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

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "year,month,day")
          .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          // Disable complex key generator validation so that the writer can succeed
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Append)
          .save(tablePath)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (year='2021', month='10', day='01')")
        ensureLastCommitIncludesProperSchema(tablePath, schemaFields)

        // trigger clean so that partition deletion kicks in.
        spark.sql(s"set ${HoodieCleanConfig.CLEANER_POLICY.key}=${HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.name()}")
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()
        ensureLastCommitIncludesProperSchema(tablePath, schemaFields)

        val cleanMetadata: HoodieCleanMetadata = getLastCleanMetadata(spark, tablePath)
        val cleanPartitionMeta = new java.util.ArrayList(cleanMetadata.getPartitionMetadata.values()).toArray()
        var totalDeletedFiles = 0
        cleanPartitionMeta.foreach(entry =>
        {
          totalDeletedFiles += entry.asInstanceOf[HoodieCleanPartitionMetadata].getSuccessDeleteFiles.size()
        })
        assertTrue(totalDeletedFiles > 0)

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
        val schemaFields = Seq("id", "name", "ts", "year", "month", "day")

        import spark.implicits._
        val df = Seq((1, "z3", "v1", "2021", "10", "01")).toDF(schemaFields :_*)

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "year,month,day")
          .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStyle)
          // Disable complex key generator validation so that the writer can succeed
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
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
             |  orderingFields = 'ts',
             |  hoodie.clean.commits.retained= '1'
             | )
             |""".stripMargin)

        df.write.format("hudi")
          .option(HoodieWriteConfig.TBL_NAME.key, tableName)
          .option(DataSourceWriteOptions.TABLE_TYPE.key, DataSourceWriteOptions.COW_TABLE_TYPE_OPT_VAL)
          .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "id")
          .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
          .option(DataSourceWriteOptions.PARTITIONPATH_FIELD.key, "year,month,day")
          .option(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING.key, hiveStyle)
          .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
          // Disable complex key generator validation so that the writer can succeed
          .option(HoodieWriteConfig.ENABLE_COMPLEX_KEYGEN_VALIDATION.key, "false")
          .option(HoodieWriteConfig.INSERT_PARALLELISM_VALUE.key, "1")
          .option(HoodieWriteConfig.UPSERT_PARALLELISM_VALUE.key, "1")
          .mode(SaveMode.Append)
          .save(tablePath)

        // drop 2021-10-01 partition
        spark.sql(s"alter table $tableName drop partition (year='2021', month='10', day='01')")
        ensureLastCommitIncludesProperSchema(tablePath, schemaFields)

        disableComplexKeygenValidation(spark, tableName)
        spark.sql(s"""insert into $tableName values (2, "l4", "v1", "2021", "10", "02")""")
        enableComplexKeygenValidation(spark, tableName)

        // trigger clean so that partition deletion kicks in.
        spark.sql(s"call run_clean(table => '$tableName', retain_commits => 1)")
          .collect()
        ensureLastCommitIncludesProperSchema(tablePath, schemaFields)

        val cleanMetadata: HoodieCleanMetadata = getLastCleanMetadata(spark, tablePath)
        val cleanPartitionMeta = new java.util.ArrayList(cleanMetadata.getPartitionMetadata.values()).toArray()
        var totalDeletedFiles = 0
        cleanPartitionMeta.foreach(entry =>
        {
          totalDeletedFiles += entry.asInstanceOf[HoodieCleanPartitionMetadata].getSuccessDeleteFiles.size()
        })
        assertTrue(totalDeletedFiles > 0)

        disableComplexKeygenValidation(spark, tableName)
        // insert data
        spark.sql(s"""insert into $tableName values (2, "l4", "v1", "2021", "10", "02")""")
        enableComplexKeygenValidation(spark, tableName)

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
        val schemaFields = Seq("id", "name", "price", "ts", "dt")
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
             |  orderingFields = 'ts'
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
        ensureLastCommitIncludesProperSchema(s"${tmp.getCanonicalPath}/$tableName", schemaFields)

        // check schema
        val metaClient = createMetaClient(spark, s"${tmp.getCanonicalPath}/$tableName")
        val lastInstant = metaClient.getActiveTimeline.getCommitsTimeline.lastInstant()
        val commitMetadata =
          metaClient.getActiveTimeline.readCommitMetadata(lastInstant.get())
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
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val basePath = s"${tmp.getCanonicalPath}t/$tableName"
        val schemaFields = Seq("id", "name", "price", "ts")
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
             |  type = '$tableType',
             |  orderingFields = 'ts'
             | )
             | partitioned by(ts)
             | location '$basePath'
             | """.stripMargin)
        spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
        spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
        val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))

        // Generate the first clustering plan
        val firstScheduleInstant = client.scheduleClustering(HOption.empty()).get()

        checkAnswer(s"call show_clustering('$tableName')")(
          Seq(firstScheduleInstant, 3, HoodieInstant.State.REQUESTED.name(), "*")
        )

        val partition = "ts=1002"
        val errMsg = s"Failed to drop partitions. Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: [$partition]"
        checkExceptionContain(s"ALTER TABLE $tableName DROP PARTITION($partition)")(errMsg)
      }
    }
  }

  test("Prevent a partition from being dropped if there are pending COMPACTs jobs") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val basePath = s"${tmp.getCanonicalPath}t/$tableName"
      val schemaFields = Seq("id", "name", "price", "ts")
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
           |  orderingFields = 'ts',
           |  hoodie.index.type = 'INMEMORY'
           | )
           | partitioned by(ts)
           | location '$basePath'
           | """.stripMargin)
      // disable automatic inline compaction to test with pending compaction instants
      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.schedule.inline=false")
      // Create 5 deltacommits to ensure that it is >= default `hoodie.compact.inline.max.delta.commits`
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1001)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1002)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1003)")
      spark.sql(s"insert into $tableName values(5, 'a5', 10, 1004)")
      val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))

      // Generate the first compaction plan
      val firstScheduleInstant = client.scheduleCompaction(HOption.empty())
      assertTrue(firstScheduleInstant.isPresent)

      checkAnswer(s"call show_compaction('$tableName')")(
        Seq(firstScheduleInstant.get(), 5, HoodieInstant.State.REQUESTED.name())
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
      val schemaFields = Seq("id", "name", "price", "ts")
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
           |  orderingFields = 'ts',
           |  hoodie.index.type = 'INMEMORY'
           | )
           | partitioned by(ts)
           | location '$basePath'
           | """.stripMargin)
      // disable automatic inline compaction to test with pending compaction instants
      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.schedule.inline=false")
      // Create 5 deltacommits to ensure that it is >= default `hoodie.compact.inline.max.delta.commits`
      // Write everything into the same FileGroup but into separate blocks
      spark.sql(s"insert into $tableName values(1, 'a1', 10, 1000)")
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000)")
      spark.sql(s"insert into $tableName values(3, 'a3', 10, 1000)")
      spark.sql(s"insert into $tableName values(4, 'a4', 10, 1000)")
      spark.sql(s"insert into $tableName values(5, 'a5', 10, 1000)")
      val client = HoodieCLIUtils.createHoodieWriteClient(spark, basePath, Map.empty, Option(tableName))

      // Generate the first log_compaction plan
      assertTrue(client.scheduleLogCompaction(HOption.empty()).isPresent)

      val partition = "ts=1000"
      val errMsg = s"Failed to drop partitions. Please ensure that there are no pending table service actions (clustering/compaction) for the partitions to be deleted: [$partition]"
      checkExceptionContain(s"ALTER TABLE $tableName DROP PARTITION($partition)")(errMsg)
    }
  }

  test("Test drop partition with wildcards") {
    withTempDir { tmp =>
      Seq("cow", "mor").foreach { tableType =>
        val tableName = generateTableName
        val schemaFields = Seq("id", "name", "price", "ts", "partition_date_col")
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price double,
             |  ts long,
             |  partition_date_col string
             |) using hudi
             | location '${tmp.getCanonicalPath}/$tableName'
             | tblproperties (
             |  primaryKey ='id',
             |  type = '$tableType',
             |  orderingFields = 'ts'
             | ) partitioned by (partition_date_col)
         """.stripMargin)
        spark.sql(s"insert into $tableName values " +
          s"(1, 'a1', 10, 1000, '2023-08-01'), (2, 'a2', 10, 1000, '2023-08-02'), (3, 'a3', 10, 1000, '2023-09-01')")
        checkAnswer(s"show partitions $tableName")(
          Seq("partition_date_col=2023-08-01"),
          Seq("partition_date_col=2023-08-02"),
          Seq("partition_date_col=2023-09-01")
        )
        spark.sql(s"alter table $tableName drop partition(partition_date_col='2023-08-*')")
        ensureLastCommitIncludesProperSchema(s"${tmp.getCanonicalPath}/$tableName", schemaFields)
        // show partitions will still return all partitions for tests, use select distinct as a stop-gap
        checkAnswer(s"select distinct partition_date_col from $tableName")(
          Seq("2023-09-01")
        )
      }
    }
  }
}
