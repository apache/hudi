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

package org.apache.hudi.functional

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.metadata.HoodieMetadataFileSystemView
import org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, HoodieFileIndex}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, GreaterThan, Literal}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag

import scala.collection.JavaConverters._

@Tag("functional")
class TestPartitionStatsIndexWithSql extends HoodieSparkSqlTestBase {

  val sqlTempTable = "hudi_tbl"

  test("Test partition stats index following insert, merge into, update and delete") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Create table with date type partition
      spark.sql(
        s"""
           | create table $tableName using hudi
           | partitioned by (dt)
           | tblproperties(
           |    primaryKey = 'id',
           |    preCombineField = 'ts',
           |    'hoodie.metadata.index.partition.stats.enable' = 'true',
           |    'hoodie.metadata.index.column.stats.column.list' = 'name'
           | )
           | location '$tablePath'
           | AS
           | select 1 as id, 'a1' as name, 10 as price, 1000 as ts, cast('2021-05-06' as date) as dt
         """.stripMargin
      )

      assertResult(WriteOperationType.BULK_INSERT) {
        HoodieSparkSqlTestBase.getLastCommitMetadata(spark, tablePath).getOperationType
      }
      checkAnswer(s"select id, name, price, ts, cast(dt as string) from $tableName")(
        Seq(1, "a1", 10, 1000, "2021-05-06")
      )

      val partitionValue = "2021-05-06"

      // Check the missing properties for spark sql
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      val properties = metaClient.getTableConfig.getProps.asScala.toMap
      assertResult(true)(properties.contains(HoodieTableConfig.CREATE_SCHEMA.key))
      assertResult("dt")(properties(HoodieTableConfig.PARTITION_FIELDS.key))
      assertResult("ts")(properties(HoodieTableConfig.PRECOMBINE_FIELD.key))
      assertResult(tableName)(metaClient.getTableConfig.getTableName)
      // Validate partition_stats index exists
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))

      // Test insert into
      spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, cast('$partitionValue' as date))")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 10, 1000, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 10, 1000, partitionValue)
      )
      // Test merge into
      spark.sql(
        s"""
           |merge into $tableName h0
           |using (select 1 as id, 'a1' as name, 11 as price, 1001 as ts, '$partitionValue' as dt) s0
           |on h0.id = s0.id
           |when matched then update set *
           |""".stripMargin)
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 11, 1001, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 10, 1000, partitionValue)
      )
      // Test update
      spark.sql(s"update $tableName set price = price + 1 where id = 2")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("1", s"dt=$partitionValue", 1, "a1", 11, 1001, partitionValue),
        Seq("2", s"dt=$partitionValue", 2, "a2", 11, 1000, partitionValue)
      )
      // Test delete
      spark.sql(s"delete from $tableName where id = 1")
      checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
        Seq("2", s"dt=$partitionValue", 2, "a2", 11, 1000, partitionValue)
      )
    }
  }

  test("Test partition stats index on string type field with insert and file pruning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      // Create table with date type partition
      spark.sql(
        s"""
           | create table $tableName (
           |    ts BIGINT,
           |    uuid STRING,
           |    rider STRING,
           |    driver STRING,
           |    fare DOUBLE,
           |    city STRING,
           |    state STRING
           |) using hudi
           | options(
           |    primaryKey ='uuid',
           |    preCombineField = 'ts',
           |    hoodie.metadata.index.partition.stats.enable = 'true',
           |    hoodie.metadata.index.column.stats.column.list = 'rider'
           |)
           |PARTITIONED BY (state)
           |location '$tablePath';
         """.stripMargin
      )

      //spark.sql("set hoodie.parquet.small.file.limit=0")
      spark.sql(s"INSERT INTO $tableName VALUES (1695159649,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K',19.10,'san_francisco','california')")
      spark.sql(s"INSERT INTO $tableName VALUES (1695332066,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-B','driver-L',93.50,'new york city','new york')")
      spark.sql(s"INSERT INTO $tableName VALUES (1695516137,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-C','driver-P',34.15,'houston','texas')")
      spark.sql(s"INSERT INTO $tableName VALUES (1695516137,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-D','driver-M',34.15,'princeton','new jersey')")
      spark.sql(s"INSERT INTO $tableName VALUES (1695332066,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O',93.50,'austin','texas')")
      spark.sql(s"INSERT INTO $tableName VALUES (1695091554,'e96c4396-3fad-413a-a942-4cb36106d721','rider-F','driver-M',27.70,'sunnyvale','california')")

      // Validate partition_stats index exists
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assertResult(tableName)(metaClient.getTableConfig.getTableName)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))

      // Test pruning
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      checkAnswer(s"select uuid, rider, city, state from $tableName where rider > 'rider-D'")(
        Seq("1dced545-862b-4ceb-8b43-d2a568f6616b", "rider-E", "austin", "texas"),
        Seq("e96c4396-3fad-413a-a942-4cb36106d721", "rider-F", "sunnyvale", "california"),
      )

      verifyFilePruning(Map.apply(
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
        HoodieMetadataConfig.ENABLE.key -> "true"),
        GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-D")),
        HoodieTableMetaClient.reload(metaClient))
    }
  }

  test(s"Test partition stats index on int type field with update and file pruning") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
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
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'price',
           |  hoodie.metadata.index.partition.stats.enable = 'true',
           |  hoodie.metadata.index.column.stats.column.list = 'price'
           |)
           |location '$tablePath'
           |""".stripMargin
      )

      spark.sql(
        s"""
           | insert into $tableName
           | values (1, 'a1', 1000, 10), (2, 'a2', 2000, 20), (3, 'a3', 3000, 30), (4, 'a4', 2000, 10), (5, 'a5', 3000, 20), (6, 'a6', 4000, 30)
           | """.stripMargin
      )

      // Validate partition_stats index exists
      val metaClient = HoodieTableMetaClient.builder()
        .setBasePath(tablePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assertResult(tableName)(metaClient.getTableConfig.getTableName)
      assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))

      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      checkAnswer(s"select id, name, price, ts from $tableName where price>3000")(
        Seq(6, "a6", 4000, 30)
      )

      // Test price update, assert latest value and ensure file pruning
      spark.sql(s"update $tableName set price = price + 1 where id = 6")
      checkAnswer(s"select id, name, price, ts from $tableName where price>3000")(
        Seq(6, "a6", 4001, 30)
      )

      verifyFilePruning(
        Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true"),
        GreaterThan(AttributeReference("price", IntegerType)(), Literal(3000)),
        HoodieTableMetaClient.reload(metaClient))
    }
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression, metaClient: HoodieTableMetaClient): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    assertTrue(filteredFilesCount < getLatestDataFilesCount(metaClient = metaClient))

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filesCountWithNoSkipping == getLatestDataFilesCount(metaClient = metaClient))
  }

  private def getLatestDataFilesCount(includeLogFiles: Boolean = true, metaClient: HoodieTableMetaClient) = {
    var totalLatestDataFiles = 0L
    getTableFileSystemView(metaClient).getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
      .values()
      .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
        (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
          slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
            + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    totalLatestDataFiles
  }

  private def getTableFileSystemView(metaClient: HoodieTableMetaClient): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(
      new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext)),
      metaClient,
      metaClient.getActiveTimeline,
      HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexPartitionStats(true).build())
  }

}
