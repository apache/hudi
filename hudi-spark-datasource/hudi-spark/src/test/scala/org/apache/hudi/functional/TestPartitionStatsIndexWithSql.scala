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

import org.apache.hudi.{DataSourceReadOptions, HoodieFileIndex}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, WriteOperationType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, MetadataPartitionType}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.getPartitionStatsIndexKey
import org.apache.hudi.metadata.MetadataPartitionType.PARTITION_STATS
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, Expression, GreaterThan, IsNotNull, LessThan, Literal, Or}
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.junit.jupiter.api.{BeforeAll, Tag}
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}

import scala.collection.JavaConverters._

@Tag("functional")
class TestPartitionStatsIndexWithSql extends HoodieSparkSqlTestBase {

  val sqlTempTable = "hudi_tbl"

  @BeforeAll
  def init(): Unit = {
    initQueryIndexConf()
  }

  test("Test drop partition stats index") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table with date type partition
        spark.sql(
          s"""
             | create table $tableName using hudi
             | partitioned by (dt)
             | tblproperties(
             |    type = '$tableType',
             |    primaryKey = 'id',
             |    preCombineField = 'ts',
             |    'hoodie.metadata.index.partition.stats.enable' = 'true',
             |    'hoodie.metadata.index.column.stats.enable' = 'true',
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
        // Test insert into
        spark.sql(s"insert into $tableName values(2, 'a2', 10, 1000, cast('$partitionValue' as date))")
        checkAnswer(s"select _hoodie_record_key, _hoodie_partition_path, id, name, price, ts, cast(dt as string) from $tableName order by id")(
          Seq("1", s"dt=$partitionValue", 1, "a1", 10, 1000, partitionValue),
          Seq("2", s"dt=$partitionValue", 2, "a2", 10, 1000, partitionValue)
        )

        var metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        // Validate partition_stats index exists
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))
        spark.sql(s"drop index partition_stats on $tableName")
        metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        // Validate partition_stats index does not exist
        assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))
      }
    }
  }

  test("Test partition stats index following insert, merge into, update and delete") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // Create table with date type partition
        spark.sql(
          s"""
             | create table $tableName using hudi
             | partitioned by (dt)
             | tblproperties(
             |    type = '$tableType',
             |    primaryKey = 'id',
             |    preCombineField = 'ts',
             |    'hoodie.metadata.index.partition.stats.enable' = 'true',
             |    'hoodie.metadata.index.column.stats.enable' = 'true',
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
        assertResult("ts")(properties(HoodieTableConfig.ORDERING_FIELDS.key))
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
             |using (select 1 as id, 'a1' as name, 11 as price, 1001 as ts, cast('$partitionValue' as Date) as dt) s0
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
  }

  test("Test partition stats index on string type field with insert and file pruning") {
    Seq("cow", "mor").foreach { tableType =>
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
             |    city STRING,
             |    state STRING
             |) using hudi
             | options(
             |    type = '$tableType',
             |    primaryKey ='uuid',
             |    preCombineField = 'ts',
             |    hoodie.metadata.index.partition.stats.enable = 'true',
             |    hoodie.metadata.index.column.stats.enable = 'true',
             |    hoodie.metadata.index.column.stats.column.list = 'rider'
             |)
             |PARTITIONED BY (state)
             |location '$tablePath'
         """.stripMargin
        )
        // set small file limit to 0 and parquet max file size to 1 so that each insert creates a new file
        spark.sql("set hoodie.parquet.small.file.limit=0")
        spark.sql("set hoodie.parquet.max.file.size=1")
        // insert data in below pattern so that multiple records for 'texas' and 'california' partition are in same file
        spark.sql(
          s"""
             | insert into $tableName
             | values (1695159649,'334e26e9-8355-45cc-97c6-c31daf0df330','rider-A','driver-K','san_francisco','california'), (1695091554,'e96c4396-3fad-413a-a942-4cb36106d721','rider-F','driver-M','sunnyvale','california')
             | """.stripMargin
        )
        spark.sql(s"INSERT INTO $tableName VALUES (1695332066,'7a84095f-737f-40bc-b62f-6b69664712d2','rider-B','driver-L','new york city','new york')")
        spark.sql(s"INSERT INTO $tableName VALUES (1695516137,'3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04','rider-D','driver-M','princeton','new jersey')")
        spark.sql(
          s"""
             | insert into $tableName
             | values
             | (1695516137,'e3cf430c-889d-4015-bc98-59bdce1e530c','rider-C','driver-P','houston','texas'),
             | (1695332066,'1dced545-862b-4ceb-8b43-d2a568f6616b','rider-E','driver-O','austin','texas'),
             | (1695516138,'e3cf430c-889d-4015-bc98-59bdce1e530d','rider-C','driver-P','houston','texas')
             | """.stripMargin
        )

        // Validate partition_stats index exists
        val metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        assertResult(tableName)(metaClient.getTableConfig.getTableName)
        assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(PARTITION_STATS.getPartitionPath))
        val fileIndex = HoodieFileIndex(spark, metaClient, None, Map("path" -> metaClient.getBasePath.toString))
        // list files and group by partition
        val partitionFiles = fileIndex.listFiles(Seq.empty, Seq.empty).map(dir => (dir.values, dir.files))
          .flatMap(p => p._2.map(f => (p._1, f))).groupBy(f => f._1)
        // Make sure there are partition(s) with a single file and multiple files
        assertTrue(partitionFiles.exists(p => p._2.size == 1) && partitionFiles.exists(p => p._2.size > 1))

        // Test pruning
        spark.sql("set hoodie.metadata.enable=true")
        spark.sql("set hoodie.enable.data.skipping=true")
        spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
        checkAnswer(s"select uuid, rider, city, state from $tableName where rider > 'rider-D'")(
          Seq("1dced545-862b-4ceb-8b43-d2a568f6616b", "rider-E", "austin", "texas"),
          Seq("e96c4396-3fad-413a-a942-4cb36106d721", "rider-F", "sunnyvale", "california")
        )

        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-D")),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = true)
        // Include an isNotNull check
        verifyFilePruningExpressions(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          Seq(IsNotNull(AttributeReference("rider", StringType)()), GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-D"))),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = true)
        // if we predicate on a col which is not indexed, we expect full scan.
        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          GreaterThan(AttributeReference("driver", StringType)(), Literal("driver-O")),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = false)

        // if we predicate on two cols, one of which is indexed, while the other is not indexed. and using `AND` operator
        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          And(GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-D")), GreaterThan(AttributeReference("driver", StringType)(), Literal("driver-O"))),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = true) // pruning should happen

        // if we predicate on two cols, one of which is indexed, while the other is not indexed. and using `OR` operator
        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          Or(GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-D")), GreaterThan(AttributeReference("driver", StringType)(), Literal("driver-O"))),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = false)

        // Test predicate that does not match any partition, should scan no files
        checkAnswer(s"select uuid, rider, city, state from $tableName where rider > 'rider-Z'")()
        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          GreaterThan(AttributeReference("rider", StringType)(), Literal("rider-Z")),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = true,
          isNoScanExpected = true)
        // Test predicate that matches all partitions, will end up scanning all partitions
        checkAnswer(s"select uuid, rider, city, state from $tableName where rider < 'rider-Z'")(
          Seq("334e26e9-8355-45cc-97c6-c31daf0df330", "rider-A", "san_francisco", "california"),
          Seq("7a84095f-737f-40bc-b62f-6b69664712d2", "rider-B", "new york city", "new york"),
          Seq("e3cf430c-889d-4015-bc98-59bdce1e530c", "rider-C", "houston", "texas"),
          Seq("e3cf430c-889d-4015-bc98-59bdce1e530d", "rider-C", "houston", "texas"),
          Seq("3eeb61f7-c2b0-4636-99bd-5d7a5a1d2c04", "rider-D", "princeton", "new jersey"),
          Seq("1dced545-862b-4ceb-8b43-d2a568f6616b", "rider-E", "austin", "texas"),
          Seq("e96c4396-3fad-413a-a942-4cb36106d721", "rider-F", "sunnyvale", "california")
        )

        verifyFilePruning(
          Map(
            DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true",
            HoodieMetadataConfig.ENABLE.key -> "true"),
          LessThan(AttributeReference("rider", StringType)(), Literal("rider-Z")),
          HoodieTableMetaClient.reload(metaClient),
          isDataSkippingExpected = false)
      }
    }
  }

  test(s"Test partition stats index without configuring columns to index") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        // create table and enable partition stats without configuring columns to index
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price int,
             |  ts long
             |) using hudi
             |partitioned by (ts)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'price',
             |  hoodie.metadata.index.partition.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.enable = 'true'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        writeAndValidatePartitionStats(tableName, tablePath)
        // validate partition stats index for id column
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='id'")(
          Seq(getPartitionStatsIndexKey("ts=10", "id"), 1, 4),
          Seq(getPartitionStatsIndexKey("ts=20", "id"), 2, 5),
          Seq(getPartitionStatsIndexKey("ts=30", "id"), 3, 6)
        )
        // validate partition stats index for name column
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member6.value, ColumnStatsMetadata.maxValue.member6.value from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='name'")(
          Seq(getPartitionStatsIndexKey("ts=10", "name"), "a1", "a4"),
          Seq(getPartitionStatsIndexKey("ts=20", "name"), "a2", "a5"),
          Seq(getPartitionStatsIndexKey("ts=30", "name"), "a3", "a6")
        )
      }
    }
  }

  /**
   * Test the partition stats consolidation:
   *
   * 1. Insert values: Initially insert records with values that create wide bounds for the price field across different partitions.
   * 2. Update to widen the bounds: Perform updates to increase the price values, causing the min/max stats to widen.
   * 3. Update to remove or lower the max value: Delete or update the record that holds the max value to simulate a
   * scenario where the bounds need to be tightened.
   * 4. Trigger stats recomputation: Assuming tighter bounds is enabled on every write, validate that the partition stats
   * have adjusted correctly after scanning and recomputing accurate stats.
   */
  test("Test partition stats index with tight bound") {
    Seq("cow", "mor").foreach { tableType =>
      withTempDir { tmp =>
        val tableName = generateTableName + s"_tight_bound_$tableType"
        val tablePath = s"${tmp.getCanonicalPath}/$tableName"
        spark.sql(
          s"""
             |create table $tableName (
             |  id int,
             |  name string,
             |  price int,
             |  ts long
             |) using hudi
             |partitioned by (ts)
             |tblproperties (
             |  type = '$tableType',
             |  primaryKey = 'id',
             |  preCombineField = 'price',
             |  hoodie.metadata.index.partition.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.enable = 'true',
             |  hoodie.metadata.index.column.stats.column.list = 'price'
             |)
             |location '$tablePath'
             |""".stripMargin
        )

        /**
         * Insert: Insert values for price across multiple partitions (ts=10, ts=20, ts=30):
         *
         * Partition ts=10: price = 1000, 1500
         * Partition ts=20: price = 2000, 2500
         * Partition ts=30: price = 3000, 3500
         *
         * This will initialize the partition stats with bounds like [1000, 1500], [2000, 2500], and [3000, 3500].
         */
        spark.sql(s"insert into $tableName values (1, 'a1', 1000, 10), (2, 'a2', 1500, 10)")
        spark.sql(s"insert into $tableName values (3, 'a3', 2000, 20), (4, 'a4', 2500, 20)")
        spark.sql(s"insert into $tableName values (5, 'a5', 3000, 30), (6, 'a6', 3500, 30)")

        // validate partition stats initialization
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
          Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 1500),
          Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 2500),
          Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000, 3500)
        )

        // First Update (widen the bounds): Update the price in partition ts=30, where price = 4000 for id=6.
        //                                  This will widen the max bounds in ts=30 from 3500 to 4000.
        spark.sql(s"update $tableName set price = 4000 where id = 6")
        // Validate widened stats
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value, ColumnStatsMetadata.isTightBound from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
          Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 1500, true),
          Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 2500, true),
          Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000, 4000, true)
        )
        // verify file pruning
        var metaClient = HoodieTableMetaClient.builder()
          .setBasePath(tablePath)
          .setConf(HoodieTestUtils.getDefaultStorageConf)
          .build()
        verifyFilePruning(Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true"),
          GreaterThan(AttributeReference("price", IntegerType)(), Literal(3000)), metaClient, isDataSkippingExpected = true)

        // Second update (reduce max value)
        spark.sql(s"delete from $tableName where id = 6")
        // Validate that stats have recomputed and tightened
        // if tighter bound, note that record with prev max was deleted
        checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value, ColumnStatsMetadata.isTightBound from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
          Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 1500, true),
          Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 2500, true),
          Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000, 3000, true)
        )
        // verify file pruning
        metaClient = HoodieTableMetaClient.reload(metaClient)
        verifyFilePruning(Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true", HoodieMetadataConfig.ENABLE.key -> "true"),
          GreaterThan(AttributeReference("price", IntegerType)(), Literal(3000)), metaClient, isDataSkippingExpected = true, isNoScanExpected = true)
      }
    }
  }

  /**
   * 1. Create MOR table with compaction enabled.
   * 2. Do an insert and validate the partition stats index initialization.
   * 3. Do an update and validate the partition stats index.
   * 4. Schedule a compaction and validate the partition stats index.
   * 5. Do an update and validate the partition stats index.
   * 6. Complete the compaction and validate the partition stats index.
   */
  test("Test partition stats index with inflight compaction") {
    withTempDir { tmp =>
      val tableName = generateTableName
      val tablePath = s"${tmp.getCanonicalPath}/$tableName"
      spark.sql(
        s"""
           |create table $tableName (
           |  id int,
           |  name string,
           |  price int,
           |  ts long
           |) using hudi
           |partitioned by (ts)
           |tblproperties (
           |  type = 'mor',
           |  primaryKey = 'id',
           |  preCombineField = 'price',
           |  hoodie.metadata.index.partition.stats.enable = 'true',
           |  hoodie.metadata.index.column.stats.enable = 'true',
           |  hoodie.metadata.index.column.stats.column.list = 'price'
           |)
           |location '$tablePath'
           |""".stripMargin
      )

      // insert data
      writeAndValidatePartitionStats(tableName, tablePath)

      // update data
      spark.sql(s"update $tableName set price = price + 1 where id = 6")
      checkAnswer(s"select id, name, price, ts from $tableName where price>3000")(
        Seq(6, "a6", 4002, 30)
      )

      // validate partition stats index after update
      checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
        Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 2000),
        Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 3000),
        Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000, 4002)
      )

      // schedule compaction
      spark.sql(s"refresh table $tableName")
      spark.sql("set hoodie.compact.inline=false")
      spark.sql("set hoodie.compact.inline.max.delta.commits=2")
      spark.sql(s"schedule compaction on $tableName")
      val compactionRows = spark.sql(s"show compaction on $tableName").collect()
      val timestamps = compactionRows.map(_.getString(0))
      assertTrue(timestamps.length == 1)

      // update data
      spark.sql(s"update $tableName set price = price + 1 where id = 6")
      checkAnswer(s"select id, name, price, ts from $tableName where price>3000")(
        Seq(6, "a6", 4003, 30)
      )

      // complete compaction
      // set partition stats related configs
      spark.sql(s"refresh table $tableName")
      spark.sql("set hoodie.metadata.index.partition.stats.enable=true")
      spark.sql("set hoodie.metadata.index.column.stats.enable=true")
      spark.sql("set hoodie.metadata.index.column.stats.column.list=price")
      spark.sql(s"run compaction on $tableName at ${timestamps(0)}")

      // validate partition stats index
      checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value, ColumnStatsMetadata.isTightBound from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
        Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 2000, true),
        Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 3000, true),
        Seq(getPartitionStatsIndexKey("ts=30", "price"), 4003, 4003, true)
      )
    }
  }

  test(s"Test partition stats index on int type field with update and file pruning") {
    Seq("cow", "mor").foreach { tableType =>
      Seq(true, false).foreach { shouldCompact =>
        withTempDir { tmp =>
          val tableName = generateTableName
          val tablePath = s"${tmp.getCanonicalPath}/$tableName"
          spark.sql(
            s"""
               |create table $tableName (
               |  id int,
               |  name string,
               |  price int,
               |  ts long
               |) using hudi
               |partitioned by (ts)
               |tblproperties (
               |  type = '$tableType',
               |  primaryKey = 'id',
               |  preCombineField = 'price',
               |  hoodie.metadata.index.partition.stats.enable = 'true',
               |  hoodie.metadata.index.column.stats.enable = 'true',
               |  hoodie.metadata.index.column.stats.column.list = 'price'
               |)
               |location '$tablePath'
               |""".stripMargin
          )

          // trigger compaction after update and validate stats
          if (tableType == "mor" && shouldCompact) {
            spark.sql("set hoodie.compact.inline=true")
            spark.sql("set hoodie.compact.inline.max.delta.commits=2")
          }
          spark.sql("set hoodie.metadata.enable=true")
          spark.sql("set hoodie.enable.data.skipping=true")
          spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
          writeAndValidatePartitionStats(tableName, tablePath)
          if (tableType == "mor" && shouldCompact) {
            // check partition stats records with tightBound
            checkAnswer(s"select key, ColumnStatsMetadata.minValue.member1.value, ColumnStatsMetadata.maxValue.member1.value, ColumnStatsMetadata.isTightBound from hudi_metadata('$tableName') where type=${MetadataPartitionType.PARTITION_STATS.getRecordType} and ColumnStatsMetadata.columnName='price'")(
              Seq(getPartitionStatsIndexKey("ts=10", "price"), 1000, 2000, true),
              Seq(getPartitionStatsIndexKey("ts=20", "price"), 2000, 3000, true),
              Seq(getPartitionStatsIndexKey("ts=30", "price"), 3000, 4001, true)
            )
          }
        }
      }
    }
  }

  private def writeAndValidatePartitionStats(tableName: String, tablePath: String): Unit = {
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
    spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
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
      HoodieTableMetaClient.reload(metaClient),
      isDataSkippingExpected = true)

    verifyFilePruning(
      Map.apply(DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false", HoodieMetadataConfig.ENABLE.key -> "true"),
      GreaterThan(AttributeReference("price", IntegerType)(), Literal(3000)),
      HoodieTableMetaClient.reload(metaClient),
      isDataSkippingExpected = false)
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression, metaClient: HoodieTableMetaClient,
                                isDataSkippingExpected: Boolean, isNoScanExpected: Boolean = false): Unit = {
    verifyFilePruningExpressions(opts, Seq(dataFilter), metaClient, isDataSkippingExpected, isNoScanExpected)
  }

  private def verifyFilePruningExpressions(opts: Map[String, String], dataFilters: Seq[Expression], metaClient: HoodieTableMetaClient,
                                           isDataSkippingExpected: Boolean, isNoScanExpected: Boolean = false): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> metaClient.getBasePath.toString)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    try {
      val filteredPartitionDirectories = fileIndex.listFiles(Seq(), dataFilters)
      val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
      val latestDataFilesCount = getLatestDataFilesCount(metaClient = metaClient)
      if (isDataSkippingExpected) {
        assertTrue(filteredFilesCount < latestDataFilesCount)
        if (isNoScanExpected) {
          assertTrue(filteredFilesCount == 0)
        } else {
          assertTrue(filteredFilesCount > 0)
        }
      } else {
        assertTrue(filteredFilesCount == latestDataFilesCount)
      }

      // with no data skipping
      fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
      val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), dataFilters).flatMap(s => s.files).size
      assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
    } finally {
      fileIndex.close()
    }
  }

  private def getLatestDataFilesCount(includeLogFiles: Boolean = true, metaClient: HoodieTableMetaClient) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieTableFileSystemView = getTableFileSystemView(metaClient)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().requestedTime)
        .values()
        .forEach(JFunction.toJavaConsumer[java.util.stream.Stream[FileSlice]]
          (slices => slices.forEach(JFunction.toJavaConsumer[FileSlice](
            slice => totalLatestDataFiles += (if (includeLogFiles) slice.getLogFiles.count() else 0)
              + (if (slice.getBaseFile.isPresent) 1 else 0)))))
    } finally {
      fsView.close()
    }
    totalLatestDataFiles
  }

  private def getTableFileSystemView(metaClient: HoodieTableMetaClient): HoodieTableFileSystemView = {
    val metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withMetadataIndexPartitionStats(true).build()
    val metadataTable = new HoodieBackedTableMetadata(new HoodieSparkEngineContext(spark.sparkContext),
      metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)
    new HoodieTableFileSystemView(
      metadataTable,
      metaClient,
      metaClient.getActiveTimeline)
  }

}
