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

import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{FileSlice, HoodieTableType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.{HoodieBackedTableMetadataWriter, HoodieMetadataFileSystemView, SparkHoodieBackedTableMetadataWriter}
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.util.JFunction
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex, HoodieSparkUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Expression, Literal}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.scalatest.Assertions.assertResult

import scala.collection.JavaConverters

/**
 * Test cases for secondary index
 */
@Tag("functional")
class TestSecondaryIndexPruning extends SparkClientFunctionalTestHarness {

  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true"
  )
  val commonOpts: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
    RECORDKEY_FIELD.key -> "record_key_col",
    PARTITIONPATH_FIELD.key -> "partition_key_col",
    HIVE_STYLE_PARTITIONING.key -> "true",
    PRECOMBINE_FIELD.key -> "ts"
  ) ++ metadataOpts
  var mergedDfList: List[DataFrame] = List.empty
  var tableName = "hoodie_"
  var metaClient: HoodieTableMetaClient = _

  override def conf: SparkConf = conf(getSparkSqlConf)

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testSecondaryIndexWithFilters(tableType: String): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_with_filters" + sqlTableType

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  record_key_col string,
           |  not_record_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  type = '$sqlTableType',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")

      // create another secondary index on non-string column
      spark.sql(s"create index idx_ts on $tableName using secondary_index(ts)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_ts"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "ts")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("1", "row1"),
        Seq("2", "row2"),
        Seq("3", "row3"),
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
    }
  }

  @Test
  def testSecondaryIndexPruningWithUpdates(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> HoodieTableType.COPY_ON_WRITE.name(),
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      tableName += "test_secondary_index_pruning_with_updates"

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  record_key_col string,
           |  not_record_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_not_record_key_col on $tableName using secondary_index(not_record_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_not_record_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where not_record_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")

      // update the secondary key column
      spark.sql(s"update $tableName set not_record_key_col = 'xyz' where record_key_col = 'row1'")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey, SecondaryIndexMetadata.isDeleted from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1", true),
        Seq("cde", "row2", false),
        Seq("def", "row3", false),
        Seq("xyz", "row1", false)
      )
      // validate data and data skipping
      checkAnswer(s"select ts, record_key_col, not_record_key_col, partition_key_col from $tableName where record_key_col = 'row1'")(
        Seq(1, "row1", "xyz", "p1")
      )
      verifyQueryPredicate(hudiOpts, "not_record_key_col")
    }
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testSecondaryIndexWithPartitionStatsIndex(tableType: String): Unit = {
    if (HoodieSparkUtils.gteqSpark3_3) {
      var hudiOpts = commonOpts
      hudiOpts = hudiOpts + (
        DataSourceWriteOptions.TABLE_TYPE.key -> tableType,
        DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val sqlTableType = if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name())) "cow" else "mor"
      tableName += "test_secondary_index_with_partition_stats_index" + sqlTableType

      spark.sql(
        s"""
           |create table $tableName (
           |  ts bigint,
           |  name string,
           |  record_key_col string,
           |  secondary_key_col string,
           |  partition_key_col string
           |) using hudi
           | options (
           |  primaryKey ='record_key_col',
           |  type = '$sqlTableType',
           |  hoodie.metadata.enable = 'true',
           |  hoodie.metadata.record.index.enable = 'true',
           |  hoodie.datasource.write.recordkey.field = 'record_key_col',
           |  'hoodie.metadata.index.partition.stats.enable' = 'true',
           |  'hoodie.metadata.index.column.stats.column.list' = 'name',
           |  hoodie.enable.data.skipping = 'true'
           | )
           | partitioned by(partition_key_col)
           | location '$basePath'
       """.stripMargin)
      spark.sql(s"insert into $tableName values(1, 'gandhi', 'row1', 'abc', 'p1')")
      spark.sql(s"insert into $tableName values(2, 'nehru', 'row2', 'cde', 'p2')")
      spark.sql(s"insert into $tableName values(3, 'patel', 'row3', 'def', 'p2')")
      // create secondary index
      spark.sql(s"create index idx_secondary_key_col on $tableName using secondary_index(secondary_key_col)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.builder()
        .setBasePath(basePath)
        .setConf(HoodieTestUtils.getDefaultStorageConf)
        .build()
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_secondary_key_col"))
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
      // validate data skipping with filters on secondary key column
      spark.sql("set hoodie.metadata.enable=true")
      spark.sql("set hoodie.enable.data.skipping=true")
      spark.sql("set hoodie.fileIndex.dataSkippingFailureMode=strict")
      checkAnswer(s"select ts, record_key_col, secondary_key_col, partition_key_col from $tableName where secondary_key_col = 'abc'")(
        Seq(1, "row1", "abc", "p1")
      )
      verifyQueryPredicate(hudiOpts, "secondary_key_col")

      // create another secondary index on non-string column
      spark.sql(s"create index idx_ts on $tableName using secondary_index(ts)")
      // validate index created successfully
      metaClient = HoodieTableMetaClient.reload(metaClient)
      assert(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_ts"))
      // validate data skipping
      verifyQueryPredicate(hudiOpts, "ts")
      // validate the secondary index records themselves
      checkAnswer(s"select key, SecondaryIndexMetadata.recordKey from hudi_metadata('$basePath') where type=7")(
        Seq("1", "row1"),
        Seq("2", "row2"),
        Seq("3", "row3"),
        Seq("abc", "row1"),
        Seq("cde", "row2"),
        Seq("def", "row3")
      )
    }
  }

  private def checkAnswer(query: String)(expects: Seq[Any]*): Unit = {
    assertResult(expects.map(row => Row(row: _*)).toArray.sortBy(_.toString()))(spark.sql(query).collect().sortBy(_.toString()))
  }

  private def verifyQueryPredicate(hudiOpts: Map[String, String], columnName: String): Unit = {
    mergedDfList = spark.read.format("hudi").options(hudiOpts).load(basePath).repartition(1).cache() :: mergedDfList
    val secondaryKey = mergedDfList.last.limit(1).collect().map(row => row.getAs(columnName).toString)
    val dataFilter = EqualTo(attribute(columnName), Literal(secondaryKey(0)))
    verifyFilePruning(hudiOpts, dataFilter)
  }

  private def attribute(partition: String): AttributeReference = {
    AttributeReference(partition, StringType, nullable = true)()
  }

  private def verifyFilePruning(opts: Map[String, String], dataFilter: Expression): Unit = {
    // with data skipping
    val commonOpts = opts + ("path" -> basePath)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts, includeLogFiles = true)
    val filteredPartitionDirectories = fileIndex.listFiles(Seq(), Seq(dataFilter))
    val filteredFilesCount = filteredPartitionDirectories.flatMap(s => s.files).size
    val latestDataFilesCount = getLatestDataFilesCount(opts)
    assertTrue(filteredFilesCount > 0 && filteredFilesCount < latestDataFilesCount)

    // with no data skipping
    fileIndex = HoodieFileIndex(spark, metaClient, None, commonOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"), includeLogFiles = true)
    val filesCountWithNoSkipping = fileIndex.listFiles(Seq(), Seq(dataFilter)).flatMap(s => s.files).size
    assertTrue(filesCountWithNoSkipping == latestDataFilesCount)
  }

  private def getLatestDataFilesCount(opts: Map[String, String], includeLogFiles: Boolean = true) = {
    var totalLatestDataFiles = 0L
    val fsView: HoodieMetadataFileSystemView = getTableFileSystemView(opts)
    try {
      fsView.getAllLatestFileSlicesBeforeOrOn(metaClient.getActiveTimeline.lastInstant().get().getTimestamp)
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

  private def getTableFileSystemView(opts: Map[String, String]): HoodieMetadataFileSystemView = {
    new HoodieMetadataFileSystemView(metaClient, metaClient.getActiveTimeline, metadataWriter(getWriteConfig(opts)).getTableMetadata)
  }

  private def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(JavaConverters.mapAsJavaMapConverter(hudiOpts).asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath)
      .build()
  }

  private def metadataWriter(clientConfig: HoodieWriteConfig): HoodieBackedTableMetadataWriter[_] = SparkHoodieBackedTableMetadataWriter.create(
    storageConf, clientConfig, new HoodieSparkEngineContext(jsc)).asInstanceOf[HoodieBackedTableMetadataWriter[_]]
}
