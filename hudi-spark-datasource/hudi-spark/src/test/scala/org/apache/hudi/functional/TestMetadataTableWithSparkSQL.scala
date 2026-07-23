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

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.SparkRDDWriteClient
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableMetaClient, HoodieTableVersion, TableSchemaResolver}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.core.transaction.lock.InProcessLockProvider
import org.apache.hudi.metadata.HoodieMetadataPayload.SECONDARY_INDEX_RECORD_KEY_SEPARATOR
import org.apache.hudi.metadata.MetadataPartitionType
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness.getSparkSqlConf
import org.apache.hudi.testutils.SparkClientFunctionalTestHarnessScala

import org.apache.spark.SparkConf
import org.junit.jupiter.api.{BeforeEach, Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}

import scala.collection.JavaConverters.mapAsJavaMapConverter

/**
 * Exercises {@code SparkHoodieBackedTableMetadataWriter} and its table-version-six
 * implementation through real Spark writes.
 */
@Tag("functional-c")
class TestMetadataTableWithSparkSQL extends SparkClientFunctionalTestHarnessScala {

  override def conf: SparkConf = conf(getSparkSqlConf)

  @BeforeEach
  override def runBeforeEach(): Unit = {
    super.runBeforeEach()
    spark.sql(s"set ${HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key()} = ${classOf[InProcessLockProvider].getName}")
  }

  @Test
  def testAllMetadataIndexesAcrossUpsertAndRollback(): Unit = {
    // Validate the full MDT lifecycle against a current-version COW table.
    val tableName = "metadata_writer_all_indexes"
    val tablePath = s"$basePath/$tableName"
    val writeOptions = metadataWriteOptions(tableName, streamingWrites = true)

    // Bootstrap all supported MDT partitions through real writes.
    createTable(tableName, tablePath, tableVersion = None)
    spark.sql(
      s"""insert into $tableName values
         |  (1, 'row1', 'alpha', 'p1'),
         |  (2, 'row2', 'beta', 'p1'),
         |  (3, 'row3', 'gamma', 'p2')
         |""".stripMargin)
    spark.sql(s"create index idx_rider on $tableName (rider)")

    var metaClient = createMetaClient(tablePath)
    assertMetadataPartitions(metaClient, includePartitionStats = true, includeSecondaryIndex = true)
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 3, includePartitionStats = true)
    // Metadata payload type 7 stores secondary-index mappings.
    checkAnswer(s"select key from hudi_metadata('$tablePath') where type=7")(
      Seq(s"alpha${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
      Seq(s"beta${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
      Seq(s"gamma${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
    )

    // Rebuild the secondary index and verify its records are removed and restored.
    spark.sql(s"drop index idx_rider on $tableName")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains("secondary_index_idx_rider"))
    assertEquals(0L, spark.sql(s"select key from hudi_metadata('$tablePath') where type=7").count())

    spark.sql(s"create index idx_rider on $tableName (rider)")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertMetadataPartitions(metaClient, includePartitionStats = true, includeSecondaryIndex = true)
    checkAnswer(s"select key from hudi_metadata('$tablePath') where type=7")(
      Seq(s"alpha${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
      Seq(s"beta${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
      Seq(s"gamma${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
    )

    // Upsert changes both data and the secondary-index key.
    spark.sql(s"update $tableName set rider = 'delta', ts = 4 where id = 'row1'")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val upsertInstant = metaClient.getActiveTimeline.getCommitsTimeline
      .filterCompletedInstants.lastInstant().get().requestedTime()

    checkAnswer(s"select id, rider, part from $tableName order by id")(
      Seq("row1", "delta", "p1"),
      Seq("row2", "beta", "p1"),
      Seq("row3", "gamma", "p2")
    )
    checkAnswer(s"select key from hudi_metadata('$tablePath') where type=7")(
      Seq(s"delta${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
      Seq(s"beta${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
      Seq(s"gamma${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
    )
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 3, includePartitionStats = true)

    // Rollback must restore data and all MDT indexes.
    rollback(metaClient, writeOptions, upsertInstant)
    spark.catalog.refreshTable(tableName)
    metaClient = HoodieTableMetaClient.reload(metaClient)

    checkAnswer(s"select id, rider, part from $tableName order by id")(
      Seq("row1", "alpha", "p1"),
      Seq("row2", "beta", "p1"),
      Seq("row3", "gamma", "p2")
    )
    assertMetadataPartitions(metaClient, includePartitionStats = true, includeSecondaryIndex = true)
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 3, includePartitionStats = true)
    checkAnswer(s"select key from hudi_metadata('$tablePath') where type=7")(
      Seq(s"alpha${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row1"),
      Seq(s"beta${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row2"),
      Seq(s"gamma${SECONDARY_INDEX_RECORD_KEY_SEPARATOR}row3")
    )
  }

  @Test
  def testTableVersionSixMetadataWritesAndRollback(): Unit = {
    // Validate the legacy writer with the indexes supported by table version 6.
    val tableName = "metadata_writer_v6"
    val tablePath = s"$basePath/$tableName"
    val writeOptions = metadataWriteOptions(tableName, streamingWrites = false) +
      (HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> HoodieTableVersion.SIX.versionCode().toString)

    // Version 6 uses the legacy writer and supports a smaller index set.
    createTable(tableName, tablePath, tableVersion = Some(HoodieTableVersion.SIX.versionCode()))
    spark.sql(
      s"""insert into $tableName values
         |  (1, 'row1', 'alpha', 'p1'),
         |  (2, 'row2', 'beta', 'p2')
         |""".stripMargin)

    var metaClient = createMetaClient(tablePath)
    val metadataMetaClient = createMetaClient(metaClient.getMetaPath + "/metadata")
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig.getTableVersion)
    assertEquals(HoodieTableVersion.SIX, metadataMetaClient.getTableConfig.getTableVersion)
    assertMetadataPartitions(metaClient, includePartitionStats = false, includeSecondaryIndex = false)
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 2, includePartitionStats = false)

    spark.sql(s"update $tableName set rider = 'updated', ts = 3 where id = 'row1'")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    val upsertInstant = metaClient.getActiveTimeline.getCommitsTimeline
      .filterCompletedInstants.lastInstant().get().requestedTime()
    checkAnswer(s"select id, rider from $tableName order by id")(
      Seq("row1", "updated"),
      Seq("row2", "beta")
    )
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 2, includePartitionStats = false)

    rollback(metaClient, writeOptions, upsertInstant)
    spark.catalog.refreshTable(tableName)
    metaClient = HoodieTableMetaClient.reload(metaClient)

    checkAnswer(s"select id, rider from $tableName order by id")(
      Seq("row1", "alpha"),
      Seq("row2", "beta")
    )
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig.getTableVersion)
    assertMetadataPartitions(metaClient, includePartitionStats = false, includeSecondaryIndex = false)
    assertCommonMetadataRecords(tablePath, expectedRecordIndexCount = 2, includePartitionStats = false)

    // Verify record-index deletion and bootstrap on the legacy table.
    spark.sql(s"drop index record_index on $tableName")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertFalse(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertEquals(0L, spark.sql(s"select key from hudi_metadata('$tablePath') where type=5").count())

    spark.sql(s"create index record_index on $tableName (id)")
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getTableConfig.getMetadataPartitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertEquals(2L, spark.sql(s"select key from hudi_metadata('$tablePath') where type=5").count())
  }

  private def createTable(
      tableName: String,
      tablePath: String,
      tableVersion: Option[Int]): Unit = {
    val tableVersionOption = tableVersion
      .map(version => s"hoodie.write.table.version = '$version',")
      .getOrElse("")
    val streamingWrites = tableVersion.isEmpty
    spark.sql(
      s"""
         |create table $tableName (
         |  ts bigint,
         |  id string,
         |  rider string,
         |  part string
         |) using hudi
         | options (
         |  primaryKey = 'id',
         |  orderingFields = 'ts',
         |  type = 'cow',
         |  $tableVersionOption
         |  hoodie.metadata.enable = 'true',
         |  hoodie.metadata.index.column.stats.enable = 'true',
         |  hoodie.metadata.index.partition.stats.enable = 'true',
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.metadata.streaming.write.enabled = '$streamingWrites',
         |  hoodie.metadata.record.preparation.parallelism = '1',
         |  hoodie.metrics.on = 'true',
         |  hoodie.metrics.reporter.type = 'CONSOLE',
         |  hoodie.metrics.executor.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'id',
         |  hoodie.datasource.write.partitionpath.field = 'part',
         |  hoodie.datasource.write.payload.class =
         |    'org.apache.hudi.common.model.OverwriteWithLatestAvroPayload'
         | )
         | partitioned by (part)
         | location '$tablePath'
         |""".stripMargin)
  }

  private def metadataWriteOptions(tableName: String, streamingWrites: Boolean): Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key() -> tableName,
    TABLE_TYPE.key() -> HoodieTableType.COPY_ON_WRITE.name(),
    RECORDKEY_FIELD.key() -> "id",
    PARTITIONPATH_FIELD.key() -> "part",
    PRECOMBINE_FIELD.key() -> "ts",
    HoodieMetadataConfig.ENABLE.key() -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key() -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key() -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key() -> "true",
    HoodieMetadataConfig.STREAMING_WRITE_ENABLED.key() -> streamingWrites.toString,
    HoodieMetadataConfig.RECORD_PREPARATION_PARALLELISM.key() -> "1",
    "hoodie.metrics.on" -> "true",
    "hoodie.metrics.reporter.type" -> "CONSOLE",
    "hoodie.metrics.executor.enable" -> "true",
    HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> classOf[InProcessLockProvider].getName
  )

  private def createMetaClient(tablePath: String): HoodieTableMetaClient =
    HoodieTableMetaClient.builder()
      .setBasePath(tablePath)
      .setConf(HoodieTestUtils.getDefaultStorageConf)
      .build()

  private def rollback(
      metaClient: HoodieTableMetaClient,
      writeOptions: Map[String, String],
      instantTime: String): Unit = {
    val props = TypedProperties.fromMap(writeOptions.asJava)
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(metaClient.getBasePath)
      .withSchema(new TableSchemaResolver(metaClient).getTableSchema(false).toString)
      .withProps(props)
      .withEmbeddedTimelineServerEnabled(false)
      .build()
    val writeClient = new SparkRDDWriteClient(new HoodieSparkEngineContext(jsc), writeConfig)
    try {
      assertTrue(writeClient.rollback(instantTime))
    } finally {
      writeClient.close()
    }
  }

  private def assertMetadataPartitions(
      metaClient: HoodieTableMetaClient,
      includePartitionStats: Boolean,
      includeSecondaryIndex: Boolean): Unit = {
    val partitions = metaClient.getTableConfig.getMetadataPartitions
    assertTrue(partitions.contains(MetadataPartitionType.FILES.getPartitionPath))
    assertTrue(partitions.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath))
    assertTrue(partitions.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    if (includePartitionStats) {
      assertTrue(partitions.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath))
    }
    if (includeSecondaryIndex) {
      assertTrue(partitions.contains("secondary_index_idx_rider"))
    }
  }

  private def assertCommonMetadataRecords(
      tablePath: String,
      expectedRecordIndexCount: Long,
      includePartitionStats: Boolean): Unit = {
    // Payload types 3, 6, and 5 represent column stats, partition stats, and record index.
    assertTrue(spark.sql(s"select key from hudi_metadata('$tablePath') where type=3").count() > 0)
    if (includePartitionStats) {
      assertTrue(spark.sql(s"select key from hudi_metadata('$tablePath') where type=6").count() > 0)
    }
    assertEquals(
      expectedRecordIndexCount,
      spark.sql(s"select key from hudi_metadata('$tablePath') where type=5").count())
  }

  private def checkAnswer(query: String)(expected: Seq[Any]*): Unit = {
    val expectedRows = expected.map(_.mkString("|")).sorted.toList
    val actualRows = spark.sql(query).collect().map(_.toSeq.mkString("|")).sorted.toList
    assertEquals(expectedRows, actualRows)
  }
}
