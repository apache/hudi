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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.client.transaction.lock.InProcessLockProvider
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.WriteConcurrencyMode
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.{HoodieIndexConfig, HoodieLockConfig, HoodieWriteConfig}
import org.apache.hudi.core.index.record.HoodieRecordIndex
import org.apache.hudi.exception.HoodieMetadataIndexException
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, MetadataBuilder, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

import java.io.File

import scala.collection.JavaConverters._

/**
 * End-to-end coverage for the MDT-backed IVF RaBitQ vector index: CREATE INDEX through Spark SQL,
 * then hudi_vector_search against brute force.
 *
 * The fixture has two well-separated groups and the queries probe every cluster, so exact rerank
 * must match brute force exactly. Results are ordered by (distance, key) and distances are compared
 * with an epsilon so ties and floating-point noise cannot make the test flaky.
 */
class TestHoodieVectorIndexBootstrapSearch extends HoodieSparkClientTestBase {

  private val IndexName = "vec_idx"
  private val IndexPartition = s"vector_index_$IndexName"
  private val NumClusters = 2
  private val DistanceEpsilon = 1e-4
  private val GlobalRecordIndex = Map(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true")
  private val NoRecordIndex = Map(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false")
  // A partitioned RLI is keyed by (partition, record key); several RLI file groups per partition
  // exercise the per-file-group lookup.
  private val PartitionedRecordIndex = Map(
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "4")
  // NBCC with a single bucket writes every partition's bucket under the same fixed file id.
  private val NbccSingleBucketIndex = Map(
    HoodieIndexConfig.INDEX_TYPE.key -> HoodieIndex.IndexType.BUCKET.name,
    HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key -> "1",
    HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key -> WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL.name,
    HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key -> classOf[InProcessLockProvider].getName)

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @CsvSource(Array(
    // metric, rabitq bits
    "l2, 4",
    "l2, 1",
    "cosine, 1",
    "dot_product, 1"))
  def testSearchAfterBootstrap(metric: String, bits: String): Unit = {
    val (tableName, tablePath) = createTableWithRows(s"bootstrap_${metric}_$bits", GlobalRecordIndex)
    createIndex(tableName, Map("vector.metric" -> metric, "vector.rabitq.bits" -> bits))
    assertTrue(metadataPartitions(tablePath).contains(IndexPartition))

    Seq(Array(10.0, 10.0), Array(-10.0, -10.0)).foreach { query =>
      val bruteForce = search(tableName, query, 6, metric, "brute_force", "")
      val exact = search(tableName, query, 6, metric, "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.query.mode=exact_rerank")
      assertEquals(bruteForce.map(_._1), exact.map(_._1))
      bruteForce.zip(exact).foreach { case ((_, expected), (_, actual)) =>
        assertEquals(expected, actual, DistanceEpsilon)
      }

      // Single-bit residual scoring estimates distances between residuals, which ranks correctly only
      // for l2; cosine and dot product are covered by exact rerank above.
      if (metric == "l2") {
        val approximate = search(tableName, query, 3, metric, "ivf_rabitq_mdt",
          s"vector.query.nprobes=$NumClusters,vector.query.mode=approximate")
        assertEquals(bruteForce.take(3).map(_._1).toSet, approximate.map(_._1).toSet)
      }
    }
  }

  @Test
  def testCreateIndexWithDefaultOptions(): Unit = {
    val (tableName, tablePath) = createTableWithRows("defaults", GlobalRecordIndex)
    createIndex(tableName, Map.empty)
    assertTrue(metadataPartitions(tablePath).contains(IndexPartition))
  }

  /**
   * Partitioned record index with several RLI file groups per data partition: a partitioned RLI
   * lookup reads one file group per batch, so finalists must be grouped by partition AND RLI file
   * group or live rows are dropped as deleted.
   */
  @Test
  def testSearchWithPartitionedRecordIndex(): Unit = {
    val (tableName, tablePath) = createTableWithRows("partitioned_rli", PartitionedRecordIndex, partitioned = true)
    assertTrue(HoodieTestUtils.createMetaClient(storageConf, tablePath)
      .getIndexForMetadataPartition(PARTITION_NAME_RECORD_INDEX)
      .map[Boolean](indexDefinition => HoodieRecordIndex.isPartitioned(indexDefinition)).orElse(false),
      "fixture must build a partitioned record index")
    createIndex(tableName, Map("vector.metric" -> "l2", "vector.rabitq.bits" -> "4"))

    Seq(Array(10.0, 10.0), Array(-10.0, -10.0)).foreach { query =>
      val bruteForce = search(tableName, query, 6, "l2", "brute_force", "")
      val exact = search(tableName, query, 6, "l2", "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.query.mode=exact_rerank")
      assertEquals(bruteForce.map(_._1), exact.map(_._1))
      val approximate = search(tableName, query, 3, "l2", "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.query.mode=approximate")
      assertEquals(bruteForce.take(3).map(_._1).toSet, approximate.map(_._1).toSet)
    }
  }

  /**
   * NBCC bucket-index tables reuse one fixed file id per bucket in every partition, so exact rerank
   * must resolve file slices by partition-qualified file group id, not by bare file id.
   */
  @Test
  def testExactSearchWithFileIdsReusedAcrossPartitions(): Unit = {
    val (tableName, tablePath) = createTableWithRows("nbcc_bucket", GlobalRecordIndex ++ NbccSingleBucketIndex,
      partitioned = true, tableType = "mor", writeOptions = Map(OPERATION.key -> BULK_INSERT_OPERATION_OPT_VAL))
    val sharedFileIds = Seq("label=left", "label=right").map(baseFileIds(tablePath, _)).reduce(_ intersect _)
    assertTrue(sharedFileIds.nonEmpty, "fixture must reuse a file id across partitions")
    createIndex(tableName, Map("vector.metric" -> "l2", "vector.rabitq.bits" -> "4"))

    Seq(Array(10.0, 10.0), Array(-10.0, -10.0)).foreach { query =>
      val bruteForce = search(tableName, query, 6, "l2", "brute_force", "")
      val exact = search(tableName, query, 6, "l2", "ivf_rabitq_mdt",
        s"vector.query.nprobes=$NumClusters,vector.query.mode=exact_rerank")
      assertEquals(bruteForce.map(_._1), exact.map(_._1))
      bruteForce.zip(exact).foreach { case ((_, expected), (_, actual)) =>
        assertEquals(expected, actual, DistanceEpsilon)
      }
    }
  }

  @Test
  def testCreateIndexRequiresRecordIndex(): Unit = {
    val (tableName, tablePath) = createTableWithRows("no_record_index", NoRecordIndex)
    val exception = assertThrows(classOf[Exception], () => createIndex(tableName, Map.empty))
    assertTrue(causes(exception).exists(_.isInstanceOf[HoodieMetadataIndexException]), s"unexpected failure: $exception")
    assertFalse(indexDefinitions(tablePath).contains(IndexPartition))
  }

  /**
   * Creates a table partitioned by `label` when `partitioned`, applying `tableOptions` to both the
   * table definition and the write, and `writeOptions` to the write only.
   */
  private def createTableWithRows(
      suffix: String,
      tableOptions: Map[String, String],
      partitioned: Boolean = false,
      tableType: String = "cow",
      writeOptions: Map[String, String] = Map.empty): (String, String) = {
    val tableName = s"vector_search_$suffix"
    val tablePath = s"$basePath/$tableName"
    val tableOptionSql = tableOptions.map { case (key, value) => s"$key = '$value'" }.mkString(",\n  ")
    val partitionClause = if (partitioned) "PARTITIONED BY (label)" else ""
    spark.sql(
      s"""
         |CREATE TABLE $tableName (
         |  id STRING,
         |  ts BIGINT,
         |  embedding VECTOR(2),
         |  label STRING
         |) USING hudi
         |OPTIONS (
         |  primaryKey = 'id',
         |  preCombineField = 'ts',
         |  type = '$tableType',
         |  hoodie.metadata.enable = 'true',
         |  $tableOptionSql
         |)
         |$partitionClause
         |LOCATION '$tablePath'
         |""".stripMargin)

    val metadata = new MetadataBuilder().putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(2)").build()
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata),
      StructField("label", StringType, nullable = false)))
    val rows = Seq(
      Row("left-1", 1L, Seq(-10.2f, -10.1f), "left"),
      Row("left-2", 1L, Seq(-10.0f, -9.6f), "left"),
      Row("left-3", 1L, Seq(-9.5f, -10.0f), "left"),
      Row("right-1", 1L, Seq(10.0f, 9.7f), "right"),
      Row("right-2", 1L, Seq(9.4f, 10.0f), "right"),
      Row("right-3", 1L, Seq(10.6f, 10.3f), "right"))
    val partitionOptions = if (partitioned) Map(PARTITIONPATH_FIELD.key -> "label") else Map.empty[String, String]
    val writeTableType = if (tableType == "mor") MOR_TABLE_TYPE_OPT_VAL else COW_TABLE_TYPE_OPT_VAL
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.format("hudi")
      .option(TABLE_NAME.key, tableName)
      .option(TABLE_TYPE.key, writeTableType)
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "ts")
      .option(HoodieMetadataConfig.ENABLE.key, "true")
      .options(tableOptions ++ partitionOptions ++ writeOptions)
      .mode(SaveMode.Append)
      .save(tablePath)
    (tableName, tablePath)
  }

  private def createIndex(tableName: String, options: Map[String, String]): Unit = {
    val allOptions = options ++ Map(
      "vector.num_clusters" -> NumClusters.toString,
      "vector.query.nprobes" -> NumClusters.toString,
      "vector.max_iter" -> "10",
      "vector.rabitq.seed" -> "42")
    val optionSql = allOptions.map { case (key, value) => s"`$key` = '$value'" }.mkString(", ")
    spark.sql(s"CREATE INDEX $IndexName ON $tableName USING VECTOR (embedding) OPTIONS ($optionSql)")
  }

  private def search(
      tableName: String,
      query: Array[Double],
      k: Int,
      metric: String,
      algorithm: String,
      runtimeOptions: String): Seq[(String, Double)] = {
    // Approximate search returns index-side columns only; the other modes return corpus columns.
    val keyColumn = if (runtimeOptions.contains("approximate")) "_hoodie_record_key" else "id"
    val optionArg = if (runtimeOptions.isEmpty) "" else s", '$runtimeOptions'"
    spark.sql(
      s"""
         |SELECT $keyColumn AS id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$tableName', 'embedding', ${query.mkString("ARRAY(", ",", ")")}, $k, '$metric', '$algorithm'$optionArg
         |)
         |""".stripMargin)
      .collect()
      .map(row => row.getAs[String]("id") -> row.getAs[Double]("_hudi_distance"))
      .sortBy { case (key, distance) => (distance, key) }
      .toSeq
  }

  private def baseFileIds(tablePath: String, partition: String): Set[String] =
    Option(new File(s"${tablePath.stripPrefix("file:")}/$partition").listFiles()).toSeq.flatten
      .map(_.getName).filter(_.endsWith(".parquet")).map(FSUtils.getFileId).toSet

  private def metadataPartitions(tablePath: String): Set[String] =
    HoodieTestUtils.createMetaClient(storageConf, tablePath).getTableConfig.getMetadataPartitions.asScala.toSet

  private def indexDefinitions(tablePath: String): Set[String] =
    HoodieTestUtils.createMetaClient(storageConf, tablePath).getIndexMetadata
      .map[Set[String]](metadata => metadata.getIndexDefinitions.keySet.asScala.toSet)
      .orElse(Set.empty[String])

  private def causes(throwable: Throwable): Seq[Throwable] =
    Iterator.iterate(throwable)(_.getCause).takeWhile(_ != null).toSeq
}
