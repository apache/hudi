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
import org.apache.hudi.avro.model.HoodieVectorIndexSourceInstantMarker
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.index.vector.{PostingBlockView, VectorIndexMdtSearchUtils, VectorIndexMetadataCache}
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieMetadataPayload, RawKey, VectorIndexMetadataKey, VectorPostingPrefixRawKey}
import org.apache.hudi.testutils.HoodieSparkClientTestBase

import org.apache.avro.generic.GenericRecord
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, FloatType, LongType, MetadataBuilder, StringType, StructField, StructType}
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotEquals, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util.Collections

import scala.collection.JavaConverters._

/**
 * Production-path lifecycle coverage for the MDT-backed IVF RaBitQ index.
 *
 * The fixture deliberately probes every cluster. Exact-mode equality with brute force is therefore
 * a plumbing invariant, not a recall expectation. Results are ordered by (distance, record key) and
 * distances use an epsilon so ties and floating-point noise cannot make the test flaky.
 *
 * Dispatch is witnessed before any query: every ordinary DataFrame write must leave its source
 * instant marker in the MDT and advance the active manifest frontier. The test never calls a vector
 * updater directly and never substitutes a test-only metadata writer.
 */
class TestHoodieVectorIndexSearch extends HoodieSparkClientTestBase {

  private val IndexName = "vec_lifecycle"
  private val IndexPartition = s"vector_index_$IndexName"
  private val Generation = 1
  private val NumClusters = 2
  private val DistanceEpsilon = 1e-5

  private var spark: SparkSession = _

  @BeforeEach
  override def setUp(): Unit = {
    initPath()
    initSparkContexts()
    spark = sqlContext.sparkSession
    spark.sql("set hoodie.write.lock.provider = org.apache.hudi.client.transaction.lock.InProcessLockProvider")
    initTestDataGenerator()
    initHoodieStorage()
  }

  @AfterEach
  override def tearDown(): Unit = {
    cleanupSparkContexts()
    cleanupTestDataGenerator()
    cleanupFileSystem()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("COPY_ON_WRITE", "MERGE_ON_READ"))
  def testProductionWriteDispatchAndClusterMovingUpdate(tableType: String): Unit = {
    val tableName = s"vector_lifecycle_${tableType.toLowerCase}"
    val tablePath = s"$basePath/$tableName"
    createTable(tableName, tablePath, tableType)

    val writeOptions = productionWriteOptions(tableName, tableType)
    writeRows(tablePath, fixtureRows, vectorSchema, writeOptions, SaveMode.Append)
    val bootstrapInstant = latestSourceWriteInstant(tablePath)

    spark.sql(
      s"""
         |CREATE INDEX $IndexName ON $tableName
         |USING VECTOR (embedding)
         |OPTIONS (
         |  `vector.num_clusters` = '$NumClusters',
         |  `vector.query.nprobes` = '$NumClusters',
         |  `vector.metric` = 'L2',
         |  `vector.max_iter` = '10',
         |  `vector.rabitq.seed` = '42'
         |)
         |""".stripMargin)

    // Bootstrap coverage is anchored by the manifest baseline, not by a synthetic per-instant marker.
    assertBootstrapEvidence(tablePath, bootstrapInstant)

    val oldCluster = bootstrapClusterFor("move-me", tablePath)

    writeRows(
      tablePath,
      Seq(Row("move-me", 2L, Seq(10.2f, 10.1f), "moved")),
      vectorSchema,
      writeOptions,
      SaveMode.Append)
    val updateInstant = latestSourceWriteInstant(tablePath)

    // This is the proof that an ordinary DataFrame upsert reached VectorIndexer.buildUpdate through
    // HoodieBackedTableMetadataWriter. It intentionally runs before either indexed query below.
    assertNotEquals(bootstrapInstant, updateInstant)
    assertDispatchEvidence(tablePath, updateInstant)
    assertClusterMovePersisted(tablePath, "move-me", oldCluster)

    spark.catalog.refreshTable(tableName)
    // Exact rerank now materializes MOR log-resident candidates (the moved record lives in an
    // uncompacted log block) via the merged base+log read, so exact == brute-force holds for both
    // table types, including the log-resident "move-me" row.
    assertExactMatchesFullCoverageBruteForce(tableName, Array(10.0, 10.0), 6)
    assertOldClusterProbeSuppressesMovedPosting(tableName)

    // Empty source writes are still source instants. The marker proves dispatch was unconditional;
    // no posting delta is needed for this commit.
    val postingsBeforeNoOp = postingRecordsFor("move-me", tablePath).map(_.getRecordKey).toSet
    val empty = spark.read.format("hudi").load(tablePath)
      .select("id", "ts", "embedding", "label").limit(0)
    empty.write.format("hudi")
      .options(writeOptions)
      .option(HoodieWriteConfig.ALLOW_EMPTY_COMMIT.key, "true")
      .mode(SaveMode.Append)
      .save(tablePath)
    val noOpInstant = latestSourceWriteInstant(tablePath)

    assertNotEquals(updateInstant, noOpInstant)
    assertDispatchEvidence(tablePath, noOpInstant)
    assertEquals(postingsBeforeNoOp, postingRecordsFor("move-me", tablePath).map(_.getRecordKey).toSet)
  }

  private def createTable(tableName: String, tablePath: String, tableType: String): Unit = {
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
         |  hoodie.metadata.record.index.enable = 'true',
         |  hoodie.datasource.write.recordkey.field = 'id',
         |  hoodie.datasource.write.precombine.field = 'ts'
         |)
         |LOCATION '$tablePath'
         |""".stripMargin)
  }

  private def productionWriteOptions(tableName: String, tableType: String): Map[String, String] = Map(
    TABLE_NAME.key -> tableName,
    TABLE_TYPE.key -> tableType,
    RECORDKEY_FIELD.key -> "id",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    "hoodie.write.lock.provider" -> "org.apache.hudi.client.transaction.lock.InProcessLockProvider")

  private def vectorSchema: StructType = {
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, "VECTOR(2)")
      .build()
    StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("ts", LongType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata),
      StructField("label", StringType, nullable = false)))
  }

  private def fixtureRows: Seq[Row] = Seq(
    Row("move-me", 1L, Seq(-10.2f, -10.1f), "moves between clusters"),
    Row("left-1", 1L, Seq(-10.0f, -9.8f), "left"),
    Row("left-2", 1L, Seq(-9.7f, -10.0f), "left"),
    Row("right-1", 1L, Seq(10.0f, 9.8f), "right"),
    Row("right-2", 1L, Seq(9.7f, 10.0f), "right"),
    Row("right-3", 1L, Seq(10.3f, 10.2f), "right"))

  private def writeRows(
      tablePath: String,
      rows: Seq[Row],
      schema: StructType,
      options: Map[String, String],
      mode: SaveMode): Unit = {
    spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
      .write.format("hudi")
      .options(options)
      .mode(mode)
      .save(tablePath)
  }

  private def latestSourceWriteInstant(tablePath: String): String = {
    val metaClient = metaClientFor(tablePath)
    metaClient.getActiveTimeline.getWriteTimeline.filterCompletedInstants.lastInstant.get.requestedTime
  }

  private def assertBootstrapEvidence(tablePath: String, bootstrapInstant: String): Unit = {
    withMetadataTable(tablePath) { metadataTable =>
      val markers = records(metadataTable, () => VectorIndexMetadataKey.sourceInstantMarkerPrefix(Generation))
        .flatMap(vectorMetadata)
        .filter(isVectorMetadata(_, "HoodieVectorIndexSourceInstantMarker"))
      assertTrue(markers.isEmpty,
        "bootstrap snapshot coverage must not be represented by per-instant source markers")

      val manifests = records(metadataTable, () => VectorIndexMetadataKey.manifest(Generation))
        .flatMap(vectorMetadata)
        .filter(isVectorMetadata(_, "HoodieVectorIndexManifest"))
        .map(VectorIndexMetadataCache.asManifest)
      assertEquals(1, manifests.size)
      assertEquals(bootstrapInstant, manifests.head.getBootstrapBaseline.toString,
        "manifest baseline does not match the source snapshot represented by bootstrap")
      assertEquals(bootstrapInstant, manifests.head.getLastContiguousSourceInstant.toString,
        "fresh generation frontier must begin at its bootstrap baseline")
    }
  }

  private def assertDispatchEvidence(tablePath: String, instant: String): Unit = {
    withMetadataTable(tablePath) { metadataTable =>
      val marker = records(metadataTable, () => VectorIndexMetadataKey.sourceInstantMarker(Generation, instant))
        .flatMap(vectorMetadata)
        .filter(isVectorMetadata(_, "HoodieVectorIndexSourceInstantMarker"))
      assertEquals(1, marker.size, s"ordinary write instant $instant did not reach vector-index maintenance")
      assertEquals(instant, vectorStringField(marker.head, "sourceInstant"))

      val manifests = records(metadataTable, () => VectorIndexMetadataKey.manifest(Generation))
        .flatMap(vectorMetadata)
        .filter(isVectorMetadata(_, "HoodieVectorIndexManifest"))
        .map(VectorIndexMetadataCache.asManifest)
      assertEquals(1, manifests.size)
      assertEquals(instant, manifests.head.getLastContiguousSourceInstant.toString,
        s"manifest frontier did not advance through dispatched source instant $instant")
    }
  }

  private def assertClusterMovePersisted(tablePath: String, recordKey: String, oldCluster: Int): Unit = {
    val recordsAfter = postingRecordsFor(recordKey, tablePath)
    val tombstoneClusters = recordsAfter
      .filter(record => vectorMetadata(record).exists(isVectorMetadata(_, "HoodieVectorIndexTombstone")))
      .map(record => VectorIndexMetadataKey.postingClusterId(record.getRecordKey))
      .toSet
    assertEquals(1, tombstoneClusters.size,
      s"cluster-moving update did not leave exactly one old-cluster tombstone")
    assertEquals(oldCluster, tombstoneClusters.head,
      "derived tombstone did not target the record's bootstrap cluster")

    val liveClusters = recordsAfter
      .filter(record => vectorMetadata(record).exists(isVectorMetadata(_, "HoodieVectorIndexPostingDelta")))
      .map(record => VectorIndexMetadataKey.postingClusterId(record.getRecordKey))
      .toSet
    assertEquals(1, liveClusters.size)
    assertNotEquals(tombstoneClusters.head, liveClusters.head,
      s"fixture failed to force '$recordKey' across a centroid boundary")
  }

  private def assertExactMatchesFullCoverageBruteForce(
      tableName: String,
      query: Array[Double],
      k: Int): Unit = {
    val bruteForce = search(tableName, query, k, "brute_force", "")
    val exact = search(
      tableName,
      query,
      k,
      "ivf_rabitq_mdt",
      s"vector.query.nprobes=$NumClusters,vector.query.mode=exact_rerank,vector.fetch.verify.keys=true")

    assertEquals(bruteForce.map(_._1), exact.map(_._1))
    assertEquals(k, exact.map(_._1).distinct.size)
    assertEquals(1, exact.count(_._1 == "move-me"))
    bruteForce.zip(exact).foreach { case ((expectedKey, expectedDistance), (actualKey, actualDistance)) =>
      assertEquals(expectedKey, actualKey)
      assertEquals(expectedDistance, actualDistance, DistanceEpsilon)
    }
  }

  private def assertOldClusterProbeSuppressesMovedPosting(tableName: String): Unit = {
    val oldNeighborhood = approximateKeys(
      tableName,
      Array(-10.0, -10.0),
      6,
      "vector.query.nprobes=1,vector.query.mode=approximate")
    assertFalse(oldNeighborhood.contains("move-me"),
      "the old cluster probe resurrected the cluster-moving record's packed posting")
  }

  private def approximateKeys(
      tableName: String,
      query: Array[Double],
      k: Int,
      runtimeOptions: String): Seq[String] = {
    val querySql = query.mkString("ARRAY(", ",", ")")
    spark.sql(
      s"""
         |SELECT _hoodie_record_key
         |FROM hudi_vector_search(
         |  '$tableName', 'embedding', $querySql, $k, 'l2', 'ivf_rabitq_mdt', '$runtimeOptions'
         |)
         |""".stripMargin)
      .collect()
      .map(_.getAs[String](HoodieRecord.RECORD_KEY_METADATA_FIELD))
      .toSeq
  }

  private def search(
      tableName: String,
      query: Array[Double],
      k: Int,
      algorithm: String,
      runtimeOptions: String): Seq[(String, Double)] = {
    val optionArg = if (runtimeOptions.isEmpty) "" else s", '$runtimeOptions'"
    val querySql = query.mkString("ARRAY(", ",", ")")
    spark.sql(
      s"""
         |SELECT id, _hudi_distance
         |FROM hudi_vector_search(
         |  '$tableName', 'embedding', $querySql, $k, 'l2', '$algorithm'$optionArg
         |)
         |""".stripMargin)
      .collect()
      .map(row => row.getAs[String]("id") -> row.getAs[Double]("_hudi_distance"))
      .sortBy { case (key, distance) => (distance, key) }
      .toSeq
  }

  private def bootstrapClusterFor(recordKey: String, tablePath: String): Int =
    withMetadataTable(tablePath) { metadataTable =>
      val clusters = (0 until NumClusters).flatMap { clusterId =>
        records(metadataTable, new VectorPostingPrefixRawKey(Generation, clusterId, null))
          .flatMap { record =>
            vectorMetadata(record) match {
              case Some(value) if isVectorMetadata(value, "HoodieVectorIndexPostingBlock") =>
                val view = new PostingBlockView(VectorIndexMdtSearchUtils.asPostingBlock(value))
                if ((0 until view.numVectors()).exists(index => view.recordKey(index) == recordKey)) {
                  Seq(clusterId)
                } else {
                  Seq.empty
                }
              case _ => Seq.empty
            }
          }
      }.toSet
      assertEquals(1, clusters.size, s"bootstrap packed posting missing for '$recordKey'")
      clusters.head
    }

  private def postingRecordsFor(
      recordKey: String,
      tablePath: String): Seq[HoodieRecord[HoodieMetadataPayload]] =
    withMetadataTable(tablePath) { metadataTable =>
      (0 until NumClusters).flatMap { clusterId =>
        records(metadataTable, new VectorPostingPrefixRawKey(Generation, clusterId, null))
      }.filter(record => recordKey == VectorIndexMetadataKey.postingRecordKey(record.getRecordKey))
    }

  private def vectorMetadata(record: HoodieRecord[HoodieMetadataPayload]): Option[AnyRef] = {
    val metadata = record.getData.getVectorIndexMetadata
    if (metadata.isPresent) Some(metadata.get.asInstanceOf[AnyRef]) else None
  }

  private def isVectorMetadata(value: AnyRef, avroName: String): Boolean = value match {
    case generic: GenericRecord => generic.getSchema.getName == avroName
    case specific => specific.getClass.getSimpleName == avroName
  }

  private def vectorStringField(value: AnyRef, field: String): String = value match {
    case marker: HoodieVectorIndexSourceInstantMarker if field == "sourceInstant" =>
      marker.getSourceInstant.toString
    case generic: GenericRecord => generic.get(field).toString
    case _ => throw new IllegalArgumentException(s"Unsupported vector metadata field '$field'")
  }

  private def records(
      metadataTable: HoodieBackedTableMetadata,
      rawKey: RawKey): Seq[HoodieRecord[HoodieMetadataPayload]] =
    metadataTable.getRecordsByKeyPrefixes(
      HoodieListData.eager(Collections.singletonList(rawKey)), IndexPartition, true)
      .collectAsList().asScala.toSeq

  private def withMetadataTable[T](tablePath: String)(f: HoodieBackedTableMetadata => T): T = {
    val metaClient = metaClientFor(tablePath)
    // Feed properties through the builder instead of calling enable(true): this guards the
    // property-swallowing regression that originally hid metadata/index settings from TVF readers.
    val properties = new java.util.Properties()
    properties.setProperty(HoodieMetadataConfig.ENABLE.key, "true")
    properties.setProperty(HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key, "true")
    val config = HoodieMetadataConfig.newBuilder().fromProperties(properties).build()
    val context = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
    val metadataTable = new HoodieBackedTableMetadata(
      context, metaClient.getStorage, config, tablePath)
    try f(metadataTable) finally metadataTable.close()
  }

  private def metaClientFor(tablePath: String): HoodieTableMetaClient =
    HoodieTestUtils.createMetaClient(storageConf, tablePath)
}
