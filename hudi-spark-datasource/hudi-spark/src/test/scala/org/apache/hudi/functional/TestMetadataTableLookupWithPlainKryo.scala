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

import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieFileIndex}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{EmptyHoodieRecordPayload, HoodieAvroRecord, HoodieKey, HoodieRecord}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.index.HoodieIndex.IndexType
import org.apache.hudi.index.SparkHoodieIndexFactory
import org.apache.hudi.table.HoodieSparkTable
import org.apache.hudi.testutils.HoodieClientTestUtils

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal}
import org.apache.spark.sql.types.StringType
import org.junit.jupiter.api.{Tag, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.io.TempDir

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._

/**
 * Runs the record level index lookups whose metadata table readers are broadcast (write-path tagging and
 * partitioned record index pruning) in a session that uses Kryo without the Hudi Kryo registrator, as plain
 * Spark SQL readers are often configured. Storage memory is too small to keep broadcast values, so tasks read
 * them back through the serializer, as executors of a cluster do.
 */
@Tag("functional")
class TestMetadataTableLookupWithPlainKryo {

  @TempDir
  var tempDir: Path = _

  private val recordsPerBatch = 40

  @Test
  def testRecordIndexLookupsWithKryoWithoutHudiRegistrator(): Unit = {
    val globalPath = tempDir.resolve("global").toUri.toString
    val partitionedPath = tempDir.resolve("partitioned").toUri.toString
    withSession(HoodieClientTestUtils.getSparkConfForTest(getClass.getName)) { spark =>
      writeTable(spark, globalPath, globalRecordIndexOpts)
      writeTable(spark, partitionedPath, partitionedRecordIndexOpts)
    }

    withSession(plainKryoConf) { spark =>
      assertFalse(spark.sparkContext.getConf.contains("spark.kryo.registrator"))
      assertBroadcastValuesAreDeserializedInTasks(spark)

      val keys = (0 until 2 * recordsPerBatch by 7).map(i => f"key$i%04d") ++ Seq("missing0", "missing1")
      val tagged = tagLocation(spark, globalPath, keys)
      keys.foreach(key => assertEquals(!key.startsWith("missing"), tagged(key), s"unexpected tagging for $key"))

      val readOpts = partitionedRecordIndexOpts ++ Map(
        "path" -> partitionedPath, DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "true")
      val filter = EqualTo(AttributeReference("id", StringType, nullable = true)(), Literal("key0007"))
      spark.sessionState.conf.setConfString("hoodie.fileIndex.dataSkippingFailureMode", "strict")
      def listedFiles(opts: Map[String, String]): Int = {
        val fileIndex = HoodieFileIndex(spark, newMetaClient(spark, partitionedPath), None, opts, includeLogFiles = true)
        try fileIndex.listFiles(Seq.empty, Seq(filter)).flatMap(_.files).size finally fileIndex.close()
      }
      val prunedFiles = listedFiles(readOpts)
      val allFiles = listedFiles(readOpts + (DataSourceReadOptions.ENABLE_DATA_SKIPPING.key -> "false"))
      assertTrue(prunedFiles > 0 && prunedFiles < allFiles, s"record index should prune files: $prunedFiles of $allFiles")
    }
  }

  /** Guards against a vacuous pass: tasks must see a deserialized copy of a broadcast value, not the driver's. */
  private def assertBroadcastValuesAreDeserializedInTasks(spark: SparkSession): Unit = {
    val probe = new BroadcastProbe
    BroadcastProbe.driverInstance.set(probe)
    val broadcast = spark.sparkContext.broadcast(probe)
    val sameInstance = spark.sparkContext.parallelize(Seq(1), 1)
      .map(_ => broadcast.value eq BroadcastProbe.driverInstance.get())
      .collect().head
    assertFalse(sameInstance, "tasks read the driver's broadcast value without deserializing it")
  }

  /** Returns, per record key, whether the global record level index found an existing location. */
  private def tagLocation(spark: SparkSession, tablePath: String, keys: Seq[String]): Map[String, Boolean] = {
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(tablePath)
      .withProps(toProps(globalRecordIndexOpts))
      .withSchema(new TableSchemaResolver(newMetaClient(spark, tablePath)).getTableSchema(false).toString)
      .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(IndexType.GLOBAL_RECORD_LEVEL_INDEX).build())
      .build()
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)
    val context = new HoodieSparkEngineContext(jsc)
    val records = jsc.parallelize(keys.map { key =>
      new HoodieAvroRecord(new HoodieKey(key, "p0"), new EmptyHoodieRecordPayload()).asInstanceOf[HoodieRecord[EmptyHoodieRecordPayload]]
    }.asJava, 2)
    SparkHoodieIndexFactory.createIndex(writeConfig)
      .tagLocation(HoodieJavaRDD.of(records), context, HoodieSparkTable.create(writeConfig, context))
      .collectAsList().asScala
      .map(r => r.getRecordKey -> r.isCurrentLocationKnown).toMap
  }

  private def writeTable(spark: SparkSession, tablePath: String, opts: Map[String, String]): Unit = {
    (0 until 2).foreach { batch =>
      val rows = (batch * recordsPerBatch until (batch + 1) * recordsPerBatch).map { i =>
        (f"key$i%04d", f"name$i%04d", i.toDouble, s"p${i % 4}", batch.toLong)
      }
      spark.createDataFrame(rows).toDF("id", "name", "price", "part", "ts").write.format("hudi")
        .options(opts)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(if (batch == 0) SaveMode.Overwrite else SaveMode.Append)
        .save(tablePath)
    }
  }

  private def withSession[T](conf: SparkConf)(f: SparkSession => T): T = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    val spark = SparkSession.builder().config(conf).getOrCreate()
    try f(spark) finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  // Kryo without the Hudi registrator, and about 1 KB of storage memory so no broadcast value stays deserialized.
  private def plainKryoConf: SparkConf = HoodieClientTestUtils.getSparkConfForTest(getClass.getName)
    .remove("spark.kryo.registrator")
    .set("spark.testing.memory", (1024 * 1024).toString)
    .set("spark.testing.reservedMemory", "0")
    .set("spark.memory.fraction", "0.001")

  private def newMetaClient(spark: SparkSession, tablePath: String): HoodieTableMetaClient = HoodieTableMetaClient.builder()
    .setBasePath(tablePath)
    .setConf(HadoopFSUtils.getStorageConfWithCopy(spark.sparkContext.hadoopConfiguration))
    .build()

  private def toProps(opts: Map[String, String]): java.util.Properties = {
    val props = new java.util.Properties()
    opts.foreach { case (k, v) => props.setProperty(k, v) }
    props
  }

  private val commonOpts: Map[String, String] = Map(
    HoodieWriteConfig.TBL_NAME.key -> "lookup_plain_kryo",
    DataSourceWriteOptions.RECORDKEY_FIELD.key -> "id",
    DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> "part",
    HoodieTableConfig.ORDERING_FIELDS.key -> "ts",
    "hoodie.insert.shuffle.parallelism" -> "2",
    "hoodie.upsert.shuffle.parallelism" -> "2",
    "hoodie.parquet.small.file.limit" -> "0",
    HoodieMetadataConfig.ENABLE.key -> "true")

  private val globalRecordIndexOpts: Map[String, String] = commonOpts ++ Map(
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "2",
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "2")

  private val partitionedRecordIndexOpts: Map[String, String] = commonOpts ++ Map(
    HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "false",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MIN_FILE_GROUP_COUNT_PROP.key -> "2",
    HoodieMetadataConfig.RECORD_LEVEL_INDEX_MAX_FILE_GROUP_COUNT_PROP.key -> "2")
}

/** A broadcast value large enough never to fit in the test's storage memory. */
class BroadcastProbe extends Serializable {
  val payload: Array[Byte] = new Array[Byte](64 * 1024)
}

object BroadcastProbe {
  val driverInstance = new AtomicReference[BroadcastProbe]()
}
