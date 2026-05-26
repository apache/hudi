/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.apache.hudi.common.schema.{HoodieSchema, HoodieSchemaType}
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil}

import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._
import org.junit.jupiter.api.{AfterEach, Assertions, BeforeEach, Test}
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable

import java.nio.ByteBuffer
import java.util.UUID

/**
 * Integration test: writes a Hudi table with a VECTOR-annotated column to GCS and reads it back.
 *
 * Requires:
 *   - `HOODIE_GCS_VECTOR_IT_BASE` — e.g. `gs://your-bucket/path/prefix` (no trailing slash required)
 *   - Application Default Credentials or `GOOGLE_APPLICATION_CREDENTIALS` for the bucket
 *
 * Run (example):
 * {{{
 *   export HOODIE_GCS_VECTOR_IT_BASE=gs://my-bucket/hudi-it
 *   mvn -pl hudi-spark-datasource/hudi-spark -am test -Dtest=ITVectorIndexGcs -DfailIfNoTests=false
 * }}}
 */
@EnabledIfEnvironmentVariable(named = "HOODIE_GCS_VECTOR_IT_BASE", matches = "gs://.+")
class ITVectorIndexGcs {

  private var spark: SparkSession = _

  @BeforeEach def setup(): Unit = {
    spark = SparkSession.builder()
      .appName("ITVectorIndexGcs")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
      .getOrCreate()
  }

  @AfterEach def tearDown(): Unit = {
    if (spark != null) {
      spark.stop()
      spark = null
    }
  }

  @Test
  def testVectorColumnRoundTripOnGcs(): Unit = {
    val base = sys.env("HOODIE_GCS_VECTOR_IT_BASE").replaceAll("/+$", "")
    val tablePath = s"$base/hudi_vector_it_${UUID.randomUUID()}"
    val dim = 8
    val metadata = new MetadataBuilder()
      .putString(HoodieSchema.TYPE_METADATA_FIELD, s"VECTOR($dim)")
      .build()
    val schema = StructType(Seq(
      StructField("id", StringType, nullable = false),
      StructField("embedding", ArrayType(FloatType, containsNull = false), nullable = false, metadata),
      StructField("label", StringType, nullable = true)
    ))
    val rnd = new scala.util.Random(7)
    val rows = (0 until 5).map { i =>
      Row(s"k$i", Array.fill(dim)(rnd.nextFloat()).toSeq, s"l$i")
    }
    val df = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)
    df.write.format("hudi")
      .option(RECORDKEY_FIELD.key, "id")
      .option(PRECOMBINE_FIELD.key, "id")
      .option(TABLE_NAME.key, "it_vector_gcs")
      .option(TABLE_TYPE.key, "COPY_ON_WRITE")
      .mode(SaveMode.Overwrite)
      .save(tablePath)

    val readDf = spark.read.format("hudi").load(tablePath)
    Assertions.assertEquals(5L, readDf.count())
    val emb = readDf.schema("embedding")
    Assertions.assertTrue(emb.dataType.isInstanceOf[ArrayType])
    val md = emb.metadata
    Assertions.assertTrue(md.contains(HoodieSchema.TYPE_METADATA_FIELD))
    val parsed = HoodieSchema.parseTypeDescriptor(md.getString(HoodieSchema.TYPE_METADATA_FIELD))
    Assertions.assertEquals(HoodieSchemaType.VECTOR, parsed.getType)

    // Vector-index MDT payload factories (metadata table write path is separate; this checks Avro wiring)
    val part = HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX + "it"
    val rec = HoodieMetadataPayload.createVectorIndexAssignmentRecord("rk1", 3, part)
    Assertions.assertTrue(rec.getData.getVectorIndexMetadata.isPresent)
    Assertions.assertEquals(3, rec.getData.getVectorIndexMetadata.get().getClusterId)
    val buf = ByteBuffer.allocate(dim * java.lang.Float.BYTES)
    var j = 0
    while (j < dim) {
      buf.putFloat(j.toFloat)
      j += 1
    }
    buf.flip()
    val cen = HoodieMetadataPayload.createVectorIndexCentroidsRecord(buf, part)
    Assertions.assertTrue(cen.getData.getVectorIndexMetadata.isPresent)
    Assertions.assertNotNull(cen.getData.getVectorIndexMetadata.get().getCentroidBytes)

    val manifest = HoodieMetadataPayload.createVectorIndexManifestRecord(
      "gen-1", "IVF_RABITQ", 1, 42L, false, System.currentTimeMillis(), part)
    Assertions.assertEquals(
      HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_MANIFEST,
      manifest.getData.getVectorIndexMetadata.get().getEntryType)
    Assertions.assertEquals("gen-1", manifest.getData.getVectorIndexMetadata.get().getGenerationId)

    val clusterManifest = HoodieMetadataPayload.createVectorIndexClusterManifestRecord(
      "7", 3, 4, java.util.Arrays.asList("file-1", "file-2"), 100L, System.currentTimeMillis(), part)
    Assertions.assertEquals(
      HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER,
      clusterManifest.getData.getVectorIndexMetadata.get().getEntryType)
    Assertions.assertEquals(4, clusterManifest.getData.getVectorIndexMetadata.get().getShardCount)
    Assertions.assertEquals(Seq("file-1", "file-2"), clusterManifest.getData.getVectorIndexMetadata.get().getFileGroupIds)

    val posting = HoodieMetadataPayload.createVectorIndexPostingRecord(
      "7", "rk1", 3, 2, "file-1", "default", Array[Byte](1, 2), 1.5f, System.currentTimeMillis(), part)
    Assertions.assertEquals(
      HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_POSTING,
      posting.getData.getVectorIndexMetadata.get().getEntryType)
    Assertions.assertEquals("gen-1", posting.getData.getVectorIndexMetadata.get().getGenerationId)
    Assertions.assertEquals(2, posting.getData.getVectorIndexMetadata.get().getShardId)
    Assertions.assertNotNull(posting.getData.getVectorIndexMetadata.get().getBinaryCode)
    Assertions.assertEquals(1.5f, posting.getData.getVectorIndexMetadata.get().getScalar)
    Assertions.assertTrue(
      HoodieTableMetadataUtil.isVectorIndexPostingKey(posting.getRecordKey))
    Assertions.assertTrue(posting.getRecordKey.startsWith("P|"))
  }
}
