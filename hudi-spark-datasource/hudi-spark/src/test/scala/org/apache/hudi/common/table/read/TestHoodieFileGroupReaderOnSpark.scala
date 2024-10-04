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

package org.apache.hudi.common.table.read

import org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED
import org.apache.hudi.common.config.{HoodieReaderConfig, RecordMergeMode}
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, FileSlice, HoodieRecord, OverwriteWithLatestAvroPayload, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.testutils.{HoodieTestUtils, RawTripTestPayload}
import org.apache.hudi.storage.StorageConfiguration
import org.apache.hudi.{DefaultSparkRecordMerger, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, HoodieInternalRowUtils, HoodieUnsafeUtils, Row, SaveMode, SparkSession}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.util

import scala.collection.JavaConverters._

/**
 * Tests {@link HoodieFileGroupReader} with {@link SparkFileFormatInternalRowReaderContext}
 * on Spark
 */
class TestHoodieFileGroupReaderOnSpark extends TestHoodieFileGroupReaderBase[InternalRow] with SparkAdapterSupport {
  var spark: SparkSession = _

  var customPayloadName: String = classOf[CustomPayloadForTesting].getName

  @BeforeEach
  def setup() {
    val sparkConf = new SparkConf
    sparkConf.set("spark.app.name", getClass.getName)
    sparkConf.set("spark.master", "local[8]")
    sparkConf.set("spark.default.parallelism", "4")
    sparkConf.set("spark.sql.shuffle.partitions", "4")
    sparkConf.set("spark.driver.maxResultSize", "2g")
    sparkConf.set("spark.hadoop.mapred.output.compress", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "true")
    sparkConf.set("spark.hadoop.mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    sparkConf.set("spark.hadoop.mapred.output.compression.type", "BLOCK")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar")
    sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    sparkConf.set("spark.sql.parquet.enableVectorizedReader", "false")
    HoodieSparkKryoRegistrar.register(sparkConf)
    spark = SparkSession.builder.config(sparkConf).getOrCreate
  }

  @AfterEach
  def teardown() {
    if (spark != null) {
      spark.stop()
    }
  }

  override def getStorageConf: StorageConfiguration[_] = {
    HoodieTestUtils.getDefaultStorageConf.getInline
  }

  override def getBasePath: String = {
    tempDir.toAbsolutePath.toUri.toString
  }

  override def getHoodieReaderContext(tablePath: String, avroSchema: Schema, storageConf: StorageConfiguration[_]): HoodieReaderContext[InternalRow] = {
    val reader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]))
    val metaClient = HoodieTableMetaClient.builder().setConf(storageConf).setBasePath(tablePath).build
    val recordKeyField = new DefaultSparkRecordMerger().getMandatoryFieldsForMerging(metaClient.getTableConfig)(0)
    new SparkFileFormatInternalRowReaderContext(reader, recordKeyField, Seq.empty, Seq.empty)
  }

  override def commitToTable(recordList: util.List[HoodieRecord[_]], operation: String, options: util.Map[String, String]): Unit = {
    val recs = RawTripTestPayload.recordsToStrings(recordList)
    val inputDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recs.asScala.toList, 2))

    inputDF.write.format("hudi")
      .options(options)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option("hoodie.datasource.write.operation", operation)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .mode(if (operation.equalsIgnoreCase(WriteOperationType.INSERT.value())) SaveMode.Overwrite
      else SaveMode.Append)
      .save(getBasePath)
  }

  override def validateRecordsInFileGroup(basePath: String,
                                          actualRecordList: util.List[InternalRow],
                                          schema: Schema,
                                          fileSlice: FileSlice,
                                          isSkipMerge: Boolean): Unit = {
    //TODO [HUDI-8207] get rid of this if block, and revert the argument change from (fileGroupId: String -> fileSlice: FileSlice)
    if (!isSkipMerge || fileSlice.getLogFiles.count() < 2) {
      val expectedDf = spark.read.format("hudi")
        .option(FILE_GROUP_READER_ENABLED.key(), "false")
        .option(HoodieReaderConfig.MERGE_TYPE.key, if (isSkipMerge) HoodieReaderConfig.REALTIME_SKIP_MERGE else HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE)
        .load(basePath)
        .where(col(HoodieRecord.FILENAME_METADATA_FIELD).contains(fileSlice.getFileId))
      assertEquals(expectedDf.count, actualRecordList.size)
      val actualDf = HoodieUnsafeUtils.createDataFrameFromInternalRows(
        spark, actualRecordList.asScala.toSeq, HoodieInternalRowUtils.getCachedSchema(schema))
      assertEquals(0, expectedDf.except(actualDf).count())
      assertEquals(0, actualDf.except(expectedDf).count())
    }
  }

  override def getRecordPayloadForMergeMode(mergeMode: RecordMergeMode): String = {
    mergeMode match {
      case RecordMergeMode.EVENT_TIME_ORDERING => classOf[DefaultHoodieRecordPayload].getName
      case RecordMergeMode.OVERWRITE_WITH_LATEST => classOf[OverwriteWithLatestAvroPayload].getName
      case RecordMergeMode.CUSTOM => customPayloadName
    }
  }
}
