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

import org.apache.hudi.{DataSourceWriteOptions, HoodieDataSourceHelpers, SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.common.config.HoodieReaderConfig
import org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.model.{FileSlice, HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.common.testutils.{HoodieTestUtils, RawTripTestPayload}
import org.apache.hudi.config.{HoodieBootstrapConfig, HoodieWriteConfig}
import org.apache.hudi.keygen.SimpleKeyGenerator
import org.apache.hudi.storage.{StorageConfiguration, StoragePath}
import org.apache.hudi.util.SparkConfigUtils

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.apache.spark.sql.{Dataset, HoodieInternalRowUtils, HoodieUnsafeUtils, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.{AfterEach, BeforeEach}
import org.junit.jupiter.api.Assertions.assertEquals

import java.util

import scala.collection.JavaConverters._

/**
 * Tests {@link HoodieFileGroupReader} with {@link SparkFileFormatInternalRowReaderContext}
 * on Spark
 */
class TestHoodieFileGroupReaderOnSpark extends TestHoodieFileGroupReaderBase[InternalRow] with SparkAdapterSupport {
  var spark: SparkSession = _

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
    new StoragePath(tempDir.toAbsolutePath.toUri.toString, "hudi_table").toString
  }

  override def getHoodieReaderContext(tablePath: String, avroSchema: Schema, storageConf: StorageConfiguration[_]): HoodieReaderContext[InternalRow] = {
    val reader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, storageConf.unwrapAs(classOf[Configuration]))
    new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty)
  }

  override def bootstrapTable(recordList: util.List[HoodieRecord[_]],
                              options: util.Map[String, String]): Unit = {
    val recs = RawTripTestPayload.recordsToStrings(recordList)
    val bootstrapDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recs.asScala.toList, 4))
    val partitionPathField = SparkConfigUtils.getStringWithAltKeys(
      options.asScala.toMap, DataSourceWriteOptions.PARTITIONPATH_FIELD)
    val bootstrapSourcePath = SparkConfigUtils.getStringWithAltKeys(
      options.asScala.toMap, HoodieBootstrapConfig.BASE_PATH)

    // Write parquet table
    bootstrapDF.write.format("parquet")
      .partitionBy(partitionPathField)
      .mode(SaveMode.Overwrite)
      .save(bootstrapSourcePath)

    val basePath = getBasePath
    // Write Hudi table with bootstrap operation
    spark.emptyDataFrame.write
      .format("hudi")
      .options(options.asScala)
      .option("hoodie.datasource.write.operation", DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option("hoodie.datasource.write.table.type", "MERGE_ON_READ")
      .option(HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key, classOf[SimpleKeyGenerator].getName)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val bootstrapInstantTime: String =
      HoodieDataSourceHelpers.latestCommit(HoodieTestUtils.getStorage(basePath), basePath)
    assertEquals(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, bootstrapInstantTime)
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
                                          isSkipMerge: Boolean,
                                          partitionColumns: util.List[String]): Unit = {
    //TODO [HUDI-8207] get rid of this if block, and revert the argument change from (fileGroupId: String -> fileSlice: FileSlice)
    // For a file slice with bootstrap data and skeleton files, the skip merging without the
    // file group reader enabled does not take effect, so we skip the validation in this case.
    if (!isSkipMerge || (fileSlice.getLogFiles.count() < 2 && !fileSlice.hasBootstrapBase)) {
      // TODO(HUDI-8896): support reading partition columns for bootstrapped table
      //  in the file group reader directly, instead of engine-specific handling
      val excludedColumns = if (fileSlice.hasBootstrapBase) partitionColumns.asScala.toSeq else Seq[String]()
      val expectedDf = spark.read.format("hudi")
        .option(FILE_GROUP_READER_ENABLED.key(), "false")
        .option(HoodieReaderConfig.MERGE_TYPE.key, if (isSkipMerge) HoodieReaderConfig.REALTIME_SKIP_MERGE else HoodieReaderConfig.REALTIME_PAYLOAD_COMBINE)
        .load(basePath)
        .where(col(HoodieRecord.FILENAME_METADATA_FIELD).contains(fileSlice.getFileId))
        .drop(excludedColumns: _*)
      assertEquals(expectedDf.count, actualRecordList.size)
      val actualDf = HoodieUnsafeUtils.createDataFrameFromInternalRows(
        spark, actualRecordList.asScala.toSeq, HoodieInternalRowUtils.getCachedSchema(schema))
        .drop(excludedColumns: _*)
      assertEquals(0, expectedDf.except(actualDf).count())
      assertEquals(0, actualDf.except(expectedDf).count())
    }
  }

  override def getCustomPayload: String = classOf[CustomPayloadForTesting].getName

  override def assertRecordsEqual(schema: Schema, expected: InternalRow, actual: InternalRow): Unit = {
    assertEquals(expected.numFields, actual.numFields)
    val expectedStruct = sparkAdapter.getAvroSchemaConverters.toSqlType(schema)._1.asInstanceOf[StructType]
    expected.toSeq(expectedStruct).zip(actual.toSeq(expectedStruct)).foreach( converted => {
      assertEquals(converted._1, converted._2)
    })
  }
}
