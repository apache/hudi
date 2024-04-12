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

import org.apache.avro.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED
import org.apache.hudi.common.engine.HoodieReaderContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieRecord, WriteOperationType}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.{SparkAdapterSupport, SparkFileFormatInternalRowReaderContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, HoodieInternalRowUtils, HoodieUnsafeUtils, Row, SaveMode, SparkSession}
import org.apache.spark.{HoodieSparkKryoRegistrar, SparkConf}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach}

import java.util
import scala.collection.JavaConversions._

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

  override def getHadoopConf: Configuration = {
    FSUtils.buildInlineConf(new Configuration)
  }

  override def getBasePath: String = {
    tempDir.toAbsolutePath.toUri.toString
  }

  override def getHoodieReaderContext(tablePath: String, avroSchema: Schema, hadoopConf: Configuration): HoodieReaderContext[InternalRow] = {
    val reader = sparkAdapter.createParquetFileReader(vectorized = false, spark.sessionState.conf, Map.empty, hadoopConf)
    val metaClient = HoodieTableMetaClient.builder().setConf(getHadoopConf).setBasePath(tablePath).build
    val recordKeyField = if (metaClient.getTableConfig.populateMetaFields()) {
      HoodieRecord.RECORD_KEY_METADATA_FIELD
    } else {
      val keyFields = metaClient.getTableConfig.getRecordKeyFields.get()
      checkState(keyFields.length == 1)
      keyFields.head
    }
    new SparkFileFormatInternalRowReaderContext(reader, recordKeyField, Seq.empty, false)
  }

  override def commitToTable(recordList: util.List[String], operation: String, options: util.Map[String, String]): Unit = {
    val inputDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(recordList.toList, 2))

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
                                          fileGroupId: String): Unit = {
    val expectedDf = spark.read.format("hudi")
      .option(FILE_GROUP_READER_ENABLED.key(), "false")
      .load(basePath)
      .where(col(HoodieRecord.FILENAME_METADATA_FIELD).contains(fileGroupId))
    assertEquals(expectedDf.count, actualRecordList.size)
    val actualDf = HoodieUnsafeUtils.createDataFrameFromInternalRows(
      spark, actualRecordList, HoodieInternalRowUtils.getCachedSchema(schema))
    assertEquals(0, expectedDf.except(actualDf).count())
    assertEquals(0, actualDf.except(expectedDf).count())
  }
}
