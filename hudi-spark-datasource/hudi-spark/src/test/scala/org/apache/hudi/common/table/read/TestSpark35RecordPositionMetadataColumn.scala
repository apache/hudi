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

import org.apache.hadoop.conf.Configuration
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.config.HoodieStorageConfig
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.testutils.HoodieTestTable
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils, SparkFileFormatInternalRowReaderContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.{BeforeEach, Test}

class TestSpark35RecordPositionMetadataColumn extends SparkClientFunctionalTestHarness {
  private val PARQUET_FORMAT = "parquet"
  private val SPARK_MERGER = "org.apache.hudi.HoodieSparkRecordMerger"

  @BeforeEach
  def setUp(): Unit = {
    val _spark = spark
    import _spark.implicits._

    val userToCountryDF = Seq(
      (1, "US", "1001"),
      (2, "China", "1003"),
      (3, "US", "1002"),
      (4, "Singapore", "1004"))
      .toDF("userid", "country", "ts")

    // Create the file with record positions.
    userToCountryDF.write.format("hudi")
      .option(DataSourceWriteOptions.RECORDKEY_FIELD.key, "userid")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
      .option(HoodieWriteConfig.RECORD_MERGER_IMPLS.key, SPARK_MERGER)
      .option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key, "true")
      .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key, PARQUET_FORMAT)
      .option(
        DataSourceWriteOptions.TABLE_TYPE.key(),
        HoodieTableType.MERGE_ON_READ.name())
      .save(basePath)
  }

  @Test
  def testRecordPositionColumn(): Unit = {
    if (HoodieSparkUtils.gteqSpark3_5) {
      val _spark = spark
      // Prepare the schema
      val dataSchema = new StructType(
        Array(
          StructField("userid", IntegerType, nullable = false),
          StructField("country", StringType, nullable = false),
          StructField("ts", StringType, nullable = false)
        )
      )
      val hadoopConf = new Configuration(spark().sparkContext.hadoopConfiguration)
      val props = Map("spark.sql.parquet.enableVectorizedReader" -> "false")
      _spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
      val reader = sparkAdapter.createParquetFileReader(vectorized = false, _spark.sessionState.conf, props, hadoopConf)
      val requiredSchema = SparkFileFormatInternalRowReaderContext.getAppliedRequiredSchema(dataSchema)

      // Confirm if the schema is as expected.
      assertEquals(4, requiredSchema.fields.length)
      assertEquals(
        "StructField(_tmp_metadata_row_index,LongType,false)",
        requiredSchema.fields(3).toString)

      // Prepare the file and Parquet file reader.
      val metaClient = getHoodieMetaClient(
        _spark.sparkContext.hadoopConfiguration, basePath)
      val allBaseFiles = HoodieTestTable.of(metaClient).listAllBaseFiles
      assertTrue(allBaseFiles.nonEmpty)
      val readerContext = new SparkFileFormatInternalRowReaderContext(reader, HoodieRecord.RECORD_KEY_METADATA_FIELD, Seq.empty, true)
      val readerState = readerContext.getReaderState
      readerState.hasLogFiles = true
      readerState.needsBootstrapMerge = false
      readerState.hasBootstrapBaseFile = false
      //dataschema param is set to null because it is not used
      val fileRecordIterator = readerContext.getFileRecordIterator(allBaseFiles.head.getPath, 0, allBaseFiles.head.getLen, null,
        sparkAdapter.getAvroSchemaConverters.toAvroType(dataSchema, nullable = true, "record"), hadoopConf)

      // Make sure we can read all the positions out from base file.
      // Here we don't add filters since enabling filter push-down
      // for parquet file is tricky.
      var rowIndices: Set[Long] = Set()
      while (fileRecordIterator.hasNext) {
        val row = fileRecordIterator.next()
        rowIndices += row.getLong(3)
      }
      fileRecordIterator.close()
      val expectedRowIndices: Set[Long] = Set(0L, 1L, 2L, 3L)
      assertEquals(expectedRowIndices, rowIndices)
    }
  }
}
