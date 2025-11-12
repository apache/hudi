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

import org.apache.hudi.{DataSourceWriteOptions, HoodieSparkUtils, SparkFileFormatInternalRowReaderContext}
import org.apache.hudi.SparkAdapterSupport.sparkAdapter
import org.apache.hudi.common.config.{HoodieReaderConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.common.testutils.HoodieTestTable
import org.apache.hudi.common.util
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hadoop.fs.HadoopFSUtils
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness
import org.apache.hudi.util.CloseableInternalRowIterator

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.jupiter.api.{BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertArrayEquals, assertEquals, assertFalse}

class TestSpark35RecordPositionMetadataColumn extends SparkClientFunctionalTestHarness {
  private val PARQUET_FORMAT = "parquet"
  private val SPARK_MERGER = "org.apache.hudi.DefaultSparkRecordMerger"

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
      .option(HoodieTableConfig.ORDERING_FIELDS.key, "ts")
      .option(HoodieWriteConfig.TBL_NAME.key, "user_to_country")
      .option(HoodieWriteConfig.RECORD_MERGE_IMPL_CLASSES.key, SPARK_MERGER)
      .option(HoodieWriteConfig.WRITE_RECORD_POSITIONS.key, "true")
      .option(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key, PARQUET_FORMAT)
      .option(
        DataSourceWriteOptions.TABLE_TYPE.key(),
        HoodieTableType.MERGE_ON_READ.name())
      .save(basePath)
  }

  @Test
  def testRecordPositionColumn(): Unit = {
    val _spark = spark
    // Prepare the schema
    val dataSchema = new StructType(
      Array(
        StructField("userid", IntegerType, nullable = false),
        StructField("country", StringType, nullable = false),
        StructField("ts", StringType, nullable = false)
      )
    )

    // Prepare the file and Parquet file reader.
    _spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    val hadoopConf = new Configuration(spark().sparkContext.hadoopConfiguration)
    val props = Map("spark.sql.parquet.enableVectorizedReader" -> "false")
    _spark.conf.set("spark.sql.parquet.enableVectorizedReader", "false")
    val reader = sparkAdapter.createParquetFileReader(vectorized = false, _spark.sessionState.conf, props, hadoopConf)

    val metaClient = getHoodieMetaClient(HadoopFSUtils.getStorageConfWithCopy(_spark.sparkContext.hadoopConfiguration), basePath)
    val allBaseFiles = HoodieTestTable.of(metaClient).listAllBaseFiles
    assertFalse(allBaseFiles.isEmpty)

    val requiredSchema = SparkFileFormatInternalRowReaderContext.getAppliedRequiredSchema(dataSchema,
      new SparkFileFormatInternalRowReaderContext(reader, Seq.empty, Seq.empty, storageConf(), metaClient.getTableConfig).getRecordContext.supportsParquetRowIndex)

    // Confirm if the schema is as expected.
    if (HoodieSparkUtils.gteqSpark3_5) {
      assertEquals(4, requiredSchema.fields.length)
      assertEquals(
        "StructField(_tmp_metadata_row_index,LongType,false)",
        requiredSchema.fields(3).toString)
    }

    // Make sure we can read all the positions out from base file.
    // Here we don't add filters since enabling filter push-down
    // for parquet file is tricky.
    if (HoodieSparkUtils.gteqSpark3_5) {
      val fileInfo = sparkAdapter.getSparkPartitionedFileUtils
        .createPartitionedFile(
          InternalRow.empty,
          allBaseFiles.get(0).getPath,
          0,
          allBaseFiles.get(0).getLength)
      val iterator = new CloseableInternalRowIterator(reader.read(fileInfo, requiredSchema,
        StructType(Seq.empty), util.Option.empty(), Seq.empty, new HadoopStorageConfiguration(hadoopConf)))
      var rowIndices: Set[Long] = Set()
      while (iterator.hasNext) {
        val row = iterator.next()
        rowIndices += row.getLong(3)
      }
      iterator.close()
      val expectedRowIndices: Set[Long] = Set(0L, 1L, 2L, 3L)
      assertEquals(expectedRowIndices, rowIndices)
    }
  }

  @Test
  def testUseFileGroupReaderDirectly(): Unit = {
    val _spark = spark
    import _spark.implicits._

    // Read the records out with record positions.
    val allRecords = _spark.read.format("hudi")
      .option(HoodieReaderConfig.MERGE_USE_RECORD_POSITIONS.key, "true")
      .load(basePath)

    // Ensure the number of outcomes are correct for all Spark versions
    // including Spark3.5.
    val usRecords = allRecords
      .select("userid")
      .filter("country = 'US'").map(_.getInt(0)).collect()
    assertArrayEquals(Array[Int](1, 3), usRecords)
  }
}
