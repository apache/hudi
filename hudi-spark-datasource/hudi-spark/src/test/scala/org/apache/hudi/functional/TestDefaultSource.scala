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

package org.apache.hudi.functional;

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.TimestampBasedAvroKeyGenerator.Config
import org.apache.hudi.testutils.HoodieClientTestBase
import org.apache.hudi.DataSourceWriteOptions
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}

/**
 * Extended tests on the spark datasource for COW table that include
 * auto inferring of partition path name when reading entire tables.
 */
class TestDefaultSource extends HoodieClientTestBase {
    var spark: SparkSession = null
    val commonOpts = Map(
        "hoodie.insert.shuffle.parallelism" -> "4",
        "hoodie.upsert.shuffle.parallelism" -> "4",
        "hoodie.bulkinsert.shuffle.parallelism" -> "2",
        "hoodie.delete.shuffle.parallelism" -> "1",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY.key -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY.key -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY.key -> "timestamp",
    HoodieWriteConfig.TABLE_NAME.key -> "hoodie_test"
        )

    val verificationCol: String = "driver"
    val updatedVerificationVal: String = "driver_update"

    @BeforeEach override def setUp() {
      initPath()
      initSparkContexts()
      spark = sqlContext.sparkSession
      initTestDataGenerator()
      initFileSystem()
    }

    @AfterEach override def tearDown() = {
      cleanupSparkContexts()
      cleanupTestDataGenerator()
      cleanupFileSystem()
    }


  @Test def testSimpleAutoInferDefaultPartition(): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    // Default partition
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val supplyBlobs = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/")
    assertEquals(inputDF.count(), supplyBlobs.count())

    // No need to specify basePath/*/*/*
    val withoutBlobs = spark.read.format("org.apache.hudi")
      .load(basePath)
    assertEquals(inputDF.count(), withoutBlobs.count())

    // Testing with trailing backslash basePath/
    val trailingSlash = spark.read.format("org.apache.hudi")
      .load(basePath + "/")
    assertEquals(inputDF.count(), trailingSlash.count())
  }

  @Test def testSimpleAutoInferCustomPartition(): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))
    // Partition with org.apache.hudi.keygen.CustomKeyGenerator
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key, "org.apache.hudi.keygen.CustomKeyGenerator")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY.key, "rider:SIMPLE,begin_lon:SIMPLE,end_lon:SIMPLE")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val supplyBlobs = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*")
    assertEquals(inputDF.count(), supplyBlobs.count())

    // No need to specify basePath/*/*/*
    val withoutBlobs = spark.read.format("org.apache.hudi")
      .load(basePath)
    assertEquals(inputDF.count(), withoutBlobs.count())

    val riderVal = inputDF.first().getAs[String]("rider")
    val beginLonVal = inputDF.first().getAs[Double]("begin_lon")
    val endLonVal = inputDF.first().getAs[Double]("end_lon")

    val partitionBlob = spark.read.format("org.apache.hudi")
      .load(basePath + "/" + riderVal + "/" + beginLonVal + "/*")
    assertEquals(inputDF.filter(col("begin_lon") === beginLonVal).count(),
      partitionBlob.count())

    val specificPartition = spark.read.format("org.apache.hudi")
      .load(basePath + "/" + riderVal + "/" + beginLonVal + "/" + endLonVal + "/")
    assertEquals(inputDF.filter(
      col("rider") === riderVal
        && col("begin_lon") === beginLonVal
        && col("end_lon") === endLonVal).count(),
      specificPartition.count())

    val wildcardBlob = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/0.4*/*")
    assertEquals(inputDF.filter(col("begin_lon").startsWith("0.4")).count(),
      wildcardBlob.count())
  }

  @Test def testSimpleAutoInferNoPartition(): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    // No partition
    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key, "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY.key, "")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val supplyBlobs = spark.read.format("org.apache.hudi")
      .load(basePath + "/*")
    assertEquals(inputDF.count(), supplyBlobs.count())

    // No need to specify basePath/*/*/*
    val withoutBlobs = spark.read.format("org.apache.hudi")
      .load(basePath)
    assertEquals(inputDF.count(), withoutBlobs.count())
  }

  @Test def testSimpleAutoInferDatePartition(): Unit = {
    val records = recordsToStrings(dataGen.generateInserts("000", 100)).toList
    val inputDF = spark.read.json(spark.sparkContext.parallelize(records, 2))

    inputDF.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY.key, "org.apache.hudi.keygen.CustomKeyGenerator")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY.key, "current_ts:TIMESTAMP")
      .option(Config.TIMESTAMP_TYPE_FIELD_PROP, "EPOCHMILLISECONDS")
      .option(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, "yyyyMMdd")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val supplyBlobs = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*")
    assertEquals(inputDF.count(), supplyBlobs.count())

    // No need to specify basePath/*/*/*
    val withoutBlobs = spark.read.format("org.apache.hudi")
      .load(basePath)
    assertEquals(inputDF.count(), withoutBlobs.count())

    // Specify basePath/yyyyMMdd/*
    val date = new DateTime().toString(DateTimeFormat.forPattern("yyyyMMdd"))
    val specificDatePartition = spark.read.format("org.apache.hudi")
      .load(basePath + s"/$date")
    assertEquals(supplyBlobs.filter(col("_hoodie_partition_path") === date).count(),
      specificDatePartition.count())
  }
}
