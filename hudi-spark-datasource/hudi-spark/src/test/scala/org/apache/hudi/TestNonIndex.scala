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

package org.apache.hudi

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.config.{HoodieIndexConfig, HoodieWriteConfig}
import org.apache.hudi.index.HoodieIndex
import org.apache.hudi.keygen.EmptyKeyGenerator
import org.apache.hudi.keygen.constant.KeyGeneratorOptions
import org.apache.hudi.testutils.{DataSourceTestUtils, HoodieClientTestBase}
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.junit.jupiter.api.Test

import scala.collection.JavaConversions._
import scala.collection.JavaConverters

class TestNonIndex extends HoodieClientTestBase {
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key() -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD.key() -> "timestamp",
    HoodieIndexConfig.INDEX_TYPE.key() -> HoodieIndex.IndexType.NON_INDEX.name(),
    HoodieWriteConfig.KEYGENERATOR_CLASS_NAME.key() -> classOf[EmptyKeyGenerator].getName
  )

  @Test
  def testNonIndexMORInsert(): Unit = {
    val spark = sqlContext.sparkSession

    val records1 = recordsToStrings(dataGen.generateInserts("001", 100)).toList
    // first insert, parquet files
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val records2 = recordsToStrings(dataGen.generateInserts("002", 100)).toList
    // second insert, log files
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE.key(), DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Append)
      .save(basePath)

    val fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
    assert(fs.globStatus(new Path(basePath, "201*/*/*/.*.log*")).length > 0)
    assert(fs.globStatus(new Path(basePath, "201*/*/*/*.parquet")).length > 0)

    // check data
    val result = spark.read.format("org.apache.hudi").load(basePath + "/*/*/*/*")
      .selectExpr(inputDF2.schema.map(_.name): _*)
    val inputAll = inputDF1.unionAll(inputDF2)
    assert(result.except(inputAll).count() == 0)
    assert(inputAll.except(result).count == 0)
  }

  @Test
  def testBulkInsertDatasetWithOutIndex(): Unit = {
    val spark = sqlContext.sparkSession
    val schema = DataSourceTestUtils.getStructTypeExampleSchema

    // create a new table
    val fooTableModifier = commonOpts
      .updated(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL)
      .updated(DataSourceWriteOptions.ENABLE_ROW_WRITER.key, "true")
      .updated(HoodieIndexConfig.INDEX_TYPE.key(), HoodieIndex.IndexType.NON_INDEX.toString)
      .updated(HoodieWriteConfig.AVRO_SCHEMA_STRING.key(), schema.toString)
      .updated(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key(), "4")
      .updated("path", basePath)
    val fooTableParams = HoodieWriterUtils.parametersWithWriteDefaults(fooTableModifier)

    val structType = AvroConversionUtils.convertAvroSchemaToStructType(schema)
    for (_ <- 0 to 0) {
      // generate the inserts
      val records = DataSourceTestUtils.generateRandomRows(200)
      val recordsSeq = JavaConverters.asScalaIteratorConverter(records.iterator).asScala.toSeq
      val df = spark.createDataFrame(spark.sparkContext.parallelize(recordsSeq), structType)
      // write to Hudi
      HoodieSparkSqlWriter.write(sqlContext, SaveMode.Append, fooTableParams ++ Seq(), df)

      // Fetch records from entire dataset
      val actualDf = sqlContext.read.format("org.apache.hudi").load(basePath + "/*/*/*")

      assert(actualDf.where("_hoodie_record_key = '_hoodie_empty_record_key_'").count() == 200)
    }
  }
}
